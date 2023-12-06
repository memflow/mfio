use crate::std_prelude::*;

use cglue::result::IntError;
use cglue::task::CWaker;
use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::num::NonZeroI32;
use core::pin::Pin;
use core::ptr::NonNull;
use core::sync::atomic::*;
use core::task::{Context, Poll};
use futures::Stream;
use tarc::BaseArc;

use super::{OpaqueStore, PacketPerms, PacketView};
use crate::error::Error;

/// Typical output of packets.
///
/// Whenever a backend returns a packet view to the user, it will have both the view, and optional
/// error case attached. If `Option<Error>` is not `None` then the packet should be considered
/// failed. However, note that the given packet view may have been accessed. If it was accessed,
/// the user should still assume the operation has failed.
///
/// Meanwhile, if the error is `None`, then the packet will have been accessed. If it hasn't, then
/// it's a logic bug of the backend.
///
/// Access considers allocation (+ transfer of data), or invokation of transfer function.
pub type Output<'a, Perms> = (PacketView<'a, Perms>, Option<Error>);

/// Represents a reference where final packet views are collected.
#[repr(C)]
pub struct OutputRef<'a, Perms: PacketPerms> {
    pub(crate) out: NonNull<PacketOutput<Perms>>,
    pub(crate) arc: bool,
    phantom: PhantomData<&'a PacketOutput<Perms>>,
}

unsafe impl<Perms: PacketPerms> Send for OutputRef<'_, Perms> {}
unsafe impl<Perms: PacketPerms> Sync for OutputRef<'_, Perms> {}

impl<Perms: PacketPerms> core::ops::Deref for OutputRef<'_, Perms> {
    type Target = PacketOutput<Perms>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.out.as_ptr() }
    }
}

impl<Perms: PacketPerms> Clone for OutputRef<'_, Perms> {
    fn clone(&self) -> Self {
        if self.arc {
            unsafe {
                BaseArc::increment_strong_count(self.out.as_ptr());
            }
        }

        Self {
            out: self.out,
            arc: self.arc,
            phantom: PhantomData,
        }
    }
}

impl<Perms: PacketPerms> Drop for OutputRef<'_, Perms> {
    fn drop(&mut self) {
        if self.arc {
            unsafe {
                BaseArc::decrement_strong_count(self.out.as_ptr());
            }
        }
    }
}

impl<Perms: PacketPerms> OutputRef<'_, Perms> {
    /// Create `OutputRef` from `PacketOutput` arc.
    pub fn from_arc(out: BaseArc<PacketOutput<Perms>>) -> Self {
        Self::from_arc_ref(&out)
    }

    /// Create `PacketView` from packet arc.
    pub fn from_arc_ref(out: &BaseArc<PacketOutput<Perms>>) -> Self {
        let out = out.as_ptr();

        let out = NonNull::new(out.cast_mut()).unwrap();

        // SAFETY: we are using a pointer source from an arc, and we are correctly decrementing the
        // strong reference count upon drop.
        unsafe { BaseArc::increment_strong_count(out.as_ptr()) };
        Self {
            out,
            arc: true,
            phantom: PhantomData,
        }
    }
}

impl<Perms: PacketPerms> From<BaseArc<PacketOutput<Perms>>> for OutputRef<'static, Perms> {
    fn from(out: BaseArc<PacketOutput<Perms>>) -> Self {
        // TODO: deal with tags
        Self::from_arc(out)
    }
}

impl<T: AsRef<PacketOutput<Perms>>, Perms: PacketPerms> From<BaseArc<T>>
    for OutputRef<'static, Perms>
{
    fn from(out: BaseArc<T>) -> Self {
        // TODO: deal with tags
        Self::from_arc(out.transpose().into_base().unwrap())
    }
}

pub trait OutputStore<'a, Perms: PacketPerms>:
    'a + OpaqueStore<Opaque<'a> = OutputRef<'a, Perms>, ConstHdr = PacketOutput<Perms>>
{
}
impl<'a, Perms: PacketPerms, T> OutputStore<'a, Perms> for T where
    T: 'a + OpaqueStore<Opaque<'a> = OutputRef<'a, Perms>, ConstHdr = PacketOutput<Perms>>
{
}

/// Describes an object where packet output is collected.
///
/// Note that this is an opaque object with data referenced beyond the size of this struct.
#[repr(C)]
#[derive(Debug)]
pub struct PacketOutput<Perms: PacketPerms> {
    pub(crate) vtbl: &'static PacketOutputVtbl<Perms>, /* output, while drop is called by the object owner. */
    pub(crate) bound_views: AtomicUsize,
    // data afterwards
}

/// Describes operations that can be performed on a packet output object.
#[repr(C)]
#[derive(Debug)]
pub struct PacketOutputVtbl<Perms: PacketPerms> {
    /// Output handler.
    ///
    /// This function will be invoked whenever a packet view is returned to the final destination.
    /// `Packet::on_output` will be called before this function is invoked, but before the
    /// potential waker (of the last packet view standing) gets triggered.
    ///
    /// TODO: make this a polled async function.
    pub output: unsafe extern "C" fn(
        OutputRef<'static, Perms>,
        PacketView<'static, Perms>,
        Option<NonZeroI32>,
    ),
}

/// Invokes a closure on each packet segment.
///
/// The closure must be const, and reentrable, i.e. `Fn`.
///
/// # Examples
///
/// ```rust
/// # mod sample {
/// #     include!("../../sample.rs");
/// # }
/// # use sample::SampleIo;
/// # fn work() -> mfio::error::Result<()> {
/// use mfio::backend::*;
/// use mfio::io::*;
///
/// let mut mem = vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144];
/// let handle = SampleIo::new(mem.clone());
///
/// handle.block_on(async {
///     let (packet, _) = handle.io_to_fn(10, Packet::<Write>::new_buf(4), |packet, err| {
///         if err.is_some() {
///             assert_eq!(packet.len(), 1);
///         } else {
///             assert_eq!(packet.len(), 3);
///         }
///     }).await;
///
///     assert_eq!(packet.simple_contiguous_slice(), Some(&[55, 89, 144][..]));
///
///     Ok(())
/// })
/// # }
/// # work().unwrap();
/// ```
#[repr(C)]
pub struct OutputFunction<F, Perms: PacketPerms> {
    hdr: PacketOutput<Perms>,
    func: F,
}

impl<F, Perms: PacketPerms> AsRef<PacketOutput<Perms>> for OutputFunction<F, Perms> {
    fn as_ref(&self) -> &PacketOutput<Perms> {
        &self.hdr
    }
}

impl<'a, F: Fn(PacketView<'a, Perms>, Option<Error>) + Send + Sync, Perms: PacketPerms>
    OutputFunction<F, Perms>
{
    pub fn new(func: F) -> Self {
        unsafe extern "C" fn output<
            'a,
            F: Fn(PacketView<'a, Perms>, Option<Error>) + Send + Sync,
            Perms: PacketPerms,
        >(
            out: OutputRef<'static, Perms>,
            view: PacketView<'static, Perms>,
            err: Option<NonZeroI32>,
        ) {
            out.bound_views.fetch_sub(1, Ordering::Release);

            let out = core::mem::transmute::<&PacketOutput<_>, &OutputFunction<F, Perms>>(&*out);

            (out.func)(view, err.map(Error::from_int_err))
        }

        Self {
            hdr: PacketOutput {
                vtbl: &PacketOutputVtbl {
                    output: output::<'a, F, Perms>,
                },
                bound_views: 0.into(),
            },
            func,
        }
    }
}

unsafe impl<'b, F: Fn(PacketView<'b, Perms>, Option<Error>) + Send + Sync, Perms: PacketPerms>
    OpaqueStore for OutputFunction<F, Perms>
{
    type ConstHdr = PacketOutput<Perms>;
    type Opaque<'a> = OutputRef<'a, Perms> where Self: 'a;
    // TODO: cfg switch this to a stack based obj.
    type StackReq<'a> = BaseArc<Self> where Self: 'a;
    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self.into()
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'c, 'c>(stack: &'c Self::StackReq<'a>) -> &'c Self::ConstHdr
    where
        Self: 'a,
    {
        &stack.hdr
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        OutputRef::from_arc(stack.clone().transpose().into_base().unwrap())
    }
}

/// Outputs resulting packet views to a stream.
///
/// Custom backing storage may be supported through [`PushPop`] trait.
///
/// # Examples
///
/// ```rust
/// # mod sample {
/// #     include!("../../sample.rs");
/// # }
/// # use sample::SampleIo;
/// # fn work() -> mfio::error::Result<()> {
/// use mfio::backend::*;
/// use mfio::io::*;
/// use core::pin::pin;
/// use futures::stream::StreamExt;
///
/// let mut mem = vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144];
/// let handle = SampleIo::new(mem.clone());
///
/// handle.block_on(async {
///     let mut fut = handle.io_to_stream(10, Packet::<Write>::new_buf(4), vec![]);
///     let mut fut = pin!(fut);
///
///     let stream = fut.as_mut().submit();
///
///     while let Some((packet, err)) = (&**stream).next().await {
///         if err.is_some() {
///             assert_eq!(packet.len(), 1);
///         } else {
///             assert_eq!(packet.len(), 3);
///         }
///     }
///
///     Ok(())
/// })
/// # }
/// # work().unwrap();
/// ```
#[repr(C)]
pub struct PacketStream<'a, T, Perms: PacketPerms> {
    hdr: PacketOutput<Perms>,
    lock_and_flags: AtomicU8,
    waker: UnsafeCell<MaybeUninit<CWaker>>,
    data: UnsafeCell<T>,
    phantom: PhantomData<&'a ()>,
}

impl<T, Perms: PacketPerms> AsRef<PacketOutput<Perms>> for PacketStream<'_, T, Perms> {
    fn as_ref(&self) -> &PacketOutput<Perms> {
        &self.hdr
    }
}

impl<'a, T: PushPop<Output<'a, Perms>>, Perms: PacketPerms> PacketStream<'_, T, Perms> {
    fn lock(&self) -> LockGuard<T, Perms> {
        while (self.lock_and_flags.fetch_or(1, Ordering::AcqRel) & 1) != 0 {
            while (self.lock_and_flags.load(Ordering::Relaxed) & 1) != 0 {
                core::hint::spin_loop();
            }
        }

        LockGuard { stream: self }
    }

    pub fn new(container: T) -> Self {
        unsafe extern "C" fn output<'a, T: PushPop<Output<'a, Perms>>, Perms: PacketPerms>(
            out: OutputRef<'static, Perms>,
            view: PacketView<'static, Perms>,
            err: Option<NonZeroI32>,
        ) {
            out.bound_views.fetch_sub(1, Ordering::Release);

            let out = core::mem::transmute::<&PacketOutput<_>, &PacketStream<T, Perms>>(&*out);

            let mut guard = out.lock();

            // Only wake if this is the very first element we are pushing
            if guard.data().is_empty() {
                if let Some(waker) = guard.take_waker() {
                    waker.wake();
                }
            }

            guard.data().push((view, err.map(Error::from_int_err)));
        }

        Self {
            hdr: PacketOutput {
                vtbl: &PacketOutputVtbl {
                    output: output::<T, Perms>,
                },
                bound_views: 0.into(),
            },
            lock_and_flags: 0.into(),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
            data: UnsafeCell::new(container),
            phantom: PhantomData,
        }
    }
}

struct LockGuard<'a, 'b, T, Perms: PacketPerms> {
    stream: &'a PacketStream<'b, T, Perms>,
}

impl<T, Perms: PacketPerms> Drop for LockGuard<'_, '_, T, Perms> {
    fn drop(&mut self) {
        self.stream.lock_and_flags.fetch_and(!1, Ordering::Release);
    }
}

impl<T, Perms: PacketPerms> LockGuard<'_, '_, T, Perms> {
    pub fn data(&mut self) -> &mut T {
        unsafe { &mut *self.stream.data.get() }
    }

    pub fn take_waker(&mut self) -> Option<CWaker> {
        if self
            .stream
            .lock_and_flags
            .fetch_and(!0b10, Ordering::Relaxed)
            & 0b10
            != 0
        {
            Some(unsafe { (*self.stream.waker.get()).assume_init_read() })
        } else {
            None
        }
    }

    pub fn put_waker(&mut self, waker: CWaker) {
        unsafe { (*self.stream.waker.get()).write(waker) };
        self.stream.lock_and_flags.fetch_or(0b10, Ordering::Relaxed);
    }
}

impl<'a, T: PushPop<Output<'a, Perms>>, Perms: PacketPerms> Stream for &PacketStream<'a, T, Perms> {
    type Item = Output<'a, Perms>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        let mut guard = this.lock();

        if let Some((pkt, err)) = guard.data().pop() {
            Poll::Ready(Some((pkt, err)))
        } else if guard.stream.hdr.bound_views.load(Ordering::Acquire) == 0 {
            Poll::Ready(None)
        } else {
            guard.put_waker(cx.waker().clone().into());
            Poll::Pending
        }
    }
}

unsafe impl<Perms: PacketPerms> OpaqueStore for BaseArc<PacketOutput<Perms>> {
    type ConstHdr = PacketOutput<Perms>;
    type Opaque<'a> = OutputRef<'a, Perms> where Self: 'a;
    // TODO: cfg switch this to a stack based obj.
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = Self where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        OutputRef::from_arc_ref(stack)
    }
}

unsafe impl<'c, Perms: PacketPerms> OpaqueStore for &'c BaseArc<PacketOutput<Perms>> {
    type ConstHdr = PacketOutput<Perms>;
    type Opaque<'a> = OutputRef<'a, Perms> where Self: 'a;
    // TODO: cfg switch this to a stack based obj.
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = BaseArc<PacketOutput<Perms>> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.clone()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        OutputRef::from_arc_ref(stack)
    }
}

unsafe impl<T, Perms: PacketPerms> OpaqueStore for BaseArc<PacketStream<'_, T, Perms>> {
    type ConstHdr = PacketOutput<Perms>;
    type Opaque<'a> = OutputRef<'a, Perms> where Self: 'a;
    // TODO: cfg switch this to a stack based obj.
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = Self where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        &stack.hdr
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        OutputRef::from_arc(stack.clone().transpose().into_base().unwrap())
    }
}

unsafe impl<T, Perms: PacketPerms> OpaqueStore for PacketStream<'_, T, Perms> {
    type ConstHdr = PacketOutput<Perms>;
    type Opaque<'a> = OutputRef<'a, Perms> where Self: 'a;
    // TODO: cfg switch this to a stack based obj.
    type StackReq<'a> = BaseArc<Self> where Self: 'a;
    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self.into()
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        &stack.hdr
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        OutputRef::from_arc(stack.clone().transpose().into_base().unwrap())
    }
}

/// Providees a stack/queue mechanism for [`PacketStream`]s.
pub trait PushPop<T> {
    fn push(&mut self, val: T);
    fn pop(&mut self) -> Option<T>;
    fn is_empty(&self) -> bool;
}

impl<T> PushPop<T> for Vec<T> {
    fn push(&mut self, val: T) {
        Vec::push(self, val)
    }

    fn pop(&mut self) -> Option<T> {
        Vec::pop(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

use std::collections::VecDeque;

impl<T> PushPop<T> for VecDeque<T> {
    fn push(&mut self, val: T) {
        VecDeque::push_back(self, val)
    }

    fn pop(&mut self) -> Option<T> {
        VecDeque::pop_front(self)
    }

    fn is_empty(&self) -> bool {
        VecDeque::is_empty(self)
    }
}
