//! Describes I/O packets.
//!
//! # Introduction
//!
//! A packet is a fungible unit of I/O operation. It is paired with an address and then is
//! transferred  throughout the I/O chain, until it reaches the final I/O backend, completing the
//! operation. Along the way, the packet may be split into smaller ones, parts of it may be
//! rejected, while other parts may get forwarded, with potentially diverging addresses. In the
//! end, all parts of the packet are collected back into one place.
//!
//! A packet represents an abstract source or destination for I/O operations. It is always
//! parameterized with [`PacketPerms`](PacketPerms), describing how the underlying data of the
//! packet can be accessed. Accessing the data means that the packet will no longer be forwarded,
//! but before that point, the packet may be split up into smaller chunks, and sent to different
//! I/O subsystems.
//!
//! # Lifecycle
//!
//! Most packet interactions can be traced back to [`PacketIo`](PacketIo) - a trait enabling the
//! user to send packets to the I/O system. [`PacketIo::new_id`](PacketIo::new_id) is used to
//! allocate a [`PacketId`](PacketId). This ID acts as an entrypoint for packets, and the place for
//! their results to be collected. The packet then gets sent to the I/O system and its results are
//! made available on the [`Stream`](futures::stream::Stream) that is implemented by the
//! `PakcetId`. Complete flow is as follows:
//!
//! 1. A `PacketId` is allocated through [`PacketIo::new_id`](PacketIo::new_id).
//!
//! 2. Packets may be submitted to the system through [`PacketId::send_io`](PacketId::send_io)
//!    function.
//!
//! 3. Packet gets directed to the I/O backend through [`PacketStream`](PacketStream::send_io), and
//!    then [`PacketIoHandle::send_input`](PacketIoHandle::send_input).
//!
//! 4. The I/O backend stores the packet internally, and processes it.
//!
//! 5. Result is fed back to [`PacketOutput`](PacketOutput), which is accessible through
//!    [`PacketId`].
//!
//! 6. Caller gets the packets and their respective results by invoking
//!    [`Stream::poll_next`](futures::stream::Stream::poll_next).
//!
//! # Copy constraint negotiation
//!
//! I/O systems have various constraints on kinds of I/O operations possible. Some systems work by
//! exposing a publicly accessible byte buffer, while in other systems those buffers are opaque and
//! hidden behind hardware mechanisms or OS APIs. The simplest way to tackle varying requirements
//! is to allocate intermediary buffers on the endpoints and expose those for I/O. However, that
//! can be highly inefficient, because multiple copies may be involved, before data ends up at the
//! final destination. Ideal scenario, for any I/O system, is to have only one copy per operation.
//! And in I/O system where data is generated on-the-fly, ideal scenario would be to write output
//! directly to the destination.
//!
//! To achieve this in mfio, we attach constraints to various parts of the I/O chain, and allocate
//! temporary buffers only when needed. For any I/O end, we have the following constraint options:
//!
//! 1. Publicly exposed aligned byte-addressable buffer - this is the lower constraint tier, as
//!    individual bytes can be modified at neglibible cost.
//! 2. Accepts byte-addressable input - this is more constarined, because the caller must provide a
//!    byte buffer, and cannot generate data on the fly. The callee takes this buffer and processes
//!    it internally using opaque mechanisms.
//!
//! I/O has 2 ends - input and output. These constraint levels are similar on both ends. See how
//! these levels are described in the context of input (caller):
//!
//! 1. Sends byte-addressable buffer - this is the lower constraint tier, because the callee can
//!    process the input in any way possible.
//! 2. Fills a byte-addressable buffer - this is more constrained, because the callee needs to
//!    provide a buffer to write to. However, this may also mean that the caller generates data on
//!    the fly, thus memory usage is lower.
//!
//! This is not exhaustive, but generally sufficient for most I/O cases. In practice, a backend
//! that is able to access byte-addressable buffer directly will simply provide it to the packet,
//! which will then process it. If the backend instead needs a buffer from the packet, it will
//! call [`BoundPacket::try_alloc`](BoundPacket::try_alloc) with desired alignment parameters. If
//! the allocation is not successful, it will then fall back to allocating an intermediary buffer.

use crate::error::Error;
use crate::multistack::{MultiStack, StackHandle};
use crate::util::ReadOnly;
use cglue::option::COption;
pub use cglue::task::{CWaker, FastCWaker};
use core::cell::UnsafeCell;
use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicIsize, Ordering};
use core::task::{Context, Poll};
use futures::stream::Stream;
use parking_lot::Mutex;
use tarc::{Arc, BaseArc};

pub type Output<'a, DataType> = (PacketObj<'a, DataType>, Option<Error>);

pub type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Debug)]
#[repr(C)]
pub struct PacketId<'a, Perms: PacketPerms, Param> {
    inner: ReadOnly<PacketIdInner>,
    stream: &'a PacketStream<'a, Perms, Param>,
}

impl<'a, Perms: PacketPerms, Param> Drop for PacketId<'a, Perms, Param> {
    fn drop(&mut self) {
        assert!(self.inner.size.load(Ordering::SeqCst) <= 0);
        self.stream
            .ctx
            .output
            .stack
            .lock()
            .free_stack(&self.inner.id);
    }
}

impl<'a, Perms: PacketPerms, Param> PacketId<'a, Perms, Param> {
    pub fn project_inner<'b>(self: Pin<&'b Self>) -> Pin<&'b PacketIdInner> {
        let this: &'b Self = self.get_ref();
        unsafe { Pin::new_unchecked(&this.inner) }
    }
    pub fn send_io(self: Pin<&Self>, param: Param, packet: impl Into<Packet<'a, Perms>>) {
        self.stream
            .send_io(self.project_inner(), param, packet.into())
    }
}

impl<'a, Perms: PacketPerms, Param> Stream for PacketId<'a, Perms, Param> {
    type Item = Output<'a, Perms::DataType>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.into_ref();
        this.stream.poll_id(this, cx)
    }
}

impl<'a, Perms: PacketPerms, Param> core::ops::Deref for PacketId<'a, Perms, Param> {
    type Target = PacketIdInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct PacketIdInner {
    id: StackHandle,
    size: AtomicIsize,
    wake: UnsafeCell<COption<CWaker>>,
    _pinned: PhantomData<PhantomPinned>,
}

// SAFETY: we are handling synchronization for the unsafe cell that is making this type not
// implement Send + Sync
unsafe impl Send for PacketIdInner {}
// SAFETY: we are handling synchronization for the unsafe cell that is making this type not
// implement Send + Sync
unsafe impl Sync for PacketIdInner {}

use cglue::prelude::v1::*;

#[cglue_trait]
pub trait PacketIo<Perms: PacketPerms, Param>: Sized {
    fn try_new_id<'a>(&'a self, context: &mut FastCWaker) -> Option<PacketId<'a, Perms, Param>>;

    #[skip_func]
    fn new_id(&self) -> NewIdFut<Self, Perms, Param> {
        NewIdFut {
            this: self,
            _phantom: PhantomData,
        }
    }

    #[skip_func]
    fn io<'a>(
        &'a self,
        param: Param,
        packet: impl Into<Packet<'a, Perms>>,
    ) -> IoFut<'a, Self, Perms, Param> {
        IoFut::NewId(self, param, packet.into())
    }
}

pub trait StreamIo<Perms: PacketPerms>: PacketIo<Perms, NoPos> {
    fn stream_io<'a>(
        &'a self,
        packet: impl Into<Packet<'a, Perms>>,
    ) -> IoFut<'a, Self, Perms, NoPos> {
        self.io(NoPos::new(), packet)
    }
}

#[repr(transparent)]
#[derive(Clone)]
pub struct NoPos(core::marker::PhantomData<()>);

impl NoPos {
    pub const fn new() -> Self {
        Self(core::marker::PhantomData)
    }
}

pub enum IoFut<'a, T, Perms: PacketPerms, Param> {
    NewId(&'a T, Param, Packet<'a, Perms>),
    InProgress(PacketId<'a, Perms, Param>),
}

impl<'a, T: PacketIo<Perms, Param>, Perms: PacketPerms, Param> Stream
    for IoFut<'a, T, Perms, Param>
{
    type Item = Output<'a, Perms::DataType>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let state = unsafe { self.get_unchecked_mut() };
        loop {
            match state {
                IoFut::NewId(this, _, _) => {
                    if let Some(packet_id) = (*this).try_new_id(&mut cx.waker().into()) {
                        let in_progress = IoFut::InProgress(packet_id);
                        let prev = core::mem::replace(state, in_progress);
                        match (&mut *state, prev) {
                            (IoFut::InProgress(packet_id), IoFut::NewId(_, param, packet)) => {
                                unsafe { Pin::new_unchecked(&*packet_id) }.send_io(param, packet);
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        break Poll::Pending;
                    }
                }
                IoFut::InProgress(packet_id) => {
                    let packet_id = unsafe { Pin::new_unchecked(packet_id) };
                    break packet_id.poll_next(cx);
                }
            }
        }
    }
}

pub struct NewIdFut<'a, T, Perms: PacketPerms, Param> {
    this: &'a T,
    _phantom: PhantomData<(Perms, Param)>,
}

impl<'a, T: PacketIo<Perms, Param>, Perms: PacketPerms, Param: 'a> Future
    for NewIdFut<'a, T, Perms, Param>
{
    type Output = PacketId<'a, Perms, Param>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(packet_id) = self.this.try_new_id(&mut cx.waker().into()) {
            Poll::Ready(packet_id)
        } else {
            Poll::Pending
        }
    }
}

pub struct PacketStream<'a, Perms: PacketPerms, Param> {
    pub ctx: Arc<PacketCtx<'a, Perms, Param>>,
}

impl<'a, Perms: PacketPerms, Param: core::fmt::Debug> core::fmt::Debug
    for PacketStream<'a, Perms, Param>
{
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "({:?})", self.ctx)
    }
}

impl<'a, Perms: PacketPerms, Param> PacketStream<'a, Perms, Param> {
    pub fn poll_id(
        &self,
        id: Pin<&PacketId<'a, Perms, Param>>,
        cx: &mut Context,
    ) -> Poll<Option<Output<'a, Perms::DataType>>> {
        let closed = id.inner.size.load(Ordering::Relaxed) <= 0;

        let mut output_stack = self.ctx.output.stack.lock();

        let ret = match output_stack.pop(&id.inner.id) {
            Some(elem) => Poll::Ready(Some(elem)),
            _ if closed => Poll::Ready(None),
            _ => {
                // Install the waker. Normally we'd want to check the output queue afterwards to avoid
                // deadlocks, but since we are holding output queue lock, we don't have to do that.
                // SAFETY: we are holding the lock to the output stack. The only other place where this
                // waker is being accessed from, also does so holding the output stack lock.
                unsafe {
                    *id.inner.wake.get() = Some(cx.waker().clone().into()).into();
                }

                Poll::Pending
            }
        };

        core::mem::drop(output_stack);

        ret
    }

    pub fn new_packet_id(&self) -> PacketId<Perms, Param> {
        let id = self.ctx.output.stack.lock().new_stack();

        // Shorten lifetime of the stream.
        // This is "okay", because we do not allow to put any data into the stream with shorter
        // lifetime, apart from the borrowed byte buffers, which are safe if the stream does not
        // get forgotten. See mfio top level documentation about the safety guarantees.
        let stream = unsafe { core::mem::transmute::<&Self, _>(self) };

        PacketId {
            inner: ReadOnly::from(PacketIdInner {
                id,
                size: 0.into(),
                wake: Default::default(),
                _pinned: PhantomData,
            }),
            stream,
        }
    }

    pub fn send_io<'b>(&self, id: Pin<&'b PacketIdInner>, param: Param, packet: Packet<'b, Perms>)
    where
        'a: 'b,
    {
        // Shorten lifetime of self.
        // According to safety guarantees of the crate, this is valid.
        // PacketId is pinned, thus it will be polled to completion, meaning no data of lifetime 'b
        // will be left in the system by the time 'b is dropped.
        let stream: &PacketStream<'b, Perms, Param> =
            unsafe { core::mem::transmute::<&Self, _>(self) };
        let packet = packet.bind(stream, id);
        PacketIoHandle::send_input(&stream.ctx.io, param, packet);
    }
}

#[derive(Debug)]
pub struct PacketOutput<'a, Perms: PacketPerms> {
    pub stack: Mutex<MultiStack<Output<'a, <Perms as PacketPerms>::DataType>>>,
}

impl<'a, Perms: PacketPerms> Default for PacketOutput<'a, Perms> {
    fn default() -> Self {
        Self {
            stack: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct PacketCtx<'a, Perms: PacketPerms, Param> {
    pub io: Arc<PacketIoHandle<'a, Perms, Param>>,
    pub output: PacketOutput<'a, Perms>,
}

impl<'a, Perms: PacketPerms, Param> AsRef<PacketOutput<'a, Perms>> for PacketCtx<'a, Perms, Param> {
    fn as_ref(&self) -> &PacketOutput<'a, Perms> {
        &self.output
    }
}

impl<'a, Perms: PacketPerms, Param> PacketCtx<'a, Perms, Param> {
    pub fn new<T: IntoIoHandle<'a, Perms, Param>>(io: BaseArc<T>) -> Self {
        Self {
            output: Default::default(),
            io: IntoIoHandle::into_handle(io),
        }
    }
}

impl<'a, Perms: PacketPerms, Param, T: IntoIoHandle<'a, Perms, Param>>
    core::cmp::PartialEq<BaseArc<T>> for PacketCtx<'a, Perms, Param>
{
    fn eq(&self, other: &BaseArc<T>) -> bool {
        self.io.as_ptr() == IntoIoHandle::into_handle(other.clone()).as_ptr()
    }
}

impl<'a, Perms: PacketPerms, Param, T: IntoIoHandle<'a, Perms, Param>>
    core::cmp::PartialEq<PacketCtx<'a, Perms, Param>> for BaseArc<T>
{
    fn eq(&self, other: &PacketCtx<'a, Perms, Param>) -> bool {
        other == self
    }
}

pub trait IntoIoHandle<'a, Perms: PacketPerms, Param> {
    fn into_handle(this: BaseArc<Self>) -> Arc<PacketIoHandle<'a, Perms, Param>>;
}

impl<'a, T: AsRef<PacketIoHandle<'a, Perms, Param>>, Perms: PacketPerms, Param>
    IntoIoHandle<'a, Perms, Param> for T
{
    fn into_handle(this: BaseArc<Self>) -> Arc<PacketIoHandle<'a, Perms, Param>> {
        this.transpose()
    }
}

pub trait PacketIoHandleable<'a, Perms: PacketPerms, Param> {
    extern "C" fn send_input(&self, param: Param, buffer: BoundPacket<'a, Perms>);
}

impl<'a, Perms: PacketPerms, Param> core::fmt::Debug for PacketIoHandle<'a, Perms, Param> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "({:?})", self.send_input as *const (),)
    }
}

#[repr(C)]
pub struct PacketIoHandle<'a, Perms: PacketPerms, Param> {
    send_input: unsafe extern "C" fn(*const (), Param, BoundPacket<'a, Perms>),
}

impl<'a, Perms: PacketPerms, Param> PacketIoHandle<'a, Perms, Param> {
    /// Create a new PacketIoHandle vtable
    pub fn new<T: PacketIoHandleable<'a, Perms, Param>>() -> Self {
        Self {
            send_input: unsafe { core::mem::transmute(T::send_input as extern "C" fn(_, _, _)) },
        }
    }

    fn send_input(this: &Arc<Self>, param: Param, buffer: BoundPacket<'a, Perms>) {
        // We must use raw ptr in order to satisfy miri stacked borrows rules
        unsafe { (this.send_input)(this.as_original_ptr::<()>(), param, buffer) };
    }
}

/// Consumes `BoundPacketObj` and returns allocated version of it.
///
/// If the `alignment` constraint is met, this function consumes `packet`, fills `out_aligned`, and
/// returns `true`. If the constraint cannot be satisfied, `false` is returned.
///
/// If this function returns `false`, the caller should consider allocating an intermediate buffer,
/// and invoking [`TransferDataFn`](TransferDataFn) instead.
///
/// # Remarks
///
/// The caller should prefer [`TransferDataFn`](TransferDataFn), as opposed to this function, if
/// they can access their internal buffer with zero allocations.
pub type AllocFn<T> = for<'a, 'b> unsafe extern "C" fn(
    packet: &'a mut ManuallyDrop<BoundPacketObj<'b, T>>,
    alignment: usize,
    out_alloced: &'a mut MaybeUninit<<T as PacketPerms>::Alloced<'b>>,
) -> bool;

/// Transfers data between a `BoundPacketObj` and reverse data type.
///
/// This function consumes `packet`, and transfers data between it and `input`. The caller must
/// ensure that `input` bounds are sufficient for the size of the packet.
///
/// TODO: decide on whether this function can fail.
pub type TransferDataFn<T> = for<'a, 'b> unsafe extern "C" fn(
    packet: &'a mut BoundPacketObj<'b, T>,
    input: <T as PacketPerms>::ReverseDataType,
);

pub type MaybeAlloced<'a, T> = Result<<T as PacketPerms>::Alloced<'a>, BoundPacket<'a, T>>;

pub type AllocedOrTransferred<'a, T> =
    Result<<T as PacketPerms>::Alloced<'a>, TransferredPacket<'a, T>>;

/// Describes type constraints on packet operations.
pub trait PacketPerms: 'static + core::fmt::Debug + Clone + Copy {
    type DataType: Clone + Copy + core::fmt::Debug;
    type ReverseDataType: Clone + Copy + core::fmt::Debug;
    type Alloced<'a>: AllocatedPacket + 'a;

    fn alloc_fn(&self) -> AllocFn<Self>;
    fn transfer_data_fn(&self) -> TransferDataFn<Self>;
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReadWrite {
    get_mut: for<'a> unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketObj<'a, Self>>,
        usize,
        &mut MaybeUninit<ReadWritePacketObj<'a>>,
    ) -> bool,
    transfer_data: for<'a, 'b> unsafe extern "C" fn(&'a mut BoundPacketObj<'b, Self>, *mut ()),
}

impl core::fmt::Debug for ReadWrite {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl PacketPerms for ReadWrite {
    type DataType = *mut ();
    type ReverseDataType = *mut ();
    type Alloced<'a> = ReadWritePacketObj<'a>;

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get_mut
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Write {
    get_mut: for<'a> unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketObj<'a, Self>>,
        usize,
        &mut MaybeUninit<WritePacketObj<'a>>,
    ) -> bool,
    transfer_data: for<'a, 'b> unsafe extern "C" fn(&'a mut BoundPacketObj<'b, Self>, *const ()),
}

impl core::fmt::Debug for Write {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl PacketPerms for Write {
    type DataType = *mut ();
    type ReverseDataType = *const ();
    type Alloced<'a> = WritePacketObj<'a>;

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get_mut
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Read {
    get: for<'a> unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketObj<'a, Self>>,
        usize,
        &mut MaybeUninit<ReadPacketObj<'a>>,
    ) -> bool,
    transfer_data: for<'a, 'b> unsafe extern "C" fn(&'a mut BoundPacketObj<'b, Self>, *mut ()),
}

impl core::fmt::Debug for Read {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get as *const ())
    }
}

impl PacketPerms for Read {
    type DataType = *const ();
    type ReverseDataType = *mut ();
    type Alloced<'a> = ReadPacketObj<'a>;

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct PacketObj<'a, DataType> {
    data: DataType,
    start: usize,
    end: usize,
    _phantom: PhantomData<&'a u8>,
}

impl<'a, DataType: Copy> PacketObj<'a, DataType> {
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data(&self) -> DataType {
        self.data
    }

    pub fn split_local(self, pos: usize) -> (Self, Self) {
        assert!(pos < self.len());
        let Self {
            data,
            start,
            end,
            _phantom,
        } = self;
        (
            Self {
                data,
                start,
                end: start + pos,
                _phantom,
            },
            Self {
                data,
                start: start + pos,
                end,
                _phantom,
            },
        )
    }
}

unsafe impl<'a, DataType> Send for PacketObj<'a, DataType> {}
unsafe impl<'a, DataType> Sync for PacketObj<'a, DataType> {}

#[derive(Debug)]
#[repr(C)]
pub struct Packet<'a, Perms: PacketPerms> {
    vtable: &'static Perms,
    obj: PacketObj<'a, Perms::DataType>,
}

/*impl<'a, Perms: PacketPerms> Packet<'a, Perms> {
    pub unsafe fn upgrade(self) -> Packet<'static, Perms> {
        core::mem::transmute(self)
    }
}*/

impl<'a> From<&'a mut [u8]> for Packet<'a, ReadWrite> {
    fn from(slc: &'a mut [u8]) -> Self {
        unsafe extern "C" fn get_mut<'a, 'b>(
            obj: &'a mut ManuallyDrop<BoundPacketObj<'b, ReadWrite>>,
            _: usize,
            out: &'a mut MaybeUninit<ReadWritePacketObj<'b>>,
        ) -> bool {
            out.write(ReadWritePacketObj {
                alloced_packet: (obj.buffer.data as *mut u8).add(obj.buffer.start),
                buffer: ManuallyDrop::take(obj),
            });
            true
        }

        unsafe extern "C" fn transfer_data(obj: &mut BoundPacketObj<ReadWrite>, src: *mut ()) {
            // TODO: does this operation even make sense?
            core::ptr::swap_nonoverlapping(
                src as *mut u8,
                obj.buffer.data as *mut u8,
                obj.buffer.len(),
            );
        }

        Self {
            obj: PacketObj {
                data: slc.as_mut_ptr() as *mut _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &ReadWrite {
                get_mut,
                transfer_data,
            },
        }
    }
}

impl<'a, D: AnyBytes, const N: usize> From<&'a mut [D; N]> for Packet<'a, Write> {
    fn from(slc: &'a mut [D; N]) -> Self {
        Self::from(&mut slc[..])
    }
}

trait AnyBytes {}

impl AnyBytes for u8 {}
impl AnyBytes for MaybeUninit<u8> {}

impl<'a, D: AnyBytes> From<&'a mut [D]> for Packet<'a, Write> {
    fn from(slc: &'a mut [D]) -> Self {
        unsafe extern "C" fn get_mut<'a, 'b>(
            obj: &'a mut ManuallyDrop<BoundPacketObj<'b, Write>>,
            _: usize,
            out: &'a mut MaybeUninit<WritePacketObj<'b>>,
        ) -> bool {
            out.write(WritePacketObj {
                alloced_packet: (obj.buffer.data as *mut MaybeUninit<u8>).add(obj.buffer.start),
                buffer: ManuallyDrop::take(obj),
            });
            true
        }

        unsafe extern "C" fn transfer_data(obj: &mut BoundPacketObj<Write>, src: *const ()) {
            // TODO: consider changing this to copy_nonoverlapping
            core::ptr::copy(
                src as *const u8,
                obj.buffer.data as *mut u8,
                obj.buffer.len(),
            );
        }

        Self {
            obj: PacketObj {
                data: slc.as_mut_ptr() as *mut _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &Write {
                get_mut,
                transfer_data,
            },
        }
    }
}

impl<'a> From<Packet<'a, ReadWrite>> for Packet<'a, Write> {
    fn from(packet: Packet<'a, ReadWrite>) -> Self {
        unsafe { core::mem::transmute(packet) }
    }
}

impl<'a, T: AsRef<[u8]> + ?Sized> From<&'a T> for Packet<'a, Read> {
    fn from(slc: &'a T) -> Self {
        // Just to be sure that the lifetimes are correct
        let slc: &'a [u8] = slc.as_ref();
        unsafe extern "C" fn get<'a, 'b>(
            obj: &'a mut ManuallyDrop<BoundPacketObj<'b, Read>>,
            _: usize,
            out: &'a mut MaybeUninit<ReadPacketObj<'b>>,
        ) -> bool {
            out.write(ReadPacketObj {
                alloced_packet: (obj.buffer.data as *const u8).add(obj.buffer.start),
                buffer: ManuallyDrop::take(obj),
            });
            true
        }

        unsafe extern "C" fn transfer_data(obj: &mut BoundPacketObj<Read>, src: *mut ()) {
            // TODO: consider changing this to copy_nonoverlapping
            core::ptr::copy(
                obj.buffer.data as *const u8,
                src as *mut u8,
                obj.buffer.len(),
            );
        }

        Self {
            obj: PacketObj {
                data: slc.as_ptr() as *const _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &Read { get, transfer_data },
        }
    }
}

pub trait Splittable: Sized {
    fn split_at(self, len: usize) -> (Self, Self);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<A: Splittable, B: Splittable> Splittable for Result<A, B> {
    fn split_at(self, len: usize) -> (Self, Self) {
        match self {
            Ok(v) => {
                let (a, b) = v.split_at(len);
                (Ok(a), Ok(b))
            }
            Err(v) => {
                let (a, b) = v.split_at(len);
                (Err(a), Err(b))
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Ok(v) => v.len(),
            Err(v) => v.len(),
        }
    }
}

pub trait Errorable: Sized {
    fn error(self, err: Error);
}

impl<A: Errorable, B: Errorable> Errorable for Result<A, B> {
    fn error(self, err: Error) {
        match self {
            Ok(v) => v.error(err),
            Err(v) => v.error(err),
        }
    }
}

pub trait AllocatedPacket: Splittable + Errorable {
    type Pointer: Copy;

    fn as_ptr(&self) -> Self::Pointer;
    fn id(&self) -> *const ();
}

#[repr(C)]
pub struct ReadWritePacketObj<'a> {
    alloced_packet: *mut u8,
    buffer: BoundPacketObj<'a, ReadWrite>,
}

impl<'a> Splittable for ReadWritePacketObj<'a> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> usize {
        self.buffer.buffer.len()
    }
}

impl<'a> Errorable for ReadWritePacketObj<'a> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<'a> AllocatedPacket for ReadWritePacketObj<'a> {
    type Pointer = *mut u8;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }

    fn id(&self) -> *const () {
        self.buffer.id.get_ref() as *const _ as *const ()
    }
}

unsafe impl<'a> Send for ReadWritePacketObj<'a> {}

impl core::ops::Deref for ReadWritePacketObj<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

impl core::ops::DerefMut for ReadWritePacketObj<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { core::slice::from_raw_parts_mut(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

#[repr(C)]
pub struct WritePacketObj<'a> {
    alloced_packet: *mut MaybeUninit<u8>,
    buffer: BoundPacketObj<'a, Write>,
}

impl<'a> Splittable for WritePacketObj<'a> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> usize {
        self.buffer.buffer.len()
    }
}

impl<'a> Errorable for WritePacketObj<'a> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<'a> AllocatedPacket for WritePacketObj<'a> {
    type Pointer = *mut MaybeUninit<u8>;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }

    fn id(&self) -> *const () {
        self.buffer.id.get_ref() as *const _ as *const ()
    }
}

unsafe impl<'a> Send for WritePacketObj<'a> {}

impl core::ops::Deref for WritePacketObj<'_> {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

impl core::ops::DerefMut for WritePacketObj<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { core::slice::from_raw_parts_mut(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

#[repr(C)]
pub struct ReadPacketObj<'a> {
    alloced_packet: *const u8,
    buffer: BoundPacketObj<'a, Read>,
}

impl<'a> Splittable for ReadPacketObj<'a> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> usize {
        self.buffer.buffer.len()
    }
}

impl<'a> Errorable for ReadPacketObj<'a> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<'a> AllocatedPacket for ReadPacketObj<'a> {
    type Pointer = *const u8;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }

    fn id(&self) -> *const () {
        self.buffer.id.get_ref() as *const _ as *const ()
    }
}

unsafe impl<'a> Send for ReadPacketObj<'a> {}

impl core::ops::Deref for ReadPacketObj<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

impl<'a, Perms: PacketPerms> Packet<'a, Perms> {
    pub fn bind<'b, Param>(
        self,
        stream: &PacketStream<'b, Perms, Param>,
        id: Pin<&'b PacketIdInner>,
    ) -> BoundPacket<'b, Perms>
    where
        'a: 'b,
    {
        let Packet { obj, vtable } = self;
        id.size.fetch_add(1, Ordering::Acquire);
        BoundPacket {
            obj: BoundPacketObj {
                buffer: ManuallyDrop::new(obj),
                output: stream.ctx.clone().transpose(),
                id,
            },
            vtable,
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct BoundPacketObj<'a, T: PacketPerms> {
    buffer: ManuallyDrop<PacketObj<'a, T::DataType>>,
    output: Arc<PacketOutput<'a, T>>,
    id: Pin<&'a PacketIdInner>,
}

impl<'a, T: PacketPerms> Drop for BoundPacketObj<'a, T> {
    fn drop(&mut self) {
        // FIXME: output failure by default.
        self.output(None)
    }
}

impl<'a, T: PacketPerms> Splittable for BoundPacketObj<'a, T> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let mut this = ManuallyDrop::new(self);
        let buffer = unsafe { ManuallyDrop::take(&mut this.buffer) };
        let (b1, b2) = buffer.split_local(len);

        this.id.size.fetch_add(1, Ordering::Release);

        (
            Self {
                buffer: ManuallyDrop::new(b1),
                output: this.output.clone(),
                id: this.id,
            },
            Self {
                buffer: ManuallyDrop::new(b2),
                // SAFETY:
                // we are doing this, because otherwise the Arc would leak.
                output: unsafe { core::ptr::read(&this.output) },
                id: this.id,
            },
        )
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<'a, T: PacketPerms> Errorable for BoundPacketObj<'a, T> {
    fn error(self, err: Error) {
        let mut this = ManuallyDrop::new(self);
        this.output(Some(err));
        // Manually drop all the fields, without invoking our actual drop implementation.
        // Note that `buffer` is being taken out by the `output` function.
        // SAFETY: this function is consuming `self`, thus the data will not be touched again.
        unsafe {
            core::ptr::drop_in_place(&mut this.output);
            core::ptr::drop_in_place(&mut this.id);
        }
    }
}

impl<'a, T: PacketPerms> BoundPacketObj<'a, T> {
    fn output(&mut self, err: Option<Error>) {
        let id = &self.id.id;
        let mut output_stack = self.output.stack.lock();
        output_stack.push(id, (unsafe { ManuallyDrop::take(&mut self.buffer) }, err));
        self.id.size.fetch_sub(1, Ordering::Release);
        {
            // We need to keep holding the lock to the output stack in this block to prevent
            // situations where wakee reads from the output queue and drops this waker all while we
            // are making changes (and vice-versa).
            if let Some(wake) = unsafe { &mut *self.id.wake.get() }.take() {
                //lock().take() {
                wake.wake();
            }
        }
        core::mem::drop(output_stack);
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct BoundPacket<'a, T: PacketPerms> {
    vtable: &'static T,
    obj: BoundPacketObj<'a, T>,
}

impl<'a, T: PacketPerms> Splittable for BoundPacket<'a, T> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let (b1, b2) = self.obj.split_at(len);

        (
            Self {
                vtable: self.vtable,
                obj: b1,
            },
            Self {
                vtable: self.vtable,
                obj: b2,
            },
        )
    }

    fn len(&self) -> usize {
        self.obj.buffer.len()
    }
}

impl<'a, T: PacketPerms> Errorable for BoundPacket<'a, T> {
    fn error(self, err: Error) {
        self.obj.error(err)
    }
}

impl<'a, T: PacketPerms> BoundPacket<'a, T> {
    pub fn try_alloc(self) -> MaybeAlloced<'a, T> {
        let BoundPacket { obj, vtable } = self;
        let mut obj = ManuallyDrop::new(obj);
        let mut out = MaybeUninit::uninit();
        let ret = unsafe { (vtable.alloc_fn())(&mut obj, 1, &mut out) };
        if ret {
            Ok(unsafe { out.assume_init() })
        } else {
            Err(BoundPacket {
                obj: ManuallyDrop::into_inner(obj),
                vtable,
            })
        }
    }

    /// Transfers data between the packet and the `input`.
    ///
    /// If packet has [`Read`](Read) permissions, `input` will be written to. If packet has
    /// [`Write`](Write) permissions, `input` will be read.
    ///
    /// If packet has [`ReadWrite`](ReadWrite) permissions, behavior is not fully specified, but
    /// currently buffers perform memory swap operation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `input` pointer is sufficient for the bounds of the packet. In
    /// addition, the data pointed by `input` must be properly initialized and byte-addressible.
    pub unsafe fn transfer_data(self, input: T::ReverseDataType) -> TransferredPacket<'a, T> {
        let BoundPacket { mut obj, vtable } = self;
        (vtable.transfer_data_fn())(&mut obj, input);
        TransferredPacket(obj)
    }
}

#[repr(transparent)]
pub struct TransferredPacket<'a, T: PacketPerms>(BoundPacketObj<'a, T>);

impl<'a, T: PacketPerms> Splittable for TransferredPacket<'a, T> {
    fn split_at(self, len: usize) -> (Self, Self) {
        let (b1, b2) = self.0.split_at(len);

        (Self(b1), Self(b2))
    }

    fn len(&self) -> usize {
        self.0.buffer.len()
    }
}

impl<'a, T: PacketPerms> Errorable for TransferredPacket<'a, T> {
    fn error(self, err: Error) {
        self.0.error(err)
    }
}

/*macro_rules! downgrade_packet {
    ($from:expr, $to:expr) => {
        impl<'a> From<Packet<'a, { $from }>> for Packet<'a, { $to }> {
            fn from(
                Packet {
                    data,
                    start,
                    end,
                    _phantom,
                }: Packet<'a, { $from }>,
            ) -> Self {
                Self {
                    data,
                    start,
                    end,
                    _phantom,
                }
            }
        }

        impl<'a> AsRef<Packet<'a, { $to }>> for Packet<'a, { $from }> {
            fn as_ref(&self) -> &Packet<'a, { $to }> {
                unsafe { &*(self as *const Self as *const _) }
            }
        }

        impl<'a> AsMut<Packet<'a, { $to }>> for Packet<'a, { $from }> {
            fn as_mut(&mut self) -> &mut Packet<'a, { $to }> {
                unsafe { &mut *(self as *mut Self as *mut _) }
            }
        }
    };
}*/

/*downgrade_packet!(perms::READ, perms::NONE);
downgrade_packet!(perms::WRITE, perms::NONE);
downgrade_packet!(perms::READ_WRITE, perms::NONE);
downgrade_packet!(perms::READ_WRITE, perms::READ);
downgrade_packet!(perms::READ_WRITE, perms::WRITE);
*/
/*impl<'a> Packet<'a, {perms::READ}> {

}*/
