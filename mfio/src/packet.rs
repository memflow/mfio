use crate::heap::Release;
use crate::multistack::{MultiStack, StackHandle};
use crate::shared_future::SharedFuture;
use crate::util::ReadOnly;
use core::cell::UnsafeCell;
use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicIsize, Ordering};
use core::task::{Context, Poll, Waker};
use futures::stream::Stream;
use parking_lot::Mutex;
use tarc::{Arc, BaseArc};

type Output<'a, DataType> = (PacketObj<'a, DataType>, Option<()>);

pub type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Debug)]
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
pub struct PacketIdInner {
    id: StackHandle,
    size: AtomicIsize,
    wake: UnsafeCell<Option<Waker>>,
    _pinned: PhantomPinned,
}

// SAFETY: we are handling synchronization for the unsafe cell that is making this type not
// implement Send + Sync
unsafe impl Send for PacketIdInner {}
// SAFETY: we are handling synchronization for the unsafe cell that is making this type not
// implement Send + Sync
unsafe impl Sync for PacketIdInner {}

pub trait PacketIo<Perms: PacketPerms, Param>: Sized {
    fn separate_thread_state(&mut self);

    fn try_new_id<'a>(&'a self, context: &mut Context) -> Option<PacketId<'a, Perms, Param>>;

    fn new_id(&self) -> NewIdFut<Self, Perms, Param> {
        NewIdFut {
            this: self,
            _phantom: PhantomData,
        }
    }

    fn io<'a>(
        &'a self,
        param: Param,
        packet: impl Into<Packet<'a, Perms>>,
    ) -> IoFut<'a, Self, Perms, Param> {
        IoFut::NewId(self, param, packet.into())
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
                    if let Some(packet_id) = (*this).try_new_id(cx) {
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
        if let Some(packet_id) = self.this.try_new_id(cx) {
            Poll::Ready(packet_id)
        } else {
            Poll::Pending
        }
    }
}

pub struct PacketStream<'a, Perms: PacketPerms, Param> {
    pub ctx: Arc<PacketCtx<'a, Perms, Param>>,
    pub future: Option<SharedFuture<BoxedFuture>>,
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

        // Try polling the backend future if it should be run
        if !closed {
            let _ = self.future.as_ref().map(|f| f.try_run_once_sync(cx));
        }

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
                    *id.inner.wake.get() = Some(cx.waker().clone());
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
                _pinned: PhantomPinned,
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

pub trait PacketPerms: 'static + core::fmt::Debug + Clone + Copy {
    type DataType: Clone + Copy + core::fmt::Debug;
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReadWrite {
    get_mut: for<'a> unsafe extern "C" fn(BoundPacketObj<'a, Self>) -> ReadWritePacketObj<'a>,
}

impl core::fmt::Debug for ReadWrite {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl PacketPerms for ReadWrite {
    type DataType = *mut ();
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Write {
    get_mut: for<'a> unsafe extern "C" fn(BoundPacketObj<'a, Self>) -> WritePacketObj<'a>,
}

impl core::fmt::Debug for Write {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl PacketPerms for Write {
    type DataType = *mut ();
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Read {
    get: for<'a> unsafe extern "C" fn(BoundPacketObj<'a, Self>) -> ReadPacketObj<'a>,
}

impl core::fmt::Debug for Read {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get as *const ())
    }
}

impl PacketPerms for Read {
    type DataType = *const ();
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

#[derive(Debug)]
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
        unsafe extern "C" fn get_mut(obj: BoundPacketObj<ReadWrite>) -> ReadWritePacketObj {
            ReadWritePacketObj {
                alloced_packet: (obj.buffer.data as *mut u8).add(obj.buffer.start),
                buffer: obj,
            }
        }

        Self {
            obj: PacketObj {
                data: slc.as_mut_ptr() as *mut _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &ReadWrite { get_mut },
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
        unsafe extern "C" fn get_mut(obj: BoundPacketObj<Write>) -> WritePacketObj {
            WritePacketObj {
                alloced_packet: (obj.buffer.data as *mut MaybeUninit<u8>).add(obj.buffer.start),
                buffer: obj,
            }
        }

        Self {
            obj: PacketObj {
                data: slc.as_mut_ptr() as *mut _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &Write { get_mut },
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
        unsafe extern "C" fn get(obj: BoundPacketObj<Read>) -> ReadPacketObj {
            ReadPacketObj {
                alloced_packet: (obj.buffer.data as *const u8).add(obj.buffer.start),
                buffer: obj,
            }
        }

        Self {
            obj: PacketObj {
                data: slc.as_ptr() as *const _,
                start: 0,
                end: slc.len(),
                _phantom: PhantomData,
            },
            vtable: &Read { get },
        }
    }
}

#[repr(C)]
pub struct ReadWritePacketObj<'a> {
    alloced_packet: *mut u8,
    buffer: BoundPacketObj<'a, ReadWrite>,
}

impl<'a> ReadWritePacketObj<'a> {
    pub fn split_at(self, len: usize) -> (Self, Self) {
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

    pub fn error(self, err: Option<()>) {
        self.buffer.error(err)
    }
}

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

unsafe impl<'a> Send for ReadWritePacketObj<'a> {}

#[repr(C)]
pub struct WritePacketObj<'a> {
    alloced_packet: *mut MaybeUninit<u8>,
    buffer: BoundPacketObj<'a, Write>,
}

impl<'a> WritePacketObj<'a> {
    pub fn split_at(self, len: usize) -> (Self, Self) {
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

    pub fn error(self, err: Option<()>) {
        self.buffer.error(err)
    }
}

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

unsafe impl<'a> Send for WritePacketObj<'a> {}

unsafe impl<'a, DataType> Send for PacketObj<'a, DataType> {}
unsafe impl<'a, DataType> Sync for PacketObj<'a, DataType> {}

#[repr(C)]
pub struct ReadPacketObj<'a> {
    alloced_packet: *const u8,
    buffer: BoundPacketObj<'a, Read>,
}

impl<'a> ReadPacketObj<'a> {
    pub fn split_at(self, len: usize) -> (Self, Self) {
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

    pub fn error(self, err: Option<()>) {
        self.buffer.error(err)
    }
}

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

impl<'a, T: PacketPerms> BoundPacketObj<'a, T> {
    fn output(&mut self, err: Option<()>) {
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

    pub fn split_at(self, len: usize) -> (Self, Self) {
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

    pub fn error(self, err: Option<()>) {
        let mut this = ManuallyDrop::new(self);
        this.output(err);
        // Manually drop all the fields, without invoking our actual drop implementation.
        // Note that `buffer` is being taken out by the `output` function.
        // SAFETY: this function is consuming `self`, thus the data will not be touched again.
        unsafe {
            core::ptr::drop_in_place(&mut this.output);
            core::ptr::drop_in_place(&mut this.id);
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct BoundPacket<'a, T: PacketPerms> {
    vtable: &'static T,
    obj: BoundPacketObj<'a, T>,
}

impl<'a> BoundPacket<'a, ReadWrite> {
    pub fn get_mut(self) -> ReadWritePacketObj<'a> {
        let BoundPacket { obj, vtable } = self;
        unsafe { (vtable.get_mut)(obj) }
    }
}

impl<'a> BoundPacket<'a, Write> {
    pub fn get_mut(self) -> WritePacketObj<'a> {
        let BoundPacket { obj, vtable } = self;
        unsafe { (vtable.get_mut)(obj) }
    }
}

impl<'a> BoundPacket<'a, Read> {
    pub fn get(self) -> ReadPacketObj<'a> {
        let BoundPacket { obj, vtable } = self;
        unsafe { (vtable.get)(obj) }
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
