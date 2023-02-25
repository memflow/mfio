use crate::heap::AllocHandle;
use crate::shared_future::SharedFuture;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicIsize, Ordering};
use core::task::{Context, Poll, Waker};
use futures::stream::Stream;
use parking_lot::Mutex;
use std::collections::VecDeque;
use tarc::{Arc, BaseArc};

type Output<'a, DataType> = (PacketObj<'a, DataType>, Option<()>);

pub type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub trait PacketIo<Perms: PacketPerms>: Sized {
    fn separate_thread_state(&mut self);

    fn try_alloc_stream(
        &self,
        context: &mut Context,
    ) -> Option<Pin<AllocHandle<PacketStream<'_, Perms>>>>;

    fn alloc_stream(&self) -> AllocStreamFut<'_, Self, Perms> {
        AllocStreamFut {
            this: self,
            poll_fn: Self::try_alloc_stream,
        }
    }

    fn io<'a>(
        &'a self,
        address: usize,
        packet: impl Into<Packet<'a, Perms>>,
    ) -> IoFut<'a, Self, Perms> {
        IoFut {
            this: self,
            poll_fn: Self::try_alloc_stream,
            state: Some((address, packet.into())),
        }
    }
}

pub struct IoFut<'a, T: PacketIo<Perms>, Perms: PacketPerms> {
    this: &'a T,
    poll_fn: fn(&'a T, &mut Context) -> Option<<Self as Future>::Output>,
    state: Option<(usize, Packet<'a, Perms>)>,
}

impl<'a, T: PacketIo<Perms>, Perms: PacketPerms> Future for IoFut<'a, T, Perms> {
    type Output = <AllocStreamFut<'a, T, Perms> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(stream) = (this.poll_fn)(this.this, cx) {
            let (address, buffer) = this.state.take().unwrap();
            stream.send_io(address, buffer);
            Poll::Ready(stream)
        } else {
            Poll::Pending
        }
    }
}

pub struct AllocStreamFut<'a, T, Perms: PacketPerms> {
    this: &'a T,
    poll_fn: fn(&'a T, &mut Context) -> Option<<Self as Future>::Output>,
}

impl<'a, T, Perms: PacketPerms> Future for AllocStreamFut<'a, T, Perms> {
    type Output = Pin<AllocHandle<PacketStream<'a, Perms>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(stream) = (self.poll_fn)(self.this, cx) {
            Poll::Ready(stream)
        } else {
            Poll::Pending
        }
    }
}

#[pin_project::pin_project]
pub struct PacketStream<'a, Perms: PacketPerms> {
    pub ctx: Arc<PacketCtx<'a, Perms>>,
    pub future: SharedFuture<BoxedFuture>,
}

impl<'a, Perms: PacketPerms> Stream for PacketStream<'a, Perms> {
    type Item = Output<'a, Perms::DataType>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.as_ref();

        let closed = this.ctx.size.load(Ordering::Relaxed) <= 0;

        match this.ctx.output.lock().pop_front() {
            Some(elem) => return Poll::Ready(Some(elem)),
            _ if closed => return Poll::Ready(None),
            _ => {}
        }

        PacketIoHandle::flush(&this.ctx.io);

        // Try running the future once
        if this.future.try_run_once_sync(cx).is_some() {
            if let Some(elem) = this.ctx.output.lock().pop_front() {
                return Poll::Ready(Some(elem));
            }
        }

        // Install the waker. Note that after this we must check for end condition once more to
        // avoid deadlocks.
        *this.ctx.wake.lock() = Some(cx.waker().clone());

        match this.ctx.output.lock().pop_front() {
            Some(elem) => return Poll::Ready(Some(elem)),
            // Check for one final time if we got closed while inserting the waker,
            // and if so, avoid returning Pending to avoid deadlock.
            _ if this.ctx.size.load(Ordering::Acquire) <= 0 => return Poll::Ready(None),
            _ => {}
        }

        Poll::Pending
    }
}

impl<'a, Perms: PacketPerms> PacketStream<'a, Perms> {
    pub fn send_io(&self, address: usize, packet: impl Into<Packet<'a, Perms>>) {
        let packet = packet.into().bind(self);
        PacketIoHandle::send_input(&self.ctx.io, address, packet);
    }
}

#[derive(Debug)]
pub struct PacketCtx<'a, Perms: PacketPerms> {
    pub io: Arc<PacketIoHandle<'a, Perms>>,
    pub output: Mutex<VecDeque<Output<'a, Perms::DataType>>>,
    pub size: AtomicIsize,
    pub wake: Mutex<Option<Waker>>,
}

impl<'a, Perms: PacketPerms> Drop for PacketCtx<'a, Perms> {
    fn drop(&mut self) {
        //unsafe { Arc::decrement_strong_count(self.io) }
    }
}

impl<'a, Perms: PacketPerms> PacketCtx<'a, Perms> {
    pub fn new<T: IntoIoHandle<'a, Perms>>(read_io: BaseArc<T>) -> Self {
        Self {
            output: Default::default(),
            wake: Default::default(),
            size: 0.into(),
            io: IntoIoHandle::into_handle(read_io),
        }
    }
}

pub trait IntoIoHandle<'a, Perms: PacketPerms> {
    fn into_handle(this: BaseArc<Self>) -> Arc<PacketIoHandle<'a, Perms>>;
}

impl<'a, T: AsRef<PacketIoHandle<'a, Perms>>, Perms: PacketPerms> IntoIoHandle<'a, Perms> for T {
    fn into_handle(this: BaseArc<Self>) -> Arc<PacketIoHandle<'a, Perms>> {
        this.transpose()
    }
}

pub trait PacketIoHandleable<'a, Perms: PacketPerms> {
    extern "C" fn send_input(&self, address: usize, buffer: BoundPacket<'a, Perms>);
    extern "C" fn flush(&self);
}

impl<'a, Perms: PacketPerms> core::fmt::Debug for PacketIoHandle<'a, Perms> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(
            fmt,
            "({:?} {:?})",
            self.send_input as *const (), self.flush as *const ()
        )
    }
}

#[repr(C)]
pub struct PacketIoHandle<'a, Perms: PacketPerms> {
    send_input: unsafe extern "C" fn(*const (), usize, BoundPacket<'a, Perms>),
    flush: unsafe extern "C" fn(*const ()),
}

impl<'a, Perms: PacketPerms> PacketIoHandle<'a, Perms> {
    /// Create a new PacketIoHandle vtable
    ///
    /// # Safety
    ///
    /// The caller must ensure that T contains one, and only one instance of `PacketIoHandle`, and it
    /// is located at the very start of the structure. T must also ensure that its drop
    /// implementation does not drop the underlying `PacketIoHandle`.
    pub unsafe fn new<T: PacketIoHandleable<'a, Perms>>() -> Self {
        Self {
            send_input: unsafe { core::mem::transmute(T::send_input as extern "C" fn(_, _, _)) },
            flush: unsafe { core::mem::transmute(T::flush as extern "C" fn(_)) },
        }
    }

    fn send_input(this: &Arc<Self>, address: usize, buffer: BoundPacket<'a, Perms>) {
        // We must use raw ptr in order to satisfy miri stacked borrows rules
        unsafe { (this.send_input)(this.as_original_ptr::<()>(), address, buffer) };
    }

    fn flush(this: &Arc<Self>) {
        unsafe { (this.flush)(this.as_original_ptr::<()>()) };
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data(&self) -> DataType {
        self.data
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

impl<'a, T: AsMut<[u8]> + ?Sized> From<&'a mut T> for Packet<'a, ReadWrite> {
    fn from(slc: &'a mut T) -> Self {
        // Just to be sure that the lifetimes are correct
        let slc: &'a mut [u8] = slc.as_mut();
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

impl<'a, T: AsMut<[MaybeUninit<u8>]> + ?Sized> From<&'a mut T> for Packet<'a, Write> {
    fn from(slc: &'a mut T) -> Self {
        // Just to be sure that the lifetimes are correct
        let slc: &'a mut [MaybeUninit<u8>] = slc.as_mut();
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

/*impl<'a, T: Into<Packet<'a, ReadWrite>>> From<T> for Packet<'a, Write> {
    fn from(data: T) -> Self {
        let packet = data.into();

        Self {
            obj: packet.vtable,
            vtable: unsafe { core::mem::transmute(packet.vtable) }
        }
    }
}*/

impl<'a> From<Packet<'a, ReadWrite>> for Packet<'a, Write> {
    fn from(packet: Packet<'a, ReadWrite>) -> Self {
        unsafe { core::mem::transmute(packet) }
    }
}

/*impl<'a, T: AsMut<[u8]> + ?Sized> From<&'a mut T> for Packet<'a, Write> {
    fn from(slc: &'a mut T) -> Self {
        // Just to be sure that the lifetimes are correct
        let slc: &'a mut [u8] = slc.as_mut();
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
}*/

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

impl core::ops::Deref for ReadPacketObj<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.alloced_packet, self.buffer.buffer.len()) }
    }
}

impl<'a, Perms: PacketPerms> Packet<'a, Perms> {
    pub fn bind(self, stream: &PacketStream<'a, Perms>) -> BoundPacket<'a, Perms> {
        let Packet { obj, vtable } = self;
        stream.ctx.size.fetch_add(1, Ordering::Acquire);
        BoundPacket {
            obj: BoundPacketObj {
                buffer: ManuallyDrop::new(obj),
                output: stream.ctx.clone(),
            },
            vtable,
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct BoundPacketObj<'a, T: PacketPerms> {
    buffer: ManuallyDrop<PacketObj<'a, T::DataType>>,
    output: Arc<PacketCtx<'a, T>>,
}

impl<'a, T: PacketPerms> Drop for BoundPacketObj<'a, T> {
    fn drop(&mut self) {
        self.output
            .output
            .lock()
            .push_back((unsafe { ManuallyDrop::take(&mut self.buffer) }, None));
        self.output.size.fetch_sub(1, Ordering::Release);
        if let Some(wake) = self.output.wake.lock().take() {
            wake.wake();
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
