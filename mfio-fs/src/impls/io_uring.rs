use std::fs::File;

use core::future::{poll_fn, Future};
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::task::{Context, Poll};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rio::*;

use mfio::heap::{AllocHandle, PinHeap, Release};
use mfio::packet::*;
use mfio::shared_future::SharedFuture;
use mfio::tarc::{Arc, BaseArc};
use mfio::util::Event;

static RING: Lazy<Rio> = Lazy::new(|| new().unwrap());

struct WakeWrap(std::task::Waker, *const Completion<'static, usize>);

unsafe impl Send for WakeWrap {}
unsafe impl Sync for WakeWrap {}

#[no_mangle]
static mut CH: Option<std::sync::Arc<WakeWrap>> = None;

impl std::task::Wake for WakeWrap {
    fn wake(self: std::sync::Arc<Self>) {
        unsafe { CH = Some(self.clone()) };
        self.0.wake_by_ref();
    }
}

enum MfioCompletion<'a, Perms: PacketPerms> {
    Submitted(Perms::Alloced<'a>, Completion<'a, usize>),
    Finished,
}

impl<'a, Perms: PacketPerms> Future for MfioCompletion<'a, Perms> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match this {
            Self::Submitted(_, c) => {
                let waker = std::sync::Arc::new(WakeWrap(
                    cx.waker().clone(),
                    c as *const Completion<'a, usize> as u64 as *const Completion<'static, usize>,
                ));
                let waker = core::task::Waker::from(waker);

                let mut cx2 = Context::from_waker(&waker);

                match unsafe { Pin::new_unchecked(c).poll(&mut cx2) } {
                    Poll::Ready(r) => {
                        let prev = core::mem::replace(&mut *this, Self::Finished);
                        match prev {
                            Self::Submitted(p, _) => match r {
                                Ok(len) if len < p.len() => {
                                    let (_, right) = p.split_at(len);
                                    right.error(Some(()));
                                }
                                Err(_) => p.error(Some(())),
                                _ => (),
                            },
                            _ => unreachable!(),
                        }
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Self::Finished => unreachable!(),
        }
    }
}

impl<'a, Perms: PacketPerms> Release for MfioCompletion<'a, Perms> {
    fn release(&mut self) {}
}

struct IoThreadHandle<Perms: PacketPerms> {
    file: BaseArc<File>,
    handle: PacketIoHandle<'static, Perms, u64>,
    heap: Arc<PinHeap<MfioCompletion<'static, Perms>>>,
    completions: Mutex<Vec<AllocHandle<MfioCompletion<'static, Perms>>>>,
    event: Event,
}

impl<Perms: IntoCompletion> IoThreadHandle<Perms> {
    fn new(file: BaseArc<File>) -> Self {
        Self {
            file,
            handle: PacketIoHandle::new::<Self>(),
            heap: Arc::new(PinHeap::new(0)),
            completions: Default::default(),
            event: Default::default(),
        }
    }

    async fn main_loop(this: BaseArc<Self>) {
        loop {
            poll_fn(|cx| {
                let mut completions = this.completions.lock();

                completions.retain_mut(|c| {
                    match unsafe { Pin::new_unchecked(&mut **c).poll(cx) } {
                        Poll::Pending => true,
                        _ => false,
                    }
                });

                let ret = if completions.is_empty() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                };

                core::mem::drop(completions);

                ret
            })
            .await;

            this.event.wait().await;
        }
    }
}

impl<Perms: PacketPerms> AsRef<PacketIoHandle<'static, Perms, u64>> for IoThreadHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, u64> {
        &self.handle
    }
}

trait IntoCompletion: PacketPerms {
    fn into_completion(
        file: &File,
        pos: u64,
        alloced: Self::Alloced<'static>,
    ) -> MfioCompletion<'static, Self>;
}

impl IntoCompletion for Read {
    fn into_completion(
        file: &File,
        pos: u64,
        alloced: Self::Alloced<'static>,
    ) -> MfioCompletion<'static, Self> {
        let data = alloced.as_ptr();
        let len = alloced.len();
        // SAFETY: the data is fully allocated and valid for the duration of the completion this
        // data will go to.
        let slice: &'static [_] = unsafe { core::slice::from_raw_parts(data, len) };
        // SAFETY: this is an absolute hack - rio has not been updated in 3 years and their
        // `write_at` trait bound does not have ?Sized, however, buffer is only accessed throughout
        // the duration of the call, thus it is okay for us to pass a stack variable, because it
        // won't be referenced after this the function exits.
        let slice: &'static &'static [_] = unsafe { &*(&slice as *const &'static [_]) };
        // SAFETY: see above, same conditions apply
        let file: &'static File = unsafe { &*(file as *const File) };
        let completion = RING.write_at(file, slice, pos);
        MfioCompletion::Submitted(alloced, completion)
    }
}

impl IntoCompletion for Write {
    fn into_completion(
        file: &File,
        pos: u64,
        alloced: Self::Alloced<'static>,
    ) -> MfioCompletion<'static, Self> {
        let data = alloced.as_ptr();
        let len = alloced.len();
        // SAFETY: the data is fully allocated and valid for the duration of the completion this
        // data will go to. As for the cast... :)
        let slice: &'static mut [_] =
            unsafe { core::slice::from_raw_parts_mut(data as *mut u8, len) };
        // SAFETY: this is an absolute hack - rio has not been updated in 3 years and their
        // `read_at` trait bound does not have ?Sized, however, buffer is only accessed throughout
        // the duration of the call, thus it is okay for us to pass a stack variable, because it
        // won't be referenced after this the function exits.
        let slice: &'static &'static mut [_] = unsafe { &*(&slice as *const &'static mut [_]) };
        // SAFETY: see above, same conditions apply
        let file: &'static File = unsafe { &*(file as *const File) };
        let completion = RING.read_at(file, slice, pos);
        MfioCompletion::Submitted(alloced, completion)
    }
}

impl<Perms: IntoCompletion> PacketIoHandleable<'static, Perms, u64> for IoThreadHandle<Perms> {
    extern "C" fn send_input(&self, pos: u64, packet: BoundPacket<'static, Perms>) {
        let alloced = packet.alloc();
        let handle = PinHeap::alloc(
            self.heap.clone(),
            Perms::into_completion(&*self.file, pos, alloced),
        );
        self.completions.lock().push(handle);
        self.event.signal();
    }
}

pub struct FileWrapper {
    file: BaseArc<File>,
    read_stream: ManuallyDrop<BaseArc<PacketStream<'static, Write, u64>>>,
    write_stream: ManuallyDrop<BaseArc<PacketStream<'static, Read, u64>>>,
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.read_stream);
            ManuallyDrop::drop(&mut self.write_stream);
        }
    }
}

impl From<File> for FileWrapper {
    fn from(file: File) -> Self {
        Self::from(BaseArc::from(file))
    }
}

impl From<BaseArc<File>> for FileWrapper {
    fn from(file: BaseArc<File>) -> Self {
        let write_io = BaseArc::new(IoThreadHandle::<Read>::new(file.clone()));
        let write = IoThreadHandle::main_loop(write_io.clone());
        let write = SharedFuture::from(Box::pin(write) as BoxedFuture);

        let write_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
            future: Some(write),
        }));

        let read_io = BaseArc::new(IoThreadHandle::<Write>::new(file.clone()));
        let read = IoThreadHandle::main_loop(read_io.clone());
        let read = SharedFuture::from(Box::pin(read) as BoxedFuture);

        let read_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
            future: Some(read),
        }));

        Self {
            file,
            read_stream,
            write_stream,
        }
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        *self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Read, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        *self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Write, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}
