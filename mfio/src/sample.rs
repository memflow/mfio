use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Context;
use event_listener::Event;

use tarc::{Arc, BaseArc};

use parking_lot::Mutex;
use std::collections::VecDeque;

type Address = usize;

struct IoThreadHandle<Perms: PacketPerms> {
    handle: PacketIoHandle<'static, Perms, Address>,
    input: Mutex<VecDeque<(usize, BoundPacket<'static, Perms>)>>,
    flush: (AtomicBool, Event),
}

impl<Perms: PacketPerms> Default for IoThreadHandle<Perms> {
    fn default() -> Self {
        Self {
            handle: unsafe { PacketIoHandle::new::<Self>() },
            input: Default::default(),
            flush: Default::default(),
        }
    }
}

impl<Perms: PacketPerms> AsRef<PacketIoHandle<'static, Perms, Address>> for IoThreadHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, Address> {
        &self.handle
    }
}

impl<Perms: PacketPerms> PacketIoHandleable<'static, Perms, Address> for IoThreadHandle<Perms> {
    extern "C" fn send_input(&self, address: usize, packet: BoundPacket<'static, Perms>) {
        self.input.lock().push_back((address, packet))
    }

    extern "C" fn flush(&self) {
        self.flush.0.store(true, Ordering::Release);
        self.flush.1.notify(usize::MAX);
    }
}

struct VolatileMem {
    buf: *mut u8,
    len: usize,
}

impl VolatileMem {
    fn read(&self, pos: usize, dest: &mut [MaybeUninit<u8>]) {
        assert!(pos <= self.len - dest.len());
        unsafe {
            (self.buf as *mut MaybeUninit<u8>)
                .add(pos)
                .copy_to_nonoverlapping(dest.as_mut_ptr(), dest.len());
        }
    }

    fn write(&self, pos: usize, src: &[u8]) {
        println!(
            "{pos} {} {} {:?} {:?}",
            src.len(),
            self.len,
            src.as_ptr(),
            self.buf
        );
        assert!(pos <= self.len - src.len());
        unsafe {
            src.as_ptr()
                .copy_to_nonoverlapping(self.buf.add(pos), src.len())
        }
    }
}

impl From<Vec<u8>> for VolatileMem {
    fn from(buf: Vec<u8>) -> Self {
        let len = buf.len();

        let buf = Box::leak(buf.into_boxed_slice());

        let buf = buf.as_mut_ptr();

        Self { buf, len }
    }
}

unsafe impl Send for VolatileMem {}
unsafe impl Sync for VolatileMem {}

impl Drop for VolatileMem {
    fn drop(&mut self) {
        println!("Drop mem");
        unsafe {
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(self.buf, self.len));
        }
    }
}

#[derive(Clone)]
pub struct IoThreadState {
    read_io: BaseArc<IoThreadHandle<Write>>,
    write_io: BaseArc<IoThreadHandle<Read>>,
    read_streams: Arc<PinHeap<PacketStream<'static, Write, Address>>>,
    write_streams: Arc<PinHeap<PacketStream<'static, Read, Address>>>,
}

impl IoThreadState {
    fn new(io: &SampleIo) -> (Self, BoxedFuture) {
        let read_io = BaseArc::new(IoThreadHandle::default());
        let write_io = BaseArc::new(IoThreadHandle::default());

        let read = {
            let mem = io.mem.clone();
            let read_io = read_io.clone();

            async move {
                loop {
                    if !read_io.flush.0.load(Ordering::Acquire) {
                        let listen = read_io.flush.1.listen();
                        if !read_io.flush.0.load(Ordering::Acquire) {
                            listen.await;
                        }
                    }

                    read_io.flush.0.store(false, Ordering::Release);

                    let mut yielded = false;

                    core::future::poll_fn(|cx| {
                        if !yielded {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            core::task::Poll::Pending
                        } else {
                            core::task::Poll::Ready(())
                        }
                    })
                    .await;

                    //CNT.fetch_add(1, Ordering::Relaxed);
                    let proc_inp = |(addr, buf): (usize, BoundPacket<'static, Write>)| {
                        let mut pkt = buf.get_mut();
                        mem.read(addr, &mut pkt);
                    };

                    // try_pop here many elems
                    {
                        let mut input = read_io.input.lock();
                        loop {
                            match input.pop_front() {
                                Some(inp) => proc_inp(inp),
                                //Err(concurrent_queue::PopError::Closed) => return,
                                _ => break,
                            }
                        }
                    }

                    //std::thread::sleep(std::time::Duration::from_micros(1000));

                    /*std::thread::yield_now();

                    let mut yielded = false;

                    core::future::poll_fn(|cx| {
                        if !yielded {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            Poll::Ready(())
                        }
                    })
                    .await;*/
                }
            }
        };

        let write = {
            let mem = io.mem.clone();
            let write_io = write_io.clone();

            async move {
                loop {
                    if !write_io.flush.0.load(Ordering::Acquire) {
                        let listen = write_io.flush.1.listen();
                        if !write_io.flush.0.load(Ordering::Acquire) {
                            listen.await;
                        }
                    }

                    write_io.flush.0.store(false, Ordering::Release);

                    let proc_inp = |(pos, buf): (usize, BoundPacket<'static, Read>)| {
                        let pkt = buf.get();
                        mem.write(pos, &pkt);
                    };

                    // try_pop here many elems
                    {
                        let mut input = write_io.input.lock();
                        loop {
                            match input.pop_front() {
                                Some(inp) => proc_inp(inp),
                                //Err(concurrent_queue::PopError::Closed) => return,
                                _ => break,
                            }
                        }
                    }
                }
            }
        };

        let future = Box::pin(async move {
            tokio::join!(read, write);
        }) as BoxedFuture;
        //let future = SharedFuture::from(future);

        (
            Self {
                read_io,
                write_io,
                read_streams: io.read_streams.clone(),
                write_streams: io.write_streams.clone(),
            },
            future,
        )
    }
}

#[derive(Clone)]
pub struct SampleIo {
    read_streams: Arc<PinHeap<PacketStream<'static, Write, Address>>>,
    write_streams: Arc<PinHeap<PacketStream<'static, Read, Address>>>,
    mem: Arc<VolatileMem>,
}

impl Default for SampleIo {
    fn default() -> Self {
        Self::new(vec![0; 0x100000])
    }
}

impl SampleIo {
    pub fn new(mem: Vec<u8>) -> Self {
        let mem = Arc::new(mem.into());

        Self {
            read_streams: Default::default(),
            write_streams: Default::default(),
            mem,
        }
    }
}

impl IoHandle for SampleIo {
    type BackendFuture = BoxedFuture;
    type Backend = IoThreadState;

    fn local_backend(&self) -> (Self::Backend, Self::BackendFuture) {
        IoThreadState::new(self)
    }
}

impl PacketIo<Read, Address> for IoThreadState {
    fn try_alloc_stream(
        &self,
        _: &mut Context,
    ) -> Option<Pin<AllocHandle<PacketStream<Read, Address>>>> {
        let stream = PinHeap::alloc_or_cached(
            self.write_streams.clone(),
            || {
                let stream = PacketStream {
                    ctx: PacketCtx::new(self.write_io.clone()).into(),
                };

                unsafe { core::mem::transmute(stream) }
            },
            |e| {
                // If queue is referenced somewhere else
                if e.ctx.strong_count() > 1 {
                    e.ctx = Arc::new(PacketCtx::new(self.write_io.clone()));
                } else {
                    e.ctx.output.queue.lock().clear();
                    *e.ctx.output.wake.lock() = None;
                    e.ctx.output.size.store(0, Ordering::Relaxed);
                }
                // No need to clone the future - it's the same future!
                //e.future = self.future.clone();
            },
        );

        let stream = unsafe { core::mem::transmute(Pin::from(stream)) };

        Some(stream)
    }
}

impl PacketIo<Write, Address> for IoThreadState {
    fn try_alloc_stream(
        &self,
        _: &mut Context,
    ) -> Option<Pin<AllocHandle<PacketStream<Write, Address>>>> {
        let stream = PinHeap::alloc_or_cached(
            self.read_streams.clone(),
            || {
                let stream = PacketStream {
                    ctx: PacketCtx::new(self.read_io.clone()).into(),
                    //future: self.thread_state.future.clone(),
                };

                unsafe { core::mem::transmute(stream) }
            },
            |e| {
                // If queue is referenced somewhere else, or is pointing to different IO handle
                if e.ctx.strong_count() > 1 || self.read_io != *e.ctx {
                    e.ctx = Arc::new(PacketCtx::new(self.read_io.clone()));
                } else {
                    e.ctx.output.queue.lock().clear();
                    *e.ctx.output.wake.lock() = None;
                    e.ctx.output.size.store(0, Ordering::Relaxed);
                }
                // No need to clone the future - it's the same future!
                //e.future = self.future.clone();
            },
        );

        let stream = unsafe { core::mem::transmute(Pin::from(stream)) };

        Some(stream)
    }
}
