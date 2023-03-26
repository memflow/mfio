use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Waker};
use event_listener::Event;

use tarc::{Arc, BaseArc};

use parking_lot::Mutex;
use std::collections::VecDeque;

type Address = usize;

struct IoThreadHandle<Perms: PacketPerms> {
    handle: PacketIoHandle<'static, Perms, Address>,
    input: Mutex<(
        VecDeque<(usize, BoundPacket<'static, Perms>)>,
        Option<Waker>,
    )>,
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
        let mut guard = self.input.lock();
        let (input, waker) = &mut *guard;
        input.push_back((address, packet));
        if let Some(waker) = waker.take() {
            waker.wake()
        }
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
        unsafe {
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(self.buf, self.len));
        }
    }
}

#[derive(Clone)]
pub struct IoThreadState {
    read_io: BaseArc<IoThreadHandle<Write>>,
    write_io: BaseArc<IoThreadHandle<Read>>,
    future: SharedFuture<BoxedFuture>,
}

impl IoThreadState {
    fn new(mem: &Arc<VolatileMem>) -> Self {
        let read_io: BaseArc<IoThreadHandle<Write>> = Default::default();
        let write_io: BaseArc<IoThreadHandle<Read>> = Default::default();

        let read = {
            let mem = mem.clone();
            let read_io = read_io.clone();

            async move {
                loop {
                    let proc_inp = |(addr, buf): (usize, BoundPacket<'static, Write>)| {
                        let mut pkt = buf.get_mut();
                        mem.read(addr, &mut pkt);
                    };

                    // try_pop here many elems
                    {
                        let mut guard = read_io.input.lock();

                        while let Some(inp) = guard.0.pop_front() {
                            proc_inp(inp);
                        }
                    }

                    let mut yielded = false;

                    core::future::poll_fn(|cx| {
                        if !yielded {
                            read_io.input.lock().1 = Some(cx.waker().clone());
                            // TODO: handle race conditions in multithreaded env, where elements
                            // get pusehd after installing the waker
                            yielded = true;
                            core::task::Poll::Pending
                        } else {
                            core::task::Poll::Ready(())
                        }
                    })
                    .await;
                }
            }
        };

        let write = {
            let mem = mem.clone();
            let write_io = write_io.clone();

            async move {
                loop {
                    let proc_inp = |(pos, buf): (usize, BoundPacket<'static, Read>)| {
                        let pkt = buf.get();
                        mem.write(pos, &pkt);
                    };

                    // try_pop here many elems
                    {
                        let mut guard = write_io.input.lock();

                        while let Some(inp) = guard.0.pop_front() {
                            proc_inp(inp);
                        }
                    }

                    let mut yielded = false;

                    core::future::poll_fn(|cx| {
                        if !yielded {
                            write_io.input.lock().1 = Some(cx.waker().clone());
                            yielded = true;
                            core::task::Poll::Pending
                        } else {
                            core::task::Poll::Ready(())
                        }
                    })
                    .await;
                }
            }
        };

        let future = Box::pin(async move {
            tokio::join!(read, write);
        }) as BoxedFuture;
        let future = SharedFuture::from(future);

        Self {
            read_io,
            write_io,
            future,
        }
    }
}

#[derive(Clone)]
pub struct SampleIo {
    mem: Arc<VolatileMem>,
    thread_state: IoThreadState,
    read_streams: Arc<PinHeap<PacketStream<'static, Write, Address>>>,
    write_streams: Arc<PinHeap<PacketStream<'static, Read, Address>>>,
}

impl Default for SampleIo {
    fn default() -> Self {
        Self::new(vec![0; 0x100000])
    }
}

impl SampleIo {
    pub fn new(mem: Vec<u8>) -> Self {
        let mem = Arc::new(mem.into());

        let thread_state = IoThreadState::new(&mem);

        Self {
            mem,
            thread_state,
            read_streams: Default::default(),
            write_streams: Default::default(),
        }
    }
}

impl PacketIo<Read, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.write_streams = Default::default();
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_alloc_stream(
        &self,
        _: &mut Context,
    ) -> Option<Pin<AllocHandle<PacketStream<Read, Address>>>> {
        let stream = PinHeap::alloc_or_cached(
            self.write_streams.clone(),
            || {
                let stream = PacketStream {
                    ctx: PacketCtx::new(self.thread_state.write_io.clone()).into(),
                    future: self.thread_state.future.clone(),
                };

                unsafe { core::mem::transmute(stream) }
            },
            |e| {
                // If queue is referenced somewhere else
                if e.ctx.strong_count() > 1 {
                    e.ctx = Arc::new(PacketCtx::new(self.thread_state.write_io.clone()));
                    e.future = self.thread_state.future.clone();
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

impl PacketIo<Write, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.read_streams = Default::default();
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_alloc_stream(
        &self,
        _: &mut Context,
    ) -> Option<Pin<AllocHandle<PacketStream<Write, Address>>>> {
        let stream = PinHeap::alloc_or_cached(
            self.read_streams.clone(),
            || {
                let stream = PacketStream {
                    ctx: PacketCtx::new(self.thread_state.read_io.clone()).into(),
                    future: self.thread_state.future.clone(),
                };

                unsafe { core::mem::transmute(stream) }
            },
            |e| {
                // If queue is referenced somewhere else, or is pointing to different IO handle
                if e.ctx.strong_count() > 1 || self.thread_state.read_io != *e.ctx {
                    e.ctx = Arc::new(PacketCtx::new(self.thread_state.read_io.clone()));
                    e.future = self.thread_state.future.clone();
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
