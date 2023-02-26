use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Context;
use event_listener::Event;

use crate::heap::{AllocHandle, PinHeap};
use crate::packet::*;
use crate::shared_future::SharedFuture;
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

#[derive(Clone)]
struct IoThreadState {
    future: SharedFuture<BoxedFuture>,
    read_io: BaseArc<IoThreadHandle<Write>>,
    write_io: BaseArc<IoThreadHandle<Read>>,
}

impl Default for IoThreadState {
    fn default() -> Self {
        Self::new()
    }
}

impl IoThreadState {
    fn new() -> Self {
        let read_io = BaseArc::new(IoThreadHandle::default());
        let write_io = BaseArc::new(IoThreadHandle::default());

        let read = {
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

                    //CNT.fetch_add(1, Ordering::Relaxed);
                    let proc_inp = move |(addr, buf): (usize, BoundPacket<'static, Write>)| {
                        let mut pkt = buf.get_mut();
                        pkt[0].write(addr as u8);
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

                    let proc_inp = move |(_addr, buf): (usize, BoundPacket<'static, Read>)| {
                        let _pkt = buf.get();
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
        let future = SharedFuture::from(future);

        Self {
            future,
            read_io,
            write_io,
        }
    }
}

#[derive(Clone)]
pub struct SampleIo {
    read_streams: Arc<PinHeap<PacketStream<'static, Write, Address>>>,
    write_streams: Arc<PinHeap<PacketStream<'static, Read, Address>>>,
    thread_state: IoThreadState,
}

impl PacketIo<Read, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.write_streams = Default::default();
        self.thread_state = Default::default();
    }

    fn try_alloc_stream(&self, _: &mut Context) -> Option<Pin<AllocHandle<PacketStream<Read, Address>>>> {
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
        self.thread_state = Default::default();
    }

    fn try_alloc_stream(&self, _: &mut Context) -> Option<Pin<AllocHandle<PacketStream<Write, Address>>>> {
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
                // If queue is referenced somewhere else
                if e.ctx.strong_count() > 1 {
                    e.ctx = Arc::new(PacketCtx::new(self.thread_state.read_io.clone()));
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

impl Default for SampleIo {
    fn default() -> Self {
        SampleIo {
            read_streams: Default::default(),
            write_streams: Default::default(),
            thread_state: Default::default(),
        }
    }
}
