use core::mem::MaybeUninit;


use core::task::Context;

use tarc::{Arc, BaseArc};

use parking_lot::Mutex;
use std::collections::VecDeque;

type Address = usize;

struct IoThreadHandle<Perms: PacketPerms> {
    handle: PacketIoHandle<'static, Perms, Address>,
    input: Mutex<VecDeque<(usize, BoundPacket<'static, Perms>)>>,
    event: Arc<Event>,
}

impl<Perms: PacketPerms> Default for IoThreadHandle<Perms> {
    fn default() -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            input: Default::default(),
            event: Default::default(),
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
        let mut input = self.input.lock();
        input.push_back((address, packet));
        self.event.signal();
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
    read_stream: BaseArc<PacketStream<'static, Write, Address>>,
    write_stream: BaseArc<PacketStream<'static, Read, Address>>,
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
                        let mut input = read_io.input.lock();

                        while let Some(inp) = input.pop_front() {
                            proc_inp(inp);
                        }
                    }

                    read_io.event.wait().await;
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
                        let mut input = write_io.input.lock();

                        while let Some(inp) = input.pop_front() {
                            proc_inp(inp);
                        }
                    }

                    write_io.event.wait().await;
                }
            }
        };

        let future = Box::pin(async move {
            tokio::join!(read, write);
        }) as BoxedFuture;
        let future = SharedFuture::from(future);

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
            future: future.clone(),
        });

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
            future: future.clone(),
        });

        Self {
            write_stream,
            read_stream,
        }
    }
}

#[derive(Clone)]
pub struct SampleIo {
    mem: Arc<VolatileMem>,
    thread_state: IoThreadState,
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
        }
    }
}

impl PacketIo<Read, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Read, Address>> {
        Some(self.thread_state.write_stream.new_packet_id())
    }
}

impl PacketIo<Write, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Write, Address>> {
        Some(self.thread_state.read_stream.new_packet_id())
    }
}
