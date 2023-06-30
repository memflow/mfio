use mfio::backend::*;
use mfio::error::{Error, Location, State, Subject, INTERNAL_ERROR};
use mfio::packet::*;
use mfio::util::*;

use mfio_derive::*;

use core::task::Waker;

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
    fn read(&self, pos: usize, dest: BoundPacket<'static, Write>) {
        if pos >= self.len {
            dest.error(Error {
                code: INTERNAL_ERROR,
                location: Location::Backend,
                subject: Subject::Address,
                state: State::Outside,
            });
            return;
        }
        let dest = if pos > self.len - dest.len() {
            println!("{pos} {} {}", self.len, dest.len());
            let (a, b) = dest.split_at(self.len - pos);
            b.error(Error {
                code: INTERNAL_ERROR,
                location: Location::Backend,
                subject: Subject::Address,
                state: State::Outside,
            });
            a
        } else {
            dest
        };
        unsafe {
            dest.transfer_data(self.buf.add(pos).cast());
        }
    }

    fn write(&self, pos: usize, src: BoundPacket<'static, Read>) {
        println!(
            "{pos} {} {} {:?}",
            src.len(),
            self.len,
            self.buf
        );
        if pos >= self.len {
            src.error(Error {
                code: INTERNAL_ERROR,
                location: Location::Backend,
                subject: Subject::Address,
                state: State::Outside,
            });
            return;
        }
        let src = if pos > self.len - src.len() {
            let (a, b) = src.split_at(self.len - pos);
            b.error(Error {
                code: INTERNAL_ERROR,
                location: Location::Backend,
                subject: Subject::Address,
                state: State::Outside,
            });
            a
        } else {
            src
        };
        unsafe {
            src.transfer_data(self.buf.add(pos).cast());
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

pub struct IoThreadState {
    read_stream: BaseArc<PacketStream<'static, Write, Address>>,
    write_stream: BaseArc<PacketStream<'static, Read, Address>>,
    backend: BackendContainer<DynBackend>,
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
                        mem.read(addr, buf);
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
                        mem.write(pos, buf);
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

        let future = async move {
            tokio::join!(read, write);
        };

        let backend = BackendContainer::new_dyn(future);

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        });

        Self {
            write_stream,
            read_stream,
            backend,
        }
    }
}

#[derive(SyncIoRead, SyncIoWrite)]
pub struct SampleIo {
    mem: Arc<VolatileMem>,
    thread_state: IoThreadState,
}

impl Clone for SampleIo {
    fn clone(&self) -> Self {
        let mem = self.mem.clone();
        let thread_state = IoThreadState::new(&mem);

        Self { mem, thread_state }
    }
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

        Self { mem, thread_state }
    }
}

impl PacketIo<Read, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Read, Address>> {
        Some(self.thread_state.write_stream.new_packet_id())
    }
}

impl PacketIo<Write, Address> for SampleIo {
    fn separate_thread_state(&mut self) {
        self.thread_state = IoThreadState::new(&self.mem);
    }

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Write, Address>> {
        Some(self.thread_state.read_stream.new_packet_id())
    }
}

impl IoBackend for SampleIo {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.thread_state.backend.acquire()
    }
}
