use mfio::backend::*;
use mfio::error::{Error, Location, State, Subject, INTERNAL_ERROR};
use mfio::io::*;
use mfio::locks::Mutex;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Waker;

use mfio_derive::*;

use tarc::{Arc, BaseArc};

use std::collections::VecDeque;

type Address = u64;


#[derive(Default)]
pub(crate) struct Event {
    waker: Mutex<Option<Waker>>,
    signaled: AtomicBool,
}

impl Event {
    pub fn signal(&self) {
        if !self.signaled.swap(true, Ordering::Release) {
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
        }
    }

    pub async fn wait(&self) {
        let mut yielded = false;
        core::future::poll_fn(|cx| {
            if !yielded && !self.signaled.swap(false, Ordering::Acquire) {
                yielded = true;
                let mut guard = self.waker.lock();
                // Check after locking to avoid race conditions
                if self.signaled.swap(false, Ordering::Acquire) {
                    core::task::Poll::Ready(())
                } else {
                    *guard = Some(cx.waker().clone());
                    core::task::Poll::Pending
                }
            } else {
                core::task::Poll::Ready(())
            }
        })
        .await;
    }
}

struct IoThreadHandle<Perms: PacketPerms> {
    input: Mutex<VecDeque<(Address, BoundPacketView<Perms>)>>,
    event: Arc<Event>,
}

impl<Perms: PacketPerms> Default for IoThreadHandle<Perms> {
    fn default() -> Self {
        Self {
            input: Default::default(),
            event: Default::default(),
        }
    }
}

impl<Perms: PacketPerms> PacketIo<Perms, Address> for IoThreadHandle<Perms> {
    fn send_io(&self, address: Address, packet: BoundPacketView<Perms>) {
        let mut input = self.input.lock();
        input.push_back((address, packet));
        self.event.signal();
    }
}

struct VolatileMem {
    buf: *mut u8,
    len: u64,
}

impl VolatileMem {
    fn read(&self, pos: u64, dest: BoundPacketView<Write>) {
        if pos >= self.len {
            dest.error(Error {
                code: INTERNAL_ERROR,
                location: Location::Backend,
                subject: Subject::Address,
                state: State::Outside,
            });
            return;
        }
        let dest = if self.len < dest.len() || pos > self.len - dest.len() {
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
            let _ = dest.transfer_data(self.buf.add(pos as usize).cast());
        }
    }

    fn write(&self, pos: u64, src: BoundPacketView<Read>) {
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
            let _ = src.transfer_data(self.buf.add(pos as usize).cast());
        }
    }
}

impl From<Vec<u8>> for VolatileMem {
    fn from(buf: Vec<u8>) -> Self {
        let len = buf.len() as u64;

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
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(self.buf, self.len as usize));
        }
    }
}

pub struct IoThreadState {
    read_io: BaseArc<IoThreadHandle<Write>>,
    write_io: BaseArc<IoThreadHandle<Read>>,
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
                    let proc_inp = |(addr, buf): (Address, BoundPacketView<Write>)| {
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
                    let proc_inp = |(pos, buf): (Address, BoundPacketView<Read>)| {
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

        Self {
            read_io,
            write_io,
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
    fn send_io(&self, param: Address, view: BoundPacketView<Read>) {
        self.thread_state.write_io.send_io(param, view)
    }
}

impl PacketIo<Write, Address> for SampleIo {
    fn send_io(&self, param: Address, view: BoundPacketView<Write>) {
        self.thread_state.read_io.send_io(param, view)
    }
}

impl IoBackend for SampleIo {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.thread_state.backend.acquire(None)
    }
}
