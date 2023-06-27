use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};

use io_uring::{opcode, squeue::Entry, types::Fixed, IoUring, SubmissionQueue};
use parking_lot::Mutex;
use slab::Slab;

use nix::sys::eventfd::{eventfd, EfdFlags};

use core::future::poll_fn;
use core::task::{Poll, Waker};

use mfio::backend::fd::FdWaker;
use mfio::backend::*;
use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{io_err, State};

enum Operation {
    Read(<WrPerm as PacketPerms>::Alloced<'static>),
    Write(<RdPerm as PacketPerms>::Alloced<'static>),
}

trait IntoOp: PacketPerms {
    fn into_op(fd: u32, pos: u64, alloced: Self::Alloced<'static>) -> (Entry, Operation);
}

impl IntoOp for RdPerm {
    fn into_op(fd: u32, pos: u64, alloced: Self::Alloced<'static>) -> (Entry, Operation) {
        let entry = opcode::Write::new(Fixed(fd), alloced.as_ptr(), alloced.len() as u32)
            .offset(pos)
            .build();

        (entry, Operation::Write(alloced))
    }
}

impl IntoOp for WrPerm {
    fn into_op(fd: u32, pos: u64, alloced: Self::Alloced<'static>) -> (Entry, Operation) {
        let entry = opcode::Read::new(Fixed(fd), alloced.as_ptr() as *mut u8, alloced.len() as u32)
            .offset(pos)
            .build();

        (entry, Operation::Read(alloced))
    }
}

struct IoOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, u64>,
    key: usize,
    state: BaseArc<Mutex<IoUringState>>,
}

impl<Perms: IntoOp> IoOpsHandle<Perms> {
    fn new(key: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            key,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, u64>> for IoOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, u64> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, u64> for IoOpsHandle<Perms> {
    extern "C" fn send_input(&self, pos: u64, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();
        let state = &mut *state;

        // TODO: handle size limitations???
        let (ring_entry, ops_entry) = Perms::into_op(self.key as _, pos, packet.alloc());

        state.all_ssub += 1;

        if state.ops.len() + 1 < state.ring_capacity {
            IoUringState::push_op(
                &mut state.ring.submission(),
                &mut state.ops,
                ring_entry,
                ops_entry,
                &mut state.all_sub,
                &mut state.flushed,
            );
        } else {
            state.pending_ops.push_back((ring_entry, ops_entry));
        }
    }
}

pub struct FileWrapper {
    key: usize,
    state: BaseArc<Mutex<IoUringState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, u64>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, u64>>,
}

impl FileWrapper {
    fn new(key: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        let write_io = BaseArc::new(IoOpsHandle::new(key, state.clone()));

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_io = BaseArc::new(IoOpsHandle::new(key, state.clone()));

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        });

        Self {
            key,
            state,
            write_stream,
            read_stream,
        }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let _ = state.files.remove(self.key);

        state
            .ring
            .submitter()
            .register_files_update(self.key as _, &[-1])
            .unwrap();
    }
}

impl PacketIo<RdPerm, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        //*self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        //*self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}

struct IoUringState {
    ring: IoUring,
    event_fd: File,
    files: Slab<File>,
    ops: Slab<Operation>,
    ring_capacity: usize,
    pending_ops: VecDeque<(Entry, Operation)>,
    all_ssub: usize,
    all_sub: usize,
    all_comp: usize,
    flushed: bool,
}

impl IoUringState {
    fn register_file(&mut self, file: File) -> usize {
        let file_fd = file.as_raw_fd();
        let key = self.files.insert(file);

        self.ring
            .submitter()
            .register_files_update(key as u32, &[file_fd])
            .unwrap();

        key
    }

    fn push_op(
        sub: &mut SubmissionQueue<'_>,
        ops: &mut Slab<Operation>,
        ring_entry: Entry,
        ops_entry: Operation,
        all_sub: &mut usize,
        flushed: &mut bool,
    ) {
        let id = ops.insert(ops_entry);
        let ring_entry = ring_entry.user_data(id as u64);

        unsafe {
            sub.push(&ring_entry).unwrap();
        }
        *all_sub += 1;
        *flushed = false;
    }
}

impl Default for IoUringState {
    fn default() -> Self {
        // Default to 256 in-flight ops. Appears to be a good default.
        let ring_capacity = 256;

        let ring = IoUring::builder().build(ring_capacity as u32).unwrap();

        let event_fd = eventfd(0, EfdFlags::all()).unwrap();
        ring.submitter().register_eventfd(event_fd).unwrap();
        let event_fd = unsafe { File::from_raw_fd(event_fd) };

        ring.submitter().register_files(&[-1; 1024]).unwrap();

        Self {
            ring,
            event_fd,
            ops: Slab::with_capacity(ring_capacity),
            files: Default::default(),
            ring_capacity,
            pending_ops: Default::default(),
            all_ssub: 0,
            all_sub: 0,
            all_comp: 0,
            flushed: true,
        }
    }
}

pub struct NativeFs {
    // NOTE: this must be before `state`, because `backend` contains references to data, owned by
    // `state`.
    backend: BackendContainer<DynBackend>,
    state: BaseArc<Mutex<IoUringState>>,
    waker: FdWaker<RawFd>,
}

impl Default for NativeFs {
    fn default() -> Self {
        let mut state = IoUringState::default();

        let wake_fd = eventfd(0, EfdFlags::all()).unwrap();
        set_nonblock(wake_fd).unwrap();
        let wake_read = unsafe { File::from_raw_fd(wake_fd) };
        let wake_key = state.register_file(wake_read);
        let waker = FdWaker::from(BaseArc::new(wake_fd));

        let poll_event = opcode::PollAdd::new(
            Fixed(wake_key as _),
            nix::poll::PollFlags::POLLIN.bits() as _,
        )
        .build()
        .user_data(u64::MAX);

        unsafe {
            state.ring.submission().push(&poll_event).unwrap();
        }
        state.ring.submitter().submit().unwrap();

        let state = BaseArc::new(Mutex::new(state));

        let backend = {
            let state = state.clone();

            async move {
                loop {
                    {
                        let mut state = state.lock();
                        let state = &mut *state;

                        // Drain the eventfd
                        {
                            let mut buf = [0u8; 8];
                            let _ = state.event_fd.read(&mut buf);
                        }

                        let (sub, mut sq, mut cq) = state.ring.split();

                        if !state.flushed {
                            sub.submit_and_wait(0).unwrap();
                            state.flushed = true;
                        }

                        loop {
                            let mut did_work = false;
                            let mut drain_waker = false;

                            for entry in &mut cq {
                                did_work = true;

                                let user_data = entry.user_data();

                                if user_data == u64::MAX {
                                    drain_waker = true;
                                    continue;
                                }

                                state.all_comp += 1;

                                let op = state.ops.remove(user_data as usize);

                                let res = entry.result();

                                let res = if res < 0 {
                                    Err(std::io::Error::from_raw_os_error(-res))
                                } else {
                                    Ok(res as usize)
                                };

                                match op {
                                    Operation::Read(pkt) => match res {
                                        Ok(read) if read < pkt.len() => {
                                            let (_, right) = pkt.split_at(read);
                                            right.error(io_err(State::Nop));
                                        }
                                        Ok(0) => {
                                            pkt.error(io_err(State::Nop));
                                        }
                                        Err(e) => pkt.error(io_err(e.kind().into())),
                                        _ => (),
                                    },
                                    Operation::Write(pkt) => match res {
                                        Ok(read) if read < pkt.len() => {
                                            let (_, right) = pkt.split_at(read);
                                            right.error(io_err(State::Nop));
                                        }
                                        Ok(0) => {
                                            pkt.error(io_err(State::Nop));
                                        }
                                        Err(e) => pkt.error(io_err(e.kind().into())),
                                        _ => (),
                                    },
                                }
                            }

                            if !state.pending_ops.is_empty() || drain_waker {
                                sq.sync();
                            }

                            if !state.pending_ops.is_empty() {
                                let mut iter = ((state.ops.len() + 1)..state.ring_capacity)
                                    .map(|_| state.pending_ops.pop_front());

                                while let Some(Some((ring_entry, ops_entry))) = iter.next() {
                                    IoUringState::push_op(
                                        &mut sq,
                                        &mut state.ops,
                                        ring_entry,
                                        ops_entry,
                                        &mut state.all_sub,
                                        &mut state.flushed,
                                    );
                                }
                            }

                            if drain_waker {
                                unsafe {
                                    sq.push(&poll_event).unwrap();
                                    state.flushed = false;
                                }

                                // Drain the waker
                                let wake_read = state.files.get_mut(wake_key).unwrap();
                                loop {
                                    let mut buf = [0u8; 64];
                                    match wake_read.read(&mut buf) {
                                        Ok(1..) => {}
                                        _ => break,
                                    }
                                }
                            }

                            // Another flush check, before cq sync so that the most results are
                            // synced up.
                            if !state.flushed {
                                // We must explicitly sync up the sq here, before submitting work
                                sq.sync();
                                sub.submit_and_wait(0).unwrap();
                                state.flushed = true;
                            }

                            if !did_work {
                                break;
                            }

                            // Defer synchronization of cq to the latest point possible, so that
                            // the most results are retrieved.
                            cq.sync();
                        }
                    }

                    let mut signaled = false;

                    poll_fn(|_| {
                        if signaled {
                            Poll::Ready(())
                        } else {
                            signaled = true;
                            Poll::Pending
                        }
                    })
                    .await;
                }
            }
        };

        Self {
            state,
            backend: BackendContainer::new_dyn(backend),
            waker,
        }
    }
}

impl IoBackend for NativeFs {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
        Some((
            self.state.lock().event_fd.as_raw_fd(),
            self.waker.clone().into_waker(),
        ))
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire()
    }
}

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut state = self.state.lock();
        let key = state.register_file(file);
        FileWrapper::new(key, self.state.clone())
    }
}

fn set_nonblock(fd: RawFd) -> Result<(), nix::errno::Errno> {
    use nix::fcntl::*;

    let flags = fcntl(fd, FcntlArg::F_GETFL)?;
    fcntl(
        fd,
        FcntlArg::F_SETFL(OFlag::from_bits_truncate(flags).union(OFlag::O_NONBLOCK)),
    )?;

    Ok(())
}
