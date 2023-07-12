use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use io_uring::{opcode, squeue::Entry, types::Fixed, IoUring, SubmissionQueue};
use parking_lot::Mutex;
use slab::Slab;

use nix::sys::eventfd::{eventfd, EfdFlags};

use core::future::poll_fn;
use core::mem::MaybeUninit;
use core::task::{Poll, Waker};

use mfio::backend::fd::FdWaker;
use mfio::backend::*;
use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{RawHandleConv, StreamBorrow, StreamHandleConv};
use crate::util::io_err;
use mfio::error::State;

#[repr(transparent)]
struct RawBox(*mut [MaybeUninit<u8>]);

impl RawBox {
    fn null() -> Self {
        Self(unsafe { core::mem::MaybeUninit::zeroed().assume_init() })
    }
}

unsafe impl Send for RawBox {}
unsafe impl Sync for RawBox {}

impl Drop for RawBox {
    fn drop(&mut self) {
        if !self.0.is_null() {
            let _ = unsafe { Box::from_raw(self.0) };
        }
    }
}

enum Operation {
    Read(MaybeAlloced<'static, WrPerm>, RawBox),
    Write(AllocedOrTransferred<'static, RdPerm>, RawBox),
}

trait PosMap {
    fn map_pos(self) -> u64;
}

impl PosMap for u64 {
    fn map_pos(self) -> u64 {
        self
    }
}

impl PosMap for NoPos {
    fn map_pos(self) -> u64 {
        !0u64
    }
}

trait IntoOp: PacketPerms {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacket<'static, Self>) -> (Entry, Operation);
}

impl IntoOp for RdPerm {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacket<'static, Self>) -> (Entry, Operation) {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let (buf, raw_box, pkt) = match pkt {
            Ok(pkt) => (pkt.as_ptr(), RawBox::null(), Ok(pkt)),
            Err(pkt) => {
                let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(len);
                unsafe { buf.set_len(len) };
                let mut buf = buf.into_boxed_slice();
                let buf_ptr = buf.as_mut_ptr();
                let buf = Box::into_raw(buf);
                let pkt = unsafe { pkt.transfer_data(buf_ptr as *mut ()) };
                (buf_ptr as *const u8, RawBox(buf), Err(pkt))
            }
        };

        let entry = opcode::Write::new(Fixed(fd), buf, len as u32)
            .offset(pos)
            .build();

        (entry, Operation::Write(pkt, raw_box))
    }
}

impl IntoOp for WrPerm {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacket<'static, Self>) -> (Entry, Operation) {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let (buf, raw_box) = match &pkt {
            Ok(pkt) => (pkt.as_ptr().cast(), RawBox::null()),
            Err(_) => {
                let mut buf = Vec::with_capacity(len);
                unsafe { buf.set_len(len) };
                let mut buf = buf.into_boxed_slice();
                let buf_ptr = buf.as_mut_ptr();
                let buf = Box::into_raw(buf);
                (buf_ptr, RawBox(buf))
            }
        };

        let buf: *mut MaybeUninit<u8> = buf;

        let entry = opcode::Read::new(Fixed(fd), buf.cast(), len as u32)
            .offset(pos)
            .build();

        (entry, Operation::Read(pkt, raw_box))
    }
}

struct IoOpsHandle<Perms: IntoOp, Param: PosMap> {
    handle: PacketIoHandle<'static, Perms, Param>,
    key: usize,
    state: BaseArc<Mutex<IoUringState>>,
}

impl<Perms: IntoOp, Param: PosMap> IoOpsHandle<Perms, Param> {
    fn new(key: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            key,
            state,
        }
    }
}

impl<Perms: IntoOp, Param: PosMap> AsRef<PacketIoHandle<'static, Perms, Param>>
    for IoOpsHandle<Perms, Param>
{
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, Param> {
        &self.handle
    }
}

impl<Perms: IntoOp, Param: PosMap> PacketIoHandleable<'static, Perms, Param>
    for IoOpsHandle<Perms, Param>
{
    extern "C" fn send_input(&self, pos: Param, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();
        let state = &mut *state;

        // TODO: handle size limitations???
        let (ring_entry, ops_entry) = Perms::into_op(self.key as _, pos.map_pos(), packet);

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

pub struct IoWrapper<Param> {
    key: usize,
    state: BaseArc<Mutex<IoUringState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, Param>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, Param>>,
}

impl<Param> IoWrapper<Param> {
    fn new(key: usize, state: BaseArc<Mutex<IoUringState>>) -> Self
    where
        Param: PosMap,
    {
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

impl<Param> Drop for IoWrapper<Param> {
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

impl<Param> PacketIo<RdPerm, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, Param>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl<Param> PacketIo<WrPerm, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, Param>> {
        Some(self.read_stream.new_packet_id())
    }
}

struct IoUringState {
    ring: IoUring,
    event_fd: File,
    files: Slab<OwnedFd>,
    ops: Slab<Operation>,
    ring_capacity: usize,
    pending_ops: VecDeque<(Entry, Operation)>,
    all_ssub: usize,
    all_sub: usize,
    all_comp: usize,
    flushed: bool,
}

impl IoUringState {
    fn register_fd(&mut self, file: impl RawHandleConv<OwnedHandle = OwnedFd>) -> usize {
        let file = file.into_owned();
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

impl IoUringState {
    fn try_new() -> std::io::Result<Self> {
        // Default to 256 in-flight ops. Appears to be a good default.
        let ring_capacity = 256;

        let ring = IoUring::builder().build(ring_capacity as u32)?;

        let event_fd = eventfd(0, EfdFlags::all())?;
        ring.submitter().register_eventfd(event_fd)?;
        let event_fd = unsafe { File::from_raw_fd(event_fd) };

        ring.submitter().register_files(&[-1; 1024])?;

        Ok(Self {
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
        })
    }
}

pub struct NativeFs {
    // NOTE: this must be before `state`, because `backend` contains references to data, owned by
    // `state`.
    backend: BackendContainer<DynBackend>,
    state: BaseArc<Mutex<IoUringState>>,
    waker: FdWaker<RawFd>,
}

impl NativeFs {
    pub fn try_new() -> std::io::Result<Self> {
        let mut state = IoUringState::try_new()?;

        let wake_fd = eventfd(0, EfdFlags::all())?;
        set_nonblock(wake_fd)?;
        let wake_read = unsafe { File::from_raw_fd(wake_fd) };
        let wake_key = state.register_fd(wake_read);
        let waker = FdWaker::from(wake_fd);

        let poll_event = opcode::PollAdd::new(
            Fixed(wake_key as _),
            nix::poll::PollFlags::POLLIN.bits() as _,
        )
        .build()
        .user_data(u64::MAX);

        unsafe {
            state
                .ring
                .submission()
                .push(&poll_event)
                .map_err(|_| std::io::ErrorKind::Other)?;
        }
        state.ring.submitter().submit()?;

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
                                    Operation::Read(pkt, buf) => match res {
                                        Ok(read) if read < pkt.len() => {
                                            let (left, right) = pkt.split_at(read);
                                            if let Err(pkt) = left {
                                                assert!(!buf.0.is_null());
                                                let buf = unsafe { &*buf.0 };
                                                unsafe { pkt.transfer_data(buf.as_ptr().cast()) };
                                            }
                                            right.error(io_err(State::Nop));
                                        }
                                        Ok(0) => {
                                            pkt.error(io_err(State::Nop));
                                        }
                                        Err(e) => pkt.error(io_err(e.kind().into())),
                                        _ => {
                                            if let Err(pkt) = pkt {
                                                assert!(!buf.0.is_null());
                                                let buf = unsafe { &*buf.0 };
                                                unsafe { pkt.transfer_data(buf.as_ptr().cast()) };
                                            }
                                        }
                                    },
                                    Operation::Write(pkt, _) => match res {
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
                                let mut wake_read = unsafe { StreamBorrow::<File>::get(wake_read) };
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

        Ok(Self {
            state,
            backend: BackendContainer::new_dyn(backend),
            waker,
        })
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
        self.backend.acquire(Some(self.waker.flags()))
    }
}

pub type FileWrapper = IoWrapper<u64>;
pub type StreamWrapper = IoWrapper<NoPos>;

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut state = self.state.lock();
        let key = state.register_fd(file);
        IoWrapper::new(key, self.state.clone())
    }

    pub fn register_stream(&self, stream: impl StreamHandleConv) -> StreamWrapper {
        let mut state = self.state.lock();
        let key = state.register_fd(stream);
        IoWrapper::new(key, self.state.clone())
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
