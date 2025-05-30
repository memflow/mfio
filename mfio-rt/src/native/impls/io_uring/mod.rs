use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::net::{self, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use io_uring::{
    opcode,
    squeue::Entry,
    types::{CancelBuilder, Fixed, Timespec},
    IoUring, SubmissionQueue, Submitter,
};
use parking_lot::Mutex;
use slab::Slab;

use nix::sys::eventfd::{eventfd, EfdFlags};
use nix::sys::socket::{AddressFamily, SockaddrStorage};

use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Poll, Waker};

use crate::util::{from_io_error, io_err};

use mfio::backend::fd::FdWakerOwner;
use mfio::backend::*;
use mfio::error::State;
use mfio::io::{Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use crate::util::{DeferredPackets, Key, RawBox};

mod file;
mod tcp_listener;
mod tcp_stream;

pub use file::FileWrapper;
pub use tcp_listener::TcpListener;
pub use tcp_stream::{TcpConnectFuture, TcpStream};

use tcp_listener::ListenerInner;
use tcp_stream::StreamInner;

enum Operation {
    FileRead(MaybeAlloced<WrPerm>, RawBox),
    FileWrite(AllocedOrTransferred<RdPerm>, RawBox),
    StreamRead(usize),
    StreamWrite(usize),
    TcpGetSock(usize),
}

struct TmpAddr {
    domain: AddressFamily,
    // TODO: find a better way to pin this
    addr: Pin<Box<(SockaddrStorage, u32)>>,
}

struct TcpGetSock {
    waker: Option<Waker>,
    res: Option<mfio::error::Result<TcpStream>>,
    fd: Option<OwnedFd>,
    tmp_addr: Option<TmpAddr>,
}

impl From<Waker> for TcpGetSock {
    fn from(waker: Waker) -> Self {
        Self {
            waker: Some(waker),
            res: None,
            fd: None,
            tmp_addr: None,
        }
    }
}

impl Operation {
    pub(crate) fn process(
        self,
        state: &BaseArc<Mutex<IoUringState>>,
        res: std::io::Result<usize>,
        streams: &mut Slab<StreamInner>,
        connections: &mut Slab<TcpGetSock>,
        submitter: &Submitter<'_>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        match self {
            Operation::FileRead(pkt, buf) => match res {
                Ok(read) if (read as u64) < pkt.len() => {
                    let (left, right) = pkt.split_at(read);
                    if let Err(pkt) = left {
                        assert!(!buf.0.is_null());
                        let buf = unsafe { &*buf.0 };
                        deferred_pkts.ok(unsafe { pkt.transfer_data(buf.as_ptr().cast()) });
                    }
                    deferred_pkts.error(right, io_err(State::Nop));
                }
                Ok(0) => {
                    deferred_pkts.error(pkt, io_err(State::Nop));
                }
                Err(e) => deferred_pkts.error(pkt, io_err(e.kind().into())),
                _ => match pkt {
                    Ok(pkt) => {
                        deferred_pkts.ok(pkt);
                    }
                    Err(pkt) => {
                        assert!(!buf.0.is_null());
                        let buf = unsafe { &*buf.0 };
                        deferred_pkts.ok(unsafe { pkt.transfer_data(buf.as_ptr().cast()) });
                    }
                },
            },
            Operation::FileWrite(pkt, _) => match res {
                Ok(read) if (read as u64) < pkt.len() => {
                    let (left, right) = pkt.split_at(read);
                    deferred_pkts.ok(left);
                    right.error(io_err(State::Nop));
                }
                Ok(0) => {
                    deferred_pkts.error(pkt, io_err(State::Nop));
                }
                Err(e) => deferred_pkts.error(pkt, io_err(e.kind().into())),
                _ => {
                    deferred_pkts.ok(pkt);
                }
            },
            // TODO: we may need to protect about races when streams get replaced with same idx
            Operation::StreamRead(idx) => {
                if let Some(stream) = streams.get_mut(idx) {
                    stream.on_read(res, deferred_pkts);
                }
            }
            Operation::StreamWrite(idx) => {
                if let Some(stream) = streams.get_mut(idx) {
                    stream.on_write(res, deferred_pkts);
                }
            }
            Operation::TcpGetSock(idx) => {
                if let Some(connection) = connections.get_mut(idx) {
                    match res {
                        Ok(res) => {
                            let fd = connection
                                .fd
                                .take()
                                .unwrap_or_else(|| unsafe { OwnedFd::from_raw_fd(res as _) })
                                .into_raw_fd();
                            let stream = unsafe { net::TcpStream::from_raw_fd(fd) };
                            let key = IoUringState::register_stream(submitter, streams, stream);
                            let stream = TcpStream::new(key.idx(), state.clone());
                            connection.res = Some(Ok(stream));
                        }
                        Err(e) => {
                            connection.res = Some(Err(from_io_error(e)));
                        }
                    }

                    if let Some(waker) = connection.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }
}

struct IoUringState {
    ring: IoUring,
    event_fd: File,
    files: Slab<OwnedFd>,
    streams: Slab<StreamInner>,
    listeners: Slab<ListenerInner>,
    ops: Slab<Operation>,
    connections: Slab<TcpGetSock>,
    ring_capacity: usize,
    pending_ops: VecDeque<(Entry, Operation)>,
    deferred_pkts: DeferredPackets,
    all_ssub: usize,
    all_sub: usize,
    all_comp: usize,
    flushed: bool,
}

impl Drop for IoUringState {
    fn drop(&mut self) {
        log::trace!("Dropping uring!");
    }
}

struct IoUringPushHandle<'a> {
    sub: SubmissionQueue<'a>,
    ops: &'a mut Slab<Operation>,
    pending_ops: &'a mut VecDeque<(Entry, Operation)>,
    all_sub: &'a mut usize,
    flushed: &'a mut bool,
    ring_capacity: usize,
}

impl<'a> IoUringPushHandle<'a> {
    pub fn push_op(&mut self, ring_entry: Entry, ops_entry: Operation) {
        IoUringState::push_op(
            &mut self.sub,
            self.ops,
            ring_entry,
            ops_entry,
            self.all_sub,
            self.flushed,
        )
    }

    pub fn try_push_op(&mut self, ring_entry: Entry, ops_entry: Operation) {
        // Clear out any pending ops to maintain order
        while self.ops.len() + 1 < self.ring_capacity {
            if let Some((ring_entry, ops_entry)) = self.pending_ops.pop_front() {
                self.push_op(ring_entry, ops_entry);
            } else {
                break;
            }
        }

        if self.ops.len() + 1 < self.ring_capacity {
            self.push_op(ring_entry, ops_entry);
        } else {
            self.pending_ops.push_back((ring_entry, ops_entry));
        }
    }
}

impl IoUringState {
    fn register_fd(submitter: &Submitter<'_>, fd: RawFd, key: Key) {
        submitter
            .register_files_update(key.key() as u32, &[fd])
            .unwrap();
    }

    fn sync_cancel_all(&mut self) {
        // TODO: look into a better way to cancel the operations - it appears that synchronous
        // cancellation may deadlock.
        if let Err(e) = self
            .ring
            .submitter()
            .register_sync_cancel(Some(Timespec::new().sec(1)), CancelBuilder::any().all())
        {
            log::trace!("Cannot cancel all events synchronously ({e}). Likely unsupported.");
        }
    }

    fn register_file(&mut self, file: impl IntoRawFd) -> Key {
        let file = file.into_raw_fd();
        let file = unsafe { OwnedFd::from_raw_fd(file) };
        let file_fd = file.as_raw_fd();
        let key = Key::File(self.files.insert(file));
        Self::register_fd(&self.ring.submitter(), file_fd, key);
        key
    }

    fn register_stream(
        submitter: &Submitter<'_>,
        streams: &mut Slab<StreamInner>,
        stream: std::net::TcpStream,
    ) -> Key {
        let stream_fd = stream.as_raw_fd();
        let key = Key::Stream(streams.insert(stream.into()));
        Self::register_fd(submitter, stream_fd, key);
        key
    }

    fn push_handle(&mut self) -> IoUringPushHandle {
        IoUringPushHandle {
            sub: self.ring.submission(),
            ops: &mut self.ops,
            pending_ops: &mut self.pending_ops,
            all_sub: &mut self.all_sub,
            flushed: &mut self.flushed,
            ring_capacity: self.ring_capacity,
        }
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

        let mut probe = io_uring::Probe::new();
        ring.submitter().register_probe(&mut probe)?;

        {
            use opcode::*;
            // TODO: separate by feature sets
            for opcode in [
                Read::CODE,
                Write::CODE,
                PollAdd::CODE,
                Connect::CODE,
                RecvMsg::CODE,
                Writev::CODE,
                Accept::CODE,
                2,  // IORING_REGISTER_FILES
                3,  // IORING_UNREGISTER_FILES
                4,  // IORING_REGISTER_EVENTFD
                6,  // IORING_REGISTER_FILES_UPDATE
                24, // IORING_REGISTER_SYNC_CANCEL
            ] {
                if !probe.is_supported(opcode) {
                    log::warn!("io_uring opcode {opcode} is not supported");
                    return Err(std::io::ErrorKind::Unsupported.into());
                } else {
                    log::trace!("io_uring opcode {opcode} is supported");
                }
            }
        }

        let event_fd = eventfd(0, EfdFlags::all())?;
        ring.submitter().register_eventfd(event_fd)?;
        let event_fd = unsafe { File::from_raw_fd(event_fd) };

        ring.submitter().register_files(&[-1; 1024])?;

        Ok(Self {
            ring,
            event_fd,
            ops: Slab::with_capacity(ring_capacity),
            files: Default::default(),
            streams: Default::default(),
            listeners: Default::default(),
            connections: Default::default(),
            ring_capacity,
            pending_ops: Default::default(),
            deferred_pkts: Default::default(),
            all_ssub: 0,
            all_sub: 0,
            all_comp: 0,
            flushed: true,
        })
    }
}

pub struct Runtime {
    // NOTE: this must be before `state`, because `backend` contains references to data, owned by
    // `state`.
    backend: BackendContainer,
    state: BaseArc<Mutex<IoUringState>>,
    waker: FdWakerOwner<RawFd>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        {
            let mut state = self.state.lock();
            log::trace!("clear {} pending_ops", state.pending_ops.len());
            state.pending_ops.clear();

            if !state.ops.is_empty() {
                // Clearing this normally is dangerous, because any completions being polled in the
                // backend would lead to a panic. However, here it is safe to do, because dropping
                // `Runtime` implies drop of the backend handle.
                log::trace!("clear {} ops", state.ops.len());
                state.ops.clear();

                state.sync_cancel_all();
            }

            if let Err(e) = state.ring.submitter().register_files_update(0, &[-1; 1024]) {
                log::trace!("Could not deregister files: {e}");
            }
            state.streams.clear();
        }
        log::trace!("Drop native FS {}", self.state.strong_count());
    }
}

impl Runtime {
    pub fn try_new() -> std::io::Result<Self> {
        let mut state = IoUringState::try_new()?;

        let wake_fd = eventfd(0, EfdFlags::all())?;
        let wake_read = unsafe { File::from_raw_fd(wake_fd) };
        let wake_key = state.register_file(wake_read);
        let waker = FdWakerOwner::from(wake_fd);

        let poll_event = opcode::PollAdd::new(
            Fixed(wake_key.key() as _),
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
            let state_arc = state.clone();

            async move {
                let mut old_deferred_pkts = DeferredPackets::default();

                loop {
                    {
                        let mut state = state_arc.lock();
                        let state = &mut *state;

                        // Drain the eventfd
                        {
                            let mut buf = [0u8; 8];
                            let _ = state.event_fd.read(&mut buf);
                        }

                        let (sub, sq, mut cq) = state.ring.split();

                        let mut push_handle = IoUringPushHandle {
                            sub: sq,
                            ops: &mut state.ops,
                            pending_ops: &mut state.pending_ops,
                            all_sub: &mut state.all_sub,
                            flushed: &mut state.flushed,
                            ring_capacity: state.ring_capacity,
                        };

                        // Submit all pending stream ops
                        // TODO: be more efficient and keep track of pending streams.
                        for (key, stream) in state.streams.iter_mut() {
                            stream.on_queue(key, &mut push_handle, &mut state.deferred_pkts);
                        }

                        if !*push_handle.flushed {
                            // We may not need to unconditionally sync this, if no streams
                            // performed any operations. But this needs to be investigated further.
                            push_handle.sub.sync();
                            sub.submit_and_wait(0).unwrap();
                            *push_handle.flushed = true;
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

                                let op = push_handle.ops.remove(user_data as usize);

                                let res = entry.result();

                                let res = if res < 0 {
                                    Err(std::io::Error::from_raw_os_error(-res))
                                } else {
                                    Ok(res as usize)
                                };

                                op.process(
                                    &state_arc,
                                    res,
                                    &mut state.streams,
                                    &mut state.connections,
                                    &sub,
                                    &mut state.deferred_pkts,
                                );
                            }

                            if !push_handle.pending_ops.is_empty() || drain_waker {
                                push_handle.sub.sync();
                            }

                            // Submit all pending stream ops
                            for (key, stream) in state.streams.iter_mut() {
                                stream.on_queue(key, &mut push_handle, &mut state.deferred_pkts);
                            }

                            if !push_handle.pending_ops.is_empty() {
                                let mut iter = ((push_handle.ops.len() + 1)..state.ring_capacity)
                                    .map(|_| push_handle.pending_ops.pop_front());

                                while let Some(Some((ring_entry, ops_entry))) = iter.next() {
                                    IoUringState::push_op(
                                        &mut push_handle.sub,
                                        push_handle.ops,
                                        ring_entry,
                                        ops_entry,
                                        push_handle.all_sub,
                                        push_handle.flushed,
                                    );
                                }
                            }

                            if drain_waker {
                                unsafe {
                                    push_handle.sub.push(&poll_event).unwrap();
                                    *push_handle.flushed = false;
                                }

                                // Drain the waker
                                let wake_read = state.files.get_mut(wake_key.idx()).unwrap();
                                loop {
                                    let mut buf = [0u8; 64];
                                    match nix::unistd::read(wake_read.as_raw_fd(), &mut buf) {
                                        Ok(1..) => {}
                                        _ => break,
                                    }
                                }
                            }

                            // Another flush check, before cq sync so that the most results are
                            // synced up.
                            if !*push_handle.flushed {
                                // We must explicitly sync up the sq here, before submitting work
                                push_handle.sub.sync();
                                sub.submit_and_wait(0).unwrap();
                                *push_handle.flushed = true;
                            }

                            if !did_work {
                                break;
                            }

                            // Defer synchronization of cq to the latest point possible, so that
                            // the most results are retrieved.
                            cq.sync();
                        }

                        core::mem::swap(&mut old_deferred_pkts, &mut state.deferred_pkts);
                    }

                    old_deferred_pkts.flush();

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

    pub fn cancel_all_ops(&self) {
        let state_arc = self.state.clone();
        let p = {
            let state = &mut *self.state.lock();

            state.sync_cancel_all();

            let sub = state.ring.submitter();

            state
                .streams
                .iter_mut()
                .for_each(|(_, v)| v.cancel_all_ops());

            state.ops.drain().for_each(|v| {
                v.process(
                    &state_arc,
                    Err(std::io::ErrorKind::Interrupted.into()),
                    &mut state.streams,
                    &mut state.connections,
                    &sub,
                    &mut state.deferred_pkts,
                )
            });

            state.pending_ops.drain(0..).for_each(|(_, v)| {
                v.process(
                    &state_arc,
                    Err(std::io::ErrorKind::Interrupted.into()),
                    &mut state.streams,
                    &mut state.connections,
                    &sub,
                    &mut state.deferred_pkts,
                )
            });

            core::mem::take(&mut state.deferred_pkts)
        };
        core::mem::drop(p);
    }
}

impl IoBackend for Runtime {
    fn polling_handle(&self) -> Option<PollingHandle> {
        static READ: PollingFlags = PollingFlags::new().read(true);
        Some(PollingHandle {
            handle: self.state.lock().event_fd.as_raw_fd(),
            cur_flags: &READ,
            max_flags: PollingFlags::new().read(true),
            waker: self.waker.clone().into_waker(),
        })
    }

    fn get_backend(&self) -> BackendHandle {
        self.backend.acquire(Some(self.waker.flags()))
    }
}

impl Runtime {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut state = self.state.lock();
        let key = state.register_file(file);
        FileWrapper::new(key.idx(), self.state.clone())
    }

    pub fn register_stream(&self, stream: std::net::TcpStream) -> TcpStream {
        let state = &mut *self.state.lock();
        let key =
            IoUringState::register_stream(&state.ring.submitter(), &mut state.streams, stream);
        TcpStream::new(key.idx(), self.state.clone())
    }

    pub fn register_listener(&self, listener: std::net::TcpListener) -> TcpListener {
        TcpListener::register_listener(&self.state, listener)
    }

    pub fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        &'a self,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        TcpStream::tcp_connect(&self.state, addrs)
    }
}

pub use core::convert::identity as map_options;
