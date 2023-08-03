use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use io_uring::{
    opcode,
    squeue::Entry,
    types::{CancelBuilder, Fixed, Timespec},
    IoUring, SubmissionQueue,
};
use parking_lot::Mutex;
use slab::Slab;

use nix::sys::eventfd::{eventfd, EfdFlags};

use core::future::poll_fn;
use core::mem::MaybeUninit;
use core::task::Poll;

use crate::util::io_err;

use mfio::backend::fd::FdWakerOwner;
use mfio::backend::*;
use mfio::error::State;
use mfio::packet::{Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{Key, RawHandleConv, StreamBorrow, StreamHandleConv};

mod file;
mod stream;

pub use file::FileWrapper;
pub use stream::StreamWrapper;

use stream::StreamInner;

#[repr(transparent)]
pub struct RawBox(*mut [MaybeUninit<u8>]);

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

pub enum Operation {
    FileRead(MaybeAlloced<'static, WrPerm>, RawBox),
    FileWrite(AllocedOrTransferred<'static, RdPerm>, RawBox),
    StreamRead(usize),
    StreamWrite(usize),
}

impl Operation {
    pub fn process(self, res: std::io::Result<usize>, streams: &mut Slab<StreamInner>) {
        match self {
            Operation::FileRead(pkt, buf) => match res {
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
            Operation::FileWrite(pkt, _) => match res {
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
            // TODO: we may need to protect about races when streams get replaced with same idx
            Operation::StreamRead(idx) => {
                if let Some(stream) = streams.get_mut(idx) {
                    stream.on_read(res);
                }
            }
            Operation::StreamWrite(idx) => {
                if let Some(stream) = streams.get_mut(idx) {
                    stream.on_write(res);
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
    ops: Slab<Operation>,
    ring_capacity: usize,
    pending_ops: VecDeque<(Entry, Operation)>,
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
        if self.ops.len() + 1 < self.ring_capacity {
            self.push_op(ring_entry, ops_entry);
        } else {
            self.pending_ops.push_back((ring_entry, ops_entry));
        }
    }
}

impl IoUringState {
    fn register_file(&mut self, file: impl RawHandleConv<OwnedHandle = OwnedFd>) -> Key {
        let file = file.into_owned();
        let file_fd = file.as_raw_fd();
        let key = Key::File(self.files.insert(file));

        self.ring
            .submitter()
            .register_files_update(key.key() as u32, &[file_fd])
            .unwrap();

        key
    }

    fn register_stream(&mut self, stream: impl RawHandleConv<OwnedHandle = OwnedFd>) -> Key {
        let stream = stream.into_owned();
        let stream_fd = stream.as_raw_fd();
        let key = Key::Stream(self.streams.insert(stream.into()));

        self.ring
            .submitter()
            .register_files_update(key.key() as u32, &[stream_fd])
            .unwrap();

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
            ring_capacity,
            pending_ops: Default::default(),
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
    backend: BackendContainer<DynBackend>,
    state: BaseArc<Mutex<IoUringState>>,
    waker: FdWakerOwner<RawFd>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        {
            let mut state = self.state.lock();
            log::trace!("clear {} pending_ops", state.pending_ops.len());
            state.pending_ops.clear();
            // Clearing this normally is dangerous, because any completions being polled in the
            // backend would lead to a panic. However, here it is safe to do, because dropping
            // `Runtime` implies drop of the backend handle.
            log::trace!("clear {} ops", state.ops.len());
            state.ops.clear();

            if let Err(e) = state
                .ring
                .submitter()
                .register_sync_cancel(Some(Timespec::new().sec(1)), CancelBuilder::any().all())
            {
                log::trace!("Cannot cancel all events synchronously ({e}). Likely unsupported.");
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

                        let (sub, sq, mut cq) = state.ring.split();

                        let mut push_handle = IoUringPushHandle {
                            sub: sq,
                            ops: &mut state.ops,
                            pending_ops: &mut state.pending_ops,
                            all_sub: &mut state.all_sub,
                            flushed: &mut state.flushed,
                            ring_capacity: state.ring_capacity,
                        };

                        let prev_flushed = *push_handle.flushed;

                        // Submit all pending stream ops
                        // TODO: be more efficient and keep track of pending streams.
                        for (key, stream) in state.streams.iter_mut() {
                            stream.on_queue(key, &mut push_handle);
                        }

                        if !*push_handle.flushed {
                            if prev_flushed {
                                push_handle.sub.sync();
                            }
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

                                op.process(res, &mut state.streams);
                            }

                            if !push_handle.pending_ops.is_empty() || drain_waker {
                                push_handle.sub.sync();
                            }

                            // Submit all pending stream ops
                            for (key, stream) in state.streams.iter_mut() {
                                stream.on_queue(key, &mut push_handle);
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

impl IoBackend for Runtime {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        static READ: PollingFlags = PollingFlags::new().read(true);
        Some(PollingHandle {
            handle: self.state.lock().event_fd.as_raw_fd(),
            cur_flags: &READ,
            max_flags: PollingFlags::new().read(true),
            waker: self.waker.clone().into_waker(),
        })
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire(Some(self.waker.flags()))
    }
}

impl Runtime {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut state = self.state.lock();
        let key = state.register_file(file);
        FileWrapper::new(key.idx(), self.state.clone())
    }

    pub fn register_stream(&self, stream: impl StreamHandleConv) -> StreamWrapper {
        let mut state = self.state.lock();
        let key = state.register_stream(stream);
        StreamWrapper::new(key.idx(), self.state.clone())
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
