use std::io;
use std::os::fd::{AsRawFd, OwnedFd};

use io_uring::{opcode, types::Fixed};
use parking_lot::Mutex;

use nix::libc::{iovec, msghdr};
use nix::sys::socket;

use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::{unix_extra::StreamBuf, Key};
use super::{IoUringPushHandle, IoUringState, Operation};

use once_cell::sync::Lazy;

static IOV_MAX: Lazy<usize> = Lazy::new(|| {
    nix::unistd::sysconf(nix::unistd::SysconfVar::IOV_MAX)
        .ok()
        .flatten()
        .unwrap_or(1024) as _
});

pub struct StreamInner {
    fd: OwnedFd,
    stream: StreamBuf,
    in_read: bool,
    in_write: usize,
    recv_msg: msghdr,
    read_queue: Vec<BoundPacket<'static, WrPerm>>,
    write_queue: Vec<BoundPacket<'static, RdPerm>>,
}

unsafe impl Send for StreamInner {}
unsafe impl Sync for StreamInner {}

impl Drop for StreamInner {
    fn drop(&mut self) {
        // For some reason we need to do this, because otherwise there is a second or-so delay
        // before the other end receives the shutdown.
        if let Err(e) = socket::shutdown(self.fd.as_raw_fd(), socket::Shutdown::Both) {
            log::warn!("Could not shutdown stream: {e:?}");
        }
    }
}

impl From<OwnedFd> for StreamInner {
    fn from(fd: OwnedFd) -> Self {
        Self {
            fd,
            stream: StreamBuf::default(),
            in_read: false,
            in_write: 0,
            recv_msg: empty_msg(),
            read_queue: Default::default(),
            write_queue: Default::default(),
        }
    }
}

fn empty_msg() -> msghdr {
    msghdr {
        msg_name: core::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: core::ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: core::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    }
}

impl StreamInner {
    pub fn on_read(&mut self, res: io::Result<usize>) {
        self.in_read = false;
        self.stream.on_read(res)
    }

    pub fn on_write(&mut self, res: io::Result<usize>) {
        self.in_write -= 1;
        self.stream.on_write(res)
    }

    #[tracing::instrument(skip(self, push_handle))]
    pub(super) fn on_queue(&mut self, idx: usize, push_handle: &mut IoUringPushHandle) {
        log::trace!(
            "Do ops file={:?} (to read={} to write={})",
            self.fd.as_raw_fd(),
            self.stream.read_ops(),
            self.stream.write_ops()
        );

        if (!self.read_queue.is_empty() || self.stream.read_ops() > 0) && !self.in_read {
            let rd_span =
                tracing::span!(tracing::Level::TRACE, "read", ops = self.stream.read_ops());
            let _span = rd_span.enter();
            for op in self.read_queue.drain(..) {
                self.stream.queue_read(op);
            }
            let queue = self.stream.read_queue();
            if !queue.is_empty() {
                self.in_read = true;
                let msg = &mut self.recv_msg;
                // Limit iov read to IOV_MAX, because we don't want to have the operation fail.
                msg.msg_iovlen = core::cmp::min(queue.len(), *IOV_MAX);
                msg.msg_iov = queue.as_mut_ptr() as *mut iovec;
                let entry = opcode::RecvMsg::new(Fixed(Key::Stream(idx).key() as _), msg).build();
                push_handle.try_push_op(entry, Operation::StreamRead(idx))
            }
        }

        if (!self.write_queue.is_empty() || self.stream.write_ops() > 0) && self.in_write == 0 {
            let wr_span = tracing::span!(
                tracing::Level::TRACE,
                "write",
                ops = self.stream.write_ops()
            );
            let _span = wr_span.enter();
            for op in self.write_queue.drain(..) {
                self.stream.queue_write(op);
            }
            let queue = self.stream.write_queue();
            if !queue.is_empty() {
                for queue in queue.chunks(*IOV_MAX) {
                    self.in_write += 1;
                    let entry = opcode::Writev::new(
                        Fixed(Key::Stream(idx).key() as _),
                        queue.as_ptr() as *mut iovec,
                        queue.len() as _,
                    )
                    .offset(!0u64)
                    .build();
                    push_handle.try_push_op(entry, Operation::StreamWrite(idx))
                }
            }
        }
    }
}

trait IntoOp: PacketPerms {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>);
}

impl IntoOp for RdPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        if stream.in_write == 0 {
            stream.stream.queue_write(pkt);
        } else {
            stream.write_queue.push(pkt);
        }
    }
}

impl IntoOp for WrPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        if !stream.in_read {
            stream.stream.queue_read(pkt);
        } else {
            stream.read_queue.push(pkt);
        }
    }
}

struct IoOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, NoPos>,
    idx: usize,
    state: BaseArc<Mutex<IoUringState>>,
}

impl<Perms: IntoOp> Drop for IoOpsHandle<Perms> {
    fn drop(&mut self) {
        log::trace!(
            "Drop handle {} {}",
            std::any::type_name::<Perms>(),
            self.state.strong_count()
        );
    }
}

impl<Perms: IntoOp> IoOpsHandle<Perms> {
    fn new(idx: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            idx,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, NoPos>> for IoOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, NoPos> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, NoPos> for IoOpsHandle<Perms> {
    extern "C" fn send_input(&self, _: NoPos, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();
        let state = &mut *state;

        let stream = state.streams.get_mut(self.idx).unwrap();

        Perms::push_op(stream, packet);
    }
}

pub struct StreamWrapper {
    idx: usize,
    state: BaseArc<Mutex<IoUringState>>,
    read_stream: PacketStream<'static, WrPerm, NoPos>,
    write_stream: PacketStream<'static, RdPerm, NoPos>,
}

impl StreamWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        let write_io = BaseArc::new(IoOpsHandle::new(idx, state.clone()));

        let write_stream = PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        };

        let read_io = BaseArc::new(IoOpsHandle::new(idx, state.clone()));

        let read_stream = PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        };

        Self {
            idx,
            state,
            write_stream,
            read_stream,
        }
    }
}

impl Drop for StreamWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.streams.remove(self.idx);

        log::trace!("Dropping {} {}", self.idx, v.fd.as_raw_fd());

        let r = state
            .ring
            .submitter()
            .register_files_update(Key::Stream(self.idx).key() as _, &[-1])
            .unwrap();

        log::trace!(
            "{r} {} | {} {}",
            self.state.strong_count(),
            self.read_stream.ctx.strong_count(),
            self.write_stream.ctx.strong_count(),
        );
    }
}

impl PacketIo<RdPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, NoPos>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, NoPos>> {
        Some(self.read_stream.new_packet_id())
    }
}
