use std::io;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use io_uring::{
    opcode,
    types::{Fd, Fixed},
};
use parking_lot::Mutex;

use nix::libc::{iovec, msghdr};
use nix::sys::socket::{self, SockaddrLike, SockaddrStorage};

use mfio::error::State;
use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::{
    unix_extra::{new_for_addr, StreamBuf},
    Key,
};
use super::{IoUringPushHandle, IoUringState, Operation, TmpAddr};
use crate::util::{from_io_error, io_err};
use crate::TcpStreamHandle;

use once_cell::sync::Lazy;

static IOV_MAX: Lazy<usize> = Lazy::new(|| {
    nix::unistd::sysconf(nix::unistd::SysconfVar::IOV_MAX)
        .ok()
        .flatten()
        .unwrap_or(1024) as _
});

pub struct StreamInner {
    fd: net::TcpStream,
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

impl From<net::TcpStream> for StreamInner {
    fn from(fd: net::TcpStream) -> Self {
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

pub struct TcpStream {
    idx: usize,
    state: BaseArc<Mutex<IoUringState>>,
    read_stream: PacketStream<'static, WrPerm, NoPos>,
    write_stream: PacketStream<'static, RdPerm, NoPos>,
}

impl TcpStream {
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

    pub(super) fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        backend: &'a BaseArc<Mutex<IoUringState>>,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        TcpConnectFuture {
            backend,
            addrs: addrs.to_socket_addrs().ok(),
            idx: None,
        }
    }
}

impl Drop for TcpStream {
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

impl TcpStreamHandle for TcpStream {
    fn local_addr(&self) -> mfio::error::Result<SocketAddr> {
        let state = self.state.lock();
        let stream = state
            .streams
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        stream.fd.local_addr().map_err(from_io_error)
    }

    fn peer_addr(&self) -> mfio::error::Result<SocketAddr> {
        let state = self.state.lock();
        let stream = state
            .streams
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        stream.fd.peer_addr().map_err(from_io_error)
    }
}

impl PacketIo<RdPerm, NoPos> for TcpStream {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, NoPos>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, NoPos> for TcpStream {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, NoPos>> {
        Some(self.read_stream.new_packet_id())
    }
}

pub struct TcpConnectFuture<'a, A: ToSocketAddrs + 'a> {
    backend: &'a BaseArc<Mutex<IoUringState>>,
    addrs: Option<A::Iter>,
    idx: Option<usize>,
}

impl<'a, A: ToSocketAddrs + 'a> Future for TcpConnectFuture<'a, A> {
    type Output = mfio::error::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // SAFETY: we are not moving out of this future
        let this = unsafe { self.get_unchecked_mut() };

        let backend = &mut *this.backend.lock();

        if let Some(idx) = this.idx {
            if let Some(conn) = backend.connections.get_mut(idx) {
                match conn.res.take() {
                    Some(Ok(stream)) => {
                        let _ = backend.connections.remove(idx);
                        return Poll::Ready(Ok(stream));
                    }
                    Some(Err(_)) => {
                        conn.waker = Some(cx.waker().clone());
                    }
                    None => {
                        conn.waker = Some(cx.waker().clone());
                        return Poll::Pending;
                    }
                }
            } else {
                return Poll::Ready(Err(io_err(State::NotFound)));
            }
        }

        // Push new op to the ring if we've got an address for it
        loop {
            if let Some(addr) = this.addrs.as_mut().and_then(|v| v.next()) {
                let &mut idx = this
                    .idx
                    .get_or_insert_with(|| backend.connections.insert(cx.waker().clone().into()));

                // The invariant here is that we have an entry within connections - if we didn't, we
                // would have returned in the previous block.
                let conn = backend.connections.get_mut(idx).unwrap();

                let Ok((domain, fd)) = new_for_addr(addr) else {
                    continue;
                };
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };

                let (addr, len) = {
                    let stor = SockaddrStorage::from(addr);
                    conn.tmp_addr = Some(TmpAddr {
                        domain,
                        addr: Box::pin((stor, 0)),
                    });
                    conn.tmp_addr
                        .as_ref()
                        .map(|v| (v.addr.0.as_ptr(), v.addr.0.len()))
                        .unwrap()
                };

                let entry = opcode::Connect::new(Fd(fd.as_raw_fd()), addr, len).build();

                conn.fd = Some(fd);

                backend
                    .push_handle()
                    .try_push_op(entry, Operation::TcpGetSock(idx));

                break Poll::Pending;
            } else {
                if let Some(idx) = this.idx {
                    backend.connections.remove(idx);
                }

                break Poll::Ready(Err(io_err(State::Exhausted)));
            }
        }
    }
}
