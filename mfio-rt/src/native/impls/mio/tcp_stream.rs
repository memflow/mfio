use std::io;
use std::io::{IoSlice, IoSliceMut};
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, RawFd};

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};
use parking_lot::Mutex;

use mfio::error::State;
use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::unix_extra::StreamBuf;
use super::{BlockTrack, Key, MioState};
use crate::util::{from_io_error, io_err};
use crate::TcpStreamHandle;

use mio::net;

pub struct StreamInner {
    fd: net::TcpStream,
    stream: StreamBuf,
    track: BlockTrack,
    poll_waker: Option<Waker>,
}

impl AsRawFd for StreamInner {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Source for StreamInner {
    // Required methods
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        // TODO: do we need to not do this on error?
        self.track.cur_interests = Some(interests);
        registry.register(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.track.cur_interests = Some(interests);
        registry.reregister(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.track.cur_interests = None;
        registry.deregister(&mut SourceFd(&self.fd.as_raw_fd()))
    }
}

impl From<net::TcpStream> for StreamInner {
    fn from(fd: net::TcpStream) -> Self {
        Self {
            fd,
            stream: StreamBuf::default(),
            track: Default::default(),
            //read: read::<T>,
            //write: write::<T>,
            poll_waker: None,
        }
    }
}

impl StreamInner {
    fn read(mut stream: &net::TcpStream, iov: &mut [IoSliceMut]) -> io::Result<usize> {
        use std::io::Read;
        stream.read_vectored(iov)
    }

    fn write(mut stream: &net::TcpStream, iov: &[IoSlice]) -> io::Result<usize> {
        use std::io::Write;
        stream.write_vectored(iov)
    }

    pub fn update_interests(&mut self, key: usize, registry: &Registry) -> std::io::Result<()> {
        let expected_interests = self.track.expected_interests();

        if self.track.cur_interests != expected_interests {
            if let Some(i) = expected_interests {
                if self.track.cur_interests.is_some() {
                    self.reregister(registry, Token(key), i)?;
                } else {
                    self.register(registry, Token(key), i)?;
                }
            } else {
                self.deregister(registry)?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops file={:?} read={read} write={write} (to read={} to write={})",
            self.fd.as_raw_fd(),
            self.stream.read_ops(),
            self.stream.write_ops()
        );

        if let Some(waker) = self.poll_waker.take() {
            waker.wake();
        }

        if read || !self.track.read_blocked {
            while self.stream.read_ops() > 0 {
                let rd_span =
                    tracing::span!(tracing::Level::TRACE, "read", ops = self.stream.read_ops());
                let _span = rd_span.enter();
                self.track.read_blocked = false;
                let queue = self.stream.read_queue();
                if !queue.is_empty() {
                    let res = Self::read(&self.fd, queue);

                    if res
                        .as_ref()
                        .err()
                        .map(|e| e.kind() != io::ErrorKind::WouldBlock)
                        .unwrap_or(true)
                    {
                        self.stream.on_read(res);
                    } else {
                        tracing::event!(tracing::Level::INFO, "read blocked");
                        self.track.read_blocked = true;
                        break;
                    }
                }
            }
        }

        if write || !self.track.write_blocked {
            while self.stream.write_ops() > 0 {
                let wr_span = tracing::span!(
                    tracing::Level::TRACE,
                    "write",
                    ops = self.stream.write_ops()
                );
                let _span = wr_span.enter();
                self.track.write_blocked = false;
                let queue = self.stream.write_queue();
                if !queue.is_empty() {
                    let res = Self::write(&self.fd, queue);

                    if res
                        .as_ref()
                        .err()
                        .map(|e| e.kind() != io::ErrorKind::WouldBlock)
                        .unwrap_or(true)
                    {
                        self.stream.on_write(res);
                    } else {
                        tracing::event!(tracing::Level::INFO, "write blocked");
                        self.track.write_blocked = true;
                        break;
                    }
                }
            }
        }
    }

    pub fn on_queue(&mut self) {
        self.track.update_queued = false;
        self.do_ops(true, true);
    }
}

trait IntoOp: PacketPerms {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>);
}

impl IntoOp for RdPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_write(pkt);
        // we would normally attempt the operation right here, but that leads to overly high
        // syscall count.
        //stream.do_ops(false, false);
    }
}

impl IntoOp for WrPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_read(pkt);
        // we would normally attempt the operation right here, but that leads to overly high
        // syscall count.
        //stream.do_ops(true, false);
    }
}

struct StreamOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, NoPos>,
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl<Perms: IntoOp> StreamOpsHandle<Perms> {
    fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            idx,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, NoPos>> for StreamOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, NoPos> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, NoPos> for StreamOpsHandle<Perms> {
    extern "C" fn send_input(&self, _: NoPos, packet: BoundPacket<'static, Perms>) {
        let state = &mut *self.state.lock();

        let stream = state.streams.get_mut(self.idx).unwrap();

        Perms::push_op(stream, packet);

        // This will trigger change in interests in the mio loop
        if !stream.track.update_queued {
            stream.track.update_queued = true;
            state.opqueue.push(Key::Stream(self.idx));
        }
    }
}

pub struct TcpStream {
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, NoPos>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, NoPos>>,
}

impl TcpStream {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        let write_io = BaseArc::new(StreamOpsHandle::new(idx, state.clone()));

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_io = BaseArc::new(StreamOpsHandle::new(idx, state.clone()));

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        });

        Self {
            idx,
            state,
            write_stream,
            read_stream,
        }
    }

    pub(super) fn register_stream(
        state_arc: &BaseArc<Mutex<MioState>>,
        stream: net::TcpStream,
    ) -> Self {
        // TODO: make this portable
        let fd = stream.as_raw_fd();
        super::set_nonblock(fd).unwrap();

        let state = &mut *state_arc.lock();
        let entry = state.streams.vacant_entry();
        // 2N mapping, to accomodate for streams
        let key = Key::Stream(entry.key());
        let stream = StreamInner::from(stream);

        log::trace!(
            "Register stream={:?} state={:?}: key={key:?}",
            stream.as_raw_fd(),
            state_arc.as_ptr()
        );

        entry.insert(stream);

        TcpStream::new(key.idx(), state_arc.clone())
    }

    pub(super) fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        backend: &'a BaseArc<Mutex<MioState>>,
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
        let mut stream = state.streams.remove(self.idx);
        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut stream);
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

pub struct TcpConnectFuture<'a, A: ToSocketAddrs + 'a> {
    backend: &'a BaseArc<Mutex<MioState>>,
    addrs: Option<A::Iter>,
    idx: Option<usize>,
}

impl<'a, A: ToSocketAddrs + 'a> Future for TcpConnectFuture<'a, A> {
    type Output = mfio::error::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // SAFETY: we are not moving out of this future
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(idx) = this.idx.take() {
                let state = &mut *this.backend.lock();
                if let Some(stream) = state.streams.get_mut(idx) {
                    if !stream.track.write_blocked {
                        let wrapper = TcpStream::new(idx, this.backend.clone());

                        let ret = match stream.fd.take_error() {
                            Ok(Some(e)) => Err(e),
                            Err(e) => Err(e),
                            Ok(None) => Ok(wrapper),
                        };

                        // We want to continue to the next address if we were not successful
                        if let Ok(ret) = ret {
                            break Poll::Ready(Ok(ret));
                        }
                    } else {
                        if stream
                            .update_interests(Key::Stream(idx).key(), state.poll.registry())
                            .is_err()
                        {
                            let _ = TcpStream::new(idx, this.backend.clone());
                            continue;
                        }
                        stream.poll_waker = Some(cx.waker().clone());
                        this.idx = Some(idx);
                        break Poll::Pending;
                    }
                } else {
                    break Poll::Ready(Err(io_err(State::NotFound)));
                }
            } else if let Some(addr) = this.addrs.as_mut().and_then(|v| v.next()) {
                let stream = net::TcpStream::connect(addr);

                if let Ok(stream) = stream {
                    let mut state = this.backend.lock();

                    let entry = state.streams.vacant_entry();
                    // 2N mapping, to accomodate for streams
                    let key = Key::Stream(entry.key());
                    let mut stream = StreamInner::from(stream);

                    log::trace!(
                        "Connect stream={:?} state={:?}: key={key:?}",
                        stream.as_raw_fd(),
                        this.backend.as_ptr()
                    );

                    // Mark as write blocked so that we can poll for the writability
                    stream.track.write_blocked = true;

                    entry.insert(stream);
                }
            } else {
                break Poll::Ready(Err(io_err(State::Exhausted)));
            }
        }
    }
}
