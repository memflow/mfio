use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use mfio::error::{Error, State};
use mfio::io::*;
use mfio::mferr;
use mfio::tarc::BaseArc;
use parking_lot::Mutex;
use std::io;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::windows::io::{AsRawHandle, AsRawSocket, RawHandle};

use super::{CSockAddr, IocpState, Operation, OperationHeader, OperationMode, TcpGetSock, WsaOp};

use crate::util::{from_io_error, io_err, stream::StreamBuf, DeferredPackets};
use crate::{Shutdown, TcpStreamHandle};

use ::windows::Win32::Foundation::HANDLE;
use ::windows::Win32::Networking::WinSock::{
    shutdown, WSAGetLastError, SD_BOTH, SD_RECEIVE, SD_SEND, SOCKET, WSABUF, WSAECONNRESET,
    WSAENOTCONN,
};
use ::windows::Win32::System::IO::CancelIoEx;
use ::windows::Win32::System::IO::OVERLAPPED;

pub struct TcpStream {
    idx: usize,
    state: BaseArc<Mutex<IocpState>>,
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.streams.remove(self.idx);

        log::trace!("Dropping {} {}", self.idx, v.socket.as_raw_socket());
    }
}

impl TcpStream {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<IocpState>>) -> Self {
        Self { idx, state }
    }

    pub(super) fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        state: &'a BaseArc<Mutex<IocpState>>,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        TcpConnectFuture {
            state,
            addrs: addrs.to_socket_addrs().ok(),
            idx: None,
        }
    }
}

pub struct StreamInner {
    pub(super) socket: net::TcpStream,
    stream: StreamBuf,
    in_read: bool,
    in_write: usize,
    //recv_msg: msghdr,
    read_queue: Vec<BoundPacketView<Write>>,
    write_queue: Vec<BoundPacketView<Read>>,
}

impl Drop for StreamInner {
    fn drop(&mut self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

impl StreamInner {
    fn shutdown(&self, how: Shutdown) -> Result<(), Error> {
        let ret = unsafe {
            shutdown(
                SOCKET(self.socket.as_raw_socket() as _),
                match how {
                    Shutdown::Read => SD_RECEIVE,
                    Shutdown::Write => SD_SEND,
                    Shutdown::Both => SD_BOTH,
                },
            )
        };
        if ret != 0 {
            match unsafe { WSAGetLastError() } {
                WSAECONNRESET => Ok(()),
                v => {
                    log::error!("Unable to shutdown stream: {ret} {v:?}");
                    Err(mferr!(500, Io, Other, Network))
                }
            }
        } else {
            Ok(())
        }
    }
}

impl From<net::TcpStream> for StreamInner {
    fn from(socket: net::TcpStream) -> Self {
        Self {
            socket,
            stream: StreamBuf::default(),
            in_read: false,
            in_write: 0,
            //recv_msg: empty_msg(),
            read_queue: Default::default(),
            write_queue: Default::default(),
        }
    }
}

unsafe impl Send for StreamInner {}
unsafe impl Sync for StreamInner {}

impl TcpStreamHandle for TcpStream {
    fn local_addr(&self) -> Result<SocketAddr, Error> {
        let state = self.state.lock();
        let stream = state
            .streams
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        stream.socket.local_addr().map_err(from_io_error)
    }

    fn peer_addr(&self) -> Result<SocketAddr, Error> {
        let state = self.state.lock();
        let stream = state
            .streams
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        stream.socket.peer_addr().map_err(from_io_error)
    }

    fn shutdown(&self, how: Shutdown) -> Result<(), Error> {
        let state = self.state.lock();
        let stream = state
            .streams
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        stream.shutdown(how)
    }
}

impl StreamInner {
    pub fn on_read(&mut self, res: io::Result<usize>, deferred_pkts: &mut DeferredPackets) {
        log::debug!("On read {res:?} {}", self.stream.read_ops());
        self.in_read = false;
        self.stream.on_read(res, Some(deferred_pkts))
    }

    pub fn on_write(&mut self, res: io::Result<usize>, deferred_pkts: &mut DeferredPackets) {
        log::debug!("On write {res:?} {}", self.stream.read_ops());
        self.in_write -= 1;
        self.stream.on_write(res, Some(deferred_pkts))
    }

    #[tracing::instrument(skip(self, ops, deferred_pkts, event))]
    pub(super) fn on_queue(
        &mut self,
        idx: usize,
        ops: &mut Vec<Operation>,
        deferred_pkts: &mut DeferredPackets,
        event: RawHandle,
    ) {
        log::trace!(
            "Do ops file={:?} (to read={} to write={})",
            self.socket.as_raw_socket(),
            self.stream.read_ops(),
            self.stream.write_ops()
        );

        if (!self.read_queue.is_empty() || self.stream.read_ops() > 0) && !self.in_read {
            let rd_span =
                tracing::span!(tracing::Level::TRACE, "read", ops = self.stream.read_ops());
            let _span = rd_span.enter();
            for op in self.read_queue.drain(..) {
                self.stream.queue_read(op, Some(deferred_pkts));
            }
            let queue = self.stream.read_queue();
            if !queue.is_empty() {
                self.in_read = true;

                let hdr = OperationHeader {
                    overlapped: OVERLAPPED {
                        hEvent: HANDLE(event as _),
                        ..Default::default()
                    },
                    idx: !0,
                    handle: HANDLE(self.socket.as_raw_socket() as _),
                };

                let operation = Operation {
                    header: hdr.into(),
                    mode: OperationMode::StreamRead(WsaOp {
                        bufs: queue as *const [_] as *const [WSABUF],
                        transferred: 0,
                        flags: 0,
                        sock_idx: idx,
                    }),
                };

                ops.push(operation);
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
                self.stream.queue_write(op, Some(deferred_pkts));
            }
            let queue = self.stream.write_queue();
            if !queue.is_empty() {
                let hdr = OperationHeader {
                    overlapped: OVERLAPPED {
                        hEvent: HANDLE(event as _),
                        ..Default::default()
                    },
                    idx: !0,
                    handle: HANDLE(self.socket.as_raw_socket() as _),
                };

                let operation = Operation {
                    header: hdr.into(),
                    mode: OperationMode::StreamWrite(WsaOp {
                        bufs: (&queue[..]) as *const [_] as *const [WSABUF],
                        transferred: 0,
                        flags: 0,
                        sock_idx: idx,
                    }),
                };

                ops.push(operation);
                self.in_write += 1;
            }
        }
    }

    pub fn cancel_all_ops(&mut self) {
        let _ = unsafe { CancelIoEx(HANDLE(self.socket.as_raw_socket() as _), None) };
        self.stream
            .on_read(Err(io::ErrorKind::Interrupted.into()), None)
    }
}

trait IntoOp: PacketPerms {
    fn push_op(
        stream: &mut StreamInner,
        pkt: BoundPacketView<Self>,
        deferred_pkts: &mut DeferredPackets,
    );
}

impl IntoOp for Read {
    fn push_op(
        stream: &mut StreamInner,
        pkt: BoundPacketView<Self>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        if stream.in_write == 0 {
            stream.stream.queue_write(pkt, Some(deferred_pkts));
        } else {
            stream.write_queue.push(pkt);
        }
    }
}

impl IntoOp for Write {
    fn push_op(
        stream: &mut StreamInner,
        pkt: BoundPacketView<Self>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        if !stream.in_read {
            stream.stream.queue_read(pkt, Some(deferred_pkts));
        } else {
            stream.read_queue.push(pkt);
        }
    }
}

impl<Perms: IntoOp> PacketIo<Perms, NoPos> for TcpStream {
    fn send_io(&self, _: NoPos, packet: BoundPacketView<Perms>) {
        log::debug!("Send io {}", packet.len());

        let mut state = self.state.lock();
        let state = &mut *state;

        let stream = state.streams.get_mut(self.idx).unwrap();

        Perms::push_op(stream, packet, &mut state.deferred_pkts);
    }
}

pub struct TcpConnectFuture<'a, A: ToSocketAddrs + 'a> {
    state: &'a BaseArc<Mutex<IocpState>>,
    addrs: Option<A::Iter>,
    idx: Option<usize>,
}

impl<'a, A: ToSocketAddrs + 'a> Future for TcpConnectFuture<'a, A> {
    type Output = mfio::error::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // SAFETY: we are not moving out of this future
        let this = unsafe { self.get_unchecked_mut() };

        let state = &mut *this.state.lock();

        loop {
            if let Some(idx) = this.idx {
                if let Some(conn) = state.connections.get_mut(idx) {
                    match conn.res.take() {
                        Some(Ok(socket_idx)) => {
                            let _ = state.connections.remove(idx);
                            let stream = TcpStream::new(socket_idx, this.state.clone());
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

            // TODO: do not recreate the socket if next ip address type matches current one.
            if let Some(idx) = this.idx.take() {
                state.connections.remove(idx);
            }

            if let Some(addr) = this.addrs.as_mut().and_then(|v| v.next()) {
                // TODO: propagate this error
                let Ok(connection) = TcpGetSock::new_for_connect(addr, state, cx.waker().clone())
                else {
                    continue;
                };

                let handle = HANDLE(
                    state
                        .streams
                        .get(connection.socket_idx.unwrap())
                        .as_ref()
                        .unwrap()
                        .socket
                        .as_raw_socket() as _,
                );

                let &mut idx = this
                    .idx
                    .get_or_insert_with(|| state.connections.insert(connection));

                let hdr = OperationHeader {
                    overlapped: OVERLAPPED {
                        hEvent: HANDLE(state.event.as_raw_handle() as _),
                        ..Default::default()
                    },
                    idx: !0,
                    handle,
                };

                let operation = Operation {
                    header: hdr.into(),
                    mode: OperationMode::TcpConnect(TcpConnect {
                        conn_id: idx,
                        addr: addr.into(),
                    }),
                };

                match unsafe { state.try_submit_op(operation) } {
                    Ok(()) => {
                        // Go to the next iteration of this loop, checking result.
                        continue;
                    }
                    Err(_) => return Poll::Pending,
                }
            } else {
                return Poll::Ready(Err(io_err(State::Exhausted)));
            }
        }
    }
}

pub(crate) struct TcpConnect {
    pub addr: CSockAddr,
    pub conn_id: usize,
}
