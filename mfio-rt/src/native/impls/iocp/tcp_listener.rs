use core::pin::Pin;
use core::task::{Context, Poll};
use futures::stream::Stream;
use mfio::error::State;
use mfio::tarc::BaseArc;
use parking_lot::Mutex;
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::windows::io::{AsRawHandle, AsRawSocket};

use super::{IocpState, Operation, OperationHeader, OperationMode, TcpGetSock, TcpStream, TmpAddr};
use crate::util::{from_io_error, io_err, Key};
use crate::TcpListenerHandle;

use ::windows::Win32::Foundation::HANDLE;
use ::windows::Win32::Networking::WinSock::{
    GetAcceptExSockaddrs, AF_INET, AF_INET6, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKET,
};
use ::windows::Win32::System::IO::OVERLAPPED;

pub struct ListenerInner {
    pub(super) socket: net::TcpListener,
}

impl From<net::TcpListener> for ListenerInner {
    fn from(socket: net::TcpListener) -> Self {
        Self { socket }
    }
}

pub struct TcpListener {
    idx: usize,
    state: BaseArc<Mutex<IocpState>>,
    accept_idx: Option<usize>,
    local_addr: Option<SocketAddr>,
}

impl TcpListener {
    pub(super) fn register_listener(
        state_arc: &BaseArc<Mutex<IocpState>>,
        listener: net::TcpListener,
    ) -> Self {
        let handle = HANDLE(listener.as_raw_socket() as _);
        let state = &mut *state_arc.lock();
        let entry = state.listeners.vacant_entry();
        let key = Key::TcpListener(entry.key());
        let listener = ListenerInner::from(listener);

        log::trace!(
            "Register listener={:?} state={:?}: key={key:?}",
            listener.socket.as_raw_socket(),
            state_arc.as_ptr()
        );

        entry.insert(listener);

        IocpState::register_handle(state.iocp, handle, key.key()).unwrap();

        TcpListener {
            idx: key.idx(),
            state: state_arc.clone(),
            accept_idx: None,
            local_addr: None,
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.listeners.remove(self.idx);

        log::trace!("Dropping {} {}", self.idx, v.socket.as_raw_socket());
    }
}

impl TcpListenerHandle for TcpListener {
    type StreamHandle = TcpStream;

    fn local_addr(&self) -> mfio::error::Result<SocketAddr> {
        let state = self.state.lock();
        let listener = state
            .listeners
            .get(self.idx)
            .ok_or_else(|| io_err(State::NotFound))?;
        listener.socket.local_addr().map_err(from_io_error)
    }
}

impl Stream for TcpListener {
    type Item = (TcpStream, SocketAddr);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // SAFETY: we are not moving out of this future
        let this = unsafe { self.get_unchecked_mut() };

        let state = &mut *this.state.lock();

        loop {
            if let Some(idx) = this.accept_idx {
                if let Some(conn) = state.connections.get_mut(idx) {
                    match conn.res.take() {
                        Some(v) => {
                            let v = v.ok().zip(conn.tmp_addr.take());
                            let _ = state.connections.remove(idx);
                            this.accept_idx = None;
                            if let Some((socket_idx, TmpAddr { addr })) = v {
                                let mut local_addr: *mut SOCKADDR = core::ptr::null_mut();
                                let mut local_sockaddr_length = 0;
                                let mut remote_addr: *mut SOCKADDR = core::ptr::null_mut();
                                let mut remote_sockaddr_length = 0;

                                unsafe {
                                    GetAcceptExSockaddrs(
                                        addr.as_ptr().cast(),
                                        0,
                                        TmpAddr::ADDR_LENGTH as _,
                                        TmpAddr::ADDR_LENGTH as _,
                                        &mut local_addr,
                                        &mut local_sockaddr_length,
                                        &mut remote_addr,
                                        &mut remote_sockaddr_length,
                                    )
                                };

                                // If anything is wrong here, we should panic.
                                let addr = match unsafe { &*remote_addr.cast::<SOCKADDR>() }
                                    .sa_family
                                {
                                    AF_INET => {
                                        let addr = unsafe { &*remote_addr.cast::<SOCKADDR_IN>() };
                                        SocketAddr::V4(SocketAddrV4::new(
                                            unsafe { addr.sin_addr.S_un.S_addr }.into(),
                                            addr.sin_port,
                                        ))
                                    }
                                    AF_INET6 => {
                                        let addr = unsafe { &*remote_addr.cast::<SOCKADDR_IN6>() };
                                        SocketAddr::V6(SocketAddrV6::new(
                                            unsafe { addr.sin6_addr.u.Word }.into(),
                                            addr.sin6_port,
                                            addr.sin6_flowinfo,
                                            unsafe { addr.Anonymous.sin6_scope_id },
                                        ))
                                    }
                                    _ => unreachable!("invalid state reached {addr:?}"),
                                };

                                let stream = TcpStream::new(socket_idx, this.state.clone());

                                return Poll::Ready(Some((stream, addr)));
                            } else {
                                continue;
                            }
                        }
                        None => {
                            conn.waker = Some(cx.waker().clone());
                            return Poll::Pending;
                        }
                    }
                } else {
                    this.accept_idx = None;
                    continue;
                }
            } else {
                if this.local_addr.is_none() {
                    this.local_addr = state
                        .listeners
                        .get(this.idx)
                        .and_then(|v| v.socket.local_addr().ok());
                }

                let handle = HANDLE(
                    state
                        .listeners
                        .get(this.idx)
                        .unwrap()
                        .socket
                        .as_raw_socket() as _,
                );

                // FIXME: If socket is gone, what shall we do here?
                let Some(local_addr) = this.local_addr else {
                    return Poll::Ready(None);
                };

                let Ok(sock) = TcpGetSock::new_for_accept(local_addr, state, cx.waker().clone())
                else {
                    return Poll::Ready(None);
                };

                let idx = state.connections.insert(sock);

                this.accept_idx = Some(idx);
                // The invariant here is that we have an entry within connections - if we didn't, we
                // would have returned in the previous block.
                let conn = state.connections.get_mut(idx).unwrap();

                let in_sock = SOCKET(
                    state
                        .streams
                        .get(conn.socket_idx.unwrap())
                        .unwrap()
                        .socket
                        .as_raw_socket() as _,
                );

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
                    mode: OperationMode::TcpAccept(TcpAccept {
                        in_sock,
                        conn_id: idx,
                        tmp_addr: Default::default(),
                    }),
                };

                match unsafe { state.try_submit_op(operation) } {
                    Ok(()) => {
                        // Go to the next iteration of this loop, checking result.
                        continue;
                    }
                    Err(_) => return Poll::Pending,
                }
            }
        }
    }
}

pub(super) struct TcpAccept {
    pub in_sock: SOCKET,
    pub conn_id: usize,
    pub tmp_addr: TmpAddr,
}
