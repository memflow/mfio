use std::os::fd::AsRawFd;

use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;

use io_uring::{opcode, types::Fixed};
use parking_lot::Mutex;

use nix::sys::socket::{AddressFamily, SockaddrLike, SockaddrStorage};

use mfio::error::State;
use mfio::tarc::BaseArc;

use super::super::{unix_extra::set_nonblock, Key};
use super::{IoUringState, Operation, TcpStream, TmpAddr};
use crate::util::{from_io_error, io_err};
use crate::TcpListenerHandle;

use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6};

pub struct ListenerInner {
    fd: net::TcpListener,
}

impl From<net::TcpListener> for ListenerInner {
    fn from(fd: net::TcpListener) -> Self {
        Self { fd }
    }
}

pub struct TcpListener {
    idx: usize,
    state: BaseArc<Mutex<IoUringState>>,
    accept_idx: Option<usize>,
}

impl TcpListener {
    pub(super) fn register_listener(
        state_arc: &BaseArc<Mutex<IoUringState>>,
        listener: net::TcpListener,
    ) -> Self {
        // TODO: make this portable
        let fd = listener.as_raw_fd();
        set_nonblock(fd).unwrap();

        let state = &mut *state_arc.lock();
        let entry = state.listeners.vacant_entry();
        let key = Key::TcpListener(entry.key());
        let listener = ListenerInner::from(listener);

        log::trace!(
            "Register listener={:?} state={:?}: key={key:?}",
            listener.fd.as_raw_fd(),
            state_arc.as_ptr()
        );

        entry.insert(listener);

        IoUringState::register_fd(&state.ring.submitter(), fd, key);

        TcpListener {
            idx: key.idx(),
            state: state_arc.clone(),
            accept_idx: None,
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.listeners.remove(self.idx);

        log::trace!("Dropping {} {}", self.idx, v.fd.as_raw_fd());

        let r = state
            .ring
            .submitter()
            .register_files_update(Key::TcpListener(self.idx).key() as _, &[-1])
            .unwrap();

        log::trace!("{r} {}", self.state.strong_count(),);
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
        listener.fd.local_addr().map_err(from_io_error)
    }
}

impl Stream for TcpListener {
    type Item = (TcpStream, SocketAddr);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // SAFETY: we are not moving out of this future
        let this = unsafe { self.get_unchecked_mut() };

        let backend = &mut *this.state.lock();

        loop {
            if let Some(idx) = this.accept_idx {
                if let Some(conn) = backend.connections.get_mut(idx) {
                    match conn.res.take() {
                        Some(v) => {
                            let v = v.ok().zip(conn.tmp_addr.take());
                            let _ = backend.connections.remove(idx);
                            this.accept_idx = None;
                            if let Some((stream, TmpAddr { domain, addr })) = v {
                                let addr_len = addr.1;
                                let addr = addr.0;

                                if addr_len > addr.len() {
                                    log::error!("Resulting address didn't fit! ({addr_len} vs {}). This shouldn't happen", addr.len());
                                }

                                // If anything is wrong here, we should panic.
                                let addr = match domain {
                                    AddressFamily::Inet => {
                                        let addr = addr.as_sockaddr_in().unwrap();
                                        SocketAddr::V4(SocketAddrV4::new(
                                            addr.ip().into(),
                                            addr.port(),
                                        ))
                                    }
                                    AddressFamily::Inet6 => {
                                        let addr = addr.as_sockaddr_in6().unwrap();
                                        SocketAddr::V6(SocketAddrV6::new(
                                            addr.ip(),
                                            addr.port(),
                                            addr.flowinfo(),
                                            addr.scope_id(),
                                        ))
                                    }
                                    _ => unreachable!("invalid state reached {domain:?}"),
                                };

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
                let idx = backend.connections.insert(cx.waker().clone().into());
                this.accept_idx = Some(idx);
                // The invariant here is that we have an entry within connections - if we didn't, we
                // would have returned in the previous block.
                let conn = backend.connections.get_mut(idx).unwrap();

                let local_addr = {
                    let listener = backend
                        .listeners
                        .get(idx)
                        .ok_or_else(|| io_err(State::NotFound))
                        .unwrap();
                    listener.fd.local_addr().map_err(from_io_error).unwrap()
                };

                let (domain, storage) = match local_addr {
                    SocketAddr::V4(_) => (
                        AddressFamily::Inet,
                        SockaddrStorage::from(SocketAddrV4::new(0.into(), 0)),
                    ),
                    SocketAddr::V6(_) => (
                        AddressFamily::Inet6,
                        SockaddrStorage::from(SocketAddrV6::new(0.into(), 0, 0, 0)),
                    ),
                };

                let len = storage.len();

                conn.tmp_addr = Some(TmpAddr {
                    domain,
                    addr: Box::pin((storage, len)),
                });
                let (addr, _, len_ptr) = conn
                    .tmp_addr
                    .as_mut()
                    .map(|TmpAddr { addr, .. }| {
                        (
                            addr.0.as_ptr() as *mut _,
                            addr.0.len(),
                            &mut addr.1 as *mut u32,
                        )
                    })
                    .unwrap();

                let entry = opcode::Accept::new(
                    Fixed(Key::TcpListener(this.idx).key() as _),
                    addr,
                    len_ptr,
                )
                .build();

                backend
                    .push_handle()
                    .try_push_op(entry, Operation::TcpGetSock(idx));

                break Poll::Pending;
            }
        }
    }
}
