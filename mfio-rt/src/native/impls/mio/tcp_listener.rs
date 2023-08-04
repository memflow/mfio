use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};

use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};
use parking_lot::Mutex;

use mfio::error::State;
use mfio::tarc::BaseArc;

use super::TcpStream;
use super::{BlockTrack, Key, MioState};
use crate::util::{from_io_error, io_err};
use crate::TcpListenerHandle;

use futures::Stream;
use mio::net;

pub struct ListenerInner {
    fd: net::TcpListener,
    track: BlockTrack,
    poll_waker: Option<Waker>,
}

impl AsRawFd for ListenerInner {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Source for ListenerInner {
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

impl From<net::TcpListener> for ListenerInner {
    fn from(fd: net::TcpListener) -> Self {
        Self {
            fd,
            track: Default::default(),
            //read: read::<T>,
            //write: write::<T>,
            poll_waker: None,
        }
    }
}

impl ListenerInner {
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
            "Do ops file={:?} read={read} write={write}",
            self.fd.as_raw_fd(),
        );

        if let Some(waker) = self.poll_waker.take() {
            waker.wake();
        }
    }
}

pub struct TcpListener {
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl TcpListener {
    pub(super) fn register_listener(
        state_arc: &BaseArc<Mutex<MioState>>,
        listener: net::TcpListener,
    ) -> Self {
        // TODO: make this portable
        let fd = listener.as_raw_fd();
        super::set_nonblock(fd).unwrap();

        let state = &mut *state_arc.lock();
        let entry = state.listeners.vacant_entry();
        let key = Key::TcpListener(entry.key());
        let listener = ListenerInner::from(listener);

        log::trace!(
            "Register listener={:?} state={:?}: key={key:?}",
            listener.as_raw_fd(),
            state_arc.as_ptr()
        );

        entry.insert(listener);

        TcpListener {
            idx: key.idx(),
            state: state_arc.clone(),
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let mut listener = state.listeners.remove(self.idx);
        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut listener);
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
        let this = unsafe { self.get_unchecked_mut() };
        let mut state = this.state.lock();

        if let Some(inner) = state.listeners.get_mut(this.idx) {
            match inner.fd.accept() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    inner.track.read_blocked = true;
                    inner.poll_waker = Some(cx.waker().clone());
                    state.opqueue.push(Key::TcpListener(this.idx));
                    Poll::Pending
                }
                Ok((stream, addr)) => {
                    log::trace!("Accept {addr} {}", stream.as_raw_fd());
                    // Avoid deadlock
                    core::mem::drop(state);
                    let stream = TcpStream::register_stream(&this.state, stream);
                    Poll::Ready(Some((stream, addr)))
                }
                Err(e) => {
                    log::error!("Polling error: {e}");
                    let mut listener = state.listeners.remove(this.idx);
                    // TODO: what to do on error?
                    let _ = state.poll.registry().deregister(&mut listener);
                    Poll::Ready(None)
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}
