use std::fs::File;
use std::io::Read;
use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use mio::{unix::SourceFd, Events, Interest, Token};
use parking_lot::Mutex;
use slab::Slab;

use core::future::poll_fn;
use core::task::Poll;

use mfio::backend::fd::FdWakerOwner;
use mfio::backend::*;
use mfio::tarc::BaseArc;

use super::Key;
use tracing::instrument::Instrument;

use file::FileInner;
pub use file::FileWrapper;

use tcp_stream::StreamInner;
pub use tcp_stream::{TcpConnectFuture, TcpStream};

use tcp_listener::ListenerInner;
pub use tcp_listener::TcpListener;

mod file;
mod tcp_listener;
mod tcp_stream;

const RW_INTERESTS: Interest = Interest::READABLE.add(Interest::WRITABLE);

#[derive(Default)]
struct BlockTrack {
    cur_interests: Option<Interest>,
    read_blocked: bool,
    write_blocked: bool,
    update_queued: bool,
}

impl BlockTrack {
    pub fn expected_interests(&self) -> Option<Interest> {
        let mut expected_interests = Some(RW_INTERESTS);

        if !self.read_blocked {
            expected_interests = expected_interests.and_then(|v| v.remove(Interest::READABLE));
        }

        if !self.write_blocked {
            expected_interests = expected_interests.and_then(|v| v.remove(Interest::WRITABLE));
        }

        expected_interests
    }
}

struct MioState {
    poll: mio::Poll,
    files: Slab<FileInner>,
    streams: Slab<StreamInner>,
    listeners: Slab<ListenerInner>,
    opqueue: Vec<Key>,
}

impl MioState {
    fn try_new() -> std::io::Result<Self> {
        Ok(Self {
            poll: mio::Poll::new()?,
            files: Default::default(),
            streams: Default::default(),
            listeners: Default::default(),
            opqueue: vec![],
        })
    }
}

pub struct Runtime {
    state: BaseArc<Mutex<MioState>>,
    backend: BackendContainer<DynBackend>,
    waker: FdWakerOwner<OwnedFd>,
}

impl Runtime {
    pub fn try_new() -> std::io::Result<Self> {
        let state = MioState::try_new()?;

        let (wake_read, wake_write) = nix::unistd::pipe()?;

        set_nonblock(wake_read)?;
        set_nonblock(wake_write)?;

        log::trace!("{wake_read} {wake_write}");

        let mut wake_read = unsafe { File::from_raw_fd(wake_read) };
        let wake_write = unsafe { OwnedFd::from_raw_fd(wake_write) };
        let waker = FdWakerOwner::from(wake_write);

        // Register the waker in a special manner
        state.poll.registry().register(
            &mut SourceFd(&wake_read.as_raw_fd()),
            Token(usize::MAX),
            Interest::READABLE,
        )?;

        let state = BaseArc::new(Mutex::new(state));

        let backend = {
            let state = state.clone();
            let waker = waker.clone();
            async move {
                // We could use a set here, but arguably, computing interests multiple times
                // should be quicker than walking down the set. TODO: verify this
                let mut interest_update_queue = vec![];

                let mut events = Events::with_capacity(1024);

                loop {
                    let ret = async {
                        let state = &mut *state.lock();

                        if let Err(_e) = state.poll.poll(&mut events, Some(Default::default())) {
                            return false;
                        }

                        let mut observed_blocking = false;

                        for event in events.iter() {
                            let key = event.token().0;

                            log::trace!("Key: {key:x}");
                            interest_update_queue.push(Key::from(key));

                            // These will fail with usize::MAX key
                            if key == usize::MAX {
                                observed_blocking = true;
                            } else {
                                match Key::from(key) {
                                    Key::File(v) => {
                                        if let Some(file) = state.files.get_mut(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            file.do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                    Key::Stream(v) => {
                                        if let Some(stream) = state.streams.get_mut(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            stream.do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                    Key::TcpListener(v) => {
                                        if let Some(listener) = state.listeners.get_mut(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            listener
                                                .do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                }
                            }
                        }

                        // process the operation queue
                        for key in state.opqueue.drain(..) {
                            interest_update_queue.push(key);
                            match key {
                                Key::File(v) => {
                                    if let Some(file) = state.files.get_mut(v) {
                                        file.on_queue();
                                    }
                                }
                                Key::Stream(v) => {
                                    if let Some(stream) = state.streams.get_mut(v) {
                                        stream.on_queue();
                                    }
                                }
                                Key::TcpListener(_) => (),
                            }
                        }

                        // Update polling interests for any FDs that did any work.
                        for key in interest_update_queue.drain(..) {
                            let res = match key {
                                Key::File(v) => {
                                    if let Some(file) = state.files.get_mut(v) {
                                        file.update_interests(key.key(), state.poll.registry())
                                    } else {
                                        Ok(())
                                    }
                                }
                                Key::Stream(v) => {
                                    if let Some(stream) = state.streams.get_mut(v) {
                                        stream.update_interests(key.key(), state.poll.registry())
                                    } else {
                                        Ok(())
                                    }
                                }
                                Key::TcpListener(v) => {
                                    if let Some(listener) = state.listeners.get_mut(v) {
                                        listener.update_interests(key.key(), state.poll.registry())
                                    } else {
                                        Ok(())
                                    }
                                }
                            };

                            // TODO: handle errors
                            match res {
                                // EPERM using epoll means file descriptor is always ready.
                                // However, this also comes with a caveat that even though the file always shows up as
                                // ready, it does not necessarily work in non-blocking mode.
                                #[cfg(target_os = "linux")]
                                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => (),
                                Err(e) => panic!("Unable to update interests: {e}"),
                                Ok(_) => (),
                            }
                        }

                        if observed_blocking {
                            log::trace!("Observe block");
                            // Drain the waker
                            loop {
                                let mut buf = [0u8; 64];
                                match wake_read.read(&mut buf) {
                                    Ok(1..) => {}
                                    _ => break,
                                }
                            }
                            // Set the self wake flag here
                            waker.wake_by_ref();
                        }

                        true
                    }
                    .instrument(tracing::span!(tracing::Level::TRACE, "mio poll"))
                    .await;

                    if !ret {
                        break;
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
            handle: self.state.lock().poll.as_raw_fd(),
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
        let fd = file.as_raw_fd();
        set_nonblock(fd).unwrap();

        let state = &mut *self.state.lock();
        let entry = state.files.vacant_entry();
        // 2N mapping, to accomodate for streams
        let key = Key::File(entry.key());
        let file = FileInner::from(file);

        log::trace!(
            "Register file={:?} self={:?} state={:?}: key={key:?}",
            file.as_raw_fd(),
            self as *const _,
            self.state.as_ptr()
        );

        entry.insert(file);

        FileWrapper::new(key.idx(), self.state.clone())
    }

    pub fn register_stream(&self, stream: std::net::TcpStream) -> TcpStream {
        TcpStream::register_stream(&self.state, mio::net::TcpStream::from_std(stream))
    }

    pub fn register_listener(&self, listener: std::net::TcpListener) -> TcpListener {
        TcpListener::register_listener(&self.state, mio::net::TcpListener::from_std(listener))
    }

    pub fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        &'a self,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        TcpStream::tcp_connect(&self.state, addrs)
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
