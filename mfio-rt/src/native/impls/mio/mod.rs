use std::fs::File;
use std::io::Read;
use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use mio::{unix::SourceFd, Events, Interest, Token};
use parking_lot::{Mutex, RwLock};
use sharded_slab::Slab;

use core::future::poll_fn;
use core::task::Poll;

use mfio::backend::fd::FdWakerOwner;
use mfio::backend::*;
use mfio::tarc::BaseArc;

use super::unix_extra::set_nonblock;
use crate::util::Key;
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
    poll: Mutex<mio::Poll>,
    files: RwLock<Slab<Mutex<FileInner>>>,
    streams: RwLock<Slab<Mutex<StreamInner>>>,
    listeners: Slab<Mutex<ListenerInner>>,
    opqueue: Mutex<Vec<Key>>,
}

impl MioState {
    fn try_new() -> std::io::Result<Self> {
        Ok(Self {
            poll: mio::Poll::new()?.into(),
            files: Default::default(),
            streams: Default::default(),
            listeners: Default::default(),
            opqueue: vec![].into(),
        })
    }
}

pub struct Runtime {
    state: BaseArc<MioState>,
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
        state.poll.lock().registry().register(
            &mut SourceFd(&wake_read.as_raw_fd()),
            Token(usize::MAX),
            Interest::READABLE,
        )?;

        let state = BaseArc::new(state);

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
                        if let Err(_e) = state
                            .poll
                            .lock()
                            .poll(&mut events, Some(Default::default()))
                        {
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
                                        if let Some(file) = state.files.read().get(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            file.lock()
                                                .do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                    Key::Stream(v) => {
                                        if let Some(stream) = state.streams.read().get(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            stream
                                                .lock()
                                                .do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                    Key::TcpListener(v) => {
                                        if let Some(listener) = state.listeners.get(v) {
                                            log::trace!(
                                                "readable={} writeable={}",
                                                event.is_readable(),
                                                event.is_writable()
                                            );
                                            listener
                                                .lock()
                                                .do_ops(event.is_readable(), event.is_writable());
                                        }
                                    }
                                }
                            }
                        }

                        // process the operation queue
                        for key in state.opqueue.lock().drain(..) {
                            interest_update_queue.push(key);
                            match key {
                                Key::File(v) => {
                                    if let Some(file) = state.files.read().get(v) {
                                        file.lock().on_queue();
                                    }
                                }
                                Key::Stream(v) => {
                                    if let Some(stream) = state.streams.read().get(v) {
                                        stream.lock().on_queue();
                                    }
                                }
                                Key::TcpListener(_) => (),
                            }
                        }

                        let poll = state.poll.lock();
                        // Update polling interests for any FDs that did any work.
                        for key in interest_update_queue.drain(..) {
                            let res = match key {
                                Key::File(v) => {
                                    if let Some(file) = state.files.read().get(v) {
                                        file.lock().update_interests(key.key(), poll.registry())
                                    } else {
                                        Ok(())
                                    }
                                }
                                Key::Stream(v) => {
                                    if let Some(stream) = state.streams.read().get(v) {
                                        stream.lock().update_interests(key.key(), poll.registry())
                                    } else {
                                        Ok(())
                                    }
                                }
                                Key::TcpListener(v) => {
                                    if let Some(listener) = state.listeners.get(v) {
                                        listener.lock().update_interests(key.key(), poll.registry())
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

    pub fn cancel_all_ops(&self) {
        self.state
            .files
            .write()
            .unique_iter()
            .for_each(|v| v.lock().cancel_all_ops());
        self.state
            .streams
            .write()
            .unique_iter()
            .for_each(|v| v.lock().cancel_all_ops());
    }
}

impl IoBackend for Runtime {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        static READ: PollingFlags = PollingFlags::new().read(true);
        Some(PollingHandle {
            handle: self.state.poll.lock().as_raw_fd(),
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

        let files = self.state.files.read();
        let entry = files.vacant_entry().unwrap();
        // 2N mapping, to accomodate for streams
        let key = Key::File(entry.key());
        let file = FileInner::from(file);

        log::trace!(
            "Register file={:?} self={:?} state={:?}: key={key:?}",
            file.as_raw_fd(),
            self as *const _,
            self.state.as_ptr()
        );

        entry.insert(file.into());

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

pub use core::convert::identity as map_options;
