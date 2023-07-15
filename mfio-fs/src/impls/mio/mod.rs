use std::fs::File;
use std::io::{ErrorKind, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use mio::{unix::SourceFd, Events, Interest, Token};
use parking_lot::Mutex;
use slab::Slab;

use core::future::poll_fn;
use core::task::{Poll, Waker};

use mfio::backend::fd::FdWaker;
use mfio::backend::*;
use mfio::tarc::BaseArc;

use super::StreamHandleConv;

use file::FileInner;
pub use file::FileWrapper;

use stream::StreamInner;
pub use stream::StreamWrapper;

mod file;
mod stream;

const RW_INTERESTS: Interest = Interest::READABLE.add(Interest::WRITABLE);

struct MioState {
    poll: mio::Poll,
    files: Slab<FileInner>,
    streams: Slab<StreamInner>,
    opqueue: Vec<usize>,
}

impl MioState {
    fn try_new() -> std::io::Result<Self> {
        Ok(Self {
            poll: mio::Poll::new()?,
            files: Default::default(),
            streams: Default::default(),
            opqueue: vec![],
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Key {
    File(usize),
    Stream(usize),
}

impl From<usize> for Key {
    fn from(raw: usize) -> Self {
        if raw & 1 != 0 {
            Self::Stream(raw >> 1)
        } else {
            Self::File(raw >> 1)
        }
    }
}

impl Key {
    fn idx(self) -> usize {
        match self {
            Self::File(v) => v,
            Self::Stream(v) => v,
        }
    }

    fn key(self) -> usize {
        match self {
            Self::File(v) => v << 1,
            Self::Stream(v) => (v << 1) | 1,
        }
    }
}

pub struct NativeFs {
    state: BaseArc<Mutex<MioState>>,
    backend: BackendContainer<DynBackend>,
    waker: FdWaker<OwnedFd>,
}

impl NativeFs {
    pub fn try_new() -> std::io::Result<Self> {
        let state = MioState::try_new()?;

        let (wake_read, wake_write) = nix::unistd::pipe()?;

        set_nonblock(wake_read)?;
        set_nonblock(wake_write)?;

        let mut wake_read = unsafe { File::from_raw_fd(wake_read) };
        let wake_write = unsafe { OwnedFd::from_raw_fd(wake_write) };
        let waker = FdWaker::from(wake_write);

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
                let mut events = Events::with_capacity(1024);

                loop {
                    {
                        let state = &mut *state.lock();

                        if let Err(_e) = state.poll.poll(&mut events, Some(Default::default())) {
                            break;
                        }

                        let mut observed_blocking = false;

                        for event in events.iter() {
                            let key = event.token().0;

                            log::trace!("Key: {key:x}");

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
                                }
                            }
                        }

                        // process the operation queue
                        for key in state.opqueue.drain(..) {
                            match Key::from(key) {
                                Key::File(_) => {
                                    unreachable!();
                                }
                                Key::Stream(v) => {
                                    if let Some(stream) = state.streams.get_mut(v) {
                                        stream.on_queue();
                                    }
                                }
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

impl IoBackend for NativeFs {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
        Some((
            self.state.lock().poll.as_raw_fd(),
            self.waker.clone().into_waker(),
        ))
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire(Some(self.waker.flags()))
    }
}

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let fd = file.as_raw_fd();
        set_nonblock(fd).unwrap();

        let state = &mut *self.state.lock();
        let entry = state.files.vacant_entry();
        // 2N mapping, to accomodate for streams
        let key = Key::File(entry.key());
        let mut file = FileInner::from(file);

        log::trace!(
            "Register file={:?} self={:?} state={:?}: key={key:?}",
            file.as_raw_fd(),
            self as *const _,
            self.state.as_ptr()
        );

        // TODO: handle errors
        match state
            .poll
            .registry()
            .register(&mut file, Token(key.key()), RW_INTERESTS)
        {
            // EPERM using epoll means file descriptor is always ready.
            // However, this also comes with a caveat that even though the file always shows up as
            // ready, it does not necessarily work in non-blocking mode.
            #[cfg(target_os = "linux")]
            Err(e) if e.kind() == ErrorKind::PermissionDenied => (),
            Err(e) => panic!("{e}"),
            Ok(_) => (),
        }

        entry.insert(file);

        FileWrapper::new(key.idx(), self.state.clone())
    }

    pub fn register_stream(&self, stream: impl StreamHandleConv) -> StreamWrapper {
        // TODO: make this portable
        let fd = stream.as_raw();
        set_nonblock(fd).unwrap();

        let state = &mut *self.state.lock();
        let entry = state.streams.vacant_entry();
        // 2N mapping, to accomodate for streams
        let key = Key::Stream(entry.key());
        let mut stream = StreamInner::from(stream);

        log::trace!(
            "Register stream={:?} self={:?} state={:?}: key={key:?}",
            stream.as_raw_fd(),
            self as *const _,
            self.state.as_ptr()
        );

        // TODO: handle errors
        match state
            .poll
            .registry()
            .register(&mut stream, Token(key.key()), RW_INTERESTS)
        {
            // EPERM using epoll means file descriptor is always ready.
            // However, this also comes with a caveat that even though the file always shows up as
            // ready, it does not necessarily work in non-blocking mode.
            #[cfg(target_os = "linux")]
            Err(e) if e.kind() == ErrorKind::PermissionDenied => (),
            Err(e) => panic!("{e}"),
            Ok(_) => (),
        }

        entry.insert(stream);

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
