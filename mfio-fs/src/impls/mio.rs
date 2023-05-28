use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use mio::{unix::SourceFd, Events, Interest, Token};
use parking_lot::Mutex;
use slab::Slab;

use core::future::poll_fn;
use core::task::{Context, Poll, Waker};

use mfio::backend::fd::FdWaker;
use mfio::backend::*;
use mfio::packet::{Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

const RW_INTERESTS: Interest = Interest::READABLE.add(Interest::WRITABLE);

enum Operation {
    Read(<WrPerm as PacketPerms>::Alloced<'static>),
    Write(<RdPerm as PacketPerms>::Alloced<'static>),
}

struct FileInner {
    file: File,
    pos: u64,
    ops: VecDeque<(u64, Operation)>,
}

impl From<File> for FileInner {
    fn from(file: File) -> Self {
        Self {
            file,
            pos: 0,
            ops: Default::default(),
        }
    }
}

impl FileInner {
    fn do_ops(&mut self) -> bool {
        while let Some((pos, op)) = self.ops.pop_front() {
            if self.pos != pos {
                match self.file.seek(SeekFrom::Start(pos)) {
                    Ok(p) if p == pos => {
                        self.pos = p;
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        self.ops.push_front((pos, op));
                        return true;
                    }
                    _ => {
                        match op {
                            Operation::Read(pkt) => pkt.error(Some(())),
                            Operation::Write(pkt) => pkt.error(Some(())),
                        }
                        continue;
                    }
                }
            }

            match op {
                Operation::Read(mut pkt) => {
                    loop {
                        let len = pkt.len();
                        let buf = pkt.as_ptr() as *mut u8;
                        // SAFETY: assume MaybeUninit<u8> is initialized,
                        // as God intended :upside_down:
                        let slice = unsafe { core::slice::from_raw_parts_mut(buf, len) };
                        match self.file.read(slice) {
                            Ok(l) => {
                                self.pos += l as u64;
                                if l == len {
                                    break;
                                } else if l > 0 {
                                    pkt = pkt.split_at(l).1;
                                    self.pos += l as u64;
                                } else {
                                    pkt.error(Some(()));
                                    break;
                                }
                            }
                            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                                self.ops.push_front((self.pos, Operation::Read(pkt)));
                                return true;
                            }
                            Err(_e) => {
                                pkt.error(Some(()));
                                break;
                            }
                        }
                    }
                }
                Operation::Write(mut pkt) => loop {
                    let len = pkt.len();
                    let buf = pkt.as_ptr();
                    let slice = unsafe { core::slice::from_raw_parts(buf, len) };
                    match self.file.write(slice) {
                        Ok(l) => {
                            self.pos += l as u64;
                            if l == len {
                                break;
                            } else if l > 0 {
                                pkt = pkt.split_at(l).1;
                                self.pos += l as u64;
                            } else {
                                pkt.error(Some(()));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            self.ops.push_front((self.pos, Operation::Write(pkt)));
                            return true;
                        }
                        Err(_e) => {
                            pkt.error(Some(()));
                            break;
                        }
                    }
                },
            }
        }

        false
    }
}

trait IntoOp: PacketPerms {
    fn into_op(alloced: Self::Alloced<'static>) -> Operation;
}

impl IntoOp for RdPerm {
    fn into_op(alloced: Self::Alloced<'static>) -> Operation {
        Operation::Write(alloced)
    }
}

impl IntoOp for WrPerm {
    fn into_op(alloced: Self::Alloced<'static>) -> Operation {
        Operation::Read(alloced)
    }
}

struct IoOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, u64>,
    key: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl<Perms: IntoOp> IoOpsHandle<Perms> {
    fn new(key: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            key,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, u64>> for IoOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, u64> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, u64> for IoOpsHandle<Perms> {
    extern "C" fn send_input(&self, pos: u64, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();

        let file = state.files.get_mut(self.key).unwrap();

        file.ops.push_back((pos, Perms::into_op(packet.alloc())));

        // If we haven't got any other ops enqueued, then trigger processing!
        if file.ops.len() < 2 {
            file.do_ops();
        }
    }
}

pub struct FileWrapper {
    key: usize,
    state: BaseArc<Mutex<MioState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, u64>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, u64>>,
}

impl FileWrapper {
    fn new(key: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        let write_io = BaseArc::new(IoOpsHandle::new(key, state.clone()));

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_io = BaseArc::new(IoOpsHandle::new(key, state.clone()));

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        });

        Self {
            key,
            state,
            write_stream,
            read_stream,
        }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let file = state.files.remove(self.key);

        let fd = file.file.as_raw_fd();
        let mut fd = SourceFd(&fd);

        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut fd);
    }
}

impl PacketIo<RdPerm, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        //*self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, RdPerm, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        //*self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, WrPerm, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}

struct MioState {
    poll: mio::Poll,
    files: Slab<FileInner>,
}

impl Default for MioState {
    fn default() -> Self {
        Self {
            poll: mio::Poll::new().unwrap(),
            files: Default::default(),
        }
    }
}

pub struct NativeFs {
    state: BaseArc<Mutex<MioState>>,
    backend: BackendContainer<DynBackend>,
    waker: FdWaker,
}

impl Default for NativeFs {
    fn default() -> Self {
        let state = MioState::default();

        let (wake_read, wake_write) = nix::unistd::pipe().unwrap();

        set_nonblock(wake_read).unwrap();
        set_nonblock(wake_write).unwrap();

        let mut wake_read = unsafe { File::from_raw_fd(wake_read) };
        let wake_write = unsafe { OwnedFd::from_raw_fd(wake_write) };

        // Register the waker in a special manner
        state
            .poll
            .registry()
            .register(
                &mut SourceFd(&wake_read.as_raw_fd()),
                Token(usize::MAX),
                Interest::READABLE,
            )
            .unwrap();

        let state = BaseArc::new(Mutex::new(state));

        let backend = {
            let state = state.clone();
            async move {
                let mut events = Events::with_capacity(1024);

                loop {
                    {
                        let mut state = state.lock();

                        if let Err(_e) = state.poll.poll(&mut events, Some(Default::default())) {
                            break;
                        }

                        let mut observed_blocking = false;

                        for event in events.iter() {
                            let key = event.token().0;

                            // This will fail with usize::MAX key
                            if let Some(file) = state.files.get_mut(key) {
                                observed_blocking = file.do_ops() || observed_blocking;
                            }
                        }

                        if observed_blocking {
                            // Drain the waker
                            loop {
                                let mut buf = [0u8; 64];
                                match wake_read.read(&mut buf) {
                                    Ok(1..) => {}
                                    _ => break,
                                }
                            }
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

        Self {
            state,
            backend: BackendContainer::new_dyn(backend),
            waker: FdWaker::from(BaseArc::new(wake_write)),
        }
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
        self.backend.acquire()
    }
}

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let fd = file.as_raw_fd();

        set_nonblock(fd).unwrap();

        let mut fd = SourceFd(&fd);

        let mut state = self.state.lock();
        let key = state.files.insert(file.into());

        // TODO: handle errors
        match state
            .poll
            .registry()
            .register(&mut fd, Token(key), RW_INTERESTS)
        {
            // EPERM using epoll means file descriptor is always ready.
            // However, this also comes with a caveat that even though the file always shows up as
            // ready, it does not necessarily work in non-blocking mode.
            #[cfg(target_os = "linux")]
            Err(e) if e.kind() == ErrorKind::PermissionDenied => (),
            Err(e) => panic!("{e}"),
            Ok(_) => (),
        }

        FileWrapper::new(key, self.state.clone())
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
