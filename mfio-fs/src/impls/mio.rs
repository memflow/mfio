use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use mio::{event::Source, unix::SourceFd, Events, Interest, Registry, Token};
use parking_lot::Mutex;
use slab::Slab;

use core::future::poll_fn;
use core::mem::MaybeUninit;
use core::task::{Poll, Waker};

use mfio::backend::fd::FdWaker;
use mfio::backend::*;
use mfio::packet::{FastCWaker, Read as RdPerm, Splittable, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{OwnedStreamHandle, RawHandleConv, StreamBorrow, StreamHandleConv};
use crate::util::io_err;
use mfio::error::State;

const RW_INTERESTS: Interest = Interest::READABLE.add(Interest::WRITABLE);

struct ReadOp {
    pkt: MaybeAlloced<'static, WrPerm>,
}

struct WriteOp {
    pkt: AllocedOrTransferred<'static, RdPerm>,
    transferred: Option<(usize, Vec<u8>)>,
}

type SeekFn<Param> = fn(&<Param as IoAt>::Handle, &mut Param, &Param) -> io::Result<()>;

struct FileInner<Param: IoAt> {
    handle: Param::Handle,
    pos: Param,
    read_ops: VecDeque<(Param, ReadOp)>,
    write_ops: VecDeque<(Param, WriteOp)>,
    tmp_buf: Vec<u8>,
    read: fn(&Param::Handle, &mut [u8]) -> io::Result<usize>,
    write: fn(&Param::Handle, &[u8]) -> io::Result<usize>,
    seek: Option<SeekFn<Param>>,
}

impl<Param: IoAt> Source for FileInner<Param> {
    // Required methods
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.register(&mut SourceFd(&self.handle.as_raw()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.reregister(&mut SourceFd(&self.handle.as_raw()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        registry.deregister(&mut SourceFd(&self.handle.as_raw()))
    }
}

impl From<File> for FileInner<u64> {
    fn from(handle: File) -> Self {
        fn read(mut file: &File, buf: &mut [u8]) -> io::Result<usize> {
            file.read(buf)
        }

        fn write(mut file: &File, buf: &[u8]) -> io::Result<usize> {
            file.write(buf)
        }

        fn seek(file: &File, cur_pos: &mut u64, new_pos: &u64) -> io::Result<()> {
            if cur_pos == new_pos {
                Ok(())
            } else {
                match (&mut &*file).seek(SeekFrom::Start(*new_pos)) {
                    Ok(p) if p == *new_pos => {
                        *cur_pos = *new_pos;
                        Ok(())
                    }
                    Ok(_) => Err(std::io::ErrorKind::Other.into()),
                    Err(e) => Err(e),
                }
            }
        }

        Self {
            handle,
            pos: 0,
            read_ops: Default::default(),
            write_ops: Default::default(),
            tmp_buf: vec![],
            read,
            write,
            seek: Some(seek),
        }
    }
}

impl<T: StreamHandleConv> From<T> for FileInner<NoPos> {
    fn from(stream: T) -> Self {
        fn read<T: StreamHandleConv>(
            stream: &OwnedStreamHandle,
            buf: &mut [u8],
        ) -> io::Result<usize> {
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            stream.read(buf)
        }

        fn write<T: StreamHandleConv>(stream: &OwnedStreamHandle, buf: &[u8]) -> io::Result<usize> {
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            stream.write(buf)
        }

        let handle = stream.into_owned();

        Self {
            handle,
            pos: NoPos::new(),
            read_ops: Default::default(),
            write_ops: Default::default(),
            tmp_buf: vec![],
            read: read::<T>,
            write: write::<T>,
            seek: None,
        }
    }
}

const KEYS: usize = 2;

trait IoAt: Sized + Clone {
    type Handle: RawHandleConv<RawHandle = RawFd>;
    const KEY_IDX: usize;

    fn our_key(key: usize) -> bool {
        key % KEYS == Self::KEY_IDX
    }

    fn add_len(&mut self, len: usize);
    fn get_slab_registry(mio: &mut MioState) -> (&mut Slab<FileInner<Self>>, &Registry);
    fn map_key(raw_key: usize) -> usize {
        raw_key * KEYS + Self::KEY_IDX
    }
    fn slab_get(mio: &mut MioState, key: usize) -> Option<&mut FileInner<Self>> {
        if !Self::our_key(key) {
            None
        } else {
            Self::slab_get_unchecked(mio, key)
        }
    }
    fn slab_get_unchecked(mio: &mut MioState, key: usize) -> Option<&mut FileInner<Self>>;
    fn slab_remove(mio: &mut MioState, key: usize) -> FileInner<Self>;
}

impl IoAt for u64 {
    type Handle = File;

    const KEY_IDX: usize = 0;

    fn add_len(&mut self, len: usize) {
        *self += len as u64;
    }

    fn get_slab_registry(mio: &mut MioState) -> (&mut Slab<FileInner<Self>>, &Registry) {
        (&mut mio.files, mio.poll.registry())
    }

    fn slab_get_unchecked(mio: &mut MioState, key: usize) -> Option<&mut FileInner<Self>> {
        mio.files.get_mut((key - Self::KEY_IDX) / KEYS)
    }

    fn slab_remove(mio: &mut MioState, key: usize) -> FileInner<Self> {
        mio.files.remove((key - Self::KEY_IDX) / KEYS)
    }
}

impl IoAt for NoPos {
    type Handle = OwnedStreamHandle;

    const KEY_IDX: usize = 1;

    fn add_len(&mut self, _: usize) {}

    fn get_slab_registry(mio: &mut MioState) -> (&mut Slab<FileInner<Self>>, &Registry) {
        (&mut mio.streams, mio.poll.registry())
    }

    fn slab_get_unchecked(mio: &mut MioState, key: usize) -> Option<&mut FileInner<Self>> {
        mio.streams.get_mut((key - Self::KEY_IDX) / KEYS)
    }

    fn slab_remove(mio: &mut MioState, key: usize) -> FileInner<Self> {
        mio.streams.remove((key - Self::KEY_IDX) / KEYS)
    }
}

impl<Param: IoAt> FileInner<Param> {
    fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops handle={:?} read={read} write={write} (to read={} to write={})",
            self.handle.as_raw(),
            self.read_ops.len(),
            self.write_ops.len()
        );
        if read {
            'outer: while let Some((pos, op)) = self.read_ops.pop_front() {
                if let Some(seek) = self.seek {
                    match seek(&self.handle, &mut self.pos, &pos) {
                        Ok(()) => (),
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            self.read_ops.push_front((pos, op));
                            break 'outer;
                        }
                        Err(e) => {
                            let err = io_err(e.kind().into());

                            match op.pkt {
                                Ok(pkt) => pkt.error(err),
                                Err(pkt) => pkt.error(err),
                            }
                            continue;
                        }
                    }
                }

                let ReadOp { mut pkt } = op;

                loop {
                    let len = pkt.len();

                    let slice = match &mut pkt {
                        Ok(pkt) => {
                            let buf = pkt.as_ptr() as *mut u8;
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            unsafe { core::slice::from_raw_parts_mut(buf, len) }
                        }
                        Err(_) => {
                            if len > self.tmp_buf.len() {
                                self.tmp_buf.reserve(len - self.tmp_buf.len());
                            }
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            unsafe { self.tmp_buf.set_len(len) }
                            &mut self.tmp_buf[..]
                        }
                    };

                    match (self.read)(&self.handle, slice) {
                        Ok(l) => {
                            log::trace!("Read {l}/{}", slice.len());
                            self.pos.add_len(l);
                            if l == len {
                                if let Err(pkt) = pkt {
                                    unsafe { pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast()) };
                                }
                                break;
                            } else if l > 0 {
                                let (a, b) = pkt.split_at(l);
                                if let Err(pkt) = a {
                                    unsafe { pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast()) };
                                }
                                pkt = b;
                            } else {
                                pkt.error(io_err(State::Nop));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            log::trace!("Would Block");
                            self.read_ops.push_front((self.pos.clone(), ReadOp { pkt }));
                            break 'outer;
                        }
                        Err(e) => {
                            log::trace!("Error {e}");
                            pkt.error(io_err(e.kind().into()));
                            break;
                        }
                    }
                }
            }
        }

        if write {
            'outer: while let Some((pos, op)) = self.write_ops.pop_front() {
                if let Some(seek) = self.seek {
                    match seek(&self.handle, &mut self.pos, &pos) {
                        Ok(()) => (),
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            self.write_ops.push_front((pos, op));
                            break 'outer;
                        }
                        Err(e) => {
                            let err = io_err(e.kind().into());

                            match op.pkt {
                                Ok(pkt) => pkt.error(err),
                                Err(pkt) => pkt.error(err),
                            }
                            continue;
                        }
                    }
                }

                let WriteOp {
                    mut pkt,
                    mut transferred,
                } = op;

                loop {
                    let len = pkt.len();

                    let slice = match &mut pkt {
                        Ok(pkt) => {
                            let buf = pkt.as_ptr();
                            unsafe { core::slice::from_raw_parts(buf, len) }
                        }
                        Err(_) => {
                            let (pos, buf) = transferred.as_ref().unwrap();
                            &buf[*pos..]
                        }
                    };

                    match (self.write)(&self.handle, slice) {
                        Ok(l) => {
                            log::trace!("Written {l}/{}", slice.len());
                            self.pos.add_len(l);
                            if let Some((pos, _)) = &mut transferred {
                                *pos += l;
                            }
                            if l == len {
                                break;
                            } else if l > 0 {
                                pkt = pkt.split_at(l).1;
                            } else {
                                pkt.error(io_err(State::Nop));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            log::trace!("WouldBlock");
                            self.write_ops
                                .push_front((self.pos.clone(), WriteOp { pkt, transferred }));
                            break 'outer;
                        }
                        Err(e) => {
                            log::trace!("Error {e}");
                            pkt.error(io_err(e.kind().into()));
                            break;
                        }
                    }
                }
            }
        }
    }
}

trait IntoOp<Param: IoAt>: PacketPerms {
    fn push_op(file: &mut FileInner<Param>, pos: Param, alloced: MaybeAlloced<'static, Self>);
}

impl<Param: IoAt> IntoOp<Param> for RdPerm {
    fn push_op(file: &mut FileInner<Param>, pos: Param, alloced: MaybeAlloced<'static, Self>) {
        let op = match alloced {
            Ok(pkt) => WriteOp {
                pkt: Ok(pkt),
                transferred: None,
            },
            Err(pkt) => {
                let mut new_trans: Vec<MaybeUninit<u8>> = Vec::with_capacity(pkt.len());
                unsafe { new_trans.set_len(pkt.len()) };

                let transferred = unsafe { pkt.transfer_data(new_trans.as_mut_ptr() as *mut ()) };
                // SAFETY: buffer has now been initialized, it is safe to transmute it into [u8].
                let new_trans = unsafe { core::mem::transmute(new_trans) };
                WriteOp {
                    pkt: Err(transferred),
                    transferred: Some((0, new_trans)),
                }
            }
        };

        file.write_ops.push_back((pos, op));

        // If we haven't got any other ops enqueued, then trigger processing!
        if file.write_ops.len() < 2 {
            file.do_ops(false, true);
        }
    }
}

impl<Param: IoAt> IntoOp<Param> for WrPerm {
    fn push_op(file: &mut FileInner<Param>, pos: Param, pkt: MaybeAlloced<'static, Self>) {
        file.read_ops.push_back((pos, ReadOp { pkt }));
        // If we haven't got any other ops enqueued, then trigger processing!
        if file.read_ops.len() < 2 {
            file.do_ops(true, false);
        }
    }
}

struct IoOpsHandle<Perms: IntoOp<Param>, Param: IoAt> {
    handle: PacketIoHandle<'static, Perms, Param>,
    key: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl<Perms: IntoOp<Param>, Param: IoAt> IoOpsHandle<Perms, Param> {
    fn new(key: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            key,
            state,
        }
    }
}

impl<Perms: IntoOp<Param>, Param: IoAt> AsRef<PacketIoHandle<'static, Perms, Param>>
    for IoOpsHandle<Perms, Param>
{
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, Param> {
        &self.handle
    }
}

impl<Perms: IntoOp<Param>, Param: IoAt> PacketIoHandleable<'static, Perms, Param>
    for IoOpsHandle<Perms, Param>
{
    extern "C" fn send_input(&self, pos: Param, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();

        let file = Param::slab_get(&mut state, self.key).unwrap();

        Perms::push_op(file, pos, packet.try_alloc());
    }
}

struct IoWrapper<Param: IoAt> {
    key: usize,
    state: BaseArc<Mutex<MioState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, Param>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, Param>>,
}

impl<Param: IoAt> IoWrapper<Param> {
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

impl<Param: IoAt> Drop for IoWrapper<Param> {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let mut file = Param::slab_remove(&mut state, self.key);
        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut file);
    }
}

impl<Param: IoAt> PacketIo<RdPerm, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, Param>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl<Param: IoAt> PacketIo<WrPerm, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, Param>> {
        Some(self.read_stream.new_packet_id())
    }
}

impl PacketIo<RdPerm, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, u64>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<WrPerm, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, u64>> {
        self.0.try_new_id(cx)
    }
}

impl PacketIo<RdPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, NoPos>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<WrPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, NoPos>> {
        self.0.try_new_id(cx)
    }
}

struct MioState {
    poll: mio::Poll,
    files: Slab<FileInner<u64>>,
    streams: Slab<FileInner<NoPos>>,
}

impl MioState {
    fn try_new() -> std::io::Result<Self> {
        Ok(Self {
            poll: mio::Poll::new()?,
            files: Default::default(),
            streams: Default::default(),
        })
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
                        let mut state = state.lock();

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
                            } else if let Some(file) = u64::slab_get(&mut state, key) {
                                log::trace!(
                                    "readable={} writeable={}",
                                    event.is_readable(),
                                    event.is_writable()
                                );
                                file.do_ops(event.is_readable(), event.is_writable());
                            } else if let Some(stream) = NoPos::slab_get(&mut state, key) {
                                log::trace!(
                                    "readable={} writeable={}",
                                    event.is_readable(),
                                    event.is_writable()
                                );
                                stream.do_ops(event.is_readable(), event.is_writable());
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

pub struct FileWrapper(IoWrapper<u64>);
pub struct StreamWrapper(IoWrapper<NoPos>);

impl NativeFs {
    fn register_io<Param: IoAt, T: Into<FileInner<Param>>>(&self, file: T) -> IoWrapper<Param> {
        let mut state = self.state.lock();
        let (slab, registry) = Param::get_slab_registry(&mut state);
        let entry = slab.vacant_entry();
        let key = Param::map_key(entry.key());
        let mut file = <T as Into<FileInner<_>>>::into(file);

        log::trace!(
            "Register handle={:?} self={:?} state={:?}: key={key}",
            file.handle.as_raw(),
            self as *const _,
            self.state.as_ptr()
        );

        // TODO: handle errors
        match registry.register(&mut file, Token(key), RW_INTERESTS) {
            // EPERM using epoll means file descriptor is always ready.
            // However, this also comes with a caveat that even though the file always shows up as
            // ready, it does not necessarily work in non-blocking mode.
            #[cfg(target_os = "linux")]
            Err(e) if e.kind() == ErrorKind::PermissionDenied => (),
            Err(e) => panic!("{e}"),
            Ok(_) => (),
        }

        entry.insert(file);

        IoWrapper::new(key, self.state.clone())
    }

    pub fn register_file(&self, file: File) -> FileWrapper {
        let fd = file.as_raw_fd();
        set_nonblock(fd).unwrap();

        FileWrapper(self.register_io(file))
    }

    pub fn register_stream(&self, stream: impl StreamHandleConv) -> StreamWrapper {
        // TODO: make this portable
        let fd = stream.as_raw();
        set_nonblock(fd).unwrap();

        StreamWrapper(self.register_io(stream))
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
