use std::fs::File;
use std::net::{self, Shutdown, SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use core::future::{pending, Future};
use core::marker::PhantomData;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use futures::Stream;
use parking_lot::{Mutex, RwLock};
use slab::Slab;

use std::io;
#[cfg(all(unix, not(miri)))]
use std::os::unix::fs::FileExt;
#[cfg(all(windows, not(miri)))]
use std::os::windows::fs::FileExt;

use mfio::backend::*;
use mfio::io::*;
use mfio::tarc::BaseArc;

use crate::util::{from_io_error, io_err};
use crate::{TcpListenerHandle, TcpStreamHandle};
use mfio::error::State;

pub trait IoHandle {
    type Param: Send + Sync + 'static;

    fn read_at(io: &Self, buf: &mut [u8], offset: Self::Param) -> io::Result<usize>;
    fn write_at(io: &Self, buf: &[u8], offset: Self::Param) -> io::Result<usize>;
    fn close(&self) {}
}

impl IoHandle for File {
    type Param = u64;

    fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(miri)]
        {
            use io::{Read, Seek, SeekFrom};
            (&*file).seek(SeekFrom::Start(offset))?;
            return (&*file).read(buf);
        }
        #[cfg(not(miri))]
        {
            #[cfg(unix)]
            return file.read_at(buf, offset);
            #[cfg(windows)]
            return file.seek_read(buf, offset);
        }
    }

    fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        #[cfg(miri)]
        {
            use io::{Seek, SeekFrom, Write};
            (&*file).seek(SeekFrom::Start(offset))?;
            (&*file).write(buf)
        }
        #[cfg(not(miri))]
        {
            #[cfg(unix)]
            return file.write_at(buf, offset);
            #[cfg(windows)]
            return file.seek_write(buf, offset);
        }
    }

    #[cfg(all(windows, not(miri)))]
    fn close(&self) {
        use std::os::windows::io::AsRawHandle;
        unsafe {
            let _ = ::windows::Win32::System::IO::CancelIoEx(
                ::windows::Win32::Foundation::HANDLE(self.as_raw_handle() as usize as isize),
                None,
            );
        };
    }
}
impl IoHandle for net::TcpStream {
    type Param = NoPos;

    fn read_at(mut stream: &net::TcpStream, mut buf: &mut [u8], _: NoPos) -> io::Result<usize> {
        use std::io::Read;
        let mut total = 0;

        while !buf.is_empty() {
            let read = stream.read(buf);
            if let Ok(read) = read {
                // Special case for eof
                if read == 0 {
                    break;
                }
                total += read;
                buf = buf.split_at_mut(read).1;
            } else if total != 0 {
                break;
            } else {
                return read;
            }
        }

        Ok(total)
    }

    fn write_at(mut stream: &net::TcpStream, mut buf: &[u8], _: NoPos) -> io::Result<usize> {
        use std::io::Write;
        let mut total = 0;

        while !buf.is_empty() {
            let written = stream.write(buf);
            if let Ok(written) = written {
                // Special case for eof
                if written == 0 {
                    break;
                }
                total += written;
                buf = buf.split_at(written).1;
            } else if total != 0 {
                break;
            } else {
                return written;
            }
        }

        Ok(total)
    }

    fn close(&self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

struct IoInner<Handle: IoHandle> {
    handle: Handle,
    closed: AtomicBool,
}

impl<Handle: IoHandle> IoInner<Handle> {
    fn read_at(&self, buf: &mut [u8], pos: Handle::Param) -> io::Result<usize> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Handle::read_at(&self.handle, buf, pos)
    }

    fn write_at(&self, buf: &[u8], pos: Handle::Param) -> io::Result<usize> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Handle::write_at(&self.handle, buf, pos)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.handle.close();
    }
}

impl From<File> for IoInner<File> {
    fn from(handle: File) -> Self {
        Self {
            handle,
            closed: AtomicBool::new(false),
        }
    }
}

impl From<net::TcpStream> for IoInner<net::TcpStream> {
    fn from(handle: net::TcpStream) -> Self {
        Self {
            handle,
            closed: AtomicBool::new(false),
        }
    }
}

struct IoThreadHandle<Perms: PacketPerms, Param> {
    tx: Sender<(Param, BoundPacketView<Perms>)>,
}

impl<Perms: PacketPerms, Param> IoThreadHandle<Perms, Param> {
    fn new(tx: Sender<(Param, BoundPacketView<Perms>)>) -> Self {
        Self { tx }
    }
}

impl<Perms: PacketPerms, Param> PacketIo<Perms, Param> for IoThreadHandle<Perms, Param> {
    fn send_io(&self, pos: Param, packet: BoundPacketView<Perms>) {
        self.tx.send((pos, packet)).unwrap();
    }
}

struct IoWrapper<Handle: IoHandle> {
    file: ManuallyDrop<BaseArc<IoInner<Handle>>>,
    read_io: ManuallyDrop<BaseArc<IoThreadHandle<Write, Handle::Param>>>,
    write_io: ManuallyDrop<BaseArc<IoThreadHandle<Read, Handle::Param>>>,
    read_thread: Option<(Receiver<()>, JoinHandle<()>)>,
    write_thread: Option<JoinHandle<()>>,
}

// We are accessing the Receivers only in drop, therefore it's safe to mark this as Sync.
unsafe impl<Handle: IoHandle> Sync for IoWrapper<Handle> where IoWrapper<Handle>: Send {}

impl<Handle: IoHandle> Drop for IoWrapper<Handle> {
    fn drop(&mut self) {
        // SAFETY: we are dropping only here and not accessing the value anywhere else.
        unsafe {
            ManuallyDrop::drop(&mut self.read_io);
            ManuallyDrop::drop(&mut self.write_io);
        }

        self.write_thread.take().unwrap().join().unwrap();

        self.file.close();

        unsafe {
            ManuallyDrop::drop(&mut self.file);
        }

        let (rrx, rjoin) = self.read_thread.take().unwrap();
        if rrx.recv_timeout(Duration::from_millis(5000000000)).is_ok() {
            rjoin.join().unwrap();
        } else {
            log::error!(
                "Unable to join read thread in 500 milliseconds! Leaving the thread detached."
            );
        }
    }
}

impl<Handle: IoHandle + Send + Sync + 'static> From<BaseArc<IoInner<Handle>>>
    for IoWrapper<Handle>
{
    fn from(file: BaseArc<IoInner<Handle>>) -> Self {
        let (read_tx, read_rx) = mpsc::channel();
        let read_io = ManuallyDrop::new(BaseArc::new(IoThreadHandle::<Write, Handle::Param>::new(
            read_tx,
        )));

        let (rtx, rrx) = mpsc::channel();
        let read_thread = Some((
            rrx,
            thread::spawn({
                let file = file.clone();
                move || {
                    let mut tmp_buf = vec![];
                    for (pos, buf) in read_rx {
                        let copy_buf = |buf: &mut [MaybeUninit<u8>]| {
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            let buf = unsafe {
                                let ptr = buf.as_mut_ptr();
                                let len = buf.len();
                                core::slice::from_raw_parts_mut(ptr as *mut u8, len)
                            };

                            file.read_at(buf, pos)
                        };

                        if !buf.is_empty() {
                            match buf.try_alloc() {
                                Ok(mut alloced) => match copy_buf(&mut alloced[..]) {
                                    Ok(read) if (read as u64) < alloced.len() => {
                                        let (_, right) = alloced.split_at(read as _);
                                        right.error(io_err(State::Nop));
                                    }
                                    Err(e) => alloced.error(io_err(e.kind().into())),
                                    _ => (),
                                },
                                Err(buf) => {
                                    // TODO: size limit the temp buffer.
                                    if (tmp_buf.len() as u64) < buf.len() {
                                        tmp_buf.reserve(buf.len() as usize - tmp_buf.len());
                                        // SAFETY: assume MaybeUninit<u8> is initialized,
                                        // as God intended :upside_down:
                                        unsafe { tmp_buf.set_len(tmp_buf.capacity()) }
                                    }
                                    match copy_buf(&mut tmp_buf[..(buf.len() as usize)]) {
                                        Ok(read) if (read as u64) < buf.len() => {
                                            let (left, right) = buf.split_at(read as u64);
                                            let _ = unsafe {
                                                left.transfer_data(tmp_buf.as_ptr().cast())
                                            };
                                            right.error(io_err(State::Nop));
                                        }
                                        Err(e) => buf.error(io_err(e.kind().into())),
                                        _ => {
                                            let _ = unsafe {
                                                buf.transfer_data(tmp_buf.as_ptr().cast())
                                            };
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let _ = rtx.send(());
                }
            }),
        ));

        let (write_tx, write_rx) = mpsc::channel();
        let write_io = ManuallyDrop::new(BaseArc::new(IoThreadHandle::new(write_tx)));

        let write_thread = Some(thread::spawn({
            let file = file.clone();
            move || {
                let mut tmp_buf: Vec<MaybeUninit<u8>> = vec![];
                for (pos, buf) in write_rx {
                    match buf.try_alloc() {
                        Ok(alloced) => {
                            // For some reason type inference loses itself
                            let alloced: ReadPacketObj = alloced;
                            match file.write_at(&alloced[..], pos) {
                                Ok(written) if (written as u64) < alloced.len() => {
                                    let (_, right) = alloced.split_at(written as u64);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => alloced.error(io_err(e.kind().into())),
                                _ => (),
                            }
                        }
                        Err(buf) => {
                            // TODO: size limit the temp buffer.
                            if (tmp_buf.len() as u64) < buf.len() {
                                tmp_buf.reserve(tmp_buf.len() - buf.len() as usize);
                                // SAFETY: assume MaybeUninit<u8> is initialized,
                                // as God intended :upside_down:
                                unsafe { tmp_buf.set_len(buf.len() as usize) }
                            }
                            let buf = unsafe { buf.transfer_data(tmp_buf.as_mut_ptr().cast()) };
                            let tmp_buf = unsafe {
                                &*(&tmp_buf[..] as *const [MaybeUninit<u8>] as *const [u8])
                            };
                            match file.write_at(tmp_buf, pos) {
                                Ok(written) if (written as u64) < buf.len() => {
                                    let (_, right) = buf.split_at(written as u64);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => buf.error(io_err(e.kind().into())),
                                _ => (),
                            }
                        }
                    }
                }
            }
        }));

        Self {
            file: ManuallyDrop::new(file),
            read_thread,
            write_thread,
            read_io,
            write_io,
        }
    }
}

impl<Handle: IoHandle> PacketIo<Read, Handle::Param> for IoWrapper<Handle> {
    fn send_io(&self, param: Handle::Param, view: BoundPacketView<Read>) {
        self.write_io.send_io(param, view);
    }
}

impl<Handle: IoHandle> PacketIo<Write, Handle::Param> for IoWrapper<Handle> {
    fn send_io(&self, param: Handle::Param, view: BoundPacketView<Write>) {
        self.read_io.send_io(param, view);
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn send_io(&self, param: u64, view: BoundPacketView<Read>) {
        let store_guard = self.1.read();
        if !store_guard.cleared {
            store_guard.slab.get(self.0).unwrap().send_io(param, view)
        }
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn send_io(&self, param: u64, view: BoundPacketView<Write>) {
        let store_guard = self.1.read();
        if !store_guard.cleared {
            store_guard.slab.get(self.0).unwrap().send_io(param, view)
        }
    }
}

impl PacketIo<Read, NoPos> for TcpStream {
    fn send_io(&self, param: NoPos, view: BoundPacketView<Read>) {
        let store_guard = self.1.read();
        if !store_guard.cleared {
            store_guard.slab.get(self.0).unwrap().send_io(param, view)
        }
    }
}

impl PacketIo<Write, NoPos> for TcpStream {
    fn send_io(&self, param: NoPos, view: BoundPacketView<Write>) {
        let store_guard = self.1.read();
        if !store_guard.cleared {
            store_guard.slab.get(self.0).unwrap().send_io(param, view)
        }
    }
}

struct StoreInner<Handle: IoHandle> {
    slab: Slab<IoWrapper<Handle>>,
    cleared: bool,
}

impl<Handle: IoHandle> StoreInner<Handle> {
    fn clear(&mut self) {
        self.slab.clear();
        self.cleared = true;
    }
}

impl<Handle: IoHandle> Default for StoreInner<Handle> {
    fn default() -> Self {
        Self {
            slab: Default::default(),
            cleared: false,
        }
    }
}

type Store<Handle> = BaseArc<RwLock<StoreInner<Handle>>>;

pub struct Runtime {
    backend: BackendContainer<DynBackend>,
    file_store: Store<File>,
    tcp_store: Store<net::TcpStream>,
}

impl Runtime {
    pub fn try_new() -> Result<Self, std::convert::Infallible> {
        Ok(Self {
            backend: BackendContainer::new_dyn(pending()),
            file_store: Default::default(),
            tcp_store: Default::default(),
        })
    }

    pub fn cancel_all_ops(&self) {
        // TODO: implement proper cancelling that does not involve invalidating all handles.
        self.file_store.write().clear();
        self.tcp_store.write().clear();
    }
}

impl IoBackend for Runtime {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire(None)
    }
}

pub struct FileWrapper(usize, Store<File>);
pub struct TcpStream(usize, Store<net::TcpStream>);

impl TcpStream {
    fn new(stream: net::TcpStream, store: Store<net::TcpStream>) -> Self {
        let mut store_guard = store.write();
        let key = store_guard
            .slab
            .insert(IoWrapper::from(BaseArc::new(IoInner::from(stream))));
        TcpStream(key, store.clone())
    }
}

impl TcpStreamHandle for TcpStream {
    fn local_addr(&self) -> mfio::error::Result<SocketAddr> {
        let store = self.1.read();
        store
            .slab
            .get(self.0)
            .unwrap()
            .file
            .handle
            .local_addr()
            .map_err(from_io_error)
    }

    fn peer_addr(&self) -> mfio::error::Result<SocketAddr> {
        let store = self.1.read();
        store
            .slab
            .get(self.0)
            .unwrap()
            .file
            .handle
            .peer_addr()
            .map_err(from_io_error)
    }
}

struct TcpConnectCtx {
    waker: Option<Waker>,
    res: Option<mfio::error::Result<net::TcpStream>>,
}

pub struct TcpConnectFuture<'a, A> {
    backend: &'a Runtime,
    ctx: BaseArc<Mutex<TcpConnectCtx>>,
    _phantom: PhantomData<A>,
}

impl<'a, A> Future for TcpConnectFuture<'a, A> {
    type Output = mfio::error::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let mut ctx = this.ctx.lock();

        if let Some(ret) = ctx.res.take() {
            Poll::Ready(ret.map(|v| this.backend.register_stream(v)))
        } else {
            ctx.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub struct TcpListener {
    listener: BaseArc<net::TcpListener>,
    rx: flume::r#async::RecvStream<'static, (net::TcpStream, SocketAddr)>,
    store: Store<net::TcpStream>,
}

impl TcpListener {
    fn new(listener: net::TcpListener, store: Store<net::TcpStream>) -> Self {
        let (tx, rx) = flume::bounded(16);
        let listener = BaseArc::from(listener);
        let rx = rx.into_stream();

        thread::spawn({
            let listener = listener.clone();
            move || loop {
                match listener.accept() {
                    Err(e) if e.kind() != io::ErrorKind::ConnectionAborted => break,
                    Ok(v) => {
                        if tx.send(v).is_err() {
                            break;
                        }
                    }
                    _ => (),
                }
            }
        });

        Self {
            listener,
            rx,
            store,
        }
    }
}

impl TcpListenerHandle for TcpListener {
    type StreamHandle = TcpStream;

    fn local_addr(&self) -> mfio::error::Result<SocketAddr> {
        self.listener.local_addr().map_err(from_io_error)
    }
}

impl Stream for TcpListener {
    type Item = (TcpStream, SocketAddr);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let rx = unsafe { Pin::new_unchecked(&mut this.rx) };

        match rx.poll_next(cx) {
            Poll::Ready(v) => {
                Poll::Ready(v.map(|(v, a)| (TcpStream::new(v, this.store.clone()), a)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Runtime {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut store_guard = self.file_store.write();
        let key = store_guard
            .slab
            .insert(IoWrapper::from(BaseArc::new(IoInner::from(file))));
        FileWrapper(key, self.file_store.clone())
    }

    pub fn register_stream(&self, stream: net::TcpStream) -> TcpStream {
        TcpStream::new(stream, self.tcp_store.clone())
    }

    pub fn register_listener(&self, listener: net::TcpListener) -> TcpListener {
        TcpListener::new(listener, self.tcp_store.clone())
    }

    pub fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        &'a self,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        let ctx = if let Ok(addrs) = addrs.to_socket_addrs() {
            let ctx = BaseArc::new(Mutex::new(TcpConnectCtx {
                waker: None,
                res: None,
            }));

            let addrs = addrs.collect::<Vec<_>>();

            // TODO: find out a way to interrupt this
            thread::spawn({
                let ctx = ctx.clone();
                move || {
                    let res = net::TcpStream::connect(&addrs[..]).map_err(from_io_error);

                    let mut ctx = ctx.lock();

                    ctx.res = Some(res);

                    if let Some(waker) = ctx.waker.take() {
                        waker.wake();
                    }
                }
            });

            ctx
        } else {
            BaseArc::new(Mutex::new(TcpConnectCtx {
                waker: None,
                res: Some(Err(io_err(State::Nop))),
            }))
        };

        TcpConnectFuture {
            backend: self,
            ctx,
            _phantom: PhantomData,
        }
    }
}
