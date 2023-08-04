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
use parking_lot::Mutex;

use std::io;
#[cfg(all(unix, not(miri)))]
use std::os::unix::fs::FileExt;
#[cfg(all(windows, not(miri)))]
use std::os::windows::fs::FileExt;

use mfio::backend::*;
use mfio::packet::*;
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
}
impl IoHandle for net::TcpStream {
    type Param = NoPos;

    fn read_at(mut stream: &net::TcpStream, buf: &mut [u8], _: NoPos) -> io::Result<usize> {
        use std::io::Read;
        stream.read(buf)
    }

    fn write_at(mut stream: &net::TcpStream, buf: &[u8], _: NoPos) -> io::Result<usize> {
        use std::io::Write;
        log::debug!("Write {}", buf.len());
        let ret = stream.write(buf);
        log::debug!("Written");
        ret
    }

    fn close(&self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

struct IoInner<Handle: IoHandle> {
    handle: Handle,
    closed: AtomicBool,
    //read_at: fn(&Handle, &mut [u8], Param) -> io::Result<usize>,
    //write_at: fn(&Handle, &[u8], Param) -> io::Result<usize>,
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
    handle: PacketIoHandle<'static, Perms, Param>,
    tx: Sender<(Param, BoundPacket<'static, Perms>)>,
}

impl<Perms: PacketPerms, Param> IoThreadHandle<Perms, Param> {
    fn new(tx: Sender<(Param, BoundPacket<'static, Perms>)>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            tx,
        }
    }
}

impl<Perms: PacketPerms, Param> AsRef<PacketIoHandle<'static, Perms, Param>>
    for IoThreadHandle<Perms, Param>
{
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, Param> {
        &self.handle
    }
}

impl<Perms: PacketPerms, Param> PacketIoHandleable<'static, Perms, Param>
    for IoThreadHandle<Perms, Param>
{
    extern "C" fn send_input(&self, pos: Param, packet: BoundPacket<'static, Perms>) {
        self.tx.send((pos, packet)).unwrap();
    }
}

struct IoWrapper<Handle: IoHandle> {
    file: ManuallyDrop<BaseArc<IoInner<Handle>>>,
    read_stream: ManuallyDrop<BaseArc<PacketStream<'static, Write, Handle::Param>>>,
    write_stream: ManuallyDrop<BaseArc<PacketStream<'static, Read, Handle::Param>>>,
    read_thread: Option<(Receiver<()>, JoinHandle<()>)>,
    write_thread: Option<JoinHandle<()>>,
}

// We are accessing the Receivers only in drop, therefore it's safe to mark this as Sync.
unsafe impl<Handle: IoHandle> Sync for IoWrapper<Handle> where IoWrapper<Handle>: Send {}

impl<Handle: IoHandle> Drop for IoWrapper<Handle> {
    fn drop(&mut self) {
        // SAFETY: we are dropping only here and not accessing the value anywhere else.
        unsafe {
            ManuallyDrop::drop(&mut self.read_stream);
            ManuallyDrop::drop(&mut self.write_stream);
        }

        self.write_thread.take().unwrap().join().unwrap();

        self.file.close();

        unsafe {
            ManuallyDrop::drop(&mut self.file);
        }

        let (rrx, rjoin) = self.read_thread.take().unwrap();
        if rrx.recv_timeout(Duration::from_millis(500)).is_ok() {
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
        let read_io = BaseArc::new(IoThreadHandle::<Write, Handle::Param>::new(read_tx));

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
                                    Ok(read) if read < alloced.len() => {
                                        let (_, right) = alloced.split_at(read);
                                        right.error(io_err(State::Nop));
                                    }
                                    Err(e) => alloced.error(io_err(e.kind().into())),
                                    _ => (),
                                },
                                Err(buf) => {
                                    // TODO: size limit the temp buffer.
                                    if tmp_buf.len() < buf.len() {
                                        tmp_buf.reserve(buf.len() - tmp_buf.len());
                                        // SAFETY: assume MaybeUninit<u8> is initialized,
                                        // as God intended :upside_down:
                                        unsafe { tmp_buf.set_len(tmp_buf.capacity()) }
                                    }
                                    match copy_buf(&mut tmp_buf[..buf.len()]) {
                                        Ok(read) if read < buf.len() => {
                                            let (left, right) = buf.split_at(read);
                                            unsafe { left.transfer_data(tmp_buf.as_ptr().cast()) };
                                            right.error(io_err(State::Nop));
                                        }
                                        Err(e) => buf.error(io_err(e.kind().into())),
                                        _ => {
                                            unsafe { buf.transfer_data(tmp_buf.as_ptr().cast()) };
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
        let write_io = BaseArc::new(IoThreadHandle::new(write_tx));

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
                                Ok(written) if written < alloced.len() => {
                                    let (_, right) = alloced.split_at(written);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => alloced.error(io_err(e.kind().into())),
                                _ => (),
                            }
                        }
                        Err(buf) => {
                            // TODO: size limit the temp buffer.
                            if tmp_buf.len() < buf.len() {
                                tmp_buf.reserve(tmp_buf.len() - buf.len());
                                // SAFETY: assume MaybeUninit<u8> is initialized,
                                // as God intended :upside_down:
                                unsafe { tmp_buf.set_len(buf.len()) }
                            }
                            let buf = unsafe { buf.transfer_data(tmp_buf.as_mut_ptr().cast()) };
                            let tmp_buf = unsafe {
                                &*(&tmp_buf[..] as *const [MaybeUninit<u8>] as *const [u8])
                            };
                            match file.write_at(tmp_buf, pos) {
                                Ok(written) if written < buf.len() => {
                                    let (_, right) = buf.split_at(written);
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

        let write_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        }));

        let read_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        }));

        Self {
            file: ManuallyDrop::new(file),
            read_thread,
            write_thread,
            read_stream,
            write_stream,
        }
    }
}

impl<Handle: IoHandle> PacketIo<Read, Handle::Param> for IoWrapper<Handle> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Read, Handle::Param>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl<Handle: IoHandle> PacketIo<Write, Handle::Param> for IoWrapper<Handle> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Write, Handle::Param>> {
        Some(self.read_stream.new_packet_id())
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, Read, u64>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, Write, u64>> {
        self.0.try_new_id(cx)
    }
}

impl PacketIo<Read, NoPos> for TcpStream {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, Read, NoPos>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<Write, NoPos> for TcpStream {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, Write, NoPos>> {
        self.0.try_new_id(cx)
    }
}

pub struct Runtime {
    backend: BackendContainer<DynBackend>,
}

impl Runtime {
    pub fn try_new() -> Result<Self, std::convert::Infallible> {
        Ok(Self {
            backend: BackendContainer::new_dyn(pending()),
        })
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

pub struct FileWrapper(IoWrapper<File>);
pub struct TcpStream(IoWrapper<net::TcpStream>);

impl From<net::TcpStream> for TcpStream {
    fn from(stream: net::TcpStream) -> Self {
        Self(IoWrapper::from(BaseArc::from(<net::TcpStream as Into<
            IoInner<_>,
        >>::into(stream))))
    }
}

impl TcpStreamHandle for TcpStream {
    fn local_addr(&self) -> mfio::error::Result<SocketAddr> {
        self.0.file.handle.local_addr().map_err(from_io_error)
    }

    fn peer_addr(&self) -> mfio::error::Result<SocketAddr> {
        self.0.file.handle.local_addr().map_err(from_io_error)
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
}

impl From<net::TcpListener> for TcpListener {
    fn from(listener: net::TcpListener) -> Self {
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

        Self { listener, rx }
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
        let this = unsafe { self.map_unchecked_mut(|v| &mut v.rx) };

        match this.poll_next(cx) {
            Poll::Ready(v) => Poll::Ready(v.map(|(v, a)| (TcpStream::from(v), a))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Runtime {
    pub fn register_file(&self, file: File) -> FileWrapper {
        FileWrapper(IoWrapper::from(BaseArc::from(IoInner::from(file))))
    }

    pub fn register_stream(&self, stream: net::TcpStream) -> TcpStream {
        stream.into()
    }

    pub fn register_listener(&self, listener: net::TcpListener) -> TcpListener {
        TcpListener::from(listener)
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
