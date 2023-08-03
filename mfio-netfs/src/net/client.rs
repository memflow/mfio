use std::collections::BTreeMap;

use core::future::poll_fn;
use core::mem::MaybeUninit;
use core::task::{Context, Poll, Waker};

use mfio::backend::*;
use mfio::error::Result;
use mfio::packet::*;

use mfio::tarc::{Arc, BaseArc};
use mfio::traits::*;

use parking_lot::Mutex;
use slab::Slab;

use super::{HeaderRouter, Request, Response};
use cglue::result::IntError;
use futures::{future::FutureExt, pin_mut};
use mfio::error::Error;
use mfio_rt::Fs;

use std::io::{self /*, Read as _, Write as _*/};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use flume::{r#async::SendFut, Sender};
use log::*;
use tracing::instrument::Instrument;

struct FileOperation {
    file_id: u32,
    pos: u64,
    ty: OpType,
}

impl FileOperation {
    fn write_msg<'a>(
        self,
        router: &HeaderRouter<'a, Request, Read>,
        id: Pin<&PacketId<'a, Read, NoPos>>,
        packet_id: u32,
    ) -> Result<InFlightOpType> {
        let Self { file_id, pos, ty } = self;
        ty.write_msg(router, id, file_id, pos, packet_id)
    }
}

trait IntoOp: PacketPerms {
    fn into_op(pkt: BoundPacket<'static, Self>) -> OpType;
}

impl IntoOp for Read {
    fn into_op(pkt: BoundPacket<'static, Self>) -> OpType {
        OpType::Write(pkt)
    }
}

impl IntoOp for Write {
    fn into_op(pkt: BoundPacket<'static, Self>) -> OpType {
        OpType::Read(pkt)
    }
}

struct ShardedPacket<T: Splittable> {
    shards: BTreeMap<usize, T>,
}

impl<T: Splittable> From<T> for ShardedPacket<T> {
    fn from(pkt: T) -> Self {
        Self {
            shards: std::iter::once((0, pkt)).collect(),
        }
    }
}

impl<T: Splittable> ShardedPacket<T> {
    fn is_empty(&self) -> bool {
        // TODO: do this or self.len() == 0?
        self.shards.is_empty()
    }

    fn extract(&mut self, idx: usize, len: usize) -> Option<T> {
        let (&shard_idx, _) = self.shards.range(..=idx).next_back()?;
        let mut shard = self.shards.remove(&shard_idx)?;

        if idx > shard_idx {
            let (left, right) = shard.split_at(idx - shard_idx);
            self.shards.insert(shard_idx, left);
            shard = right;
        }

        if len < shard.len() {
            let (left, right) = shard.split_at(len);
            self.shards.insert(idx + len, right);
            shard = left;
        }

        Some(shard)
    }
}

enum InFlightOpType {
    Reserved,
    Read(ShardedPacket<BoundPacket<'static, Write>>),
    Write(usize),
}

enum OpType {
    Read(BoundPacket<'static, Write>),
    Write(BoundPacket<'static, Read>),
}

impl OpType {
    fn write_msg<'a>(
        self,
        router: &HeaderRouter<'a, Request, Read>,
        id: Pin<&PacketId<'a, Read, NoPos>>,
        file_id: u32,
        pos: u64,
        packet_id: u32,
    ) -> Result<InFlightOpType> {
        match self {
            Self::Read(v) => {
                router.send_hdr(id, |_| Request::Read {
                    file_id,
                    pos,
                    packet_id: packet_id as _,
                    len: v.len(),
                });
                Ok(InFlightOpType::Read(v.into()))
            }
            Self::Write(v) => {
                let len = v.len();
                let key = router.send_pkt(
                    id,
                    |_| Request::Write {
                        file_id,
                        pos,
                        packet_id,
                        len,
                    },
                    v,
                );
                Ok(InFlightOpType::Write(key))
            }
        }
    }
}

#[derive(Debug)]
enum FileOpenRequest {
    Started {
        options: OpenOptions,
        path: PathBuf,
        waker: Option<Waker>,
    },
    Processing {
        waker: Option<Waker>,
    },
    Complete {
        file_handle: Result<u32>,
    },
}

struct NetFsState {
    in_flight_ops: Slab<InFlightOpType>,
    open_reqs: Slab<FileOpenRequest>,
}

struct Senders {
    ops: Sender<FileOperation>,
    open_reqs: Sender<u32>,
    close_reqs: Sender<u32>,
}

pub struct NetworkFs {
    state: BaseArc<Mutex<NetFsState>>,
    backend: BackendContainer<DynBackend>,
    senders: BaseArc<Senders>,
    fs: Arc<mfio_rt::NativeRt>,
}

impl NetworkFs {
    pub fn try_new(addr: SocketAddr) -> Result<Self> {
        let fs = Arc::new(
            mfio_rt::NativeRt::builder()
                .thread(true)
                .enable_all()
                .build()
                .unwrap(),
        );
        Self::with_fs(addr, fs)
    }

    pub fn with_fs(addr: SocketAddr, fs: Arc<mfio_rt::NativeRt>) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;

        let stream = fs.register_stream(stream);

        let state = BaseArc::new(Mutex::new(NetFsState {
            in_flight_ops: Default::default(),
            open_reqs: Default::default(),
        }));

        let (ops, ops_rx) = flume::unbounded();
        let (open_reqs, open_reqs_rx) = flume::bounded(16);
        let (close_reqs, close_reqs_rx) = flume::unbounded();

        let senders = Senders {
            ops,
            open_reqs,
            close_reqs,
        };

        let backend = {
            let state = state.clone();
            async move {
                let read_end = &stream;
                let state = &state;
                let stream = &stream;

                let router = HeaderRouter::new();
                let write_id = stream.new_id().await;
                // SAFETY: we are pinning immediately after creation - this ID is not moving
                // anywhere.
                let write_id = unsafe { Pin::new_unchecked(&write_id) };

                let results_loop = async {
                    // Parse responses

                    let mut tmp_buf: Vec<MaybeUninit<u8>> = vec![];

                    loop {
                        let resp = match {
                            IoRead::read::<Response>(read_end, NoPos::new())
                                .instrument(tracing::span!(
                                    tracing::Level::TRACE,
                                    "response header"
                                ))
                                .await
                        } {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!("No resp: {e}");
                                break;
                            }
                        };

                        debug!("Response: {resp:?}");
                        //let resp = bincode::deserialize_from(&mut state.stream).unwrap();

                        match resp {
                            Response::Read {
                                packet_id,
                                idx,
                                len,
                                err,
                            } => {
                                async {
                                    let pkt = {
                                        let mut state = state.lock();
                                        if let Some(InFlightOpType::Read(shards)) =
                                            state.in_flight_ops.get_mut(packet_id as usize)
                                        {
                                            let pkt = shards.extract(idx, len).unwrap();
                                            assert_eq!(pkt.len(), len);

                                            if shards.is_empty() {
                                                state.in_flight_ops.remove(packet_id as usize);
                                            }

                                            core::mem::drop(state);

                                            if let Some(err) = err.map(Error::from_int_err) {
                                                pkt.error(err);
                                                None
                                            } else {
                                                Some(pkt)
                                            }
                                        } else {
                                            None
                                        }
                                    };

                                    if let Some(pkt) = pkt {
                                        match pkt.try_alloc() {
                                            Ok(v) => {
                                                // SAFETY: assume MaybeUninit<u8> is initialized,
                                                // as God intended :upside_down:
                                                let buf = unsafe {
                                                    let buf = v.as_ptr() as *mut u8;
                                                    core::slice::from_raw_parts_mut(buf, len)
                                                };
                                                read_end.read_all(NoPos::new(), buf).await.unwrap();
                                            }
                                            Err(v) => {
                                                // TODO: limit allocation size
                                                if tmp_buf.capacity() < v.len() {
                                                    tmp_buf.reserve(v.len() - tmp_buf.capacity());
                                                }
                                                // SAFETY: the data here is unininitialized
                                                unsafe { tmp_buf.set_len(v.len()) };
                                                let buf = unsafe {
                                                    let buf = tmp_buf.as_ptr() as *mut u8;
                                                    core::slice::from_raw_parts_mut(buf, len)
                                                };
                                                read_end.read_all(NoPos::new(), buf).await.unwrap();
                                                // SAFETY: tmp_buf has enough capacity for the packet
                                                unsafe { v.transfer_data(tmp_buf.as_ptr().cast()) };
                                            }
                                        }
                                    }
                                }
                                .instrument(tracing::span!(
                                    tracing::Level::TRACE,
                                    "read response",
                                    packet_id,
                                    idx,
                                    len
                                ))
                                .await;
                            }
                            Response::Write {
                                packet_id,
                                idx,
                                len,
                                err,
                            } => {
                                let read_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "write response",
                                    packet_id,
                                    idx,
                                    len
                                );
                                async {
                                    let mut state = state.lock();
                                    if let Some(InFlightOpType::Write(key)) =
                                        state.in_flight_ops.get_mut(packet_id as usize)
                                    {
                                        if router.pkt_result(
                                            *key,
                                            idx,
                                            len,
                                            err.map(Error::from_int_err),
                                        ) {
                                            state.in_flight_ops.remove(packet_id as usize);
                                        }
                                    }
                                }
                                .instrument(read_span)
                                .await
                            }
                            Response::FileOpen {
                                req_id,
                                err,
                                file_id,
                            } => {
                                let read_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "open response",
                                    req_id,
                                    file_id
                                );
                                async {
                                    let mut state = state.lock();
                                    if let Some(req) = state.open_reqs.get_mut(req_id as usize) {
                                        if let FileOpenRequest::Processing { waker } = req {
                                            let waker = waker.take();

                                            let file_handle =
                                                if let Some(err) = err.map(Error::from_int_err) {
                                                    Err(err)
                                                } else {
                                                    Ok(file_id)
                                                };

                                            *req = FileOpenRequest::Complete { file_handle };

                                            if let Some(waker) = waker {
                                                waker.wake();
                                            }
                                        }
                                    }
                                }
                                .instrument(read_span)
                                .await
                            }
                        }
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "results_loop"))
                .fuse();

                let ops_loop = async {
                    while let Ok(op) = ops_rx.recv_async().await {
                        let key = state.lock().in_flight_ops.insert(InFlightOpType::Reserved);
                        assert!(key <= u32::MAX as usize);

                        async {
                            let op = op.write_msg(&router, write_id, key as u32).unwrap();

                            *state.lock().in_flight_ops.get_mut(key).unwrap() = op;
                        }
                        .instrument(tracing::span!(tracing::Level::TRACE, "op send", key))
                        .await
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "ops_loop"))
                .fuse();

                let open_loop = async {
                    while let Ok(req_id) = open_reqs_rx.recv_async().await {
                        async {
                            let msg = {
                                let mut state = state.lock();

                                if let Some(req) = state.open_reqs.get_mut(req_id as usize) {
                                    if let FileOpenRequest::Started {
                                        options,
                                        path,
                                        waker,
                                    } = req
                                    {
                                        let path = core::mem::take(path)
                                            .into_os_string()
                                            .into_string()
                                            .unwrap();
                                        let path_bytes = path.as_bytes();

                                        assert!(path_bytes.len() <= u32::MAX as usize);

                                        let msg = Request::FileOpen {
                                            req_id,
                                            path_len: path_bytes.len() as u32,
                                            options: *options,
                                        };

                                        trace!("Write open {msg:?}");

                                        *req = FileOpenRequest::Processing {
                                            waker: waker.take(),
                                        };

                                        Some((msg, path))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            };

                            if let Some((msg, path)) = msg {
                                router.send_bytes(
                                    write_id,
                                    |_| msg,
                                    path.into_bytes().into_boxed_slice(),
                                );
                            }
                        }
                        .instrument(tracing::span!(tracing::Level::TRACE, "open send", req_id))
                        .await;
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "open_loop"))
                .fuse();

                let close_loop = async {
                    while let Ok(file_id) = close_reqs_rx.recv_async().await {
                        async {
                            router.send_hdr(write_id, |_| Request::FileClose { file_id });
                        }
                        .instrument(tracing::span!(tracing::Level::TRACE, "close send", file_id))
                        .await
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "close_loop"))
                .fuse();

                let process_loop = router
                    .process_loop(write_id)
                    .instrument(tracing::span!(tracing::Level::TRACE, "process_loop"))
                    .fuse();

                let combined = async move {
                    pin_mut!(open_loop);
                    pin_mut!(close_loop);
                    pin_mut!(ops_loop);
                    pin_mut!(results_loop);
                    pin_mut!(process_loop);

                    futures::select! {
                        _ = open_loop => log::error!("Open done"),
                        _ = close_loop => log::error!("Close done"),
                        _ = ops_loop => log::error!("Ops done"),
                        _ = results_loop => log::error!("Results done"),
                        _ = process_loop => log::error!("Process done"),
                    }
                };

                combined.await;

                poll_fn(|_| {
                    log::error!("Network backend polled to completion!");
                    Poll::Pending
                })
                .await
                //fs.with_backend(combined).0.await;
            }
        };

        Ok(Self {
            fs,
            state,
            senders: senders.into(),
            backend: BackendContainer::new_dyn(backend),
        })
    }
}

impl IoBackend for NetworkFs {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        self.fs.polling_handle()
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire_nested(self.fs.get_backend())
    }
}

impl Fs for NetworkFs {
    type FileHandle = Seekable<FileWrapper, u64>;
    type StreamHandle = StreamWrapper;
    type OpenFuture<'a> = OpenFuture<'a>;

    fn open(&self, path: &Path, options: OpenOptions) -> Self::OpenFuture<'_> {
        let state = &mut *self.state.lock();

        let req_id = state.open_reqs.insert(FileOpenRequest::Started {
            options,
            path: path.into(),
            waker: None,
        });

        assert!(req_id <= u32::MAX as usize);

        OpenFuture {
            fs: self,
            req_id,
            send: Some(self.senders.open_reqs.send_async(req_id as u32)),
        }
    }
}

use core::future::Future;
use core::pin::Pin;
use mfio::stdeq::Seekable;
use mfio_rt::OpenOptions;
use std::path::Path;

pub struct OpenFuture<'a> {
    fs: &'a NetworkFs,
    //path: &'a Path,
    //options: OpenOptions,
    req_id: usize,
    send: Option<SendFut<'a, u32>>,
}

impl<'a> Future for OpenFuture<'a> {
    type Output = Result<<NetworkFs as Fs>::FileHandle>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // SAFETY: we are not moving the contents of self, or self itself.
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(send) = &mut this.send {
                let send = unsafe { Pin::new_unchecked(send) };
                match send.poll(cx) {
                    Poll::Ready(Ok(_)) => this.send = None,
                    Poll::Ready(Err(_)) => {
                        break Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
                    }
                    Poll::Pending => break Poll::Pending,
                }
            } else {
                let state = &mut *this.fs.state.lock();

                break match state
                    .open_reqs
                    .get_mut(this.req_id)
                    .expect("Request was not found")
                {
                    FileOpenRequest::Complete { file_handle } => Poll::Ready(match *file_handle {
                        Ok(file_id) => {
                            // Remove the entry
                            state.open_reqs.remove(this.req_id);

                            let write_io =
                                BaseArc::new(IoOpsHandle::new(file_id, this.fs.senders.clone()));
                            let read_io =
                                BaseArc::new(IoOpsHandle::new(file_id, this.fs.senders.clone()));

                            let write_stream = BaseArc::from(PacketStream {
                                ctx: PacketCtx::new(write_io).into(),
                            });

                            let read_stream = BaseArc::from(PacketStream {
                                ctx: PacketCtx::new(read_io).into(),
                            });

                            Ok(FileWrapper(IoWrapper {
                                file_id,
                                senders: this.fs.senders.clone(),
                                read_stream,
                                write_stream,
                            })
                            .into())
                        }
                        Err(e) => Err(e),
                    }),
                    FileOpenRequest::Started { waker, .. } => {
                        *waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                    FileOpenRequest::Processing { waker } => {
                        *waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                };
            }
        }
    }
}

struct IoOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, u64>,
    file_id: u32,
    senders: BaseArc<Senders>,
}

impl<Perms: IntoOp> IoOpsHandle<Perms> {
    fn new(file_id: u32, senders: BaseArc<Senders>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            file_id,
            senders,
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
        let operation = FileOperation {
            file_id: self.file_id,
            pos,
            ty: Perms::into_op(packet),
        };

        // TODO: what to do with error?
        let _ = self.senders.ops.send(operation);
    }
}

pub struct IoWrapper<Param: IoAt> {
    file_id: u32,
    senders: BaseArc<Senders>,
    read_stream: BaseArc<PacketStream<'static, Write, Param>>,
    write_stream: BaseArc<PacketStream<'static, Read, Param>>,
}

impl<Param: IoAt> Drop for IoWrapper<Param> {
    fn drop(&mut self) {
        Param::drop_io_wrapper(self);
    }
}

impl<Param: IoAt> PacketIo<Write, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Write, Param>> {
        Some(self.read_stream.new_packet_id())
    }
}

impl<Param: IoAt> PacketIo<Read, Param> for IoWrapper<Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Read, Param>> {
        Some(self.write_stream.new_packet_id())
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

impl PacketIo<Read, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, Read, NoPos>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<Write, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, Write, NoPos>> {
        self.0.try_new_id(cx)
    }
}

pub struct FileWrapper(IoWrapper<u64>);
pub struct StreamWrapper(IoWrapper<NoPos>);

pub trait IoAt: Sized + Clone {
    fn drop_io_wrapper(io: &mut IoWrapper<Self>);
}

impl IoAt for u64 {
    fn drop_io_wrapper(io: &mut IoWrapper<Self>) {
        let _ = io.senders.close_reqs.send(io.file_id);
    }
}

impl IoAt for NoPos {
    fn drop_io_wrapper(_: &mut IoWrapper<Self>) {}
}
