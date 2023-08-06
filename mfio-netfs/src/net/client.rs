use std::collections::{BTreeMap, VecDeque};

use core::future::poll_fn;
use core::mem::MaybeUninit;
use core::task::{Context, Poll, Waker};

use mfio::backend::*;
use mfio::error::Result;
use mfio::mferr;
use mfio::packet::*;

use mfio::tarc::{Arc, BaseArc};
use mfio::traits::*;

use parking_lot::Mutex;
use slab::Slab;

use super::{FsRequest, FsResponse, HeaderRouter, ReadDirResponse, Request, Response};
use cglue::result::IntError;
use futures::{future::FutureExt, pin_mut};
use mfio::error::Error;
use mfio_rt::{DirEntry, DirHandle, DirOp, Fs, Metadata, OpenOptions};

use core::future::Future;
use core::pin::Pin;
use mfio::stdeq::Seekable;
use std::path::Path;

use std::io::{self /*, Read as _, Write as _*/};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use flume::{r#async::SendFut, Sender};
use futures::Stream;
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
enum FsRequestState {
    Started {
        req: FsRequest,
        dir_id: u16,
        waker: Option<Waker>,
    },
    Processing {
        waker: Option<Waker>,
    },
    Complete {
        resp: Option<FsResponse>,
    },
}

struct NetFsState {
    in_flight_ops: Slab<InFlightOpType>,
    fs_reqs: Slab<FsRequestState>,
    read_dir_streams: Slab<ReadDirStream>,
}

struct Senders {
    ops: Sender<FileOperation>,
    fs_reqs: Sender<u32>,
    read_dir_reqs: Sender<u16>,
    close_reqs: Sender<u32>,
}

pub struct NetworkFs {
    backend: BackendContainer<DynBackend>,
    cwd: NetworkFsDir,
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
            fs_reqs: Default::default(),
            read_dir_streams: Default::default(),
        }));

        let (ops, ops_rx) = flume::unbounded();
        let (fs_reqs, fs_reqs_rx) = flume::bounded(16);
        let (close_reqs, close_reqs_rx) = flume::unbounded();
        let (read_dir_reqs, read_dir_reqs_rx) = flume::unbounded();

        let senders = Senders {
            ops,
            fs_reqs,
            close_reqs,
            read_dir_reqs,
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
                            Response::Fs { req_id, resp_len } => {
                                let fs_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "fs response",
                                    req_id,
                                    resp_len
                                );
                                async {
                                    // TODO: make a wrapper for this...
                                    let resp_len = resp_len as usize;
                                    if tmp_buf.capacity() < resp_len {
                                        tmp_buf.reserve(resp_len - tmp_buf.capacity());
                                    }
                                    // SAFETY: the data here is unininitialized
                                    unsafe { tmp_buf.set_len(resp_len) };
                                    let buf = unsafe {
                                        let buf = tmp_buf.as_ptr() as *mut u8;
                                        core::slice::from_raw_parts_mut(buf, resp_len)
                                    };

                                    if resp_len > 0 {
                                        read_end.read_all(NoPos::new(), &mut *buf).await.unwrap();
                                    }

                                    let resp: Option<FsResponse> = postcard::from_bytes(buf).ok();

                                    log::trace!("Fs Response: {resp:?}");

                                    let mut state = state.lock();
                                    if let Some(req) = state.fs_reqs.get_mut(req_id as usize) {
                                        log::trace!("State: {req:?}");
                                        if let FsRequestState::Processing { waker } = req {
                                            let waker = waker.take();

                                            *req = FsRequestState::Complete { resp };
                                            log::trace!("Move to fin");

                                            if let Some(waker) = waker {
                                                log::trace!("Wake");
                                                waker.wake();
                                            }
                                        }
                                    }
                                }
                                .instrument(fs_span)
                                .await
                            }
                            Response::ReadDir { stream_id, len } => {
                                let fs_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "read dir",
                                    stream_id,
                                    len,
                                );
                                async {
                                    let closed = (len & (1 << 31)) != 0;
                                    let len = len & !(1 << 31);
                                    // TODO: make a wrapper for this...
                                    let len = len as usize;
                                    if tmp_buf.capacity() < len {
                                        tmp_buf.reserve(len - tmp_buf.capacity());
                                    }
                                    // SAFETY: the data here is unininitialized
                                    unsafe { tmp_buf.set_len(len) };
                                    let buf = unsafe {
                                        let buf = tmp_buf.as_ptr() as *mut u8;
                                        core::slice::from_raw_parts_mut(buf, len)
                                    };
                                    read_end.read_all(NoPos::new(), &mut *buf).await.unwrap();

                                    let resp: Vec<ReadDirResponse> =
                                        postcard::from_bytes(buf).unwrap();

                                    let mut state = state.lock();
                                    if let Some(stream) =
                                        state.read_dir_streams.get_mut(stream_id as usize)
                                    {
                                        let waker = stream.waker.take();

                                        if closed || buf.is_empty() {
                                            stream.closed = true;
                                        }

                                        stream.resp_count += resp.len();
                                        stream.results.extend(
                                            resp.into_iter()
                                                .map(|r| r.map_err(Error::from_int_err)),
                                        );

                                        if let Some(waker) = waker {
                                            waker.wake();
                                        }
                                    }
                                }
                                .instrument(fs_span)
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

                let read_dir_loop = async {
                    while let Ok(stream_id) = read_dir_reqs_rx.recv_async().await {
                        async {
                            let mut state = state.lock();

                            if let Some(req) = state.read_dir_streams.get_mut(stream_id as usize) {
                                if !req.closed {
                                    let count = req.compute_count();
                                    core::mem::drop(state);
                                    router.send_hdr(write_id, |_| Request::ReadDir {
                                        stream_id,
                                        count,
                                    });
                                }
                            }
                        }
                        .instrument(tracing::span!(
                            tracing::Level::TRACE,
                            "read dir more",
                            stream_id
                        ))
                        .await;
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "open_loop"))
                .fuse();

                let fs_loop = async {
                    while let Ok(req_id) = fs_reqs_rx.recv_async().await {
                        async {
                            let msg = {
                                let mut state = state.lock();

                                if let Some(fs_req) = state.fs_reqs.get_mut(req_id as usize) {
                                    if let FsRequestState::Started { req, dir_id, waker } = fs_req {
                                        let buf = postcard::to_allocvec(&req).unwrap();
                                        // TODO: verify buflen
                                        let ret = Some((
                                            Request::Fs {
                                                req_id,
                                                dir_id: *dir_id,
                                                req_len: buf.len() as u16,
                                            },
                                            buf,
                                        ));

                                        *fs_req = FsRequestState::Processing {
                                            waker: waker.take(),
                                        };

                                        ret
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            };

                            if let Some((header, buf)) = msg {
                                router.send_bytes(write_id, |_| header, buf.into_boxed_slice());
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
                    pin_mut!(fs_loop);
                    pin_mut!(read_dir_loop);
                    pin_mut!(close_loop);
                    pin_mut!(ops_loop);
                    pin_mut!(results_loop);
                    pin_mut!(process_loop);

                    futures::select! {
                        _ = fs_loop => log::error!("Fs done"),
                        _ = read_dir_loop => log::error!("Read dir done"),
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
            cwd: NetworkFsDir {
                dir_id: 0,
                state,
                senders: senders.into(),
            },
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
    type DirHandle<'a> = NetworkFsDir;

    fn current_dir(&self) -> &Self::DirHandle<'_> {
        &self.cwd
    }
}

pub struct NetworkFsDir {
    dir_id: u16,
    state: BaseArc<Mutex<NetFsState>>,
    senders: BaseArc<Senders>,
}

impl DirHandle for NetworkFsDir {
    type FileHandle = Seekable<FileWrapper, u64>;
    type OpenFileFuture<'a> = OpenFileFuture<'a>;
    type PathFuture<'a> = PathFuture<'a>;
    type OpenDirFuture<'a> = OpenDirFuture<'a>;
    type ReadDir<'a> = ReadDir<'a>;
    type ReadDirFuture<'a> = ReadDirFuture<'a>;
    type MetadataFuture<'a> = MetadataFuture<'a>;
    type OpFuture<'a> = OpFuture<'a>;

    /// Gets the absolute path to this `DirHandle`.
    fn path(&self) -> Self::PathFuture<'_> {
        PathOp::make_future(self, FsRequest::Path)
    }

    /// Reads the directory contents.
    ///
    /// This function, upon success, returns a stream that can be used to list files and
    /// subdirectories within this dir.
    ///
    /// Note that on various platforms this may behave differently. For instance, Unix platforms
    /// support holding
    fn read_dir(&self) -> Self::ReadDirFuture<'_> {
        ReadDirOp::make_future(self, FsRequest::ReadDir)
    }

    /// Opens a file.
    ///
    /// This function accepts an absolute or relative path to a file for reading. If the path is
    /// relative, it is opened relative to this `DirHandle`.
    fn open_file<P: AsRef<Path>>(&self, path: P, options: OpenOptions) -> Self::OpenFileFuture<'_> {
        OpenFileOp::make_future(
            self,
            FsRequest::OpenFile {
                path: path.as_ref().to_string_lossy().into(),
                options,
            },
        )
    }

    /// Opens a directory.
    ///
    /// This function accepts an absolute or relative path to a directory for reading. If the path
    /// is relative, it is opened relative to this `DirHandle`.
    fn open_dir<P: AsRef<Path>>(&self, path: P) -> Self::OpenDirFuture<'_> {
        OpenDirOp::make_future(
            self,
            FsRequest::OpenDir {
                path: path.as_ref().to_string_lossy().into(),
            },
        )
    }

    fn metadata<P: AsRef<Path>>(&self, path: P) -> Self::MetadataFuture<'_> {
        MetadataOp::make_future(
            self,
            FsRequest::Metadata {
                path: path.as_ref().to_string_lossy().into(),
            },
        )
    }

    /// Do an operation.
    ///
    /// This function performs an operation from the [`DirOp`](DirOp) enum.
    fn do_op<P: AsRef<Path>>(&self, operation: DirOp<P>) -> Self::OpFuture<'_> {
        OpOp::make_future(self, FsRequest::DirOp(operation.as_path().into_string()))
    }
}

trait FsRequestProc {
    type Output<'a>: 'a;
    type Future<'a>: Future<Output = Self::Output<'a>> + 'a;

    fn finish<'a>(
        req: FsResponse,
        state: &mut NetFsState,
        dir: &'a NetworkFsDir,
    ) -> Self::Output<'a>;

    fn make_future2<'a>(
        fs: &'a NetworkFsDir,
        req_id: usize,
        send: Option<SendFut<'a, u32>>,
    ) -> Self::Future<'a>;

    fn make_future(fs: &NetworkFsDir, req: FsRequest) -> Self::Future<'_> {
        let state = &mut *fs.state.lock();

        let req_id = state.fs_reqs.insert(FsRequestState::Started {
            dir_id: fs.dir_id,
            waker: None,
            req,
        });

        assert!(req_id <= u32::MAX as usize);

        Self::make_future2(
            fs,
            req_id,
            Some(fs.senders.fs_reqs.send_async(req_id as u32)),
        )
    }
}

struct ReadDirStream {
    queued_read: bool,
    closed: bool,
    results: VecDeque<Result<DirEntry>>,
    waker: Option<Waker>,
    resp_count: usize,
}

impl ReadDirStream {
    fn compute_count(&self) -> u16 {
        // Automatically scale the request count to have better performance on larger directories
        core::cmp::min(128 * (self.resp_count + 1).ilog2(), u16::MAX as u32) as u16
    }
}

pub struct ReadDir<'a> {
    fs: &'a NetworkFsDir,
    stream_id: u16,
    send: Option<SendFut<'a, u16>>,
    cache: VecDeque<Result<DirEntry>>,
    closed: bool,
}

impl<'a> Stream for ReadDir<'a> {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(item) = this.cache.pop_front() {
                break Poll::Ready(Some(item));
            } else if !this.closed {
                // If there's any read request pending to be sent, try to finish sending it.
                if let Some(send) = this.send.as_mut() {
                    let send = unsafe { Pin::new_unchecked(send) };

                    if let Poll::Ready(res) = send.poll(cx) {
                        this.send = None;
                        if res.is_err() {
                            this.closed = true;
                        }
                    } else {
                        break Poll::Pending;
                    }
                }

                let state = &mut *this.fs.state.lock();
                if let Some(stream) = state.read_dir_streams.get_mut(this.stream_id as usize) {
                    // TODO: have better read-ahead caching strategy.
                    if !stream.results.is_empty() {
                        core::mem::swap(&mut this.cache, &mut stream.results);
                    }

                    if stream.closed {
                        state.read_dir_streams.remove(this.stream_id as usize);
                    } else {
                        stream.waker = Some(cx.waker().clone());
                        if !stream.queued_read {
                            stream.queued_read = true;
                            this.send =
                                Some(this.fs.senders.read_dir_reqs.send_async(this.stream_id));
                        }
                    }
                } else {
                    this.closed = true;
                    break Poll::Ready(None);
                }
            } else {
                break Poll::Ready(None);
            }
        }
    }
}

macro_rules! fs_op {
    ($fut:ident, $op:ident, $block:expr => $rettype:ty) => {

        pub struct $op;

        impl FsRequestProc for $op {
            type Output<'a> = $rettype;
            type Future<'a> = $fut<'a>;

            fn finish<'a>(
                resp: FsResponse,
                state: &mut NetFsState,
                dir: &'a NetworkFsDir,
            ) -> Self::Output<'a> {
                #[allow(clippy::redundant_closure_call)]
                ($block)(resp, state, dir)
            }

            fn make_future2<'a>(
                fs: &'a NetworkFsDir,
                req_id: usize,
                send: Option<SendFut<'a, u32>>,
            ) -> Self::Future<'a> {
                $fut {
                    fs,
                    req_id,
                    send,
                }
            }
        }

        pub struct $fut<'a> {
            fs: &'a NetworkFsDir,
            req_id: usize,
            send: Option<SendFut<'a, u32>>,
        }

        impl<'a> Future for $fut<'a> {
            type Output = $rettype;

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
                            .fs_reqs
                            .get_mut(this.req_id)
                            .expect("Request was not found")
                        {
                            FsRequestState::Complete { .. } => Poll::Ready({
                                // Remove the entry
                                let resp = state.fs_reqs.remove(this.req_id);

                                let FsRequestState::Complete { resp } = resp else { unreachable!() };

                                if let Some(resp) = resp {
                                    $op::finish(resp, state, this.fs)
                                } else {
                                    Err(mferr!(Response, NotFound, Filesystem))
                                }
                            }),
                            FsRequestState::Started { waker, .. } => {
                                *waker = Some(cx.waker().clone());
                                Poll::Pending
                            }
                            FsRequestState::Processing { waker } => {
                                *waker = Some(cx.waker().clone());
                                Poll::Pending
                            }
                        };
                    }
                }
            }
        }
    };
}

fs_op!(
    OpenFileFuture,
    OpenFileOp,
    |resp, _, dir: &NetworkFsDir| {
        if let FsResponse::OpenFile { file_id } = resp {
            file_id
                .map(|file_id| {
                    let write_io = BaseArc::new(IoOpsHandle::new(file_id, dir.senders.clone()));
                    let read_io = BaseArc::new(IoOpsHandle::new(file_id, dir.senders.clone()));

                    let write_stream = BaseArc::from(PacketStream {
                        ctx: PacketCtx::new(write_io).into(),
                    });

                    let read_stream = BaseArc::from(PacketStream {
                        ctx: PacketCtx::new(read_io).into(),
                    });

                    FileWrapper(IoWrapper {
                        file_id,
                        senders: dir.senders.clone(),
                        read_stream,
                        write_stream,
                    })
                    .into()
                })
                .map_err(Error::from_int_err)
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<Seekable<FileWrapper, u64>>
);

fs_op!(
    PathFuture,
    PathOp,
    |resp, _, _| {
        if let FsResponse::Path { path } = resp {
            path.map(From::from).map_err(Error::from_int_err)
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<PathBuf>
);

fs_op!(
    ReadDirFuture,
    ReadDirOp,
    |resp, _, dir| {
        if let FsResponse::ReadDir { stream_id } = resp {
            stream_id
                .map(|stream_id| ReadDir {
                    fs: dir,
                    stream_id,
                    cache: Default::default(),
                    closed: false,
                    send: None,
                })
                .map_err(Error::from_int_err)
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<ReadDir<'a>>
);

fs_op!(
    OpenDirFuture,
    OpenDirOp,
    |resp, _, dir: &NetworkFsDir| {
        if let FsResponse::OpenDir { dir_id } = resp {
            dir_id
                .map(|dir_id| NetworkFsDir {
                    dir_id: dir_id.into(),
                    senders: dir.senders.clone(),
                    state: dir.state.clone(),
                })
                .map_err(Error::from_int_err)
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<NetworkFsDir>
);

fs_op!(
    MetadataFuture,
    MetadataOp,
    |resp, _, _| {
        if let FsResponse::Metadata { metadata } = resp {
            metadata.map_err(Error::from_int_err)
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<Metadata>
);

fs_op!(
    OpFuture,
    OpOp,
    |resp, _, _| {
        if let FsResponse::DirOp(res) = resp {
            if let Some(err) = res.map(Error::from_int_err) {
                Err(err)
            } else {
                Ok(())
            }
        } else {
            Err(mferr!(Response, Invalid, Filesystem))
        }
    } => Result<()>
);

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

pub struct FileWrapper(IoWrapper<u64>);

pub trait IoAt: Sized + Clone {
    fn drop_io_wrapper(io: &mut IoWrapper<Self>);
}

impl IoAt for u64 {
    fn drop_io_wrapper(io: &mut IoWrapper<Self>) {
        let _ = io.senders.close_reqs.send(io.file_id);
    }
}
