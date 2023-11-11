use std::collections::{BTreeMap, VecDeque};

use core::future::poll_fn;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::task::{Context, Poll, Waker};

use mfio::backend::*;
use mfio::error::Result;
use mfio::io::*;
use mfio::mferr;

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

use std::io::{self /* , Read as _, Write as _ */};
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
    fn write_msg<Io: PacketIo<Read, NoPos>>(
        self,
        router: &HeaderRouter<'_, Request, Read, Io>,
        packet_id: u32,
    ) -> Result<InFlightOpType> {
        let Self { file_id, pos, ty } = self;
        ty.write_msg(router, file_id, pos, packet_id)
    }
}

trait IntoOp: PacketPerms {
    fn into_op(pkt: BoundPacketView<Self>) -> OpType;
}

impl IntoOp for Read {
    fn into_op(pkt: BoundPacketView<Self>) -> OpType {
        OpType::Write(pkt)
    }
}

impl IntoOp for Write {
    fn into_op(pkt: BoundPacketView<Self>) -> OpType {
        OpType::Read(pkt)
    }
}

struct ShardedPacket<T: Splittable<u64>> {
    shards: BTreeMap<u64, T>,
}

impl<T: Splittable<u64>> From<T> for ShardedPacket<T> {
    fn from(pkt: T) -> Self {
        Self {
            shards: std::iter::once((0, pkt)).collect(),
        }
    }
}

impl<T: Splittable<u64>> ShardedPacket<T> {
    fn is_empty(&self) -> bool {
        // TODO: do this or self.len() == 0?
        self.shards.is_empty()
    }

    fn extract(&mut self, idx: u64, len: u64) -> Option<T> {
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
    Read(ShardedPacket<BoundPacketView<Write>>),
    Write(usize),
}

enum OpType {
    Read(BoundPacketView<Write>),
    Write(BoundPacketView<Read>),
}

impl OpType {
    fn write_msg<Io: PacketIo<Read, NoPos>>(
        self,
        router: &HeaderRouter<'_, Request, Read, Io>,
        file_id: u32,
        pos: u64,
        packet_id: u32,
    ) -> Result<InFlightOpType> {
        match self {
            Self::Read(v) => {
                router.send_hdr(|_| Request::Read {
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
    read_dir_streams: BTreeMap<u16, ReadDirStream>,
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
    cancel_ops_on_drop: bool,
}

impl Drop for NetworkFs {
    fn drop(&mut self) {
        if self.cancel_ops_on_drop {
            self.fs.cancel_all_ops();
        }
    }
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
        Self::with_fs(addr, fs, true)
    }

    /// Create a new network FS to the provided target.
    ///
    /// # Arguments
    ///
    /// - `addr` address to connect to.
    /// - `fs` filesystem instance to use.
    /// - `cancel_ops_on_drop` whether to issue global cancellation to all outstanding operations
    ///   when drop is called. This is required when using `cfg(mfio_assume_linear_types)` in order
    ///   to prevent panic upon dropping the network filesystem.
    pub fn with_fs(
        addr: SocketAddr,
        fs: Arc<mfio_rt::NativeRt>,
        cancel_ops_on_drop: bool,
    ) -> Result<Self> {
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

                let router = HeaderRouter::new(stream);

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

                        trace!("Response: {resp:?}");

                        match resp {
                            Response::Read {
                                packet_id,
                                idx,
                                len,
                                err,
                            } => {
                                async move {
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
                                        trace!("Send read {}", pkt.len());
                                        // FIXME we should wait for this to finish perhaps?
                                        read_end.send_io(NoPos::new(), pkt);
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
                                tmp_buf = async move {
                                    // TODO: make a wrapper for this...
                                    let resp_len = resp_len as usize;
                                    if tmp_buf.capacity() < resp_len {
                                        tmp_buf.reserve(resp_len - tmp_buf.len());
                                    }
                                    // SAFETY: the data here is unininitialized
                                    unsafe { tmp_buf.set_len(resp_len) };

                                    let tmp_buf = if resp_len > 0 {
                                        let buf = VecPacket::from(tmp_buf);
                                        let buf =
                                            read_end.read_all(NoPos::new(), buf).await.unwrap();
                                        buf.take()
                                    } else {
                                        tmp_buf
                                    };

                                    let resp: Option<FsResponse> = postcard::from_bytes(unsafe {
                                        &*(&tmp_buf[..] as *const [MaybeUninit<_>] as *const [u8])
                                    })
                                    .ok();

                                    log::trace!("Fs Response {req_id}: {resp:?}");

                                    let mut state = state.lock();
                                    if let Some(req) = state.fs_reqs.get_mut(req_id as usize) {
                                        log::trace!("State: {req:?}");
                                        if let FsRequestState::Processing { waker } = req {
                                            let waker = waker.take();

                                            *req = FsRequestState::Complete { resp };

                                            if let Some(waker) = waker {
                                                waker.wake();
                                            }
                                        }
                                    }

                                    tmp_buf
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
                                tmp_buf = async move {
                                    let closed = (len & (1 << 31)) != 0;
                                    let len = len & !(1 << 31);
                                    log::trace!("ReadDir resp: {closed} {len}");
                                    // TODO: make a wrapper for this...
                                    let len = len as usize;
                                    if tmp_buf.capacity() < len {
                                        tmp_buf.reserve(len - tmp_buf.len());
                                    }

                                    // SAFETY: the data here is unininitialized
                                    unsafe { tmp_buf.set_len(len) };

                                    let buf = VecPacket::from(tmp_buf);
                                    let buf = read_end.read_all(NoPos::new(), buf).await.unwrap();
                                    let tmp_buf = buf.take();

                                    let resp: Vec<ReadDirResponse> = postcard::from_bytes(unsafe {
                                        &*(&tmp_buf[..] as *const [MaybeUninit<u8>]
                                            as *const [u8])
                                    })
                                    .unwrap();

                                    log::trace!("Resulting: {}", resp.len());

                                    let mut state = state.lock();
                                    if let Some(stream) = state.read_dir_streams.get_mut(&stream_id)
                                    {
                                        let waker = stream.waker.take();

                                        if closed || resp.is_empty() {
                                            log::trace!("Closing {closed}");
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

                                    tmp_buf
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
                            let op = op.write_msg(&router, key as u32).unwrap();

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
                        log::trace!("Read dir {stream_id}");
                        async {
                            let mut state = state.lock();

                            if let Some(req) = state.read_dir_streams.get_mut(&stream_id) {
                                if !req.closed {
                                    let count = req.compute_count();
                                    core::mem::drop(state);
                                    router.send_hdr(|_| Request::ReadDir { stream_id, count });
                                }
                            }
                        }
                        .instrument(tracing::span!(
                            tracing::Level::TRACE,
                            "read dir more",
                            stream_id,
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
                                        log::trace!("Request: {req:?}");
                                        let buf = postcard::to_allocvec(&req).unwrap();
                                        // TODO: verify buflen
                                        let ret = Some((
                                            Request::Fs {
                                                req_id,
                                                dir_id: *dir_id,
                                                req_len: buf.len() as u16,
                                            },
                                            OwnedPacket::from(buf.into_boxed_slice()),
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
                                router.send_bytes(|_| header, buf);
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
                            router.send_hdr(|_| Request::FileClose { file_id });
                        }
                        .instrument(tracing::span!(tracing::Level::TRACE, "close send", file_id))
                        .await
                    }
                }
                .instrument(tracing::span!(tracing::Level::TRACE, "close_loop"))
                .fuse();

                let combined = async move {
                    pin_mut!(fs_loop);
                    pin_mut!(read_dir_loop);
                    pin_mut!(close_loop);
                    pin_mut!(ops_loop);
                    pin_mut!(results_loop);

                    futures::select! {
                        _ = fs_loop => log::error!("Fs done"),
                        _ = read_dir_loop => log::error!("Read dir done"),
                        _ = close_loop => log::error!("Close done"),
                        _ = ops_loop => log::error!("Ops done"),
                        _ = results_loop => log::error!("Results done"),
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
            cancel_ops_on_drop,
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
    fn open_file<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        path: &'a P,
        options: OpenOptions,
    ) -> Self::OpenFileFuture<'a> {
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
    fn open_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpenDirFuture<'a> {
        OpenDirOp::make_future(
            self,
            FsRequest::OpenDir {
                path: path.as_ref().to_string_lossy().into(),
            },
        )
    }

    fn metadata<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::MetadataFuture<'a> {
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
    fn do_op<'a, P: AsRef<Path> + ?Sized>(&'a self, operation: DirOp<&'a P>) -> Self::OpFuture<'a> {
        OpOp::make_future(self, FsRequest::DirOp(operation.into_string()))
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
        core::cmp::min(128 * ((self.resp_count + 1).ilog2() + 1), u16::MAX as u32) as u16
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
                if let Some(stream) = state.read_dir_streams.get_mut(&this.stream_id) {
                    // TODO: have better read-ahead caching strategy.
                    if !stream.results.is_empty() {
                        core::mem::swap(&mut this.cache, &mut stream.results);
                    }

                    if stream.closed {
                        state.read_dir_streams.remove(&this.stream_id);
                    } else if !stream.queued_read {
                        stream.waker = Some(cx.waker().clone());
                        if !stream.queued_read {
                            stream.queued_read = true;
                            this.send =
                                Some(this.fs.senders.read_dir_reqs.send_async(this.stream_id));
                        }
                    } else {
                        stream.waker = Some(cx.waker().clone());
                        break Poll::Pending;
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
                $fut { fs, req_id, send }
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

                                let FsRequestState::Complete { resp } = resp else {
                                    unreachable!()
                                };

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
                    FileWrapper(IoWrapper {
                        file_id,
                        senders: dir.senders.clone(),
                        _phantom: PhantomData,
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
    |resp, state: &mut NetFsState, dir| {
        if let FsResponse::ReadDir { stream_id } = resp {
            stream_id
                .map(|stream_id| {
                    state.read_dir_streams.insert(stream_id, ReadDirStream {
                        queued_read: false,
                        closed: false,
                        results: Default::default(),
                        waker: None,
                        resp_count: 0,
                    });
                    ReadDir {
                        fs: dir,
                        stream_id,
                        cache: Default::default(),
                        closed: false,
                        send: None,
                    }
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

impl<Perms: IntoOp, Param: IoAt> PacketIo<Perms, u64> for IoWrapper<Param> {
    fn send_io(&self, pos: u64, packet: BoundPacketView<Perms>) {
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
    _phantom: PhantomData<Param>,
}

impl<Param: IoAt> Drop for IoWrapper<Param> {
    fn drop(&mut self) {
        Param::drop_io_wrapper(self);
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn send_io(&self, param: u64, view: BoundPacketView<Read>) {
        self.0.send_io(param, view)
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn send_io(&self, param: u64, view: BoundPacketView<Write>) {
        self.0.send_io(param, view)
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
