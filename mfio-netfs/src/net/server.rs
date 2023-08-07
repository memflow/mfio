use core::cell::RefCell;
use core::num::NonZeroU16;
use core::pin::Pin;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;

use super::{FsRequest, FsResponse, HeaderRouter, Request, Response};

use async_mutex::Mutex as AsyncMutex;
use cglue::result::IntError;
use log::*;
use mfio::backend::IoBackend;
use mfio::error::Error;
use mfio::packet::{NoPos, PacketIo};
use mfio::stdeq::Seekable;
use mfio::tarc::BaseArc;
use mfio_rt::{
    native::{NativeRtDir, ReadDir},
    DirHandle, Fs, NativeFile, NativeRt, TcpListenerHandle,
};
use parking_lot::Mutex;
use slab::Slab;
use tracing::instrument::Instrument;

use debug_ignore::DebugIgnore;
use futures::{
    future::FutureExt,
    pin_mut,
    stream::{Fuse, FusedStream, FuturesUnordered, StreamExt},
};
use mfio_rt::{
    native::{NativeTcpListener, NativeTcpStream},
    Tcp,
};

#[derive(Debug)]
enum Operation {
    Read {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        len: usize,
    },
    Write {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        buf: DebugIgnore<Vec<u8>>,
    },
    FileClose {
        file_id: u32,
    },
    ReadDir {
        stream_id: u16,
        count: u16,
    },
    Fs {
        req_id: u32,
        dir_id: u16,
        req: FsRequest,
    },
}

struct ReadPacket {
    len: usize,
    shards: Mutex<BTreeMap<usize, Box<[u8]>>>,
}

use core::marker::PhantomData;
use core::mem::{ManuallyDrop, MaybeUninit};
use mfio::packet::{BoundPacketObj, Packet, PacketObj, Write, WritePacketObj};

impl<'a> From<&'a ReadPacket> for Packet<'a, Write> {
    fn from(pkt: &'a ReadPacket) -> Self {
        unsafe extern "C" fn get_mut<'a, 'b>(
            _: &'a mut ManuallyDrop<BoundPacketObj<'b, Write>>,
            _: usize,
            _: &'a mut MaybeUninit<WritePacketObj<'b>>,
        ) -> bool {
            false
        }

        unsafe extern "C" fn transfer_data(obj: &mut BoundPacketObj<Write>, src: *const ()) {
            let this = &*(obj.buffer().data as *mut ReadPacket);
            let len = obj.buffer().len();
            let idx = obj.buffer().start();
            let buf = src as *const u8;
            let buf = core::slice::from_raw_parts(buf, len);
            this.shards
                .lock()
                .insert(idx, buf.to_vec().into_boxed_slice());
        }

        unsafe {
            Self::new(
                &Write {
                    get_mut,
                    transfer_data,
                },
                PacketObj {
                    data: pkt as *const _ as *mut (),
                    start: 0,
                    end: pkt.len,
                    tag: 0,
                    _phantom: PhantomData,
                },
            )
        }
    }
}

async fn run_server(stream: NativeTcpStream, fs: &NativeRt) {
    let stream_raw = &stream;
    let mut file_handles: Slab<BaseArc<Seekable<NativeFile, u64>>> = Default::default();
    let file_handles = RefCell::new(&mut file_handles);
    let mut dir_handles: Slab<BaseArc<NativeRtDir>> = Default::default();
    let dir_handles = RefCell::new(&mut dir_handles);
    let mut read_dir_streams: Slab<Pin<BaseArc<AsyncMutex<Fuse<ReadDir>>>>> = Default::default();
    let read_dir_streams = RefCell::new(&mut read_dir_streams);

    let router = HeaderRouter::new();
    let write_id = stream_raw.new_id().await;
    // SAFETY: we are pinning immediately after creation - this ID is not moving
    // anywhere.
    let write_id = unsafe { Pin::new_unchecked(&write_id) };

    let mut futures = FuturesUnordered::new();

    let (tx, rx) = flume::bounded(512);

    let ingress_loop = async {
        use mfio::traits::IoRead;
        while let Ok(v) = {
            let header_span = tracing::span!(tracing::Level::TRACE, "server read Request header");
            trace!("Queue req read");
            stream_raw
                .read::<Request>(NoPos::new())
                .instrument(header_span)
                .await
        } {
            let end_span = tracing::span!(tracing::Level::TRACE, "server read Request");
            let op = async {
                trace!("Receive req: {v:?}");

                let op = match v {
                    Request::Read {
                        file_id,
                        packet_id,
                        pos,
                        len,
                    } => Operation::Read {
                        file_id,
                        packet_id,
                        pos,
                        len,
                    },
                    Request::Write {
                        file_id,
                        packet_id,
                        pos,
                        len,
                    } => {
                        let mut buf = vec![0; len];
                        stream_raw
                            .read_all(NoPos::new(), &mut buf[..])
                            .await
                            .unwrap();
                        Operation::Write {
                            file_id,
                            packet_id,
                            pos,
                            buf: buf.into(),
                        }
                    }
                    Request::Fs {
                        req_id,
                        dir_id,
                        req_len,
                    } => {
                        let mut buf = vec![0; req_len as usize];
                        stream_raw
                            .read_all(NoPos::new(), &mut buf[..])
                            .await
                            .unwrap();
                        let req: FsRequest = postcard::from_bytes(&buf).unwrap();
                        Operation::Fs {
                            req_id,
                            dir_id,
                            req,
                        }
                    }
                    Request::ReadDir { stream_id, count } => {
                        Operation::ReadDir { stream_id, count }
                    }
                    Request::FileClose { file_id } => Operation::FileClose { file_id },
                };

                op
            }
            .instrument(end_span)
            .await;

            if tx.send_async(op).await.is_err() {
                break;
            }
        }

        core::mem::drop(tx);

        trace!("Ingress loop end");
    }
    .instrument(tracing::span!(tracing::Level::TRACE, "server ingress_loop"));

    let process_loop = async {
        loop {
            match futures::select! {
                res = rx.recv_async() => {
                    Ok(res)
                }
                res = futures.next() => {
                    Err(res)
                }
                complete => break,
            } {
                Ok(Ok(op)) => {
                    trace!("Io thread op {op:?}");
                    let fut = async {
                        trace!("Start process {op:?}");
                        match op {
                            Operation::Read {
                                file_id,
                                packet_id,
                                pos,
                                len,
                            } => {
                                let req_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "read request",
                                    file_id,
                                    packet_id,
                                    pos,
                                    len
                                );
                                async {
                                    /*router.send_bytes(
                                        write_id,
                                        |_| Response::Read {
                                            packet_id,
                                            idx: 0,
                                            len,
                                            err: None,
                                        },
                                        vec![0; len].into_boxed_slice(),
                                    );*/
                                    let packet = ReadPacket {
                                        len,
                                        shards: Default::default(),
                                    };

                                    let fh = file_handles.borrow().get(file_id as usize).cloned();

                                    if let Some(fh) = fh {
                                        log::trace!("Read raw");
                                        let read = {
                                            use mfio::traits::IoRead;
                                            fh.read_raw(pos, &packet)
                                        };
                                        pin_mut!(read);
                                        while let Some((pkt, err)) = read.next().await {
                                            if let Some(err) = err.map(Error::into_int_err) {
                                                log::trace!("Err {err:?}");
                                                router.send_hdr(write_id, |_| Response::Read {
                                                    packet_id,
                                                    idx: pkt.start(),
                                                    len: pkt.len(),
                                                    err: Some(err),
                                                });
                                            } else {
                                                log::trace!("Resp read {}", pkt.len());

                                                let buf = {
                                                    let mut shards = packet.shards.lock();
                                                    shards.remove(&pkt.start()).expect(
                                                        "Successful packet, but no written shard",
                                                    )
                                                };

                                                router.send_bytes(
                                                    write_id,
                                                    |_| Response::Read {
                                                        packet_id,
                                                        idx: pkt.start(),
                                                        len: pkt.len(),
                                                        err: None,
                                                    },
                                                    buf,
                                                );
                                            }
                                        }
                                    }
                                }
                                .instrument(req_span)
                                .await
                            }
                            Operation::Write {
                                file_id,
                                packet_id,
                                pos,
                                buf,
                            } => {
                                let req_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "write request",
                                    file_id,
                                    packet_id,
                                    pos,
                                    len = buf.len()
                                );
                                async {
                                    let fh = file_handles.borrow().get(file_id as usize).cloned();

                                    if let Some(fh) = fh {
                                        let read = {
                                            // TODO: rename IoWrite::write to be unambiguous and
                                            // move this to the root of the module.
                                            use mfio::traits::IoWrite;
                                            fh.write_raw(pos, &buf)
                                        };
                                        pin_mut!(read);
                                        while let Some((pkt, err)) = read.next().await {
                                            router.send_hdr(write_id, |_| Response::Write {
                                                packet_id,
                                                idx: pkt.start(),
                                                len: pkt.len(),
                                                err: err.map(Error::into_int_err),
                                            });
                                        }
                                    }
                                }
                                .instrument(req_span)
                                .await
                            }
                            Operation::ReadDir { stream_id, count } => {
                                let read_dir_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "read dir",
                                    stream_id,
                                    count,
                                );
                                async {
                                    let rm_stream = if let Some(stream) =
                                        read_dir_streams.borrow().get(stream_id as usize)
                                    {
                                        let stream_buf = &mut *stream.lock().await;
                                        // SAFETY: we already ensure the stream is pinned at the
                                        // storage level.
                                        let stream =
                                            unsafe { Pin::new_unchecked(&mut *stream_buf) };

                                        let res = stream
                                            .take(count as usize)
                                            .map(|v| v.map_err(Error::into_int_err))
                                            .collect::<Vec<_>>()
                                            .await;

                                        let buf = postcard::to_allocvec(&res).unwrap();

                                        // TODO: we need to somehow ensure that this condition
                                        // never occurs.
                                        assert_eq!(buf.len() & (1 << 31), 0);
                                        assert!(buf.len() < u32::MAX as usize);

                                        let mut len = buf.len() as u32;

                                        // SAFETY: we already ensure the stream is pinned at its
                                        // storage level.
                                        let stream = unsafe { Pin::new_unchecked(stream_buf) };

                                        if stream.is_terminated() {
                                            len |= 1 << 31;
                                        }

                                        router.send_bytes(
                                            write_id,
                                            |_| Response::ReadDir { stream_id, len },
                                            buf.into_boxed_slice(),
                                        );

                                        stream.is_terminated()
                                    } else {
                                        router.send_hdr(write_id, |_| Response::ReadDir {
                                            stream_id,
                                            len: 0,
                                        });
                                        false
                                    };

                                    if rm_stream {
                                        read_dir_streams.borrow_mut().remove(stream_id as usize);
                                    }
                                }
                                .instrument(read_dir_span)
                                .await
                            }
                            Operation::Fs {
                                req_id,
                                dir_id,
                                req,
                            } => {
                                let req_span = tracing::span!(
                                    tracing::Level::TRACE,
                                    "fs request",
                                    req_id,
                                    dir_id,
                                );
                                async {
                                    let dh = if dir_id > 0 {
                                        let ret = Some(
                                            dir_handles.borrow().get(dir_id as usize - 1).cloned(),
                                        );
                                        ret
                                    } else {
                                        None
                                    };

                                    let dh = dh.as_ref().map(|v| v.as_deref());
                                    let dh = dh.unwrap_or_else(|| Some(fs.current_dir()));

                                    if let Some(dh) = dh {
                                        let resp = match req {
                                            FsRequest::Path => {
                                                trace!("Get path");
                                                let path = dh
                                                    .path()
                                                    .await
                                                    .map(|p| p.to_string_lossy().into())
                                                    .map_err(Error::into_int_err);
                                                FsResponse::Path { path }
                                            }
                                            FsRequest::OpenFile { path, options } => {
                                                trace!("Open file {path}");
                                                let file_id = match dh
                                                    .open_file(Path::new(&path), options)
                                                    .await
                                                {
                                                    Ok(file) => {
                                                        let file_id = file_handles
                                                            .borrow_mut()
                                                            .insert(BaseArc::new(file));

                                                        assert!(file_id <= u32::MAX as usize);

                                                        Ok(file_id as u32)
                                                    }
                                                    Err(err) => Err(err.into_int_err()),
                                                };
                                                trace!("Opened file {file_id:?}");

                                                FsResponse::OpenFile { file_id }
                                            }
                                            FsRequest::OpenDir { path } => {
                                                trace!("Open dir {path}");
                                                let dir_id = match dh.open_dir(path).await {
                                                    Ok(dir) => {
                                                        let dir_id = dir_handles
                                                            .borrow_mut()
                                                            .insert(BaseArc::new(dir))
                                                            + 1;

                                                        assert!(dir_id <= u16::MAX as usize);

                                                        Ok(NonZeroU16::new(dir_id as u16).unwrap())
                                                    }
                                                    Err(err) => Err(err.into_int_err()),
                                                };
                                                trace!("Opened dir {dir_id:?}");

                                                FsResponse::OpenDir { dir_id }
                                            }
                                            FsRequest::ReadDir => {
                                                trace!("Read dir");
                                                let stream_id = match dh.read_dir().await {
                                                    Ok(stream) => {
                                                        let stream_id =
                                                            read_dir_streams.borrow_mut().insert(
                                                                BaseArc::pin(stream.fuse().into()),
                                                            );

                                                        assert!(stream_id <= u16::MAX as usize);

                                                        Ok(stream_id as u16)
                                                    }
                                                    Err(err) => Err(err.into_int_err()),
                                                };
                                                trace!("Opened read handle {stream_id:?}");

                                                FsResponse::ReadDir { stream_id }
                                            }
                                            FsRequest::Metadata { path } => {
                                                trace!("Metadata {path}");
                                                let metadata = dh
                                                    .metadata(path)
                                                    .await
                                                    .map_err(Error::into_int_err);
                                                FsResponse::Metadata { metadata }
                                            }
                                            FsRequest::DirOp(op) => {
                                                trace!("Do dir op");
                                                FsResponse::DirOp(
                                                    dh.do_op(op)
                                                        .await
                                                        .map_err(Error::into_int_err)
                                                        .err(),
                                                )
                                            }
                                        };

                                        let resp = postcard::to_allocvec(&resp).unwrap();

                                        assert!(resp.len() <= u16::MAX as usize);

                                        let resp_len = resp.len() as u16;

                                        router.send_bytes(
                                            write_id,
                                            |_| Response::Fs { req_id, resp_len },
                                            resp.into_boxed_slice(),
                                        );

                                        trace!("Written response for {req_id}");
                                    } else {
                                        router.send_hdr(write_id, |_| Response::Fs {
                                            req_id,
                                            resp_len: 0,
                                        })
                                    }
                                }
                                .instrument(req_span)
                                .await
                            }
                            Operation::FileClose { file_id } => {
                                trace!("Close {file_id}");
                                file_handles.borrow_mut().remove(file_id as _);
                            }
                        }

                        trace!("Finish processing op");
                    };
                    futures.push(fut);
                }
                Ok(Err(_)) => break,
                Err(_) => {}
            }
        }

        log::trace!("Process loop done");
    }
    .instrument(tracing::span!(tracing::Level::TRACE, "server process_loop"));

    let router_loop = router
        .process_loop(write_id)
        .instrument(tracing::span!(tracing::Level::TRACE, "server router_loop"));

    let l1 = async move { futures::join!(process_loop, ingress_loop) }.fuse();
    let l2 = router_loop.fuse();
    pin_mut!(l1);
    pin_mut!(l2);

    futures::select!(_ = l1 => (), _ = l2 => ());
}

pub fn single_client_server(addr: SocketAddr) -> (std::thread::JoinHandle<()>, SocketAddr) {
    let (tx, rx) = flume::bounded(1);

    let ret = std::thread::spawn(move || {
        let fs = NativeRt::default();

        fs.block_on(async {
            let mut listener = fs.bind(addr).await.unwrap();
            let _ = tx.send_async(listener.local_addr().unwrap()).await;
            let (stream, _) = listener.next().await.unwrap();
            run_server(stream, &fs).await
        })
    });

    let addr = rx.recv().unwrap();

    (ret, addr)
}

pub async fn server_bind(fs: &NativeRt, bind_addr: SocketAddr) {
    let listener = fs.bind(bind_addr).await.unwrap();
    server(fs, listener).await
}

pub async fn server(fs: &NativeRt, listener: NativeTcpListener) {
    let clients = listener.fuse();
    futures::pin_mut!(clients);

    // TODO: load balance clients with multiple FS instances per-thread.
    let mut futures = FuturesUnordered::new();

    loop {
        match futures::select! {
            res = clients.next() => {
                Ok(res)
            }
            res = futures.next() => {
                Err(res)
            }
            complete => break,
        } {
            Ok(Some((stream, peer))) => futures.push(async move {
                run_server(stream, fs).await;
                trace!("{peer:?} finished");
                peer
            }),
            Err(peer) => debug!("{peer:?} finished"),
            _ => break,
        }
    }
}

// TODO: test on miri
#[cfg(all(test, not(miri)))]
mod tests {
    use super::super::client::NetworkFs;
    use super::*;
    use mfio::traits::IoRead;
    use mfio_rt::{Fs, OpenOptions};
    use std::path::Path;

    #[test]
    fn fs_test() {
        let _ = ::env_logger::builder().is_test(true).try_init();
        let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        let (server, addr) = single_client_server(addr);

        let fs = mfio_rt::NativeRt::default();
        let fs = NetworkFs::with_fs(addr, fs.into()).unwrap();

        fs.block_on(async {
            println!("Conned");
            let fh = fs
                .open(Path::new("./Cargo.toml"), OpenOptions::new().read(true))
                .await
                .unwrap();
            let mut out = vec![];
            fh.read_to_end(0, &mut out).await.unwrap();
            println!("{}", String::from_utf8(out).unwrap());
        });

        println!("Drop fs");

        core::mem::drop(fs);

        println!("Dropped fs");

        server.join().unwrap();
    }

    mfio_rt::test_suite!(tests, |closure| {
        let _ = ::env_logger::builder().is_test(true).try_init();
        use super::{single_client_server, NetworkFs, SocketAddr};
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (server, addr) = single_client_server(addr);

        let rt = mfio_rt::NativeRt::default();
        let mut rt = NetworkFs::with_fs(addr, rt.into()).unwrap();

        let fs = staticify(&mut rt);

        pub fn run<'a, Func: FnOnce(&'a NetworkFs) -> F, F: Future>(
            fs: &'a mut NetworkFs,
            func: Func,
        ) -> F::Output {
            fs.block_on(func(fs))
        }

        run(fs, closure);

        core::mem::drop(rt);

        log::trace!("Joining thread");

        server.join().unwrap();
    });
}
