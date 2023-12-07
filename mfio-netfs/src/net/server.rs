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
use mfio::backend::IoBackendExt;
use mfio::error::Error;
use mfio::io::{NoPos, OwnedPacket, PacketIoExt, PacketView, PacketVtblRef, Read};
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
        len: u64,
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

#[repr(C)]
struct ReadPacket {
    hdr: Packet<Write>,
    len: u64,
    shards: Mutex<BTreeMap<u64, BaseArc<Packet<Read>>>>,
}

impl ReadPacket {
    pub fn new(capacity: u64) -> Self {
        unsafe extern "C" fn len(pkt: &Packet<Write>) -> u64 {
            unsafe {
                let this = &*(pkt as *const Packet<Write> as *const ReadPacket);
                this.len
            }
        }

        unsafe extern "C" fn get_mut(
            _: &mut ManuallyDrop<BoundPacketView<Write>>,
            _: usize,
            _: &mut MaybeUninit<WritePacketObj>,
        ) -> bool {
            false
        }

        unsafe extern "C" fn transfer_data(obj: &mut PacketView<'_, Write>, src: *const ()) {
            let this = &*(obj.pkt() as *const Packet<Write> as *const ReadPacket);
            let len = obj.len();
            let idx = obj.start();
            let buf = src as *const u8;
            let buf = core::slice::from_raw_parts(buf, len as usize);
            let pkt = Packet::<Read>::copy_from_slice(buf);
            this.shards.lock().insert(idx, pkt);
        }

        Self {
            hdr: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    vtbl: &Write {
                        len,
                        get_mut,
                        transfer_data,
                    },
                })
            },
            len: capacity,
            shards: Default::default(),
        }
    }
}

impl AsRef<Packet<Write>> for ReadPacket {
    fn as_ref(&self) -> &Packet<Write> {
        &self.hdr
    }
}

use core::mem::{ManuallyDrop, MaybeUninit};
use mfio::io::{BoundPacketView, Packet, Write, WritePacketObj};

async fn run_server(stream: NativeTcpStream, fs: &NativeRt) {
    let stream_raw = &stream;
    let mut file_handles: Slab<BaseArc<Seekable<NativeFile, u64>>> = Default::default();
    let file_handles = RefCell::new(&mut file_handles);
    let mut dir_handles: Slab<BaseArc<NativeRtDir>> = Default::default();
    let dir_handles = RefCell::new(&mut dir_handles);
    let mut read_dir_streams: Slab<Pin<BaseArc<AsyncMutex<Fuse<ReadDir>>>>> = Default::default();
    let read_dir_streams = RefCell::new(&mut read_dir_streams);

    let router = BaseArc::new(HeaderRouter::new(stream_raw));

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
                // Verify that the tag is proper, since otherwise we may jump to the wrong place of
                // code. TODO: use proper deserialization techniques
                // SAFETY: memunsafe made safe
                // while adding this check saves us from memory safety bugs, this will probably
                // still lead to arbitrarily large allocations that make us crash.
                let tag = unsafe { *(&v as *const _ as *const u8) };
                assert!(tag < 5, "incoming data tag is invalid {tag}");

                trace!("Receive req: {v:?}");

                match v {
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
                        let mut buf = vec![0; len as usize];
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
                }
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
                                        |_| Response::Read {
                                            packet_id,
                                            idx: 0,
                                            len,
                                            err: None,
                                        },
                                        vec![0; len].into_boxed_slice(),
                                    );*/
                                    let packet = BaseArc::new(ReadPacket::new(len));

                                    let fh = file_handles.borrow().get(file_id as usize).cloned();

                                    if let Some(fh) = fh {
                                        log::trace!("Read raw {len}");

                                        fh.io_to_fn(
                                            pos,
                                            packet.clone().transpose().into_base().unwrap(),
                                            {
                                                let router = router.clone();
                                                move |pkt, err| {
                                                    if let Some(err) = err.map(Error::into_int_err)
                                                    {
                                                        log::trace!("Err {err:?}");
                                                        router.send_hdr(|_| Response::Read {
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

                                                        log::trace!(
                                                            "Send bytes {:?}",
                                                            buf.simple_slice().map(|v| v.len())
                                                        );

                                                        router.send_bytes(
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
                                            },
                                        )
                                        .await;
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
                                        fh.io_to_fn(
                                            pos,
                                            OwnedPacket::<Read>::from(buf.0.into_boxed_slice()),
                                            {
                                                let router = router.clone();
                                                move |pkt, err| {
                                                    router.send_hdr(|_| Response::Write {
                                                        packet_id,
                                                        idx: pkt.start(),
                                                        len: pkt.len(),
                                                        err: err.map(Error::into_int_err),
                                                    })
                                                }
                                            },
                                        )
                                        .await;
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
                                    let stream = read_dir_streams.borrow().get(stream_id as usize).cloned();
                                    let rm_stream = if let Some(stream) = stream
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
                                            |_| Response::ReadDir { stream_id, len },
                                            OwnedPacket::from(buf.into_boxed_slice()),
                                        );

                                        stream.is_terminated()
                                    } else {
                                        router
                                            .send_hdr(|_| Response::ReadDir { stream_id, len: 0 });
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
                                                let dir_id = match dh.open_dir(&path).await {
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
                                                    .metadata(&path)
                                                    .await
                                                    .map_err(Error::into_int_err);
                                                FsResponse::Metadata { metadata }
                                            }
                                            FsRequest::DirOp(op) => {
                                                trace!("Do dir op");
                                                FsResponse::DirOp(
                                                    dh.do_op(op.as_path())
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
                                            |_| Response::Fs { req_id, resp_len },
                                            OwnedPacket::from(resp.into_boxed_slice()),
                                        );

                                        trace!("Written response for {req_id}");
                                    } else {
                                        router.send_hdr(|_| Response::Fs {
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

    let l1 = async move { futures::join!(process_loop, ingress_loop) }.fuse();
    l1.await;
}

fn single_client_server_with(
    addr: SocketAddr,
    fs: NativeRt,
) -> (std::thread::JoinHandle<()>, SocketAddr) {
    let (tx, rx) = flume::bounded(1);

    let ret = std::thread::spawn(move || {
        fs.block_on(async {
            let mut listener = fs.bind(addr).await.unwrap();
            let _ = tx.send_async(listener.local_addr().unwrap()).await;
            let (stream, _) = listener.next().await.unwrap();
            run_server(stream, &fs).await
        });

        trace!("Server done polling");
    });

    let addr = rx.recv().unwrap();

    (ret, addr)
}

pub fn single_client_server(addr: SocketAddr) -> (std::thread::JoinHandle<()>, SocketAddr) {
    single_client_server_with(addr, NativeRt::default())
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

        let fs = mfio_rt::NativeRt::builder().thread(true).build().unwrap();
        let fs = NetworkFs::with_fs(addr, fs.into(), true).unwrap();

        fs.block_on(async {
            println!("Conned");
            let fh = fs
                .open(Path::new("./Cargo.toml"), OpenOptions::new().read(true))
                .await
                .unwrap();
            println!("Got fh");
            let mut out = vec![];
            fh.read_to_end(0, &mut out).await.unwrap();
            println!("Read to end");
            println!("{}", String::from_utf8(out).unwrap());
        });

        println!("Drop fs");

        core::mem::drop(fs);

        println!("Dropped fs");

        server.join().unwrap();
    }

    mfio_rt::test_suite!(tests, |test_name, closure| {
        let _ = ::env_logger::builder().is_test(true).try_init();
        use super::{single_client_server_with, NetworkFs, SocketAddr};
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let mut rt = mfio_rt::NativeRt::default();
        let dir = TempDir::new(test_name).unwrap();
        rt.set_cwd(dir.path().to_path_buf());
        let (server, addr) = single_client_server_with(addr, rt);

        let rt = mfio_rt::NativeRt::default();

        let mut rt = NetworkFs::with_fs(addr, rt.into(), true).unwrap();

        let fs = staticify(&mut rt);

        pub fn run<'a, Func: FnOnce(&'a NetworkFs) -> F, F: Future>(
            fs: &'a mut NetworkFs,
            func: Func,
        ) -> F::Output {
            fs.block_on(func(fs))
        }

        run(fs, move |rt| {
            let run = TestRun::new(rt, dir);
            closure(run)
        });

        core::mem::drop(rt);

        log::trace!("Joining thread");

        server.join().unwrap();
    });
}
