use core::pin::Pin;
use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener, TcpStream};

use super::{HeaderRouter, Request, Response};

use cglue::result::IntError;
use log::*;
use mfio::backend::IoBackend;
use mfio::error::Error;
use mfio::packet::{NoPos, PacketIo};
use mfio::stdeq::Seekable;
use mfio::tarc::BaseArc;
use mfio_rt::{FileWrapper, Fs, NativeRt};
use parking_lot::Mutex;
use slab::Slab;
use tracing::instrument::Instrument;

use futures::{
    future::FutureExt,
    pin_mut,
    stream::{FusedStream, FuturesUnordered, Stream, StreamExt},
};
use mfio_rt::OpenOptions;
use std::path::Path;

struct SessionState {
    stream: TcpStream,
    file_handles: Slab<BaseArc<Seekable<FileWrapper, u64>>>,
}

impl From<TcpStream> for SessionState {
    fn from(stream: TcpStream) -> Self {
        Self {
            stream,
            file_handles: Default::default(),
        }
    }
}

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
        buf: Vec<u8>,
    },
    FileOpen {
        req_id: u32,
        path: String,
        options: OpenOptions,
    },
    FileClose {
        file_id: u32,
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

impl SessionState {
    async fn run(mut self, fs: &NativeRt) {
        let stream_raw = &fs.register_stream(self.stream);
        let file_handles = core::cell::RefCell::new(&mut self.file_handles);

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
                let header_span =
                    tracing::span!(tracing::Level::TRACE, "server read Request header");
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
                                buf,
                            }
                        }
                        Request::FileOpen {
                            req_id,
                            options,
                            path_len,
                        } => {
                            let mut buf = vec![0; path_len as usize];
                            stream_raw
                                .read_all(NoPos::new(), &mut buf[..])
                                .await
                                .unwrap();
                            let path = String::from_utf8(buf).unwrap();
                            Operation::FileOpen {
                                req_id,
                                path,
                                options,
                            }
                        }
                        Request::FileClose { file_id } => Operation::FileClose { file_id },
                    };

                    trace!("Parsed op: {op:?}");

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

                                        let fh =
                                            file_handles.borrow().get(file_id as usize).cloned();

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
                                        let fh =
                                            file_handles.borrow().get(file_id as usize).cloned();

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
                                Operation::FileOpen {
                                    req_id,
                                    path,
                                    options,
                                } => {
                                    let req_span = tracing::span!(
                                        tracing::Level::TRACE,
                                        "open request",
                                        req_id,
                                        path
                                    );
                                    async {
                                        trace!("Open file");
                                        let (file_id, err) =
                                            match fs.open(Path::new(&path), options).await {
                                                Ok(file) => (
                                                    file_handles
                                                        .borrow_mut()
                                                        .insert(BaseArc::new(file)),
                                                    None,
                                                ),
                                                Err(err) => (0, Some(err.into_int_err())),
                                            };
                                        trace!("Opened {file_id} {err:?}");

                                        assert!(file_id <= u32::MAX as usize);

                                        router.send_hdr(write_id, |_| Response::FileOpen {
                                            req_id,
                                            file_id: file_id as u32,
                                            err,
                                        });

                                        trace!("Written file open");
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

    //#[tracing::instrument(skip_all)]
    fn block_on(self) {
        let fs = NativeRt::default();
        fs.block_on(self.run(&fs))
    }
}

pub fn serve(sock: SocketAddr) {
    let listener = TcpListener::bind(sock).unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let state = SessionState::from(stream);
        std::thread::spawn(move || state.block_on());
    }
}

pub fn single_client_server(addr: SocketAddr) -> std::thread::JoinHandle<()> {
    let (tx, rx) = flume::bounded(0);
    let ret = std::thread::spawn(move || {
        let listener = TcpListener::bind(addr).unwrap();

        tx.send(()).unwrap();

        let stream = listener.incoming().next().unwrap().unwrap();

        let state = SessionState::from(stream);
        state.block_on();
    });

    rx.recv().unwrap();

    ret
}

pub async fn server_bind(fs: &NativeRt, bind_addr: SocketAddr) {
    let (tx, rx) = flume::bounded(0);

    let listener = TcpListener::bind(bind_addr).unwrap();

    std::thread::spawn(move || {
        // TODO: accept in async fashion.
        while let Some(Ok(stream)) = listener.incoming().next() {
            if tx.send(stream).is_err() {
                break;
            }
        }
    });

    server(fs, rx.into_stream()).await
}

pub async fn server<T: Stream<Item = TcpStream> + FusedStream>(fs: &NativeRt, clients: T) {
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
            Ok(Some(stream)) => futures.push(async move {
                let peer = stream.peer_addr();

                let state = SessionState::from(stream);
                state.run(fs).await;

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
    use mfio_rt::Fs;
    use std::path::Path;

    #[test]
    fn fs_test() {
        env_logger::init();
        let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        let server = single_client_server(addr);

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
}
