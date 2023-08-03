use core::mem::MaybeUninit;
use futures::stream::{FuturesUnordered, StreamExt};
use mfio::backend::*;
use mfio::traits::*;
use mfio_rt::*;
use rand::prelude::*;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::instrument::Instrument;

fn main() {
    env_logger::init();
    use tracing_subscriber::layer::SubscriberExt;

    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(tracing_tracy::TracyLayer::new()),
    )
    .expect("set up the subscriber");

    println!("Press enter to continue");

    let _ = std::io::stdin().read(&mut [0u8]);

    let sizes = [1 /*1, 2, 4, 8, 16, 64, 256, 1024, 4096, 16384, 65536*/];
    let iters = 0x1000;

    let mut rng = rand::thread_rng();
    let mut order = (0..MB).step_by(sizes[0]).collect::<Vec<_>>();
    order.shuffle(&mut rng);
    let order = &order;

    let mut temp_path = std::path::PathBuf::from(".");
    temp_path.push("mfio-netfs-trace");
    let temp_path = &temp_path;

    if fs::metadata(temp_path)
        .map(|v| v.len() != (MB * SPARSE) as u64)
        .unwrap_or(true)
    {
        let instant = Instant::now();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(temp_path)
            .unwrap();
        let buf = (0..(MB * 8)).map(|v| (v % 256) as u8).collect::<Vec<_>>();
        for _ in 0..(MB * SPARSE / buf.len()) {
            file.write_all(&buf).unwrap();
        }
        let elapsed = instant.elapsed();
        println!(
            "Elapsed: {:.2}; Bandwidth {:.2} MB/s",
            elapsed.as_secs_f64(),
            SPARSE as f64 / elapsed.as_secs_f64()
        );
    }

    println!("mfio-net:");

    for size in sizes {
        println!("Run {size}:");
        let (elapsed, total_read) = bench(iters, size, order, temp_path);
        println!(
            "Elapsed: {:.2}; Bandwidth {:.2} MB/s ({:.2} MB)",
            elapsed.as_secs_f64(),
            (total_read / MB) as f64 / elapsed.as_secs_f64(),
            (total_read / MB)
        );
    }

    println!("std:");

    for size in sizes {
        println!("Run {size}:");
        let (elapsed, total_read) = bench_std(iters, size, order, temp_path);
        println!(
            "Elapsed: {:.2}; Bandwidth {:.2} MB/s ({:.2} MB)",
            elapsed.as_secs_f64(),
            (total_read / MB) as f64 / elapsed.as_secs_f64(),
            (total_read / MB)
        );
    }

    for hdr in [0, 1, 32] {
        println!("tcp {hdr}:");

        for size in sizes {
            let rt = tokio::runtime::Runtime::new().unwrap();

            let (elapsed, total_read) = rt.block_on(bench_tcp(iters, size, hdr));

            println!(
                "Elapsed: {:.2}; Bandwidth {:.2} MB/s ({:.2} MB)",
                elapsed.as_secs_f64(),
                (total_read / MB) as f64 / elapsed.as_secs_f64(),
                (total_read / MB)
            );
        }
    }
}

const MB: usize = 0x10000;
const SPARSE: usize = 256 * 4 * 64;

fn drop_cache(path: &Path) -> std::io::Result<std::process::Output> {
    std::process::Command::new("/usr/bin/env")
        .args([
            "dd",
            &format!("if={}", path.to_str().unwrap()),
            "iflag=nocache",
            "count=0",
        ])
        .output()
}

async fn file_read(
    file: &impl IoRead<u64>,
    i: usize,
    num_chunks: usize,
    order: &[usize],
    mut b: Vec<MaybeUninit<u8>>,
) -> Vec<MaybeUninit<u8>> {
    file.read_all(
        (order[i % order.len()] * SPARSE % num_chunks) as u64,
        &mut b[..],
    )
    .await
    .unwrap();
    b
}

fn bench(mut iters: usize, size: usize, order: &[usize], path: &Path) -> (Duration, usize) {
    let num_chunks = (MB * 64) / size;
    let bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

    let _elapsed = Duration::default();
    let mut total_read = 0;

    let addr: std::net::SocketAddr = "127.0.0.1:54321".parse().unwrap();

    drop_cache(path).unwrap();

    let server = mfio_netfs::single_client_server(addr);
    let fs = mfio_netfs::NetworkFs::try_new(addr).unwrap();

    let elapsed = fs.block_on(async {
        let file = fs.open(path, OpenOptions::new().read(true)).await.unwrap();

        let mut futures = FuturesUnordered::new();

        let mut i = 0;

        for b in bufs.into_iter().take(iters) {
            i += 1;
            iters -= 1;
            futures.push(file_read(&file, i, num_chunks, order, b));
        }

        let now = Instant::now();

        loop {
            futures::select! {
                b = futures.next() => {
                    if let Some(b) = b {
                        total_read += b.len();

                        if iters > 0 {
                            i += 1;
                            iters -= 1;
                            futures.push(file_read(&file, i, num_chunks, order, b));
                        }
                    } else {
                        break;
                    }
                },
            }
        }

        assert_eq!(iters, 0);

        now.elapsed()
    });

    core::mem::drop(fs);
    server.join().unwrap();

    (elapsed, total_read)
}

#[tracing::instrument(skip(order))]
fn bench_std(mut iters: usize, size: usize, order: &[usize], path: &Path) -> (Duration, usize) {
    let num_chunks = MB / size;
    let mut bufs = vec![vec![0u8; size]; num_chunks];

    let mut elapsed = Duration::default();
    let mut total_read = 0;

    drop_cache(path).unwrap();

    let mut file = File::open(path).unwrap();

    while iters > 0 {
        let start = Instant::now();

        for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
            total_read += b.len();
            {
                let span = tracing::span!(tracing::Level::TRACE, "seek", pos = (i * SPARSE));
                let _guard = span.enter();
                file.seek(SeekFrom::Start((i * SPARSE) as u64)).unwrap();
            }
            let span = tracing::span!(tracing::Level::TRACE, "read", len = b.len());
            let _guard = span.enter();
            file.read_exact(&mut b[..]).unwrap();
        }

        elapsed += start.elapsed();

        iters = iters.saturating_sub(num_chunks as _);
    }

    (elapsed, total_read)
}

async fn bench_tcp(iters: usize, size: usize, header_size: usize) -> (Duration, usize) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::io::{BufReader, BufWriter};
    use tokio::net::TcpStream;

    use std::net::TcpListener;

    let addr: std::net::SocketAddr = "127.0.0.1:44321".parse().unwrap();

    let listener = TcpListener::bind(addr).unwrap();

    let listen = tokio::task::spawn_blocking(move || {
        use std::io::BufReader;
        let (mut client, _) = listener.accept().unwrap();

        let span = tracing::span!(tracing::Level::TRACE, "TCP server");
        let _guard = span.enter();
        let span2 = tracing::span!(tracing::Level::TRACE, "recv");
        let span3 = tracing::span!(tracing::Level::TRACE, "send");

        let buf = vec![0; size + header_size];

        if header_size != 0 {
            let mut rq = vec![0; header_size];
            let mut recv = BufReader::new(client.try_clone().unwrap());
            while {
                let _g = span2.enter();
                recv.read_exact(&mut rq)
            }
            .is_ok()
            {
                let _g = span3.enter();
                client.write_all(&buf).unwrap();
            }
        } else {
            for _ in 0..iters {
                let _g = span3.enter();
                client.write_all(&buf).unwrap();
            }
        }
        client.flush().unwrap();
    });

    let listen = tokio::spawn(listen);

    let client = async {
        let mut client = TcpStream::connect(addr).await.unwrap();
        let start = Instant::now();
        let (recv, send) = client.split();

        let mut recv = BufReader::new(recv);
        let mut send = BufWriter::new(send);

        let send = async {
            let rq = vec![0; header_size];
            if !rq.is_empty() {
                for i in 0..iters {
                    send.write_all(&rq)
                        .instrument(tracing::trace_span!("send", i))
                        .await
                        .unwrap();
                }
                send.flush().await.unwrap();
            }
        }
        .instrument(tracing::span!(tracing::Level::TRACE, "TCP client send"));

        let recv = async {
            let mut total_read = 0;
            let mut buf = vec![0; size + header_size];
            for i in 0..iters {
                recv.read_exact(&mut buf)
                    .instrument(tracing::trace_span!("recv", i))
                    .await
                    .unwrap();
                total_read += buf.len() - header_size;
            }
            total_read
        }
        .instrument(tracing::span!(tracing::Level::TRACE, "TCP client recv"));

        let read = futures::join!(send, recv).1;
        (start.elapsed(), read)
    };

    futures::join!(listen, client).1
}
