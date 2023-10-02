use core::mem::MaybeUninit;
use criterion::*;
use futures::stream::{FuturesUnordered, StreamExt};
#[cfg(unix)]
use mfio::backend::integrations::tokio::Tokio;
use mfio::backend::*;
use mfio::traits::*;
use mfio_rt::*;
use rand::prelude::*;
use std::fs::{write, File};
use std::path::Path;
use std::time::{Duration, Instant};

#[no_mangle]
static mut FH: *const mfio::stdeq::Seekable<NativeFile, u64> = core::ptr::null();

const MB: usize = 0x10000;
const SPARSE: usize = 256 * 4;

async fn fread(
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

async fn mfio_bench(
    fs: &impl Fs,
    size: usize,
    iters: u64,
    order: &[usize],
    temp_path: &Path,
) -> Duration {
    let mut iters = iters as usize;

    let num_chunks = MB / size;
    let bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

    let file = fs
        .open(temp_path, OpenOptions::new().read(true))
        .await
        .unwrap();

    let _elapsed = Duration::default();

    let mut futures = FuturesUnordered::new();
    let mut i = 0;

    for b in bufs.into_iter().take(iters) {
        i += 1;
        iters -= 1;
        futures.push(fread(&file, i, num_chunks, order, b));
    }

    let now = Instant::now();

    loop {
        futures::select! {
            b = futures.next() => {
                if let Some(b) = b {
                    if iters > 0 {
                        i += 1;
                        iters -= 1;
                        futures.push(fread(&file, i, num_chunks, order, b));
                    }
                } else {
                    break;
                }
            },
        }
    }

    assert_eq!(iters, 0);

    now.elapsed()
}

#[tracing::instrument(skip_all)]
fn file_read(c: &mut Criterion) {
    env_logger::init();

    let mut group = c.benchmark_group("File Read");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    let test_buf = &(0..(MB * SPARSE))
        .map(|i| (i % 256) as u8)
        .collect::<Vec<u8>>();
    let mut temp_path = std::path::PathBuf::from(".");
    temp_path.push("mfio-bench");
    let temp_path = &temp_path;

    let sizes = [/* 16, */ 64, 256, 1024, 4096, 16384, 65536];

    let mut rng = rand::thread_rng();
    let mut order = (0..MB).step_by(sizes[0]).collect::<Vec<_>>();
    order.shuffle(&mut rng);
    let order = &order;

    let drop_cache = |path: &Path| {
        std::process::Command::new("/usr/bin/env")
            .args([
                "dd",
                &format!("if={}", path.to_str().unwrap()),
                "iflag=nocache",
                "count=0",
            ])
            .output()
    };

    write(temp_path, test_buf).unwrap();

    #[cfg(unix)]
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        let addr: std::net::SocketAddr = "127.0.0.1:54321".parse().unwrap();

        group.bench_function(BenchmarkId::new("tcp-tokio", size), |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|iters| async move {
                    //println!("Do {iters}");
                    let _num_chunks = MB / size;
                    //let mut bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    use tokio::io::{BufReader, BufWriter};
                    use tokio::net::{TcpListener, TcpStream};

                    let listener = TcpListener::bind(addr).await.unwrap();

                    let listen = async move {
                        let (mut client, _) = listener.accept().await.unwrap();

                        let buf = vec![0; size];

                        let (recv, mut send) = client.split();
                        let mut recv = BufReader::new(recv);
                        //let mut send = BufWriter::new(send);

                        while let Ok(_v) = recv.read_u8().await {
                            send.write_all(&buf).await.unwrap();
                        }
                        send.flush().await.unwrap();
                    };

                    let listen = tokio::spawn(listen);

                    let client = async {
                        let mut client = TcpStream::connect(addr).await.unwrap();
                        let start = Instant::now();
                        let (recv, send) = client.split();

                        let mut recv = BufReader::new(recv);
                        let mut send = BufWriter::new(send);

                        let send = async move {
                            for _ in 0..iters {
                                send.write_all(&[0u8]).await.unwrap();
                            }
                            let _ = send.flush().await;
                        };

                        let recv = async move {
                            let mut buf = vec![0; size];
                            for _ in 0..iters {
                                recv.read_exact(&mut buf).await.unwrap();
                            }
                        };

                        futures::join!(send, recv);
                        start.elapsed()
                    };

                    futures::join!(listen, client).1
                });
        });
    }

    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        let addr: std::net::SocketAddr = "127.0.0.1:54321".parse().unwrap();

        group.bench_function(BenchmarkId::new("mfio-net", size), |b| {
            b.iter_custom(|iters| {
                drop_cache(temp_path).unwrap();

                //println!("Create thing");

                let (server, addr) = mfio_netfs::single_client_server(addr);
                let fs = mfio_netfs::NetworkFs::try_new(addr).unwrap();

                let elapsed = fs.block_on(mfio_bench(&fs, size, iters, order, temp_path));

                core::mem::drop(fs);
                server.join().unwrap();

                elapsed
            });
        });
    }

    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("mfio", size), |b| {
            b.iter_custom(|iters| {
                drop_cache(temp_path).unwrap();

                let _elapsed = Duration::default();

                NativeRt::default().run(|fs| mfio_bench(fs, size, iters, order, temp_path))
            });
        });
    }

    #[cfg(unix)]
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("mfio-tokio", size), |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|iters| async move {
                    drop_cache(temp_path).unwrap();

                    let mut fs = NativeRt::default();

                    Tokio::run_with_mut(&mut fs, |fs| mfio_bench(fs, size, iters, order, temp_path))
                        .await
                });
        });
    }

    for size in sizes {
        use tokio::fs::*;
        use tokio::io::*;

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("tokio", size), |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|mut iters| async move {
                    let num_chunks = MB / size;
                    let mut bufs = vec![vec![0u8; size]; num_chunks];

                    drop_cache(temp_path).unwrap();

                    let mut file = File::open(temp_path).await.unwrap();

                    let mut elapsed = Duration::default();

                    while iters > 0 {
                        let start = Instant::now();

                        for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
                            file.seek(SeekFrom::Start((i * SPARSE) as u64))
                                .await
                                .unwrap();
                            file.read_exact(&mut b[..]).await.unwrap();
                        }

                        elapsed += start.elapsed();

                        iters = iters.saturating_sub(num_chunks as _);
                    }

                    elapsed
                });
        });
    }

    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("std", size), |b| {
            b.iter_custom(|mut iters| {
                use std::io::{Read, Seek, SeekFrom};
                let num_chunks = MB / size;
                let mut bufs = vec![vec![0u8; size]; num_chunks];

                drop_cache(temp_path).unwrap();

                let mut elapsed = Duration::default();

                let mut file = File::open(temp_path).unwrap();

                while iters > 0 {
                    file.rewind().unwrap();

                    let start = Instant::now();

                    for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
                        file.seek(SeekFrom::Start((i * SPARSE) as u64)).unwrap();
                        file.read_exact(&mut b[..]).unwrap();
                    }

                    elapsed += start.elapsed();

                    iters = iters.saturating_sub(num_chunks as _);
                }

                elapsed
            });
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        //.plotting_backend(PlottingBackend::Plotters)
        .with_plots()
        .warm_up_time(std::time::Duration::from_millis(1000))
        .measurement_time(std::time::Duration::from_millis(5000));
    targets =
        file_read,
}
criterion_main!(benches);
