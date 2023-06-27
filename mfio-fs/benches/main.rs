use core::mem::MaybeUninit;
use criterion::async_executor::*;
use criterion::*;
#[cfg(unix)]
use mfio::backend::integrations::{async_io::AsyncIo, tokio::Tokio};
use mfio::backend::*;
use mfio::traits::*;
use mfio_fs::*;
use rand::prelude::*;
use std::fs::{write, File};
use std::path::Path;
use std::time::{Duration, Instant};

struct PollsterExecutor;

impl AsyncExecutor for PollsterExecutor {
    fn block_on<T>(&self, fut: impl core::future::Future<Output = T>) -> T {
        pollster::block_on(fut)
    }
}

#[no_mangle]
static mut FH: *const mfio::stdeq::Seekable<FileWrapper, u64> = core::ptr::null();

fn file_read(c: &mut Criterion) {
    env_logger::init();

    let mut group = c.benchmark_group("File Read");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    const MB: usize = 0x10000;
    const SPARSE: usize = 256 * 4;

    let test_buf = &(0..(MB * SPARSE))
        .map(|i| (i % 256) as u8)
        .collect::<Vec<u8>>();
    let mut temp_path = std::path::PathBuf::from(".");
    temp_path.push("mfio-bench");
    let temp_path = &temp_path;

    let sizes = [/*16,*/ 64, 256, 1024, 4096, 16384, 65536];

    let mut rng = rand::thread_rng();
    let mut order = (0..MB).step_by(sizes[0]).collect::<Vec<_>>();
    order.shuffle(&mut rng);
    let order = &order;

    let drop_cache = |path: &Path| {
        std::process::Command::new("/usr/bin/dd")
            .args([
                &format!("if={}", path.to_str().unwrap()),
                "iflag=nocache",
                "count=0",
            ])
            .output()
    };

    write(temp_path, test_buf).unwrap();

    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("mfio", size), |b| {
            b.iter_custom(|mut iters| {
                let num_chunks = MB / size;
                let mut bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

                drop_cache(temp_path).unwrap();

                let mut elapsed = Duration::default();

                let ret = NativeFs::default().run(|fs| async move {
                    let file = fs.open(temp_path, OpenOptions::new().read(true));
                    unsafe { FH = &file as *const _ };

                    while iters > 0 {
                        let mut output = vec![];
                        output.reserve(num_chunks);

                        let start = Instant::now();

                        for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
                            // Issue a direct read @ here, because we want to queue up multiple
                            // reads and have them all finish concurrently.
                            let fut = file.read_all((i * SPARSE) as u64, &mut b[..]);
                            let fut = async move {
                                fut.await;
                            };
                            output.push(fut);
                        }

                        let _ = futures::future::join_all(output).await;

                        elapsed += start.elapsed();

                        iters = iters.saturating_sub(num_chunks as _);
                    }

                    elapsed
                });
                ret
            });
        });
    }

    #[cfg(unix)]
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("mfio-tokio", size), |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|mut iters| async move {
                    let num_chunks = MB / size;
                    let mut bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

                    drop_cache(temp_path).unwrap();

                    let mut elapsed = Duration::default();

                    let mut fs = NativeFs::default();

                    Tokio::run_with_mut(&mut fs, |fs| async move {
                        let file = fs.open(temp_path, OpenOptions::new().read(true));
                        unsafe { FH = &file as *const _ };

                        while iters > 0 {
                            let mut output = vec![];
                            output.reserve(num_chunks);

                            let start = Instant::now();

                            for (i, b) in
                                order.iter().take(iters as _).copied().zip(bufs.iter_mut())
                            {
                                // Issue a direct read @ here, because we want to queue up multiple
                                // reads and have them all finish concurrently.
                                let fut = file.read_all((i * SPARSE) as u64, &mut b[..]);
                                output.push(fut);
                            }

                            let _ = futures::future::join_all(output).await;

                            elapsed += start.elapsed();

                            iters = iters.saturating_sub(num_chunks as _);
                        }

                        elapsed
                    })
                    .await
                });
        });
    }

    #[cfg(unix)]
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("mfio-smol", size), |b| {
            b.to_async(SmolExecutor)
                .iter_custom(|mut iters| async move {
                    let num_chunks = MB / size;
                    let mut bufs = vec![vec![MaybeUninit::uninit(); size]; num_chunks];

                    drop_cache(temp_path).unwrap();

                    let mut elapsed = Duration::default();

                    let mut fs = NativeFs::default();

                    AsyncIo::run_with_mut(&mut fs, |fs| async move {
                        let file = fs.open(temp_path, OpenOptions::new().read(true));
                        unsafe { FH = &file as *const _ };

                        while iters > 0 {
                            let mut output = vec![];
                            output.reserve(num_chunks);

                            let start = Instant::now();

                            for (i, b) in
                                order.iter().take(iters as _).copied().zip(bufs.iter_mut())
                            {
                                // Issue a direct read @ here, because we want to queue up multiple
                                // reads and have them all finish concurrently.
                                let fut = file.read_all((i * SPARSE) as u64, &mut b[..]);
                                output.push(fut);
                            }

                            let _ = futures::future::join_all(output).await;

                            elapsed += start.elapsed();

                            iters = iters.saturating_sub(num_chunks as _);
                        }

                        elapsed
                    })
                    .await
                });
        });
    }
    #[cfg(target_os = "linux")]
    for size in sizes {
        use glommio::io::BufferedFile;
        use glommio::LocalExecutor;

        struct GlommioExecutor(LocalExecutor);

        impl<'a> AsyncExecutor for &'a GlommioExecutor {
            fn block_on<T>(&self, fut: impl core::future::Future<Output = T>) -> T {
                self.0.run(fut)
            }
        }

        let glommio = GlommioExecutor(LocalExecutor::default());

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("glommio", size), |b| {
            b.to_async(&glommio).iter_custom(|mut iters| async move {
                let num_chunks = MB / size;
                let mut bufs = vec![vec![0u8; size]; num_chunks];

                drop_cache(temp_path).unwrap();

                let file = &BufferedFile::open(temp_path).await.unwrap();

                let mut elapsed = Duration::default();

                while iters > 0 {
                    let mut output = vec![];
                    output.reserve(num_chunks);

                    let start = Instant::now();

                    for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
                        let comp = async move {
                            let res = file.read_at((i * SPARSE) as u64, b.len()).await.unwrap();
                            b.copy_from_slice(&res[..]);
                        };
                        output.push(comp);
                    }

                    let _ = futures::future::join_all(output).await;

                    elapsed += start.elapsed();

                    iters = iters.saturating_sub(num_chunks as _);
                }

                elapsed
            });
        });
    }

    /*#[cfg(unix)]
    for size in sizes {
        use futures::AsyncReadExt;
        use futures::AsyncSeekExt;
        use nuclei::Handle;
        use std::io::SeekFrom;

        struct NucleiExecutor;

        impl AsyncExecutor for NucleiExecutor {
            fn block_on<T>(&self, fut: impl core::future::Future<Output = T>) -> T {
                nuclei::drive(fut)
            }
        }

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("nuclei", size), |b| {
            b.to_async(NucleiExecutor)
                .iter_custom(|mut iters| async move {
                    let num_chunks = MB / size;
                    let mut bufs = vec![vec![0u8; size]; num_chunks];

                    drop_cache(temp_path).unwrap();

                    let file = File::open(temp_path).unwrap();
                    let file = &mut Handle::<File>::new(file).unwrap();

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
    }*/

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
            b.to_async(PollsterExecutor)
                .iter_custom(|mut iters| async move {
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

    #[cfg(target_os = "linux")]
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        let ring = &rio::new().unwrap();

        group.bench_function(BenchmarkId::new("rio", size), |b| {
            b.to_async(PollsterExecutor)
                .iter_custom(|mut iters| async move {
                    let num_chunks = MB / size;
                    let mut bufs = vec![vec![0u8; size]; num_chunks];

                    drop_cache(temp_path).unwrap();

                    let file = File::open(temp_path).unwrap();

                    let mut elapsed = Duration::default();

                    while iters > 0 {
                        let mut output = vec![];
                        output.reserve(num_chunks);

                        let start = Instant::now();

                        for (i, b) in order.iter().take(iters as _).copied().zip(bufs.iter_mut()) {
                            let comp = ring.read_at(&file, b, (i * SPARSE) as u64);
                            output.push(comp);
                        }

                        for comp in output {
                            comp.wait().unwrap();
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
