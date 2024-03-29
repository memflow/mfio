use core::future::IntoFuture;
use core::mem::MaybeUninit;
use criterion::async_executor::*;
use criterion::measurement::Measurement;
use criterion::*;
use futures::StreamExt;
use mfio::backend::*;
use mfio::io::*;
use mfio::traits::*;
use std::cell::RefCell;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime as TokioRuntime;

use sample::*;

mod sample {
    include!("../src/sample.rs");
}

struct PollsterExecutor;

impl AsyncExecutor for PollsterExecutor {
    fn block_on<T>(&self, fut: impl core::future::Future<Output = T>) -> T {
        pollster::block_on(fut)
    }
}

trait AsyncExecutor2: AsyncExecutor {
    fn executor() -> Self;
}

impl AsyncExecutor2 for FuturesExecutor {
    fn executor() -> Self {
        Self
    }
}

impl AsyncExecutor2 for SmolExecutor {
    fn executor() -> Self {
        Self
    }
}

impl AsyncExecutor2 for TokioRuntime {
    fn executor() -> Self {
        Self::new().unwrap()
    }
}

impl AsyncExecutor2 for PollsterExecutor {
    fn executor() -> Self {
        Self
    }
}

trait AsyncExecutor3: AsyncExecutor2 {
    type Output<T>;
    type JoinHandle<T: Send + 'static>: IntoFuture<Output = Self::Output<T>>;

    fn executor_threaded(threads: usize) -> Self;

    fn spawn<T: Send + 'static>(
        fut: impl core::future::Future<Output = T> + Send + 'static,
    ) -> Self::JoinHandle<T>;
    fn unwrap<T>(out: Self::Output<T>) -> T;
}

impl AsyncExecutor3 for TokioRuntime {
    type Output<T> = Result<T, tokio::task::JoinError>;
    type JoinHandle<T: Send + 'static> = tokio::task::JoinHandle<T>;

    fn executor_threaded(threads: usize) -> Self {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .build()
            .unwrap()
    }

    fn spawn<T: Send + 'static>(
        fut: impl core::future::Future<Output = T> + Send + 'static,
    ) -> Self::JoinHandle<T> {
        tokio::spawn(fut)
    }

    fn unwrap<T>(out: Self::Output<T>) -> T {
        out.unwrap()
    }
}

fn singlestream_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Singlestream reads");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    fn read_with<T: AsyncExecutor2>(
        group: &mut BenchmarkGroup<impl Measurement<Value = Duration>>,
    ) {
        for size in [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536] {
            let mut handle = SampleIo::default();

            group.throughput(Throughput::Elements(size as u64));

            group.bench_function(
                BenchmarkId::new(
                    &format!(
                        "read {}",
                        std::any::type_name::<T>().split("::").last().unwrap()
                    ),
                    size,
                ),
                #[allow(clippy::await_holding_refcell_ref)]
                move |b| {
                    let handle = &RefCell::new(&mut handle);

                    b.to_async(T::executor()).iter_custom(|iters| async move {
                        let bufs = (0..size)
                            .map(|_| Packet::<Write>::new_buf(1))
                            .collect::<Vec<_>>();

                        Null::run_with_mut(*handle.borrow_mut(), |scope| async move {
                            let mut elapsed = Duration::default();

                            for _ in 0..iters {
                                let start = Instant::now();

                                for b in &bufs {
                                    unsafe { b.reset_err() };
                                    let pv = PacketView::from_arc_ref(b, 0);
                                    let bpv = unsafe { pv.bind(None) };
                                    scope.send_io(0, bpv);
                                }

                                // We can await for a packet until it has no more active views, and
                                // then reuse it again at the next iteration of the loop.
                                futures::stream::iter(&bufs).for_each(|v| (&**v)).await;
                                black_box(());

                                elapsed += start.elapsed();
                            }

                            elapsed
                        })
                        .await
                    });
                },
            );
        }
    }

    read_with::<FuturesExecutor>(&mut group);
    read_with::<SmolExecutor>(&mut group);
    read_with::<TokioRuntime>(&mut group);
    read_with::<PollsterExecutor>(&mut group);
}

fn reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Reads");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    fn read_with<T: AsyncExecutor2>(
        group: &mut BenchmarkGroup<impl Measurement<Value = Duration>>,
    ) {
        for size in [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536] {
            let mut handle = SampleIo::default();

            group.throughput(Throughput::Elements(size as u64));

            group.bench_function(
                BenchmarkId::new(
                    &format!(
                        "read {}",
                        std::any::type_name::<T>().split("::").last().unwrap()
                    ),
                    size,
                ),
                #[allow(clippy::await_holding_refcell_ref)]
                |b| {
                    let handle = &RefCell::new(&mut handle);

                    b.to_async(T::executor()).iter_custom(|iters| async move {
                        let bufs = (0..size)
                            .map(|_| Packet::<Write>::new_buf(1))
                            .collect::<Vec<_>>();

                        Null::run_with_mut(*handle.borrow_mut(), |scope| async move {
                            let mut elapsed = Duration::default();

                            for _ in 0..iters {
                                let streams = futures::stream::iter(bufs.iter().map(|b| {
                                    unsafe { b.reset_err() };
                                    scope.io(0, b)
                                }));

                                let start = Instant::now();

                                streams
                                    .for_each(|v| async move {
                                        v.await;
                                    })
                                    .await;
                                black_box(());

                                elapsed += start.elapsed();
                            }

                            elapsed
                        })
                        .await
                    });
                },
            );
        }
    }

    read_with::<FuturesExecutor>(&mut group);
    read_with::<SmolExecutor>(&mut group);
    read_with::<TokioRuntime>(&mut group);
    read_with::<PollsterExecutor>(&mut group);
}

fn direct_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Direct Reads");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    fn read_with<T: AsyncExecutor2>(
        group: &mut BenchmarkGroup<impl Measurement<Value = Duration>>,
    ) {
        for size in [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536] {
            let mut handle = SampleIo::default();

            group.throughput(Throughput::Elements(size as u64));

            group.bench_function(
                BenchmarkId::new(
                    &format!(
                        "read {}",
                        std::any::type_name::<T>().split("::").last().unwrap()
                    ),
                    size,
                ),
                #[allow(clippy::await_holding_refcell_ref)]
                |b| {
                    let handle = &RefCell::new(&mut handle);

                    b.to_async(T::executor()).iter_custom(|iters| async move {
                        let mut bufs = vec![MaybeUninit::new(0u8); size];

                        Null::run_with_mut(*handle.borrow_mut(), |scope| async move {
                            let mut elapsed = Duration::default();

                            for _ in 0..iters {
                                let streams = futures::stream::iter(
                                    bufs.iter_mut().map(|b| scope.read_into(0, b)),
                                )
                                .buffered(size);

                                let start = Instant::now();

                                streams
                                    .for_each(|v| async move {
                                        v.unwrap();
                                    })
                                    .await;
                                black_box(());

                                elapsed += start.elapsed();
                            }

                            elapsed
                        })
                        .await
                    });
                },
            );
        }
    }

    read_with::<FuturesExecutor>(&mut group);
    read_with::<SmolExecutor>(&mut group);
    read_with::<TokioRuntime>(&mut group);
    read_with::<PollsterExecutor>(&mut group);
}
fn reads_tasked(c: &mut Criterion) {
    let mut group = c.benchmark_group("Thread scaling");

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config);

    fn read_with<T: AsyncExecutor3>(
        group: &mut BenchmarkGroup<impl Measurement<Value = Duration>>,
    ) {
        for tasks in [1, 2, 4, 8, 16, 32, 64] {
            let handle = &SampleIo::default();
            let size = 1024 / tasks;

            group.throughput(Throughput::Elements(size * tasks));

            group.bench_function(
                BenchmarkId::new(
                    &format!(
                        "read {}",
                        std::any::type_name::<T>().split("::").last().unwrap()
                    ),
                    tasks,
                ),
                |b| {
                    b.to_async(T::executor_threaded((tasks + 1) as _))
                        .iter_custom(|iters| async move {
                            let start = Instant::now();

                            let join_tasks = (0..tasks).map(|_| {
                                let bufs = (0..size)
                                    .map(|_| Packet::<Write>::new_buf(1))
                                    .collect::<Vec<_>>();

                                T::spawn({
                                    let subtract = Instant::now();

                                    let mut scope = handle.clone();
                                    async move {
                                        Null::run_with_mut(&mut scope, move |scope| async move {
                                            let mut subtract = subtract.elapsed();

                                            for _ in 0..iters {
                                                let s2 = Instant::now();

                                                let streams =
                                                    futures::stream::iter(bufs.iter().map(|b| {
                                                        unsafe { b.reset_err() };
                                                        scope.io(0, b.clone())
                                                    }));

                                                subtract += s2.elapsed();

                                                streams
                                                    .for_each(|v| async move {
                                                        v.await;
                                                    })
                                                    .await;
                                                black_box(());
                                            }

                                            subtract
                                        })
                                        .await
                                    }
                                })
                            });

                            //let mut elapsed = Duration::default();
                            let mut subtract = Duration::default();

                            for task in join_tasks {
                                subtract = core::cmp::max(T::unwrap(task.await), subtract);
                                //elapsed += T::unwrap(task.await);
                            }

                            start.elapsed() - subtract
                        });
                },
            );
        }
    }

    //read_with::<FuturesExecutor>(&mut group);
    //read_with::<SmolExecutor>(&mut group);
    read_with::<TokioRuntime>(&mut group);
    //read_with::<PollsterExecutor>(&mut group);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        //.plotting_backend(PlottingBackend::Plotters)
        .with_plots()
        .warm_up_time(std::time::Duration::from_millis(1000))
        .measurement_time(std::time::Duration::from_millis(5000));
    targets =
        direct_reads,
        reads_tasked,
        singlestream_reads,
        reads,
}
criterion_main!(benches);
