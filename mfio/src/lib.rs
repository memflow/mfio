pub mod heap;
pub mod packet;
pub mod shared_future;
#[cfg(test)]
mod sample {
    include!("sample.rs");
}

#[cfg(test)]
mod tests {

    use packet::PacketIo;

    use super::*;
    use crate::packet::Write;
    use crate::sample::SampleIo;
    use core::mem::MaybeUninit;
    use futures::StreamExt;

    #[tokio::test]
    async fn single_elem_read() {
        let handle = SampleIo::default();

        let mut value = [MaybeUninit::uninit()];

        let stream = handle.io(100, &mut value[..]).await;

        let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
        assert_eq!(vec![None], output);
        assert_eq!(100, unsafe { value[0].assume_init() });

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn single_elem_write() {
        let handle = SampleIo::default();

        let value = [42u8];

        let stream = handle.io(100, &value[..]).await;

        let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
        assert_eq!(vec![None], output);
        assert_eq!([42], value);

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn two_elems() {
        let handle = SampleIo::default();

        for _ in 0..2 {
            let mut value = [MaybeUninit::uninit()];

            let stream = handle.io(100, &mut value[..]).await;

            let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
            assert_eq!(vec![None], output);
            assert_eq!(100, unsafe { value[0].assume_init() });
        }

        core::mem::drop(handle);
    }

    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn bench() {
        let handle = SampleIo::default();

        const MILLIS: u64 = 10;

        {
            println!("Sequential:");

            let start = Instant::now();

            let mut cnt = 0;

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;
                handle
                    .io(100, &mut [MaybeUninit::uninit()])
                    .await
                    .count()
                    .await;
            }

            println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());
        }

        {
            println!("One stream, multiple reads:");

            let start = Instant::now();

            let mut cnt = 0;

            let stream = handle.alloc_stream().await;

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;
                stream.send_io(
                    100,
                    Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                );
            }

            println!("{cnt}");

            stream
                .inspect(|(pkt, _)| {
                    unsafe { Box::from(core::slice::from_raw_parts_mut(pkt.data(), pkt.len())) };
                })
                .count()
                .await;

            println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());
        }

        let jobs_in_flight = (1..=2)
            .map(|i| {
                let mut handle = handle.clone();
                PacketIo::<Write>::separate_thread_state(&mut handle);
                async move {
                    println!("Multiple reads in-flight MT:");

                    let start = Instant::now();

                    let mut cnt = 0;

                    let mut q = std::collections::VecDeque::new();

                    while start.elapsed() < Duration::from_millis(MILLIS) {
                        cnt += 1;

                        q.push_back(
                            handle
                                .io(
                                    100,
                                    Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                                )
                                .await,
                        );

                        if q.len() >= 4096 / 16 {
                            q.pop_front()
                                .unwrap()
                                .inspect(|(pkt, _)| {
                                    unsafe {
                                        Box::from(core::slice::from_raw_parts_mut(
                                            pkt.data(),
                                            pkt.len(),
                                        ))
                                    };
                                })
                                .count()
                                .await;
                        }
                    }

                    //println!("AWAITING {cnt}");

                    let q = q
                        .into_iter()
                        .map(|q| {
                            q.inspect(|(pkt, _)| {
                                unsafe {
                                    Box::from(core::slice::from_raw_parts_mut(
                                        pkt.data(),
                                        pkt.len(),
                                    ))
                                };
                            })
                            .count()
                        })
                        .collect::<Vec<_>>();

                    // If we use join_all we gate hrtb proof error
                    for e in q {
                        e.await;
                    }

                    //futures::future::join_all(q).await;

                    let speed = cnt as f64 / start.elapsed().as_secs_f64();

                    println!("{i}: {:.2}", speed);

                    speed
                }
            })
            .map(tokio::spawn)
            .collect::<Vec<_>>();

        let cnt = futures::future::join_all(jobs_in_flight)
            .await
            .into_iter()
            .filter_map(Result::ok)
            .sum::<f64>();

        println!("CNT: {cnt:.2}");

        {
            println!("Multiple reads in-flight:");

            let start = Instant::now();

            let mut cnt = 0;

            let mut q = std::collections::VecDeque::new();

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;

                q.push_back(
                    handle
                        .io(
                            100,
                            Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                        )
                        .await,
                );

                if q.len() >= 4096 * 4 {
                    q.pop_front()
                        .unwrap()
                        .inspect(|(pkt, _)| {
                            unsafe {
                                Box::from(core::slice::from_raw_parts_mut(pkt.data(), pkt.len()))
                            };
                        })
                        .count()
                        .await;
                }
            }

            //println!("AWAITING {cnt}");

            let fut = futures::future::join_all(q.into_iter().map(|q| {
                q.inspect(|(pkt, _)| {
                    unsafe { Box::from(core::slice::from_raw_parts_mut(pkt.data(), pkt.len())) };
                })
                .count()
            }));

            //println!("AWAITING {}", handle.input.queue.len());
            //println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());

            fut.await;

            let speed = cnt as f64 / start.elapsed().as_secs_f64();

            println!("{:.2}", speed);
        }
        println!("DROP");

        core::mem::drop(handle);
    }
}
