//! # mfio
//!
//! ## Framework for Async I/O Systems
//!
//! mfio's mission is to provide building blocks for efficient I/O systems, going beyond typical OS
//! APIs. Originally built for memflow, it aims to make the following aspects of an I/O chain as simple
//! as possible:
//!
//! 1. Async
//! 2. Automatic batching (vectoring)
//! 3. Fragmentation
//! 4. Partial success
//! 5. Lack of color (full sync support)
//! 6. I/O directly to the stack
//!
//! This crate provides core, mostly unopiniated, building blocks for async I/O systems. The
//! biggest design assumption is that this crate is to be used for thread-per-core-like I/O systems.
//!
//! One could view mfio as _programmable I/O_, because native fragmentation support allows one to
//! map non-linear I/O space into a linear space. This is incredibly useful for interpretation of
//! process virtual address space on top of physical address space. Async operation allows to queue
//! up multiple I/O operations simultaneously and have them automatically batched up by the I/O
//! implementation. This results in the highest performance possible in scenarios where dispatching
//! a single I/O operation incurs heavy latency. Batching queues up operations and issues fewer
//! calls for the same amount of I/O. Partial success is critical in fragmentable context. Unlike
//! typical I/O interfaces, mfio does not enforce sequence of operations. A single packet may get
//! partially read/written, depending on which parts of the underlying I/O space is available. This
//! works really well with sparse files, albeit differs from the typical "stop as soon as an error
//! occurs" model.
//!
//! Lack of color is not true sync/async mix, instead, mfio is designed to expose minimal set of
//! data for invoking a built-in runtime, with handles of daisy chaining mfio on top of another
//! runtime. The end result is that mfio is able to provide sync wrappers that efficiently poll
//! async operations to completion, while staying runtime agnostic. We found that a single (unix)
//! file descriptor or (windows) handle is sufficient to connect multiple async runtimes together.
//!
//! ### Examples
//!
//! Read primitive values:
//!
//! ```rust
//! # mod sample {
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # fn work() -> mfio::error::Result<()> {
//! use core::mem::MaybeUninit;
//! use futures::{Stream, StreamExt};
//! use mfio::backend::*;
//! use mfio::io::{PacketIo, Write};
//! use mfio::traits::*;
//!
//! let handle = SampleIo::new(vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]);
//!
//! // mfio includes a lightweight executor
//! handle.block_on(async {
//!     // Read a single byte
//!     let byte = handle.read::<u8>(3).await?;
//!     assert_eq!(2, byte);
//!
//!     // Read an integer
//!     let int = handle.read::<u32>(0).await?;
//!     assert_eq!(u32::from_ne_bytes([0, 1, 1, 2]), int);
//!     Ok(())
//! })
//! # }
//! # work().unwrap();
//! ```
//!
//! Read primitive values synchronously:
//!
//! ```rust
//! # mod sample {
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # fn work() -> mfio::error::Result<()> {
//! use core::mem::MaybeUninit;
//! use futures::{Stream, StreamExt};
//! use mfio::backend::*;
//! use mfio::io::{PacketIo, Write};
//! use mfio::traits::sync::*;
//!
//! let handle = SampleIo::new(vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]);
//!
//! // Read a single byte
//! let byte = handle.read::<u8>(3)?;
//! assert_eq!(2, byte);
//!
//! // Read an integer
//! let int = handle.read::<u32>(0)?;
//! assert_eq!(u32::from_ne_bytes([0, 1, 1, 2]), int);
//! # Ok(())
//! # }
//! # work().unwrap();
//! ```
//!
//! Read structures:
//!
//! ```rust
//! # mod sample {
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # fn work() -> mfio::error::Result<()> {
//! # pollster::block_on(async move {
//! use bytemuck::{Pod, Zeroable};
//! use core::mem::MaybeUninit;
//! use futures::{pin_mut, Stream, StreamExt};
//! use mfio::backend::*;
//! use mfio::io::{PacketIo, Write};
//! use mfio::traits::*;
//!
//! #[repr(C, packed)]
//! #[derive(Eq, PartialEq, Default, Pod, Zeroable, Clone, Copy, Debug)]
//! struct Sample {
//!     first: usize,
//!     second: u32,
//!     third: u8,
//! }
//!
//! let sample = Sample {
//!     second: 42,
//!     ..Default::default()
//! };
//!
//! let mut handle = SampleIo::new(bytemuck::bytes_of(&sample).into());
//!
//! // mfio objects can also be plugged into existing executor.
//! // `Null` is compatible with every executor, but waking must be done externally.
//! // There is `Tokio`, compatible with tokio runtime.
//! // There is also `AsyncIo` - for smol, async-std and friends.
//! Null::run_with_mut(&mut handle, |handle| async move {
//!     // Read value
//!     let val = handle.read(0).await?;
//!     assert_eq!(sample, val);
//!     Ok(())
//! })
//! .await
//! # })
//! # }
//! # work().unwrap();
//! ```
//!
//! ## Safety
//!
//! By default mfio is conservative and does not enable invoking undefined behavior. However, with
//! a custom opt-in config switch, enabled by passing `--cfg mfio_assume_linear_types` to the rust
//! compiler, mfio is able to provide significant performance improvements, at the cost of
//! potential for invoking UB in safe code.
//!
//! With `mfio_assume_linear_types` config enabled, mfio wrappers will prefer storing data on the
//! stack, and if a future waiting for I/O operations to complete is cancelled, a `panic!` may get
//! triggered. Moreover, if a future waiting for I/O operations to complete gets forgotten using
//! `mem::forget`, undefined behavior may be invoked, because use-after-(stack)-free safeguards are
//! discarded.
//!
//! ### Safety examples
//!
//! TODO: write new safety examples for assume_linear_types

pub mod backend;
pub mod error;
pub mod heap;
pub mod io;
mod poller;
pub mod stdeq;
pub mod traits;
pub mod util;

#[cfg(not(mfio_assume_linear_types))]
#[macro_export]
macro_rules! linear_types_switch {
    (Standard => { $($matched:tt)* } $($end:ident => $block2:block)*) => {
        $($matched)*
    };
    ($start:ident => $block2:block $($end:tt)*) => {
        $crate::linear_types_switch!{
            $($end)*
        }
    }
}

#[cfg(mfio_assume_linear_types)]
#[macro_export]
macro_rules! linear_types_switch {
    (Linear => { $($matched:tt)* } $($end:ident => $block2:block)*) => {
        $($matched)*
    };
    ($start:ident => $block2:block $($end:tt)*) => {
        $crate::linear_types_switch!{
            $($end)*
        }
    }
}

pub use tarc;

#[cfg(test)]
mod sample {
    use crate as mfio;
    include!("sample.rs");
}

#[cfg(test)]
mod tests {

    use io::{IntoPacket, Packet, PacketIo, PacketIoExt, PacketView, Write};

    use super::traits::*;
    use super::*;
    use crate::backend::*;
    use crate::error::*;
    use crate::sample::SampleIo;
    use bytemuck::{Pod, Zeroable};
    use core::mem::MaybeUninit;
    use core::pin::pin;
    use futures::StreamExt;

    #[tokio::test]
    async fn oobe() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |handle| async move {
            let pkt = handle.io(200, Packet::<Write>::new_buf(200)).await;

            assert_eq!(pkt.as_ref().error_clamp(), 0);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn split_oobe() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |handle| async move {
            let pkt = handle.io(199, Packet::<Write>::new_buf(200)).await;

            assert_eq!(pkt.as_ref().error_clamp(), 1);
            assert_eq!(pkt.as_ref().min_error().unwrap().state, State::Outside);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn split_oobe_stream() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |handle| async move {
            let fut = handle.io_to_stream(199, Packet::<Write>::new_buf(200), vec![]);
            let mut fut = pin!(fut);

            let stream = fut.as_mut().submit();

            let pkts = stream.collect::<Vec<_>>().await;

            fut.await;

            assert_eq!(pkts.len(), 2);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn split_oobe_func() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |handle| async move {
            let out = tarc::BaseArc::new(parking_lot::Mutex::new(vec![]));

            handle
                .io_to_fn(199, Packet::<Write>::new_buf(200), {
                    let out = out.clone();
                    move |view, err| out.lock().push((view, err))
                })
                .await;

            let pkts = out.lock();

            assert_eq!(pkts.len(), 2);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn single_elem_read() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |handle| async move {
            let pkt = handle.io(100, Packet::<Write>::new_buf(1)).await;
            assert_eq!(pkt.simple_contiguous_slice().unwrap(), &[100]);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn two_read_scopes() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());

        Null::run_with_mut(&mut handle, |scope| async move {
            let pkt = scope.io(100, Packet::<Write>::new_buf(1)).await;
            assert_eq!(pkt.simple_contiguous_slice().unwrap(), &[100]);
        })
        .await;

        Null::run_with_mut(&mut handle, |scope| async move {
            let pkt = scope.io(100, Packet::<Write>::new_buf(1)).await;
            assert_eq!(pkt.simple_contiguous_slice().unwrap(), &[100]);
        })
        .await;
    }

    #[tokio::test]
    async fn single_elem_write() {
        let mut handle = SampleIo::default();
        let value = [42u8];

        Null::run_with_mut(&mut handle, |handle| async move {
            let (pkt, _) = value.into_packet();
            let pkt = handle.io(100, pkt).await;
            assert_eq!(pkt.min_error(), None);

            let pkt = handle.io(100, Packet::<Write>::new_buf(value.len())).await;
            assert_eq!(pkt.simple_contiguous_slice().unwrap(), &value);
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn single_elem_write_and_read() {
        let mut handle = SampleIo::default();
        let write = [42u8];

        Null::run_with_mut(&mut handle, |handle| async move {
            let (pkt, _) = write.into_packet();
            let pkt = handle.io(100, pkt).await;
            assert_eq!(pkt.min_error(), None);

            let pkt = handle.io(100, Packet::<Write>::new_buf(write.len())).await;
            let read = pkt.simple_contiguous_slice().unwrap();
            assert_eq!(&write, read);
        })
        .await;
    }

    #[tokio::test]
    async fn simple_struct_write_and_read() {
        let mut handle = SampleIo::default();
        #[repr(C)]
        #[derive(Clone, Copy, Eq, PartialEq, Debug, Pod, Zeroable)]
        struct TestStruct {
            a: u32,
            b: u32,
            c: u32,
        }

        let write = TestStruct {
            a: 57,
            b: 109,
            c: 8,
        };

        Null::run_with_mut(&mut handle, |handle| async move {
            handle.write(100, &write).await.unwrap();

            let read = handle.read::<TestStruct>(100).await.unwrap();

            assert_eq!(write, read);
        })
        .await;
    }

    /*#[tokio::test]
    async fn padded_struct_write_and_read() {
        let handle = SampleIo::default();

        #[repr(C)]
        #[derive(Clone, Copy, Eq, PartialEq, Debug, Pod, Zeroable)]
        struct TestStruct {
            a: u8,
            b: u32,
            c: u128,
        }

        let write = TestStruct {
            a: 57,
            b: 109,
            c: 8
        };

        handle.write(100, &write).await;

        let read = handle.read::<TestStruct>(100).await;

        assert_eq!(write, read);
    }*/

    #[tokio::test]
    async fn two_elems() {
        let mut handle = SampleIo::new((0..200).collect::<Vec<_>>());
        Null::run_with_mut(&mut handle, |handle| async move {
            for _ in 0..2 {
                let pkt = handle.io(100, Packet::<Write>::new_buf(1)).await;
                assert_eq!(pkt.simple_contiguous_slice().unwrap(), &[100]);
            }
        })
        .await;

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn drop_bare_stream() {
        let handle = SampleIo::default();
        let fut = handle.io(100, Packet::<Write>::new_buf(0));
        core::mem::drop(fut);
    }

    #[tokio::test]
    async fn drop_bound_stream() {
        let handle = SampleIo::default();
        let pkt = Packet::<Write>::new_buf(1);
        let pv = PacketView::from_arc_ref(&pkt, 0);
        let bpv = unsafe { pv.bind(None) };

        handle.send_io(0, bpv);

        core::mem::drop(pkt);
    }

    #[tokio::test]
    #[should_panic]
    async fn fully_drop_bound_stream() {
        let handle = SampleIo::default();
        let pkt = Packet::<Write>::new_buf(1);
        let pv = PacketView::from_arc_ref(&pkt, 0);
        let bpv = unsafe { pv.bind(None) };

        handle.send_io(0, bpv);

        // SAFETY: this is not really safe, because we are freeing a value used somewhere else.
        // However, this should be okay in most cases of this test. If it is not okay, when running
        // under miri, do investigate further.
        unsafe { tarc::BaseArc::decrement_strong_count(pkt.as_ptr()) };
        core::mem::drop(pkt);
    }

    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn bench() {
        let mut io = SampleIo::default();

        const MILLIS: u64 = 10;

        Null::run_with_mut(&mut io, |io| async move {
            println!("Sequential:");

            let start = Instant::now();

            let mut cnt = 0;

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;
                io.read_all(100, &mut [MaybeUninit::uninit()][..])
                    .await
                    .unwrap();
            }

            println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());
        })
        .await;

        /*{
            println!("Multiple reads:");

            let start = Instant::now();

            let mut cnt = 0;

            let mut streams = vec![];

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;
                streams.push(io.io(
                    100,
                    Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                ));
            }

            println!("{cnt}");

            let ret = futures::stream::iter(streams)
                .flatten()
                .inspect(|(pkt, _)| {
                    unsafe { Box::from(core::slice::from_raw_parts_mut(pkt.data(), pkt.len())) };
                })
                .count();

            ret.await;

            println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());
        }*/

        let jobs_in_flight = (1..=2)
            .map(|i| {
                let mut io = io.clone();
                async move {
                    Null::run_with_mut(&mut io, move |scope| async move {
                        println!("Multiple reads in-flight MT:");

                        let start = Instant::now();

                        let mut cnt = 0;

                        let mut q = std::collections::VecDeque::new();

                        while start.elapsed() < Duration::from_millis(MILLIS) {
                            cnt += 1;

                            q.push_back(scope.io(100, Packet::<Write>::new_buf(1)));

                            if q.len() >= 4096 / 16 {
                                q.pop_front().unwrap().await;
                            }
                        }

                        futures::future::join_all(q).await;

                        let speed = cnt as f64 / start.elapsed().as_secs_f64();

                        println!("{i}: {:.2}", speed);

                        speed
                    })
                    .await
                }
            })
            .map(tokio::spawn)
            .collect::<Vec<_>>();

        println!("AWAIT");

        let cnt = futures::future::join_all(jobs_in_flight)
            .await
            .into_iter()
            .filter_map(core::result::Result::ok)
            .sum::<f64>();

        println!("CNT: {cnt:.2}");

        Null::run_with_mut(&mut io, |io| async move {
            let io = &io;
            println!("Multiple reads in-flight:");

            let start = Instant::now();

            let mut cnt = 0;

            let mut q = std::collections::VecDeque::new();

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;

                q.push_back(io.io(100, Packet::<Write>::new_buf(1)));

                if q.len() >= 4096 * 4 {
                    q.pop_front().unwrap().await;
                }
            }

            //println!("AWAITING {cnt}");

            let fut = futures::future::join_all(q);

            //println!("AWAITING {}", io.input.queue.len());
            //println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());

            fut.await;

            let speed = cnt as f64 / start.elapsed().as_secs_f64();

            println!("{:.2}", speed);
        })
        .await;
        println!("DROP");
    }
}
