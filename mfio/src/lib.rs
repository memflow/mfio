//! # mfio
//!
//! ## Completion based I/O primitives
//!
//! mfio is memflow's async completion based I/O base. It aims to make the following aspects of an
//! I/O chain as simple as possible:
//!
//! 1. Async
//! 2. Automatic batching
//! 3. Fragmentation
//! 4. Partial success
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
//! ### Examples
//!
//! Read primitive values:
//!
//! ```rust
//! # mod sample {
//! #     use mfio::heap::{AllocHandle, PinHeap};
//! #     use mfio::packet::*;
//! #     use mfio::shared_future::*;
//! #     use mfio::util::*;
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # pollster::block_on(async move {
//! use mfio::packet::{PacketIo, Write};
//! use mfio::traits::*;
//! use core::mem::MaybeUninit;
//! use futures::{Stream, StreamExt};
//!
//! let handle = SampleIo::new(vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]);
//!
//! // Read a single byte
//! let byte = handle.read::<u8>(3).await;
//! assert_eq!(2, byte);
//!
//! // Read an integer
//! let int = handle.read::<u32>(0).await;
//! assert_eq!(u32::from_ne_bytes([0, 1, 1, 2]), int);
//! # });
//! ```
//!
//! Read structures:
//!
//! ```rust
//! # mod sample {
//! #     use mfio::heap::{AllocHandle, PinHeap};
//! #     use mfio::packet::*;
//! #     use mfio::shared_future::*;
//! #     use mfio::util::*;
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # pollster::block_on(async move {
//! use mfio::packet::{PacketIo, Write};
//! use mfio::traits::*;
//! use core::mem::MaybeUninit;
//! use futures::{Stream, StreamExt, pin_mut};
//! use bytemuck::{Pod, Zeroable};
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
//! let handle = SampleIo::new(bytemuck::bytes_of(&sample).into());
//!
//! // Read value
//! let val = handle.read(0).await;
//! assert_eq!(sample, val);
//! # });
//! ```
//!
//! ## Safety
//!
//! mfio can invoke UB in safe code if `mem::forget` is run on a packet stream and data sent to the
//! stream gets reused.
//!
//! ### Safety examples
//!
//! Wrong:
//!
//! ```rust no_run
//! # mod sample {
//! #     use mfio::heap::{AllocHandle, PinHeap};
//! #     use mfio::packet::*;
//! #     use mfio::shared_future::*;
//! #     use mfio::util::*;
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # pollster::block_on(async move {
//! use mfio::packet::{PacketIo, Write};
//! use core::mem::MaybeUninit;
//! use core::pin::Pin;
//! use futures::{Stream, StreamExt};
//!
//! let handle = SampleIo::default();
//!
//! {
//!     let mut data = [MaybeUninit::uninit()];
//!
//!     let mut stream = handle.new_id().await;
//!     let stream_pinned = unsafe { Pin::new_unchecked(&mut stream) };
//!
//!     stream_pinned.as_ref().send_io(0, &mut data);
//!
//!     // Unsafe! data is reused directly after the call.
//!     core::mem::forget(stream);
//!
//!     data[0] = MaybeUninit::new(4);
//! }
//!
//! let mut data = [MaybeUninit::uninit()];
//!
//! // This will process both I/O streams, even though the previous one was forgotten.
//! let _ = handle.io(0, &mut data).count().await;
//! # });
//! ```
//!
//! ```rust no_run
//! # mod sample {
//! #     use mfio::heap::{AllocHandle, PinHeap};
//! #     use mfio::packet::*;
//! #     use mfio::shared_future::*;
//! #     use mfio::util::*;
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # pollster::block_on(async move {
//! use mfio::packet::{PacketIo, Write};
//! use core::mem::MaybeUninit;
//! use core::pin::Pin;
//! use futures::{Stream, StreamExt};
//!
//! let handle = SampleIo::default();
//! {
//!     let mut data = [MaybeUninit::uninit()];
//!
//!     let mut stream = handle.new_id().await;
//!     let stream_pinned = unsafe { Pin::new_unchecked(&mut stream) };
//!
//!     stream_pinned.as_ref().send_io(0, &mut data);
//!
//!     // Unsafe! data is dropped, and its memory is reused later outside the handle
//!     core::mem::forget(stream);
//! }
//!
//! let mut data = [MaybeUninit::uninit()];
//!
//! // This will process both I/O streams, even though the previous one was forgotten.
//! let _ = handle.io(0, &mut data).count().await;
//! # });
//! ```
//!
//! Okay:
//!
//! ```rust
//! # mod sample {
//! #     use mfio::heap::{AllocHandle, PinHeap};
//! #     use mfio::packet::*;
//! #     use mfio::shared_future::*;
//! #     use mfio::util::*;
//! #     include!("sample.rs");
//! # }
//! # use sample::SampleIo;
//! # pollster::block_on(async move {
//! use mfio::packet::{PacketIo, Write};
//! use core::mem::MaybeUninit;
//! use core::pin::Pin;
//! use futures::{Stream, StreamExt, pin_mut};
//!
//! let handle = SampleIo::default();
//! {
//!     let mut data = Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice());
//!
//!     let mut stream = Box::pin(handle.new_id().await);
//!
//!     stream.as_ref().send_io(0, data);
//!
//!     // Okay! Data has been leaked to the heap.
//!     // In addition, we don't touch the data afterwards!
//!     Box::leak(unsafe { Pin::into_inner_unchecked(stream) });
//! }
//!
//! let mut data = [MaybeUninit::uninit()];
//!
//! // This will process both I/O streams, even though the previous one was forgotten.
//! // Processing a forgotten stream is okay, because it was allocated on the `SampleIo` heap,
//! // instead of the stack.
//! let _ = handle.io(0, &mut data).count().await;
//! # });
//! ```

pub mod heap;
pub(crate) mod multistack;
pub mod packet;
pub mod shared_future;
pub mod traits;
pub mod util;

#[cfg(test)]
mod sample {

    use crate::packet::*;
    use crate::shared_future::*;
    use crate::util::*;
    include!("sample.rs");
}

#[cfg(test)]
mod tests {

    use packet::PacketIo;

    use super::traits::*;
    use super::*;
    use crate::packet::Write;
    use crate::sample::SampleIo;
    use bytemuck::{Pod, Zeroable};
    use core::mem::MaybeUninit;
    use futures::StreamExt;

    #[tokio::test]
    async fn single_elem_read() {
        let handle = SampleIo::new((0..200).collect::<Vec<_>>());
        let mut value = [MaybeUninit::uninit()];

        let stream = handle.io(100, &mut value[..]);

        let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
        assert_eq!(vec![None], output);
        assert_eq!(100, unsafe { value[0].assume_init() });

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn two_read_scopes() {
        let handle = SampleIo::new((0..200).collect::<Vec<_>>());

        {
            let scope = handle.clone();
            let mut value = [MaybeUninit::uninit()];

            let stream = scope.io(100, &mut value[..]);

            let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
            assert_eq!(vec![None], output);
            assert_eq!(100, unsafe { value[0].assume_init() });

            core::mem::drop(scope);
        }

        {
            let scope = handle.clone();
            let mut value = [MaybeUninit::uninit()];

            let stream = scope.io(100, &mut value[..]);

            let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
            assert_eq!(vec![None], output);
            assert_eq!(100, unsafe { value[0].assume_init() });

            core::mem::drop(scope);
        }
    }

    #[tokio::test]
    async fn single_elem_write() {
        let handle = SampleIo::default();
        let value = [42u8];

        let stream = handle.io(100, &value[..]);

        let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
        assert_eq!(vec![None], output);
        assert_eq!([42], value);

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn single_elem_write_and_read() {
        let handle = SampleIo::default();
        let write = [42u8];

        handle.write_all(100, &write[..]).await;

        let mut read = (0..write.len())
            .map(|_| MaybeUninit::uninit())
            .collect::<Vec<_>>();

        handle.read_all(100, &mut read[..]).await;

        let read = read
            .into_iter()
            .map(|v| unsafe { v.assume_init() })
            .collect::<Vec<_>>();

        assert_eq!(&write[..], &read[..]);
    }

    #[tokio::test]
    async fn simple_struct_write_and_read() {
        let handle = SampleIo::default();
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

        handle.write(100, &write).await;

        let read = handle.read::<TestStruct>(100).await;

        assert_eq!(write, read);
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
        let handle = SampleIo::new((0..200).collect::<Vec<_>>());
        for _ in 0..2 {
            let mut value = [MaybeUninit::uninit()];

            let stream = handle.io(100, &mut value[..]);

            let output = stream.map(|(_, b)| b).collect::<Vec<_>>().await;
            assert_eq!(vec![None], output);
            assert_eq!(100, unsafe { value[0].assume_init() });
        }

        core::mem::drop(handle);
    }

    #[tokio::test]
    async fn drop_bare_stream() {
        let handle = SampleIo::default();
        let stream = handle.io(100, &[]);
        core::mem::drop(stream);
    }

    #[tokio::test]
    #[cfg(should_panic)]
    async fn drop_bound_stream() {
        let handle = SampleIo::default();
        let mut value = [MaybeUninit::uninit()];

        let stream = handle.alloc_stream().await;

        stream.send_io(0, &mut value[..]);

        core::mem::drop(stream);
    }

    #[tokio::test]
    #[cfg(should_panic)]
    async fn drop_io_stream() {
        let handle = SampleIo::default();
        let mut value = [MaybeUninit::uninit()];
        let stream = handle.io(100, &mut value[..]);

        core::mem::drop(stream);
    }

    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn bench() {
        let io = SampleIo::default();

        const MILLIS: u64 = 10;

        {
            println!("Sequential:");

            let start = Instant::now();

            let mut cnt = 0;

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;
                io.io(100, &mut [MaybeUninit::uninit()]).count().await;
            }

            println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());
        }

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
                let mut scope = io.clone();
                PacketIo::<Write, _>::separate_thread_state(&mut scope);
                async move {
                    println!("Multiple reads in-flight MT:");

                    let start = Instant::now();

                    let mut cnt = 0;

                    let mut q = std::collections::VecDeque::new();

                    while start.elapsed() < Duration::from_millis(MILLIS) {
                        cnt += 1;

                        q.push_back(scope.io(
                            100,
                            Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                        ));

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

        println!("AWAIT");

        let cnt = futures::future::join_all(jobs_in_flight)
            .await
            .into_iter()
            .filter_map(Result::ok)
            .sum::<f64>();

        println!("CNT: {cnt:.2}");

        {
            let io = &io;
            println!("Multiple reads in-flight:");

            let start = Instant::now();

            let mut cnt = 0;

            let mut q = std::collections::VecDeque::new();

            while start.elapsed() < Duration::from_millis(MILLIS) {
                cnt += 1;

                q.push_back(io.io(
                    100,
                    Box::leak(vec![MaybeUninit::uninit()].into_boxed_slice()),
                ));

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

            //println!("AWAITING {}", io.input.queue.len());
            //println!("{:.2}", cnt as f64 / start.elapsed().as_secs_f64());

            fut.await;

            let speed = cnt as f64 / start.elapsed().as_secs_f64();

            println!("{:.2}", speed);
        }
        println!("DROP");
    }
}
