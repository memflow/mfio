use core::mem::MaybeUninit;
use futures::{pin_mut, StreamExt};
use mfio::packet::*;
use std::time::{Duration, Instant};

use sample::*;

mod sample {
    use mfio::packet::*;
    use mfio::shared_future::*;
    use mfio::util::Event;
    include!("../src/sample.rs");
}

fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}

async fn bench(size: usize, iters: usize) -> Duration {
    let handle = &SampleIo::default();

    let mut bufs = vec![[MaybeUninit::uninit()]; size];

    let start = Instant::now();

    for _ in 0..iters {
        let stream = handle.new_id().await;
        pin_mut!(stream);

        for b in &mut bufs {
            stream.as_ref().send_io(0, b);
        }

        black_box(stream.count().await);
    }

    start.elapsed()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let size = args.next();
    let size = size.as_deref().unwrap_or("256").parse().unwrap();
    let iters = args.next();
    let iters: usize = iters.as_deref().unwrap_or("100000").parse().unwrap();

    let time = pollster::block_on(bench(size, iters / size));
    println!("Time: {time:?}");
}
