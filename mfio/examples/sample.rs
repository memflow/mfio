use mfio::backend::*;
use mfio::io::*;
use std::time::{Duration, Instant};

use sample::*;

mod sample {
    include!("../src/sample.rs");
}

fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}

fn bench(size: usize, iters: usize) -> Duration {
    let handle = SampleIo::default();

    handle.block_on(async {
        let bufs = (0..size)
            .map(|_| Packet::<Write>::new_buf(1))
            .collect::<Vec<_>>();

        let start = Instant::now();

        for _ in 0..iters {
            for b in &bufs {
                unsafe { b.reset_err() };
                let pv = PacketView::from_arc_ref(b, 0);
                let bpv = unsafe { pv.bind(None) };
                handle.send_io(0, bpv);
            }

            for b in &bufs {
                black_box(&**b).await;
            }
        }

        start.elapsed()
    })
}

fn main() {
    let mut args = std::env::args().skip(1);
    let size = args.next();
    let size = size.as_deref().unwrap_or("256").parse().unwrap();
    let iters = args.next();
    let iters: usize = iters.as_deref().unwrap_or("100000").parse().unwrap();

    let time = bench(size, iters / size);
    println!("Time: {time:?}");
}
