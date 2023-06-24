# mfio

[![Build and test]][workflows] [![Rustc 1.66]][rust] [![codecov]][codecov-link]

[Build and test]: https://github.com/memflow/mfio/actions/workflows/build.yml/badge.svg
[workflows]: https://github.com/memflow/mfio/actions/workflows/build.yml
[MIT licensed]: https://img.shields.io/badge/license-MIT-blue.svg
[Rustc 1.66]: https://img.shields.io/badge/rustc-1.66+-lightgray.svg
[rust]: https://blog.rust-lang.org/2022/12/15/Rust-1.66.0.html
[codecov]: https://codecov.io/gh/memflow/mfio/branch/main/graph/badge.svg?token=IJ1K4QPAIM
[codecov-link]: https://codecov.io/gh/memflow/mfio

## Async completion I/O with non-sequential results

mfio is memflow's async completion based I/O base. It aims to make the following aspects of an
I/O chain as simple as possible:

1. Async
2. Automatic batching (vectoring)
3. Fragmentation
4. Partial success
5. Lack of color (full sync support)

## Further reading

Please see [`mfio/src/lib.rs`](mfio/src/lib.rs) for more.
