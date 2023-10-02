# mfio

[![Build and test]][workflows] [![Rustc 1.72]][rust] [![codecov]][codecov-link]

[Build and test]: https://github.com/memflow/mfio/actions/workflows/build.yml/badge.svg
[workflows]: https://github.com/memflow/mfio/actions/workflows/build.yml
[MIT licensed]: https://img.shields.io/badge/license-MIT-blue.svg
[Rustc 1.72]: https://img.shields.io/badge/rustc-1.72+-lightgray.svg
[rust]: https://blog.rust-lang.org/2023/08/24/Rust-1.72.0.html
[codecov]: https://codecov.io/gh/memflow/mfio/branch/main/graph/badge.svg?token=IJ1K4QPAIM
[codecov-link]: https://codecov.io/gh/memflow/mfio

## Framework for Async I/O Systems

mfio's mission is to provide building blocks for efficient I/O systems, going beyond typical OS
APIs. Originally built for memflow, it aims to make the following aspects of an I/O chain as simple
as possible:

1. Async
2. Automatic batching (vectoring)
3. Fragmentation
4. Partial success
5. Lack of color (full sync support)
6. I/O directly to the stack

## Crates

The project is split into several crates:

| Crate                               | Purpose                 | Status       |
|-------------------------------------|-------------------------|--------------|
| [mfio](mfio/src/lib.rs)             | Core building blocks    | Beta         |
| [mfio-rt](mfio-rt/src/lib.rs)       | Runtime building blocks | Alpha        |
| [mfio-netfs](mfio-netfs/src/lib.rs) | Network filesystem PoC  | Experimental |

What each status means:

- Beta - API is subject to change, however, has proven to be relatively reliable.
- Alpha - API is incomplete, however, no serious bugs have been observed.
- Experimental - Incomplete, and serious bugs have been observed. DO NOT run in production.

