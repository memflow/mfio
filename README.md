# mfio

[![Build and test]][workflows] [![Rustc 1.74]][rust] [![codecov]][codecov-link]

[Build and test]: https://github.com/memflow/mfio/actions/workflows/build.yml/badge.svg
[workflows]: https://github.com/memflow/mfio/actions/workflows/build.yml
[MIT licensed]: https://img.shields.io/badge/license-MIT-blue.svg
[Rustc 1.74]: https://img.shields.io/badge/rustc-1.74+-lightgray.svg
[rust]: https://blog.rust-lang.org/2023/11/16/Rust-1.74.0.html
[codecov]: https://codecov.io/gh/memflow/mfio/branch/main/graph/badge.svg?token=IJ1K4QPAIM
[codecov-link]: https://codecov.io/gh/memflow/mfio

## Framework for Async I/O Systems

mfio is a one-stop shop for custom async I/O systems. It allows you to go wild, beyond typical OS
APIs. Originally built for memflow, it aims to make the following aspects of an I/O as simple as
possible:

1. Async
2. Automatic batching (vectoring)
3. Fragmentation
4. Partial success
5. Lack of color (full sync support)
6. I/O directly to the stack
7. Using without standard library

mfio achieves all this by building everything around two key, but tiny traits: `PacketIo`, and
`IoBackend`. Backends implement these traits, which then allow asynchronous futures to be driven to
completion. Then, high level abstractions are used to pass data to the I/O system, in the form of
packets. These packets can have multiple views to non-overlapping segments of data, and different
views may be processed differently. The end result is an incredibly flexible I/O system with
unmatched potential for efficient concurrency.

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

## no_std

`mfio` and `mfio-rt` do not require `std`, albeit `alloc` crate is still required. You can disable
standard library requirements as follows:

```toml
mfio = { version = "0.1", default-features = false }
mfio-rt = { version = "0.1", default-features = false, features = ["virt"] }
```

This will add both `mfio` and `mfio-rt` as dependencies in no\_std mode. Many features will be
disabled, such as native polling handles, native runtime, and anything else that depends on running
operating system.

However, the remaining blocks should be sufficient to build any non-blocking I/O systems. Custom
polling handles may be implemented on arbitrary types to enable cooperative operations on any
operating system.
