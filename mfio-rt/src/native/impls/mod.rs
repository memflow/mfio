#[cfg(unix)]
mod unix_extra;

#[cfg(windows)]
mod windows_extra;

pub mod thread;

#[cfg(all(not(miri), target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

#[cfg(all(not(miri), target_os = "windows", feature = "iocp"))]
pub mod iocp;

#[cfg(all(not(miri), unix, feature = "mio"))]
pub mod mio;

cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub use thread::*;
    } else if #[cfg(all(target_os = "linux", feature = "io-uring"))] {
        // io-uring provides true completion I/O, however, it's Linux-only.
        pub use self::io_uring::*;
    } else if #[cfg(all(unix, feature = "mio"))] {
        // mio allows for true async io
        // however, we are relying on file descriptors here, so we can't expose it on non-unix
        // platforms.
        pub use self::mio::*;
    } else {
        // Fallback to thread on any unmatched cases
        pub use thread::*;
    }
}
