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
