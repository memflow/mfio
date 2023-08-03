#[cfg(unix)]
mod unix_extra;

pub mod thread;

#[cfg(all(not(miri), target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

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

use core::marker::PhantomData;
use core::mem::ManuallyDrop;

struct StreamBorrow<'a, T: StreamHandleConv>(ManuallyDrop<T>, PhantomData<&'a T>);

impl<T: StreamHandleConv> Drop for StreamBorrow<'_, T> {
    fn drop(&mut self) {
        let handle = unsafe { ManuallyDrop::take(&mut self.0) };
        let _ = handle.into_raw();
    }
}

impl<'a, T: StreamHandleConv> StreamBorrow<'a, T> {
    unsafe fn get(stream: &'a OwnedStreamHandle) -> Self {
        Self(ManuallyDrop::new(T::from_raw(stream.as_raw())), PhantomData)
    }
}

impl<T: StreamHandleConv> core::ops::Deref for StreamBorrow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: StreamHandleConv> core::ops::DerefMut for StreamBorrow<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub trait StreamHandleConv:
    RawHandleConv<RawHandle = RawStreamHandle, OwnedHandle = OwnedStreamHandle>
    + std::io::Read
    + std::io::Write
{
}

impl<
        T: RawHandleConv<RawHandle = RawStreamHandle, OwnedHandle = OwnedStreamHandle>
            + std::io::Read
            + std::io::Write,
    > StreamHandleConv for T
{
}

pub trait RawHandleConv: Sized {
    type RawHandle: 'static + Send + Copy + core::fmt::Debug;
    type OwnedHandle: 'static + Send + RawHandleConv<RawHandle = Self::RawHandle>;

    fn as_raw(&self) -> Self::RawHandle;
    fn into_raw(self) -> Self::RawHandle;
    fn into_owned(self) -> Self::OwnedHandle {
        unsafe { Self::OwnedHandle::from_raw(self.into_raw()) }
    }
    /// Converts object from a raw handle.
    ///
    /// # Safety
    ///
    /// Handle must have been obtained by calling `into_raw` on the same type.
    unsafe fn from_raw(handle: Self::RawHandle) -> Self;
}

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
#[cfg(unix)]
pub type OwnedStreamHandle = OwnedFd;
#[cfg(unix)]
pub type RawStreamHandle = RawFd;
#[cfg(unix)]
impl<T: AsRawFd + IntoRawFd + FromRawFd> RawHandleConv for T {
    type RawHandle = RawFd;
    type OwnedHandle = OwnedFd;

    fn as_raw(&self) -> RawFd {
        self.as_raw_fd()
    }

    fn into_raw(self) -> RawFd {
        self.into_raw_fd()
    }

    unsafe fn from_raw(handle: RawFd) -> Self {
        Self::from_raw_fd(handle)
    }
}

#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, FromRawSocket, IntoRawSocket, OwnedSocket, RawSocket};
#[cfg(windows)]
pub type OwnedStreamHandle = OwnedSocket;
#[cfg(windows)]
pub type RawStreamHandle = RawSocket;
#[cfg(windows)]
impl<T: AsRawSocket + IntoRawSocket + FromRawSocket> RawHandleConv for T {
    type RawHandle = RawSocket;
    type OwnedHandle = OwnedSocket;

    fn as_raw(&self) -> RawSocket {
        self.as_raw_socket()
    }

    fn into_raw(self) -> RawSocket {
        self.into_raw_socket()
    }

    unsafe fn from_raw(handle: RawSocket) -> Self {
        Self::from_raw_socket(handle)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Key {
    File(usize),
    Stream(usize),
}

impl From<usize> for Key {
    fn from(raw: usize) -> Self {
        if raw & 1 != 0 {
            Self::Stream(raw >> 1)
        } else {
            Self::File(raw >> 1)
        }
    }
}

impl Key {
    pub fn idx(self) -> usize {
        match self {
            Self::File(v) => v,
            Self::Stream(v) => v,
        }
    }

    pub fn key(self) -> usize {
        match self {
            Self::File(v) => v << 1,
            Self::Stream(v) => (v << 1) | 1,
        }
    }
}
