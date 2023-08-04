use core::future::Future;

use futures::Stream;
use mfio::backend::*;
use mfio::error::Result as MfioResult;
use mfio::packet::NoPos;
use mfio::stdeq::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;

#[cfg(feature = "native")]
pub mod native;
mod util;

#[cfg(feature = "native")]
pub use native::{NativeFile, NativeRt, NativeRtBuilder};

#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
    // Append would currently require us to get file pos after opening.
    // So we don't support it at the moment.
    //pub append: bool,
}

impl OpenOptions {
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
        }
    }

    pub fn read(self, read: bool) -> Self {
        Self { read, ..self }
    }

    pub fn write(self, write: bool) -> Self {
        Self { write, ..self }
    }

    pub fn create(self, create: bool) -> Self {
        Self { create, ..self }
    }

    pub fn create_new(self, create_new: bool) -> Self {
        Self { create_new, ..self }
    }

    pub fn truncate(self, truncate: bool) -> Self {
        Self { truncate, ..self }
    }
}

pub trait Fs: IoBackend {
    type FileHandle: FileHandle;
    type OpenFuture<'a>: Future<Output = MfioResult<Self::FileHandle>> + 'a
    where
        Self: 'a;

    fn open(&self, path: &Path, options: OpenOptions) -> Self::OpenFuture<'_>;
}

pub trait FileHandle: AsyncRead<u64> + AsyncWrite<u64> {}
impl<T: AsyncRead<u64> + AsyncWrite<u64>> FileHandle for T {}

pub trait StreamHandle: AsyncRead<NoPos> + AsyncWrite<NoPos> {}
impl<T: AsyncRead<NoPos> + AsyncWrite<NoPos>> StreamHandle for T {}

pub trait Tcp: IoBackend {
    type StreamHandle: TcpStreamHandle;
    type ListenerHandle: TcpListenerHandle<StreamHandle = Self::StreamHandle>;
    type ConnectFuture<'a, A: ToSocketAddrs + Send + 'a>: Future<Output = MfioResult<Self::StreamHandle>>
        + 'a
    where
        Self: 'a;
    type BindFuture<'a, A: ToSocketAddrs + Send + 'a>: Future<Output = MfioResult<Self::ListenerHandle>>
        + 'a
    where
        Self: 'a;

    fn connect<'a, A: ToSocketAddrs + Send + 'a>(&'a self, addrs: A) -> Self::ConnectFuture<'a, A>;

    fn bind<'a, A: ToSocketAddrs + Send + 'a>(&'a self, addrs: A) -> Self::BindFuture<'a, A>;
}

pub trait TcpStreamHandle: StreamHandle {
    fn local_addr(&self) -> MfioResult<SocketAddr>;
    fn peer_addr(&self) -> MfioResult<SocketAddr>;

    // These interfaces may be slightly trickier to implement, so we omit them for now.
    //fn set_ttl(&self, ttl: u32) -> MfioResult<()>;
    //fn ttl(&self) -> MfioResult<u32>;
    //fn set_nodelay(&self, nodelay: bool) -> MfioResult<()>;
    //fn nodelay(&self) -> MfioResult<bool>;
}

pub trait TcpListenerHandle: Stream<Item = (Self::StreamHandle, SocketAddr)> {
    type StreamHandle: TcpStreamHandle;

    fn local_addr(&self) -> MfioResult<SocketAddr>;

    // These interfaces may be slightly trickier to implement, so we omit them for now.
    //fn set_ttl(&self, ttl: u32) -> MfioResult<()>;
    //fn ttl(&self) -> MfioResult<u32>;
}
