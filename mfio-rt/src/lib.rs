use core::future::Future;

use futures::Stream;
use mfio::backend::*;
use mfio::error::Result as MfioResult;
use mfio::packet::NoPos;
use mfio::stdeq::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[cfg(feature = "native")]
pub mod native;
mod util;

#[doc(hidden)]
pub mod __doctest;

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
    type DirHandle<'a>: DirHandle + 'a
    where
        Self: 'a;

    /// Gets a directory handle representing current working directory.
    ///
    /// Note that implementor is not required to maintain the location of this handle constant as
    /// CWD changes. Therefore, the handle may point to different locations as the directory gets
    /// changed in this program.
    fn current_dir(&self) -> &Self::DirHandle<'_>;

    fn open(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> <Self::DirHandle<'_> as DirHandle>::OpenFileFuture<'_> {
        self.current_dir().open_file(path, options)
    }
}

/// Represents a location in filesystem operations are performed from.
pub trait DirHandle: Sized {
    type FileHandle: FileHandle;
    type OpenFileFuture<'a>: Future<Output = MfioResult<Self::FileHandle>> + 'a
    where
        Self: 'a;
    type PathFuture<'a>: Future<Output = MfioResult<PathBuf>> + 'a
    where
        Self: 'a;
    type OpenDirFuture<'a>: Future<Output = MfioResult<Self>> + 'a
    where
        Self: 'a;
    type ReadDir<'a>: Stream<Item = MfioResult<DirEntry>> + 'a
    where
        Self: 'a;
    type ReadDirFuture<'a>: Future<Output = MfioResult<Self::ReadDir<'a>>> + 'a
    where
        Self: 'a;
    type MetadataFuture<'a>: Future<Output = MfioResult<Metadata>> + 'a
    where
        Self: 'a;
    type OpFuture<'a>: Future<Output = MfioResult<()>> + 'a
    where
        Self: 'a;

    /// Gets the absolute path to this `DirHandle`.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use mfio_rt::{DirHandle, Fs};
    /// use std::path::Path;
    ///
    /// let dir = fs.current_dir();
    ///
    /// let path = dir.path().await.unwrap();
    ///
    /// assert_ne!(path, Path::new("/"));
    /// # });
    /// ```
    fn path(&self) -> Self::PathFuture<'_>;

    /// Reads the directory contents.
    ///
    /// This function returns a stream that can be used to list files and subdirectories within
    /// this dir. Any errors will be propagated through the stream.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use futures::StreamExt;
    /// use mfio::error::Error;
    /// use mfio_rt::{DirHandle, Fs};
    ///
    /// let dir = fs.current_dir();
    ///
    /// let mut entries = dir
    ///     .read_dir()
    ///     .await
    ///     .unwrap()
    ///     .filter_map(|res| async { res.ok() })
    ///     .map(|res| res.name)
    ///     .collect::<Vec<_>>()
    ///     .await;
    ///
    /// assert!(entries.contains(&"Cargo.toml".to_string()));
    ///
    /// // The order in which `read_dir` returns entries is not guaranteed. If reproducible
    /// // ordering is required the entries should be explicitly sorted.
    ///
    /// entries.sort();
    ///
    /// // The entries have now been sorted by their path.
    ///
    /// # });
    /// ```
    fn read_dir(&self) -> Self::ReadDirFuture<'_>;

    /// Opens a file.
    ///
    /// This function accepts an absolute or relative path to a file for reading. If the path is
    /// relative, it is opened relative to this `DirHandle`.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use mfio_rt::{DirHandle, Fs, OpenOptions};
    /// use mfio::traits::IoRead;
    ///
    /// let dir = fs.current_dir();
    ///
    /// let fh = dir
    ///     .open_file("Cargo.toml", OpenOptions::new().read(true))
    ///     .await
    ///     .unwrap();
    ///
    /// // Now you may do file operations, like reading it
    ///
    /// let mut bytes = vec![];
    /// fh.read_to_end(0, &mut bytes).await.unwrap();
    /// let s = String::from_utf8(bytes).unwrap();
    ///
    /// assert!(s.contains("mfio"));
    /// # });
    /// ```
    fn open_file<P: AsRef<Path>>(&self, path: P, options: OpenOptions) -> Self::OpenFileFuture<'_>;

    /// Opens a directory.
    ///
    /// This function accepts an absolute or relative path to a directory for reading. If the path
    /// is relative, it is opened relative to this `DirHandle`.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use mfio_rt::{DirHandle, Fs, OpenOptions};
    /// use mfio::traits::IoRead;
    /// use futures::StreamExt;
    ///
    /// let dir = fs.current_dir();
    ///
    /// let subdir = dir
    ///     .open_dir("src")
    ///     .await
    ///     .unwrap();
    ///
    /// assert_ne!(dir.path().await.unwrap(), subdir.path().await.unwrap());
    ///
    /// // Now you may do directory operations, like listing it
    ///
    /// let mut entries = subdir
    ///     .read_dir()
    ///     .await
    ///     .unwrap()
    ///     .filter_map(|res| async { res.ok() })
    ///     .map(|res| res.name)
    ///     .collect::<Vec<_>>()
    ///     .await;
    ///
    /// assert!(entries.contains(&"lib.rs".to_string()));
    ///
    /// # });
    /// ```
    fn open_dir<P: AsRef<Path>>(&self, path: P) -> Self::OpenDirFuture<'_>;

    /// Retrieves file metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn metadata<P: AsRef<Path>>(&self, path: P) -> Self::MetadataFuture<'_>;

    /// Do an operation.
    ///
    /// This function performs an operation from the [`DirOp`](DirOp) enum.
    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn do_op<P: AsRef<Path>>(&self, operation: DirOp<P>) -> Self::OpFuture<'_>;

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn set_permissions<P: AsRef<Path>>(
        &self,
        path: P,
        permissions: Permissions,
    ) -> Self::OpFuture<'_> {
        self.do_op(DirOp::SetPermissions { path, permissions })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::RemoveDir { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::RemoveDirAll { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::RemoveFile { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::Rename { from, to })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    // TODO: reflinking option
    fn copy<P: AsRef<Path>>(&self, from: P, to: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::Copy { from, to })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn hard_link<P: AsRef<Path>>(&self, from: P, to: P) -> Self::OpFuture<'_> {
        self.do_op(DirOp::HardLink { from, to })
    }

    // TODO: decide on how to handle symlinks, as they differ between platforms.
    // fn symlink_file(&self, from: &Path, to: &Path) -> Self::OpFuture<'_>;
    // fn symlink_dir(&self, from: &Path, to: &Path) -> Self::OpFuture<'_>;
}

#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum DirOp<P: AsRef<Path>> {
    SetPermissions { path: P, permissions: Permissions },
    RemoveDir { path: P },
    RemoveDirAll { path: P },
    RemoveFile { path: P },
    Rename { from: P, to: P },
    Copy { from: P, to: P },
    HardLink { from: P, to: P },
}

impl<P: AsRef<Path>> DirOp<P> {
    pub fn as_path(&self) -> DirOp<&Path> {
        match self {
            Self::SetPermissions { path, permissions } => DirOp::SetPermissions {
                path: path.as_ref(),
                permissions: *permissions,
            },
            Self::RemoveDir { path } => DirOp::RemoveDir {
                path: path.as_ref(),
            },
            Self::RemoveDirAll { path } => DirOp::RemoveDirAll {
                path: path.as_ref(),
            },
            Self::RemoveFile { path } => DirOp::RemoveFile {
                path: path.as_ref(),
            },
            Self::Rename { from, to } => DirOp::Rename {
                from: from.as_ref(),
                to: to.as_ref(),
            },
            Self::Copy { from, to } => DirOp::Copy {
                from: from.as_ref(),
                to: to.as_ref(),
            },
            Self::HardLink { from, to } => DirOp::HardLink {
                from: from.as_ref(),
                to: to.as_ref(),
            },
        }
    }
}

impl<P: AsRef<Path> + Copy> DirOp<P> {
    pub fn into_pathbuf(self) -> DirOp<PathBuf> {
        match self {
            Self::SetPermissions { path, permissions } => DirOp::SetPermissions {
                path: path.as_ref().into(),
                permissions,
            },
            Self::RemoveDir { path } => DirOp::RemoveDir {
                path: path.as_ref().into(),
            },
            Self::RemoveDirAll { path } => DirOp::RemoveDirAll {
                path: path.as_ref().into(),
            },
            Self::RemoveFile { path } => DirOp::RemoveFile {
                path: path.as_ref().into(),
            },
            Self::Rename { from, to } => DirOp::Rename {
                from: from.as_ref().into(),
                to: to.as_ref().into(),
            },
            Self::Copy { from, to } => DirOp::Copy {
                from: from.as_ref().into(),
                to: to.as_ref().into(),
            },
            Self::HardLink { from, to } => DirOp::HardLink {
                from: from.as_ref().into(),
                to: to.as_ref().into(),
            },
        }
    }

    pub fn into_string(self) -> DirOp<String> {
        match self {
            Self::SetPermissions { path, permissions } => DirOp::SetPermissions {
                path: path.as_ref().to_string_lossy().into(),
                permissions,
            },
            Self::RemoveDir { path } => DirOp::RemoveDir {
                path: path.as_ref().to_string_lossy().into(),
            },
            Self::RemoveDirAll { path } => DirOp::RemoveDirAll {
                path: path.as_ref().to_string_lossy().into(),
            },
            Self::RemoveFile { path } => DirOp::RemoveFile {
                path: path.as_ref().to_string_lossy().into(),
            },
            Self::Rename { from, to } => DirOp::Rename {
                from: from.as_ref().to_string_lossy().into(),
                to: to.as_ref().to_string_lossy().into(),
            },
            Self::Copy { from, to } => DirOp::Copy {
                from: from.as_ref().to_string_lossy().into(),
                to: to.as_ref().to_string_lossy().into(),
            },
            Self::HardLink { from, to } => DirOp::HardLink {
                from: from.as_ref().to_string_lossy().into(),
                to: to.as_ref().to_string_lossy().into(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DirEntry {
    pub name: String,
    pub ty: FileType,
}

impl From<std::fs::DirEntry> for DirEntry {
    fn from(d: std::fs::DirEntry) -> Self {
        let ty = d
            .file_type()
            .map(|ty| {
                if ty.is_file() {
                    FileType::File
                } else if ty.is_dir() {
                    FileType::Directory
                } else if ty.is_symlink() {
                    FileType::Symlink
                } else {
                    FileType::Unknown
                }
            })
            .unwrap_or(FileType::Unknown);

        Self {
            name: d.file_name().to_string_lossy().into(),
            ty,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum FileType {
    Unknown,
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct Permissions {}

impl From<std::fs::Permissions> for Permissions {
    fn from(_: std::fs::Permissions) -> Self {
        Self {}
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Metadata {
    pub permissions: Permissions,
    pub len: u64,
    pub modified: Option<SystemTime>,
    pub accessed: Option<SystemTime>,
    pub created: Option<SystemTime>,
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
