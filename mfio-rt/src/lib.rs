//! # mfio-rt
//!
//! ## mfio Backed Runtime
//!
//! This crate aims to provide building blocks for mfio backed asynchronous runtimes. The traits
//! have the option to not rely on the standard library. This makes the system great for `no_std`
//! embedded environments or kernel-side code.
//!
//! `native` feature (depends on `std`) enables native implementations of the runtime through
//! [`NativeRt`] structure.
//!
//! `virt` feature enables a virtual in-memory runtime through [`VirtRt`](virt::VirtRt) structure.
//!
//! Custom runtimes may be implemented by implementing [`IoBackend`], and any of the runtime
//! traits, such as [`Fs`] or [`Tcp`].
//!
//! ## `no_std`
//!
//! Currently, only [`Fs`] is exposed in `no_std` environments. [`Tcp`] depends on structures, such
//! as [`SocketAddr`](https://doc.rust-lang.org/nightly/core/net/enum.SocketAddr.html) that are
//! currently not available in `core`. This will change once
//! [`ip_in_core`](https://github.com/rust-lang/rust/issues/108443) is stabilized.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

use alloc::string::String;

use core::future::Future;

use core::time::Duration;
use futures::Stream;
use mfio::backend::*;
use mfio::error::Result as MfioResult;
use mfio::io::NoPos;
use mfio::stdeq::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};

#[cfg(feature = "std")]
use std::net::{SocketAddr, ToSocketAddrs};

#[cfg(feature = "std")]
pub use std::path::{Component, Path, PathBuf};

// We may later consider supporting non-unix paths in no_std scenarios, but currently, this is not
// the case, because TypedPath requires a lifetime argument.
#[cfg(not(feature = "std"))]
pub use typed_path::{UnixComponent as Component, UnixPath as Path, UnixPathBuf as PathBuf};

#[cfg(feature = "native")]
#[cfg_attr(docsrs, doc(cfg(feature = "native")))]
pub mod native;
mod util;
#[cfg(any(feature = "virt", test, miri))]
#[cfg_attr(docsrs, doc(cfg(feature = "virt")))]
pub mod virt;

#[doc(hidden)]
pub mod __doctest;

#[cfg(any(feature = "test_suite", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_suite")))]
pub mod test_suite;

#[cfg(feature = "native")]
pub use native::{NativeFile, NativeRt, NativeRtBuilder};

/// File open options.
///
/// This type is equivalent to [`OpenOptions`](std::fs::OpenOptions) found in the standard library,
/// but omits `append` mode.
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

/// Network stream shutdown options.
///
/// This type is equivalent to [`Shutdown`](std::net::Shutdown) in the standard library.
#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub enum Shutdown {
    Read,
    Write,
    Both,
}

#[cfg(feature = "std")]
use std::net;

#[cfg(feature = "std")]
impl From<net::Shutdown> for Shutdown {
    fn from(o: net::Shutdown) -> Self {
        match o {
            net::Shutdown::Write => Self::Write,
            net::Shutdown::Read => Self::Read,
            net::Shutdown::Both => Self::Both,
        }
    }
}

#[cfg(feature = "std")]
impl From<Shutdown> for net::Shutdown {
    fn from(o: Shutdown) -> Self {
        match o {
            Shutdown::Write => Self::Write,
            Shutdown::Read => Self::Read,
            Shutdown::Both => Self::Both,
        }
    }
}

/// Primary filesystem trait.
///
/// This provides an entrypoint for filesystem operations. However, since operations are typically
/// performed on a directory, this trait only serves as a proxy for retrieving the current
/// directory handle.
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

    fn open<'a>(
        &'a self,
        path: &'a Path,
        options: OpenOptions,
    ) -> <Self::DirHandle<'a> as DirHandle>::OpenFileFuture<'a> {
        self.current_dir().open_file(path, options)
    }
}

/// Represents a location in filesystem operations are performed from.
///
/// Directory handles may refer to fixed directory entries throughout time, even if said entry is
/// unlinked from the filesystem. So long as the handle is held, it may be valid. However, this
/// behavior is implementation-specific, and, for instance, [`NativeRt`] does not follow it,
/// because directory handles are simply stored as paths, rather than dir FDs/handles.
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
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use mfio_rt::{DirHandle, Fs};
    /// // On no_std mfio_rt re-exports typed_path::UnixPath as Path
    /// use mfio_rt::Path;
    ///
    /// let dir = fs.current_dir();
    ///
    /// let path = dir.path().await.unwrap();
    ///
    /// assert_ne!(path, Path::new("/dev"));
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
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
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
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use mfio::traits::IoRead;
    /// use mfio_rt::{DirHandle, Fs, OpenOptions};
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
    fn open_file<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        path: &'a P,
        options: OpenOptions,
    ) -> Self::OpenFileFuture<'a>;

    /// Opens a directory.
    ///
    /// This function accepts an absolute or relative path to a directory for reading. If the path
    /// is relative, it is opened relative to this `DirHandle`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// use futures::StreamExt;
    /// use mfio::traits::IoRead;
    /// use mfio_rt::{DirHandle, Fs, OpenOptions};
    ///
    /// let dir = fs.current_dir();
    ///
    /// let subdir = dir.open_dir("src").await.unwrap();
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
    fn open_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpenDirFuture<'a>;

    /// Retrieves file metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn metadata<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::MetadataFuture<'a>;

    /// Do an operation.
    ///
    /// This function performs an operation from the [`DirOp`] enum.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn do_op<'a, P: AsRef<Path> + ?Sized>(&'a self, operation: DirOp<&'a P>) -> Self::OpFuture<'a>;
}

/// Helpers for running directory operations more ergonomically.
pub trait DirHandleExt: DirHandle {
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn set_permissions<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        path: &'a P,
        permissions: Permissions,
    ) -> Self::OpFuture<'a> {
        self.do_op(DirOp::SetPermissions { path, permissions })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::RemoveDir { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_dir_all<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::RemoveDirAll { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn create_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::CreateDir { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn create_dir_all<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::CreateDirAll { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn remove_file<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::RemoveFile { path })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn rename<'a, P: AsRef<Path> + ?Sized>(&'a self, from: &'a P, to: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::Rename { from, to })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    // TODO: reflinking option
    fn copy<'a, P: AsRef<Path> + ?Sized>(&'a self, from: &'a P, to: &'a P) -> Self::OpFuture<'a> {
        self.do_op(DirOp::Copy { from, to })
    }

    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(miri, feature = "std", feature = "virt"))]
    /// # mfio_rt::__doctest::run_each(|fs| async {
    /// # });
    /// ```
    fn hard_link<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        from: &'a P,
        to: &'a P,
    ) -> Self::OpFuture<'a> {
        self.do_op(DirOp::HardLink { from, to })
    }

    // TODO: decide on how to handle symlinks, as they differ between platforms.
    // fn symlink_file(&self, from: &Path, to: &Path) -> Self::OpFuture<'_>;
    // fn symlink_dir(&self, from: &Path, to: &Path) -> Self::OpFuture<'_>;
}

impl<T: DirHandle> DirHandleExt for T {}

/// List of operations that can be done on a filesystem.
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum DirOp<P: AsRef<Path>> {
    SetPermissions { path: P, permissions: Permissions },
    RemoveDir { path: P },
    RemoveDirAll { path: P },
    CreateDir { path: P },
    CreateDirAll { path: P },
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
            Self::CreateDir { path } => DirOp::CreateDir {
                path: path.as_ref(),
            },
            Self::CreateDirAll { path } => DirOp::CreateDirAll {
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

impl<'a, P: AsRef<Path> + ?Sized> DirOp<&'a P> {
    pub fn into_path(self) -> DirOp<&'a Path> {
        match self {
            Self::SetPermissions { path, permissions } => DirOp::SetPermissions {
                path: path.as_ref(),
                permissions,
            },
            Self::RemoveDir { path } => DirOp::RemoveDir {
                path: path.as_ref(),
            },
            Self::RemoveDirAll { path } => DirOp::RemoveDirAll {
                path: path.as_ref(),
            },
            Self::CreateDir { path } => DirOp::CreateDir {
                path: path.as_ref(),
            },
            Self::CreateDirAll { path } => DirOp::CreateDirAll {
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
            Self::CreateDir { path } => DirOp::CreateDir {
                path: path.as_ref().into(),
            },
            Self::CreateDirAll { path } => DirOp::CreateDirAll {
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
            Self::CreateDir { path } => DirOp::CreateDir {
                path: path.as_ref().to_string_lossy().into(),
            },
            Self::CreateDirAll { path } => DirOp::CreateDirAll {
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

/// Directory list entry.
///
/// This type is equivalent to [`DirEntry`](std::fs::DirEntry) in the standard library.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DirEntry {
    pub name: String,
    pub ty: FileType,
}

#[cfg(feature = "std")]
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

/// Directory list entry type.
///
/// This type is equivalent to [`FileType`](std::fs::FileType) in the standard library.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum FileType {
    Unknown,
    File,
    Directory,
    Symlink,
}

/// Directory list entry permission.
///
/// This type is equivalent to [`Permission`](std::fs::Permissions) in the standard library.
/// However, this currently contains nothing, and is effectively useless.
///
/// TODO: make this type do something.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct Permissions {}

#[cfg(feature = "std")]
impl From<std::fs::Permissions> for Permissions {
    fn from(_: std::fs::Permissions) -> Self {
        Self {}
    }
}

/// Directory list entry metadata.
///
/// This type is equivalent to [`Metadata`](std::fs::Metadata) in the standard library.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Metadata {
    pub permissions: Permissions,
    pub len: u64,
    /// Modified time (since unix epoch, or other point)
    pub modified: Option<Duration>,
    /// Accessed time (since unix epoch, or other point)
    pub accessed: Option<Duration>,
    /// Created time (since unix epoch, or other point)
    pub created: Option<Duration>,
}

impl Metadata {
    pub fn empty_file(permissions: Permissions, created: Option<Duration>) -> Self {
        Self {
            permissions,
            len: 0,
            modified: None,
            accessed: None,
            created,
        }
    }

    pub fn empty_dir(permissions: Permissions, created: Option<Duration>) -> Self {
        Self::empty_file(permissions, created)
    }
}

/// Supertrait for file handles.
pub trait FileHandle: AsyncRead<u64> + AsyncWrite<u64> {}
impl<T: AsyncRead<u64> + AsyncWrite<u64>> FileHandle for T {}

/// Supertrait for stream handles.
pub trait StreamHandle: AsyncRead<NoPos> + AsyncWrite<NoPos> {}
impl<T: AsyncRead<NoPos> + AsyncWrite<NoPos>> StreamHandle for T {}

/// Describes TCP capable runtime operations.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
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

/// Describes operations performable on a TCP connection.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub trait TcpStreamHandle: StreamHandle {
    fn local_addr(&self) -> MfioResult<SocketAddr>;
    fn peer_addr(&self) -> MfioResult<SocketAddr>;
    fn shutdown(&self, how: Shutdown) -> MfioResult<()>;

    // These interfaces may be slightly trickier to implement, so we omit them for now.
    //fn set_ttl(&self, ttl: u32) -> MfioResult<()>;
    //fn ttl(&self) -> MfioResult<u32>;
    //fn set_nodelay(&self, nodelay: bool) -> MfioResult<()>;
    //fn nodelay(&self) -> MfioResult<bool>;
}

/// Describes operations performable on a TCP listener.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub trait TcpListenerHandle: Stream<Item = (Self::StreamHandle, SocketAddr)> {
    type StreamHandle: TcpStreamHandle;

    fn local_addr(&self) -> MfioResult<SocketAddr>;

    // These interfaces may be slightly trickier to implement, so we omit them for now.
    //fn set_ttl(&self, ttl: u32) -> MfioResult<()>;
    //fn ttl(&self) -> MfioResult<u32>;
}
