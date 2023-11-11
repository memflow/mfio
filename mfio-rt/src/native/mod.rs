use core::future::{ready, Future, Ready};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;
use mfio::backend::*;
use mfio::error::{Code, Error, Location, Result as MfioResult, State, Subject};
use mfio::io::{BoundPacketView, NoPos, PacketIo, Read, Write};
use mfio::mferr;
use mfio::stdeq::Seekable;
use mfio::tarc::BaseArc;
use std::fs;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};

use crate::util::from_io_error;
use crate::{
    DirEntry, DirHandle, DirOp, Fs, Metadata, OpenOptions, Shutdown, Tcp, TcpListenerHandle,
    TcpStreamHandle,
};

#[cfg(test)]
use crate::{net_test_suite, test_suite};

mod impls;

macro_rules! fs_dispatch {
    ($($(#[cfg($meta:meta)])* $name:ident => $mod:ident),*$(,)?) => {

        pub enum NativeRtInstance {
            $($(#[cfg($meta)])* $name(impls::$mod::Runtime)),*
        }

        impl NativeRtInstance {
            fn register_file(&self, file: std::fs::File) -> NativeFile {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => NativeFile::$name(v.register_file(file))),*
                }
            }

            /// Registers a non-seekable I/O stream
            ///
            /// TODO: this perhaps should be private. We can't expose register_file publicly,
            /// because on iocp, files need to be opened with appropriate (unchangeable!) flags.
            /// Perhaps we should mirror this with streams.
            pub fn register_stream(&self, stream: TcpStream) -> NativeTcpStream {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => NativeTcpStream::$name(v.register_stream(stream))),*
                }
            }

            fn get_map_options(&self) -> fn(fs::OpenOptions) -> fs::OpenOptions {
                match self {
                    $($(#[cfg($meta)])* Self::$name(_) => impls::$mod::map_options),*
                }
            }

            pub fn cancel_all_ops(&self) {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.cancel_all_ops()),*
                }
            }
        }

        impl core::fmt::Debug for NativeRtInstance {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                match self {
                    $($(#[cfg($meta)])* Self::$name(_) => write!(f, stringify!(NativeRt::$name))),*
                }
            }
        }

        impl IoBackend for NativeRtInstance {
            type Backend = DynBackend;

            fn polling_handle(&self) -> Option<PollingHandle> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.polling_handle()),*
                }
            }

            fn get_backend(&self) -> BackendHandle<Self::Backend> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.get_backend()),*
                }
            }
        }

        /// Builder for the [`NativeRt`](NativeRt).
        ///
        /// This builder allows configuring the I/O backends to try to construct for the filesystem
        /// handle. Note that the order of backends is fixed, and is as follows:
        ///
        $(#[cfg_attr(all($($meta)*), doc = concat!("- ", stringify!($mod)))])*
        ///
        /// The full order (including unsupported/disabled backends) is as follows:
        ///
        $(#[doc = concat!("- `", stringify!($mod), "`")])*
        ///
        /// If you wish to customize the construction order, please use multiple builders.
        #[derive(Default)]
        pub struct NativeRtBuilder {
            $($(#[cfg($meta)])* $mod: bool),*
        }

        impl NativeRtBuilder {
            /// Get a `NativeRtBuilder` with all backends enabled.
            pub fn all_backends() -> Self {
                Self {
                    $($(#[cfg($meta)])* $mod: true),*
                }
            }

            /// Get a `NativeRtBuilder` with backends specified by environment.
            ///
            /// This function attempts to parse `MFIO_FS_BACKENDS` environment variable and load
            /// backends specified by it. If the environment variable is not present, or
            /// non-unicode, this function falls back to using
            /// [`all_backends`](NativeRtBuilder::all_backends).
            pub fn env_backends() -> Self {
                match std::env::var("MFIO_FS_BACKENDS") {
                    Ok(val) => {
                        let vals = val.split(',').collect::<Vec<_>>();
                        Self {
                            $($(#[cfg($meta)])* $mod: vals.contains(&stringify!($mod))),*
                        }
                    }
                    Err(_) => {
                        Self::all_backends()
                    }
                }
            }

            pub fn enable_all(self) -> Self {
                let _ = self;
                Self::all_backends()
            }

            $($(#[cfg($meta)])*
            #[doc = concat!("Enables the ", stringify!($mod), " backend.")]
            pub fn $mod(self, $mod: bool) -> Self {
                Self {
                    $mod,
                    ..self
                }
            })*

            pub fn build(self) -> mfio::error::Result<NativeRt> {
                $($(#[cfg($meta)])* if self.$mod {
                    if let Ok(v) = impls::$mod::Runtime::try_new() {
                        return Ok(NativeRtInstance::$name(v).into());
                    }
                })*

                Err(Error {
                    code: Code::from_http(501).unwrap(),
                    subject: Subject::Backend,
                    state: State::Unsupported,
                    location: Location::Filesystem,
                })
            }

            pub fn build_each(self) -> Vec<(&'static str, mfio::error::Result<NativeRt>)> {
                let mut ret = vec![];

                $($(#[cfg($meta)])* if self.$mod {
                    ret.push((
                        stringify!($mod),
                        impls::$mod::Runtime::try_new()
                            .map_err(|e| e.into())
                            .map(|v| NativeRtInstance::$name(v).into())
                    ));
                })*

                ret
            }
        }

        pub enum NativeFile {
            $($(#[cfg($meta)])* $name(impls::$mod::FileWrapper)),*
        }

        impl PacketIo<Write, u64> for NativeFile {
            fn send_io(&self, param: u64, view: BoundPacketView<Write>) {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.send_io(param, view)),*
                }
            }
        }

        impl PacketIo<Read, u64> for NativeFile {
            fn send_io(&self, param: u64, view: BoundPacketView<Read>) {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.send_io(param, view)),*
                }
            }
        }

        pub enum NativeTcpStream {
            $($(#[cfg($meta)])* $name(impls::$mod::TcpStream)),*
        }

        impl Drop for NativeTcpStream {
            fn drop(&mut self) {
                log::trace!("Drop stream");
            }
        }

        impl PacketIo<Write, NoPos> for NativeTcpStream {
            fn send_io(&self, param: NoPos, view: BoundPacketView<Write>) {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.send_io(param, view)),*
                }
            }
        }

        impl PacketIo<Read, NoPos> for NativeTcpStream {
            fn send_io(&self, param: NoPos, view: BoundPacketView<Read>) {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.send_io(param, view)),*
                }
            }
        }

        impl TcpStreamHandle for NativeTcpStream {
            fn local_addr(&self) -> MfioResult<SocketAddr> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.local_addr()),*
                }
            }

            fn peer_addr(&self) -> MfioResult<SocketAddr> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.peer_addr()),*
                }
            }

            fn shutdown(&self, how: Shutdown) -> MfioResult<()> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.shutdown(how)),*
                }
            }
        }

        pub enum NativeTcpConnectFuture<'a, A: ToSocketAddrs + 'a> {
            $($(#[cfg($meta)])* $name(impls::$mod::TcpConnectFuture<'a, A>)),*
        }

        impl<'a, A: ToSocketAddrs + Send> Future for NativeTcpConnectFuture<'a, A> {
            type Output = MfioResult<NativeTcpStream>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                // SAFETY: we are not moving the inner value
                let this = unsafe { self.get_unchecked_mut() };
                match this {
                    $($(#[cfg($meta)])* Self::$name(v) => {
                        if let Poll::Ready(v) = unsafe { Pin::new_unchecked(v).poll(cx) } {
                            Poll::Ready(v.map(NativeTcpStream::$name))
                        } else {
                            Poll::Pending
                        }
                    }),*
                }
            }
        }

        impl Tcp for NativeRtInstance {
            type StreamHandle = NativeTcpStream;
            type ListenerHandle = NativeTcpListener;
            type ConnectFuture<'a, A: ToSocketAddrs + Send + 'a> = NativeTcpConnectFuture<'a, A>;
            type BindFuture<'a, A: ToSocketAddrs + Send + 'a> = core::future::Ready<MfioResult<NativeTcpListener>>;

            fn connect<'a, A: ToSocketAddrs + Send + 'a>(
                &'a self,
                addrs: A,
            ) -> Self::ConnectFuture<'a, A> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => NativeTcpConnectFuture::$name(v.tcp_connect(addrs))),*
                }
            }

            fn bind<'a, A: ToSocketAddrs + Send + 'a>(&'a self, addrs: A) -> Self::BindFuture<'a, A> {
                let listener = std::net::TcpListener::bind(addrs);
                core::future::ready(
                    listener.map(|l| match self {
                        $($(#[cfg($meta)])* Self::$name(v) => NativeTcpListener::$name(v.register_listener(l))),*
                    }).map_err(from_io_error)
                )
            }
        }

        pub enum NativeTcpListener {
            $($(#[cfg($meta)])* $name(impls::$mod::TcpListener)),*
        }

        impl Stream for NativeTcpListener {
            type Item = (NativeTcpStream, SocketAddr);

            fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
                let this = unsafe { self.get_unchecked_mut() };
                match this {
                    $($(#[cfg($meta)])* Self::$name(v) => {
                        if let Poll::Ready(v) = unsafe { Pin::new_unchecked(v).poll_next(cx) } {
                            Poll::Ready(v.map(|(a, b)| (NativeTcpStream::$name(a), b)))
                        } else {
                            Poll::Pending
                        }
                    }),*
                }
            }
        }

        impl TcpListenerHandle for NativeTcpListener {
            type StreamHandle = NativeTcpStream;

            fn local_addr(&self) -> MfioResult<SocketAddr> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.local_addr()),*
                }
            }
        }
    }
}

fs_dispatch! {
    #[cfg(all(not(miri), target_os = "linux", feature = "io-uring"))]
    IoUring => io_uring,
    #[cfg(all(not(miri), target_os = "windows", feature = "iocp"))]
    Iocp => iocp,
    #[cfg(all(not(miri), unix, feature = "mio"))]
    Mio => mio,
    Default => thread,
}

const _: () = {
    const fn verify_send<T: Send>() {}
    const fn verify_sync<T: Sync>() {}

    verify_send::<NativeRtInstance>();
    verify_send::<NativeFile>();
    verify_send::<NativeTcpStream>();
    verify_send::<NativeTcpListener>();
};

/// Native OS backed runtime
///
/// # Examples
///
/// Read a file:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mfio::io::*;
/// use mfio::stdeq::*;
/// use mfio_rt::*;
/// use std::fs::write;
/// use std::path::Path;
///
/// let test_string = "Test test 42";
/// let mut filepath = std::env::temp_dir();
/// filepath.push("mfio-fs-test-read");
///
/// // Create a test file:
/// write(&filepath, test_string.as_bytes())?;
///
/// // Create mfio's filesystem
/// NativeRt::default()
///     .run(|fs| async move {
///         let fh = fs.open(&filepath, OpenOptions::new().read(true)).await?;
///
///         let mut output = vec![];
///         fh.read_to_end(&mut output).await?;
///
///         assert_eq!(test_string.len(), fh.get_pos() as usize);
///         assert_eq!(test_string.as_bytes(), output);
///         mfio::error::Result::Ok(())
///     })
///     .unwrap();
///
/// # Ok(())
/// # }
/// ```
///
/// Write a file:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # pollster::block_on(async move {
/// use mfio::io::*;
/// use mfio::stdeq::*;
/// use mfio_rt::*;
/// use std::io::Seek;
/// use std::path::Path;
///
/// let mut test_data = vec![];
///
/// for i in 0u8..128 {
///     test_data.extend(i.to_ne_bytes());
/// }
///
/// let mut filepath = std::env::temp_dir();
/// filepath.push("mfio-fs-test-write");
///
/// // Create mfio's filesystem
/// NativeRt::default()
///     .run(|fs| async move {
///         let mut fh = fs
///             .open(
///                 &filepath,
///                 OpenOptions::new()
///                     .read(true)
///                     .write(true)
///                     .create(true)
///                     .truncate(true),
///             )
///             .await?;
///
///         fh.write(&test_data).await?;
///
///         assert_eq!(test_data.len(), fh.get_pos() as usize);
///
///         fh.rewind();
///
///         // Read the data back out
///         let mut output = vec![];
///         fh.read_to_end(&mut output).await?;
///
///         assert_eq!(test_data.len(), fh.get_pos() as usize);
///         assert_eq!(test_data, output);
///         mfio::error::Result::Ok(())
///     })
///     .unwrap();
/// # Ok(())
/// # })
/// # }
/// ```
#[derive(Debug)]
pub struct NativeRt {
    cwd: NativeRtDir,
}

impl IoBackend for NativeRt {
    type Backend = <NativeRtInstance as IoBackend>::Backend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        self.cwd.instance.polling_handle()
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.cwd.instance.get_backend()
    }
}

impl Tcp for NativeRt {
    type StreamHandle = <NativeRtInstance as Tcp>::StreamHandle;
    type ListenerHandle = <NativeRtInstance as Tcp>::ListenerHandle;
    type ConnectFuture<'a, A: ToSocketAddrs + Send + 'a> =
        <NativeRtInstance as Tcp>::ConnectFuture<'a, A>;
    type BindFuture<'a, A: ToSocketAddrs + Send + 'a> =
        <NativeRtInstance as Tcp>::BindFuture<'a, A>;

    fn connect<'a, A: ToSocketAddrs + Send + 'a>(&'a self, addrs: A) -> Self::ConnectFuture<'a, A> {
        self.cwd.instance.connect(addrs)
    }

    fn bind<'a, A: ToSocketAddrs + Send + 'a>(&'a self, addrs: A) -> Self::BindFuture<'a, A> {
        self.cwd.instance.bind(addrs)
    }
}

impl Default for NativeRt {
    fn default() -> Self {
        NativeRtBuilder::env_backends()
            .build()
            .expect("Could not initialize any FS backend")
    }
}

impl Fs for NativeRt {
    type DirHandle<'a> = NativeRtDir;

    fn current_dir(&self) -> &Self::DirHandle<'_> {
        &self.cwd
    }
}

impl NativeRt {
    pub fn builder() -> NativeRtBuilder {
        NativeRtBuilder::default()
    }

    pub fn instance(&self) -> &BaseArc<NativeRtInstance> {
        &self.cwd.instance
    }

    pub fn run<'a, Func: FnOnce(&'a NativeRt) -> F, F: Future>(
        &'a mut self,
        func: Func,
    ) -> F::Output {
        self.block_on(func(self))
    }

    /// Registers a non-seekable I/O stream
    pub fn register_stream(&self, stream: TcpStream) -> NativeTcpStream {
        self.cwd.instance.register_stream(stream)
    }

    pub fn cancel_all_ops(&self) {
        self.cwd.instance.cancel_all_ops()
    }

    pub fn set_cwd(&mut self, dir: PathBuf) {
        self.cwd.dir = Some(dir);
    }
}

impl From<NativeRtInstance> for NativeRt {
    fn from(instance: NativeRtInstance) -> Self {
        let (ops, rx) = flume::bounded(16);

        // TODO: store the join handle
        std::thread::spawn(move || rx.into_iter().for_each(RtBgOp::process));

        Self {
            cwd: NativeRtDir {
                dir: None,
                ops,
                instance: BaseArc::from(instance),
            },
        }
    }
}

impl NativeRtDir {
    fn join_path<P: AsRef<Path>>(&self, other: P) -> std::io::Result<PathBuf> {
        if other.as_ref().is_absolute() {
            Ok(other.as_ref().into())
        } else {
            self.get_path().map(|v| v.join(other))
        }
    }
    fn get_path(&self) -> std::io::Result<PathBuf> {
        if let Some(dir) = self.dir.clone() {
            Ok(dir)
        } else {
            std::env::current_dir()
        }
    }
}

impl DirHandle for NativeRtDir {
    type FileHandle = Seekable<NativeFile, u64>;
    type OpenFileFuture<'a> = OpenFileFuture<'a>;
    type PathFuture<'a> = Ready<MfioResult<PathBuf>>;
    type OpenDirFuture<'a> = Ready<MfioResult<Self>>;
    type ReadDir<'a> = ReadDir;
    type ReadDirFuture<'a> = Ready<MfioResult<ReadDir>>;
    type MetadataFuture<'a> = MetadataFuture;
    type OpFuture<'a> = OpFuture;

    /// Gets the absolute path to this `DirHandle`.
    fn path(&self) -> Self::PathFuture<'_> {
        ready(self.get_path().map_err(from_io_error))
    }

    /// Reads the directory contents.
    ///
    /// This function, upon success, returns a stream that can be used to list files and
    /// subdirectories within this dir.
    ///
    /// Note that on various platforms this may behave differently. For instance, Unix platforms
    /// support holding
    fn read_dir(&self) -> Self::ReadDirFuture<'_> {
        ready(
            self.get_path()
                .and_then(|v| v.read_dir())
                .map_err(from_io_error)
                .map(|instance| ReadDir { instance }),
        )
    }

    /// Opens a file.
    ///
    /// This function accepts an absolute or relative path to a file for reading. If the path is
    /// relative, it is opened relative to this `DirHandle`.
    fn open_file<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        path: &'a P,
        options: OpenOptions,
    ) -> Self::OpenFileFuture<'a> {
        let (tx, rx) = oneshot::channel();

        if let Ok(path) = self.join_path(path) {
            let _ = self.ops.send(RtBgOp::OpenFile {
                path,
                options,
                map_options: self.instance.get_map_options(),
                completion: tx,
            });
        } else {
            let _ = tx.send(Err(mferr!(Directory, Unavailable, Filesystem)));
        }

        OpenFileFuture {
            rt: self,
            completion: rx,
        }
    }

    /// Opens a directory.
    ///
    /// This function accepts an absolute or relative path to a directory for reading. If the path
    /// is relative, it is opened relative to this `DirHandle`.
    fn open_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpenDirFuture<'a> {
        let dir = self.join_path(path).map_err(from_io_error).and_then(|v| {
            if v.is_dir() {
                Ok(Self {
                    dir: Some(v),
                    ops: self.ops.clone(),
                    instance: self.instance.clone(),
                })
            } else if v.exists() {
                Err(mferr!(Path, Invalid, Filesystem))
            } else {
                Err(mferr!(Path, NotFound, Filesystem))
            }
        });

        ready(dir)
    }

    fn metadata<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::MetadataFuture<'a> {
        let (tx, rx) = oneshot::channel();

        if let Ok(path) = self.join_path(path) {
            let _ = self.ops.send(RtBgOp::Metadata {
                path,
                completion: tx,
            });
        } else {
            let _ = tx.send(Err(mferr!(Directory, Unavailable, Filesystem)));
        }

        MetadataFuture { completion: rx }
    }

    /// Do an operation.
    ///
    /// This function performs an operation from the [`DirOp`] enum.
    fn do_op<'a, P: AsRef<Path> + ?Sized>(&'a self, operation: DirOp<&'a P>) -> Self::OpFuture<'a> {
        let (tx, rx) = oneshot::channel();

        let _ = self.ops.send(RtBgOp::DirOp {
            op: operation.as_path().into_pathbuf(),
            completion: tx,
        });

        OpFuture { completion: rx }
    }
}

pub struct ReadDir {
    instance: fs::ReadDir,
}

impl Stream for ReadDir {
    type Item = MfioResult<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        Poll::Ready(
            this.instance
                .next()
                .map(|v| v.map(From::from).map_err(from_io_error)),
        )
    }
}

pub struct MetadataFuture {
    completion: oneshot::Receiver<MfioResult<Metadata>>,
}

impl Future for MetadataFuture {
    type Output = MfioResult<Metadata>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let completion = unsafe { Pin::new_unchecked(&mut this.completion) };
        match completion.poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(_)) => Poll::Ready(Err(mferr!(Output, BrokenPipe, Filesystem))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct OpenFileFuture<'a> {
    rt: &'a NativeRtDir,
    completion: oneshot::Receiver<MfioResult<std::fs::File>>,
}

impl<'a> Future for OpenFileFuture<'a> {
    type Output = MfioResult<<NativeRtDir as DirHandle>::FileHandle>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let completion = unsafe { Pin::new_unchecked(&mut this.completion) };
        match completion.poll(cx) {
            Poll::Ready(Ok(file)) => {
                Poll::Ready(file.map(|f| this.rt.instance.register_file(f).into()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(mferr!(Output, BrokenPipe, Filesystem))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct OpFuture {
    completion: oneshot::Receiver<MfioResult<()>>,
}

impl Future for OpFuture {
    type Output = MfioResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let completion = unsafe { Pin::new_unchecked(&mut this.completion) };
        match completion.poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(_)) => Poll::Ready(Err(mferr!(Output, BrokenPipe, Filesystem))),
            Poll::Pending => Poll::Pending,
        }
    }
}

enum RtBgOp {
    OpenFile {
        path: PathBuf,
        options: OpenOptions,
        map_options: fn(std::fs::OpenOptions) -> std::fs::OpenOptions,
        completion: oneshot::Sender<MfioResult<std::fs::File>>,
    },
    DirOp {
        op: DirOp<PathBuf>,
        completion: oneshot::Sender<MfioResult<()>>,
    },
    Metadata {
        path: PathBuf,
        completion: oneshot::Sender<MfioResult<Metadata>>,
    },
}

impl RtBgOp {
    fn process(self) {
        match self {
            Self::OpenFile {
                path,
                options,
                map_options,
                completion,
            } => {
                let mut fs_options = fs::OpenOptions::new();

                fs_options
                    .read(options.read)
                    .write(options.write)
                    .create(options.create)
                    .create_new(options.create_new)
                    .truncate(options.truncate);

                let file = map_options(fs_options)
                    .open(path)
                    .map_err(crate::util::from_io_error);

                let _ = completion.send(file);
            }
            Self::DirOp { op, completion } => {
                let ret = match op {
                    DirOp::SetPermissions { .. } => {
                        // FIXME
                        // This needs to be made platform specific, because we don't have a way to
                        // build permissions object ourselves.
                        Err(std::io::ErrorKind::Unsupported.into())
                    }
                    DirOp::RemoveDir { path } => fs::remove_dir(path),
                    DirOp::RemoveDirAll { path } => fs::remove_dir_all(path),
                    DirOp::CreateDir { path } => fs::create_dir(path),
                    DirOp::CreateDirAll { path } => fs::create_dir_all(path),
                    DirOp::RemoveFile { path } => fs::remove_file(path),
                    DirOp::Rename { from, to } => fs::rename(from, to),
                    DirOp::Copy { from, to } => fs::copy(from, to).map(|_| ()),
                    DirOp::HardLink { from, to } => fs::hard_link(from, to),
                };
                let _ = completion.send(ret.map_err(from_io_error));
            }
            Self::Metadata { path, completion } => {
                let to_epoch_duration = |v: std::time::SystemTime| {
                    v.duration_since(std::time::SystemTime::UNIX_EPOCH).ok()
                };

                let res = path
                    .metadata()
                    .map(|m| Metadata {
                        permissions: m.permissions().into(),
                        len: m.len(),
                        modified: m.modified().ok().and_then(to_epoch_duration),
                        accessed: m.accessed().ok().and_then(to_epoch_duration),
                        created: m.created().ok().and_then(to_epoch_duration),
                    })
                    .map_err(from_io_error);
                let _ = completion.send(res);
            }
        }
    }
}

#[derive(Debug)]
pub struct NativeRtDir {
    dir: Option<PathBuf>,
    ops: flume::Sender<RtBgOp>,
    instance: BaseArc<NativeRtInstance>,
}

#[cfg(test)]
mod tests {
    //! There is not much to test here! But it is invaluable to use say thread pool based
    //! implementation to verify data races when using miri/tsan. `mfio-fs` proves to be incredibly
    //! valuable in doing that!

    use super::*;
    use core::future::poll_fn;
    use core::mem::MaybeUninit;
    use core::task::Poll;
    use mfio::stdeq::*;
    use mfio::traits::*;
    use std::fs::write;
    use std::io::Seek;

    #[test]
    fn simple_io() {
        // Running this test under miri verifies correctness of basic
        // cross-thread communication.
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-simple-io");

        write(&filepath, test_string.as_bytes()).unwrap();

        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|fs: &NativeRt| async {
                let fh = fs
                    .open(&filepath, OpenOptions::new().read(true))
                    .await
                    .unwrap();

                let mut d = [MaybeUninit::uninit(); 8];

                fh.read_all(0, &mut d[..]).await.unwrap();
            });
        }
    }

    #[test]
    fn read_all() {
        // Running this test under miri verifies correctness of basic
        // cross-thread communication.
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-read-all");

        write(&filepath, test_string.as_bytes()).unwrap();

        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|fs| async {
                let fh = fs
                    .open(&filepath, OpenOptions::new().read(true))
                    .await
                    .unwrap();

                let mut d = [MaybeUninit::uninit(); 8];

                fh.read_all(0, &mut d[..]).await.unwrap();
            });
        }
    }

    #[test]
    fn write_test() {
        let mut test_data = vec![];

        for i in 0u8..128 {
            test_data.extend(i.to_ne_bytes());
        }

        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-write");

        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|fs| async {
                let mut fh = fs
                    .open(
                        &filepath,
                        OpenOptions::new()
                            .read(true)
                            .write(true)
                            .create(true)
                            .truncate(true),
                    )
                    .await
                    .unwrap();

                AsyncWrite::write(&fh, &test_data).await.unwrap();

                assert_eq!(test_data.len(), fh.get_pos() as usize);

                fh.rewind().unwrap();

                // Read the data back out
                let mut output = vec![];
                AsyncRead::read_to_end(&fh, &mut output).await.unwrap();

                assert_eq!(test_data.len(), fh.get_pos() as usize);
                assert_eq!(test_data, output);

                core::mem::drop(fh);
            });
        }
    }

    #[test]
    fn read_to_end() {
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-read-to-end");

        // Create a test file:
        write(&filepath, test_string.as_bytes()).unwrap();

        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|fs| async {
                let fh = fs
                    .open(&filepath, OpenOptions::new().read(true))
                    .await
                    .unwrap();

                let mut output = vec![];
                AsyncRead::read_to_end(&fh, &mut output).await.unwrap();

                assert_eq!(test_string.len(), fh.get_pos() as usize);
                assert_eq!(test_string.as_bytes(), output);
            });
        }
    }

    #[test]
    fn wake_test_single() {
        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|_| async move {
                for i in 0..2 {
                    println!("{i}");
                    let mut signaled = false;
                    poll_fn(|cx| {
                        println!("{signaled}");
                        if signaled {
                            Poll::Ready(())
                        } else {
                            signaled = true;
                            let waker = cx.waker().clone();
                            std::thread::spawn(|| {
                                std::thread::sleep(std::time::Duration::from_millis(200));
                                println!("WAKE");
                                waker.wake();
                            });
                            Poll::Pending
                        }
                    })
                    .await;
                }
            });
        }
    }

    #[test]
    fn wake_test_dropped() {
        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            let (tx1, rx1) = std::sync::mpsc::channel();
            let rx1 = BaseArc::new(parking_lot::Mutex::new(rx1));
            let (tx2, rx2) = std::sync::mpsc::channel();

            {
                fs.unwrap().run(|_| async move {
                    poll_fn(|cx| {
                        let tx2 = tx2.clone();
                        let rx1 = rx1.clone();
                        let waker = cx.waker().clone();
                        std::thread::spawn(move || {
                            rx1.lock().recv().unwrap();
                            println!("WAKE");
                            waker.wake();
                            println!("Woke");
                            tx2.send(()).unwrap();
                        });
                        Poll::Ready(())
                    })
                    .await;
                });
            }

            tx1.send(()).unwrap();
            rx2.recv().unwrap();
        }
    }

    #[test]
    fn wake_test_lot() {
        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|_| async move {
                #[cfg(miri)]
                let wakes = 20;
                #[cfg(not(miri))]
                let wakes = 2000;
                for i in 0..wakes {
                    println!("{i}");
                    let mut signaled = false;
                    poll_fn(|cx| {
                        println!("{signaled}");
                        if signaled {
                            Poll::Ready(())
                        } else {
                            signaled = true;
                            let waker = cx.waker().clone();
                            std::thread::spawn(|| {
                                println!("WAKE");
                                waker.wake();
                                println!("Woke");
                            });
                            Poll::Pending
                        }
                    })
                    .await;
                }
            });
        }
    }

    #[test]
    fn self_wake() {
        // Verifies that all backends support self waking correctly
        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|_| async move {
                #[cfg(miri)]
                let wakes = 20;
                #[cfg(not(miri))]
                let wakes = 2000;
                for i in 0..wakes {
                    println!("{i}");
                    let mut signaled = false;
                    poll_fn(|cx| {
                        println!("{signaled}");
                        if signaled {
                            Poll::Ready(())
                        } else {
                            signaled = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await;
                }
            });
        }
    }

    #[test]
    fn self_no_doublewake() {
        // Verifies that no backend incorrectly wakes itself up when not needed
        for (backend, fs) in NativeRtBuilder::all_backends().build_each() {
            println!("{backend}");

            let (tx, rx) = std::sync::mpsc::channel();

            fs.unwrap().run(|_| async move {
                let mut signaled = 0;
                poll_fn(|cx| {
                    println!("{signaled}");
                    if signaled > 1 {
                        Poll::Ready(())
                    } else {
                        signaled += 1;
                        if signaled == 1 {
                            cx.waker().wake_by_ref();
                        } else {
                            let waker = cx.waker().clone();
                            let tx = tx.clone();
                            std::thread::spawn(move || {
                                std::thread::sleep(std::time::Duration::from_millis(200));
                                println!("WAKE");
                                tx.send(()).unwrap();
                                waker.wake();
                            });
                        }
                        Poll::Pending
                    }
                })
                .await;
            });

            assert_eq!(Ok(()), rx.try_recv());
        }
    }
}

#[cfg(test)]
test_suite!(tests_default, |test_name, closure| {
    let _ = ::env_logger::builder().is_test(true).try_init();
    let mut rt = crate::NativeRt::default();
    let rt = staticify(&mut rt);
    let dir = TempDir::new(test_name).unwrap();
    rt.set_cwd(dir.path().to_path_buf());
    rt.run(move |rt| {
        let run = TestRun::new(rt, dir);
        closure(run)
    });
});

#[cfg(test)]
test_suite!(tests_all, |test_name, closure| {
    let _ = ::env_logger::builder().is_test(true).try_init();
    for (name, rt) in crate::NativeRt::builder().enable_all().build_each() {
        println!("{name}");
        if let Ok(mut rt) = rt {
            let rt = staticify(&mut rt);
            let dir = TempDir::new(test_name).unwrap();
            rt.set_cwd(dir.path().to_path_buf());
            rt.run(move |rt| {
                let run = TestRun::new(rt, dir);
                closure(run)
            });
        }
    }
});

#[cfg(test)]
net_test_suite!(net_tests_default, |closure| {
    let _ = ::env_logger::builder().is_test(true).try_init();
    let mut rt = crate::NativeRt::default();
    let rt = staticify(&mut rt);
    rt.run(closure);
});

#[cfg(test)]
net_test_suite!(net_tests_all, |closure| {
    let _ = ::env_logger::builder().is_test(true).try_init();
    for (name, rt) in crate::NativeRt::builder().enable_all().build_each() {
        println!("{name}");
        if let Ok(mut rt) = rt {
            let rt = staticify(&mut rt);
            rt.run(closure);
        }
    }
});
