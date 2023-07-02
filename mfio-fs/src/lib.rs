use core::future::Future;
use core::task::Waker;
use mfio::backend::*;
use mfio::error::{Code, Error, Location, Result as MfioResult, State, Subject};
use mfio::packet::{FastCWaker, PacketId, PacketIo, Read, Write};
use mfio::stdeq::{AsyncRead, AsyncWrite, Seekable};
use std::fs;
use std::path::Path;

pub mod impls;
pub(crate) mod util;

#[repr(C)]
#[derive(Clone, Copy)]
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

macro_rules! fs_dispatch {
    ($($(#[cfg($meta:meta)])* $name:ident => $mod:ident),*$(,)?) => {

/// Native OS's filesystem
///
/// # Examples
///
/// Read a file:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mfio::packet::*;
/// use mfio::stdeq::*;
/// use mfio_fs::*;
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
/// NativeFs::default().run(|fs| async move {
///     let fh = fs.open(&filepath, OpenOptions::new().read(true)).await?;
///
///     let mut output = vec![];
///     fh.read_to_end(&mut output).await?;
///
///     assert_eq!(test_string.len(), fh.get_pos() as usize);
///     assert_eq!(test_string.as_bytes(), output);
///     mfio::error::Result::Ok(())
/// }).unwrap();
///
/// # Ok(())
/// # }
/// ```
///
/// Write a file:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # pollster::block_on(async move {
/// use mfio::packet::*;
/// use mfio::stdeq::*;
/// use mfio_fs::*;
/// use std::path::Path;
/// use std::io::Seek;
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
/// NativeFs::default().run(|fs| async move {
///     let mut fh = fs.open(
///         &filepath,
///         OpenOptions::new()
///             .read(true)
///             .write(true)
///             .create(true)
///             .truncate(true)
///         ).await?;
///
///     fh.write(&test_data).await?;
///
///     assert_eq!(test_data.len(), fh.get_pos() as usize);
///
///     fh.rewind();
///
///     // Read the data back out
///     let mut output = vec![];
///     fh.read_to_end(&mut output).await?;
///
///     assert_eq!(test_data.len(), fh.get_pos() as usize);
///     assert_eq!(test_data, output);
///     mfio::error::Result::Ok(())
/// }).unwrap();
/// # Ok(())
/// # })
/// # }
/// ```
        pub enum NativeFs {
            $($(#[cfg($meta)])* $name(impls::$mod::NativeFs)),*
        }

        impl NativeFs {
            fn register_file(&self, file: std::fs::File) -> FileWrapper {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => FileWrapper::$name(v.register_file(file))),*
                }
            }

            pub fn builder() -> NativeFsBuilder {
                NativeFsBuilder::default()
            }
        }

        impl core::fmt::Debug for NativeFs {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                match self {
                    $($(#[cfg($meta)])* Self::$name(_) => write!(f, stringify!(NativeFs::$name))),*
                }
            }
        }

        impl Default for NativeFs {
            fn default() -> Self {
                NativeFsBuilder::all_backends()
                    .build()
                    .expect("Could not initialize any FS backend")
            }
        }

        impl IoBackend for NativeFs {
            type Backend = DynBackend;

            fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
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

        /// Builder for the [`NativeFs`](NativeFs).
        ///
        /// This builder allows configuring the I/O backends to try to construct for the filesystem
        /// handle. Note that the order of backends is fixed, and is as follows:
        ///
        $(#[cfg_attr(all($($meta)*), doc = concat!("- [`", stringify!($mod), "`](impls::", stringify!($mod), ")"))])*
        ///
        /// The full order (including unsupported/disabled backends) is as follows:
        ///
        $(#[doc = concat!("- `", stringify!($mod), "`")])*
        ///
        /// If you wish to customize the construction order, please use multiple builders.
        #[derive(Default)]
        pub struct NativeFsBuilder {
            $($(#[cfg($meta)])* $mod: bool),*
        }

        impl NativeFsBuilder {
            pub fn all_backends() -> Self {
                Self {
                    $($(#[cfg($meta)])* $mod: true),*
                }
            }

            pub fn enable_all(self) -> Self {
                let _ = self;
                Self::all_backends()
            }

            $($(#[cfg($meta)])*
            #[doc = concat!("Enables the [`", stringify!($mod), "`](impls::", stringify!($mod), ") backend.")]
            pub fn $mod(self, $mod: bool) -> Self {
                Self {
                    $mod,
                    ..self
                }
            })*

            pub fn build(self) -> mfio::error::Result<NativeFs> {
                $($(#[cfg($meta)])* if self.$mod {
                    if let Ok(v) = impls::$mod::NativeFs::try_new() {
                        return Ok(NativeFs::$name(v));
                    }
                })*

                Err(Error {
                    code: Code::from_http(501).unwrap(),
                    subject: Subject::Backend,
                    state: State::Unsupported,
                    location: Location::Filesystem,
                })
            }

            pub fn build_each(self) -> Vec<(&'static str, mfio::error::Result<NativeFs>)> {
                let mut ret = vec![];

                $($(#[cfg($meta)])* if self.$mod {
                    ret.push((
                        stringify!($mod),
                        impls::$mod::NativeFs::try_new()
                            .map_err(|e| e.into())
                            .map(|v| NativeFs::$name(v))
                    ));
                })*

                ret
            }
        }

        pub enum FileWrapper {
            $($(#[cfg($meta)])* $name(impls::$mod::FileWrapper)),*
        }

        impl PacketIo<Write, u64> for FileWrapper {
            fn separate_thread_state(&mut self) {}

            fn try_new_id<'a>(&'a self, context: &mut FastCWaker) -> Option<PacketId<'a, Write, u64>> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.try_new_id(context)),*
                }
            }
        }

        impl PacketIo<Read, u64> for FileWrapper {
            fn separate_thread_state(&mut self) {}

            fn try_new_id<'a>(&'a self, context: &mut FastCWaker) -> Option<PacketId<'a, Read, u64>> {
                match self {
                    $($(#[cfg($meta)])* Self::$name(v) => v.try_new_id(context)),*
                }
            }
        }
    }
}

fs_dispatch! {
    #[cfg(all(not(miri), target_os = "linux", feature = "io-uring"))]
    IoUring => io_uring,
    #[cfg(all(not(miri), unix, feature = "mio"))]
    Mio => mio,
    Default => thread,
}

impl Fs for NativeFs {
    type FileHandle = Seekable<FileWrapper, u64>;
    type OpenFuture<'a> = core::future::Ready<MfioResult<Self::FileHandle>>;

    fn open(&self, path: &Path, options: OpenOptions) -> Self::OpenFuture<'_> {
        let file = fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .create_new(options.create_new)
            .truncate(options.truncate)
            .open(path)
            .map_err(util::from_io_error);

        core::future::ready(file.map(|file| self.register_file(file).into()))
    }
}

impl NativeFs {
    pub fn run<'a, Func: FnOnce(&'a NativeFs) -> F, F: Future>(
        &'a mut self,
        func: Func,
    ) -> F::Output {
        self.block_on(func(self))
    }
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
    use futures::stream::StreamExt;
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

        for (backend, fs) in NativeFsBuilder::all_backends().build_each() {
            println!("{backend}");
            fs.unwrap().run(|fs| async {
                let fh = fs
                    .open(&filepath, OpenOptions::new().read(true))
                    .await
                    .unwrap();

                let mut d = [MaybeUninit::uninit(); 8];

                let stream = fh.read_raw(0, &mut d[..]);
                stream.count().await;
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

        for (backend, fs) in NativeFsBuilder::all_backends().build_each() {
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

        for (backend, fs) in NativeFsBuilder::all_backends().build_each() {
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

        for (backend, fs) in NativeFsBuilder::all_backends().build_each() {
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
    fn wake_test() {
        for (backend, fs) in NativeFsBuilder::all_backends().build_each() {
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
}
