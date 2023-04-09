use mfio::stdeq::{AsyncRead, AsyncWrite, Seekable};
use std::fs;
use std::path::Path;

pub mod impls;

pub type FileWrapper = impls::FileWrapper;

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

pub trait Fs {
    type FileHandle: FileHandle;

    fn open(&self, path: &Path, options: OpenOptions) -> Self::FileHandle;
}

pub trait FileHandle: AsyncRead<u64> + AsyncWrite<u64> {}
impl<T: AsyncRead<u64> + AsyncWrite<u64>> FileHandle for T {}

/// Native OS's filesystem
///
/// # Examples
///
/// Read a file:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # pollster::block_on(async move {
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
/// let fs = NativeFs;
///
/// let mut fh = fs.open(&filepath, OpenOptions::new().read(true));
///
/// let mut output = vec![];
/// fh.read_to_end(&mut output).await.unwrap();
///
/// assert_eq!(test_string.len(), fh.get_pos() as usize);
/// assert_eq!(test_string.as_bytes(), output);
///
/// # Ok(())
/// # })
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
/// let fs = NativeFs;
///
/// let mut fh = fs.open(
///     &filepath,
///     OpenOptions::new()
///         .read(true)
///         .write(true)
///         .create(true)
///         .truncate(true)
///     );
///
/// fh.write(&test_data).await;
///
/// assert_eq!(test_data.len(), fh.get_pos() as usize);
///
/// fh.rewind();
///
/// // Read the data back out
/// let mut output = vec![];
/// fh.read_to_end(&mut output).await.unwrap();
///
/// assert_eq!(test_data.len(), fh.get_pos() as usize);
/// assert_eq!(test_data, output);
///
/// # Ok(())
/// # })
/// # }
/// ```
pub struct NativeFs;

impl Fs for NativeFs {
    type FileHandle = Seekable<FileWrapper, u64>;

    fn open(&self, path: &Path, options: OpenOptions) -> Self::FileHandle {
        let file = fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .create_new(options.create_new)
            .truncate(options.truncate)
            .open(path)
            .unwrap();

        FileWrapper::from(file).into()
    }
}

#[cfg(test)]
mod tests {
    //! There is not much to test here! But it is invaluable to use say thread pool based
    //! implementation to verify data races when using miri/tsan. `mfio-fs` proves to be incredibly
    //! valuable in doing that!

    use super::*;
    use core::mem::MaybeUninit;
    use futures::stream::StreamExt;
    use mfio::stdeq::*;
    use mfio::traits::*;
    use std::fs::write;
    use std::io::Seek;

    #[pollster::test]
    async fn simple_io() {
        // Running this test under miri verifies correctness of basic
        // cross-thread communication.
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-simple-io");

        write(&filepath, test_string.as_bytes()).unwrap();

        let fs = NativeFs;

        let fh = fs.open(&filepath, OpenOptions::new().read(true));

        let mut d = [MaybeUninit::uninit(); 8];

        let stream = fh.read_raw(0, &mut d[..]);
        stream.count().await;
    }

    #[pollster::test]
    async fn read_all() {
        // Running this test under miri verifies correctness of basic
        // cross-thread communication.
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-read-all");

        write(&filepath, test_string.as_bytes()).unwrap();

        let fs = NativeFs;

        let fh = fs.open(&filepath, OpenOptions::new().read(true));

        let mut d = [MaybeUninit::uninit(); 8];

        fh.read_all(0, &mut d[..]).await;
    }

    #[pollster::test]
    async fn write_test() {
        let mut test_data = vec![];

        for i in 0u8..128 {
            test_data.extend(i.to_ne_bytes());
        }

        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-write");

        // Create mfio's filesystem
        let fs = NativeFs;

        let mut fh = fs.open(
            &filepath,
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true),
        );

        AsyncWrite::write(&mut fh, &test_data).await.unwrap();

        assert_eq!(test_data.len(), fh.get_pos() as usize);

        fh.rewind().unwrap();

        // Read the data back out
        let mut output = vec![];
        AsyncRead::read_to_end(&mut fh, &mut output).await.unwrap();

        assert_eq!(test_data.len(), fh.get_pos() as usize);
        assert_eq!(test_data, output);

        core::mem::drop(fh);
    }

    #[pollster::test]
    async fn read_to_end() {
        let test_string = "Test test 42";
        let mut filepath = std::env::temp_dir();
        filepath.push("mfio-fs-test-read-to-end");

        // Create a test file:
        write(&filepath, test_string.as_bytes()).unwrap();

        // Create mfio's filesystem
        let fs = NativeFs;

        let mut fh = fs.open(&filepath, OpenOptions::new().read(true));

        let mut output = vec![];
        AsyncRead::read_to_end(&mut fh, &mut output).await.unwrap();

        assert_eq!(test_string.len(), fh.get_pos() as usize);
        assert_eq!(test_string.as_bytes(), output);
    }
}
