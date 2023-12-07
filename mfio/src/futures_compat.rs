//! Provides compatibility with `futures` traits.

use crate::io::*;
use crate::stdeq::{self, AsyncIoFut};
use crate::util::PosShift;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
#[cfg(not(mfio_assume_linear_types))]
use futures::io::AsyncRead;
use futures::io::{AsyncSeek, AsyncWrite};
use std::io::{Result, SeekFrom};

/// Container for intermediate values.
///
/// Currently, reading and writing is not cancel safe. Meaning, cancelling the I/O operation and
/// issuing a new one would continue the previous operation, and sync the results to the currently
/// provided buffer. Note that the types of operations are handled separately so they do not mix
/// and it is okay to cancel a read to issue a write.
///
/// If you wish to cancel the operation, do drop the entire `Compat` object. However, be warned
/// that `mfio` may panic, since it does not yet support cancellation at all.
///
/// Note that at the time of writing, `AsyncRead` is not supported when `mfio_assume_linear_types`
/// config is set.
pub struct Compat<'a, Io: ?Sized> {
    io: &'a Io,
    #[cfg(not(mfio_assume_linear_types))]
    read: Option<AsyncIoFut<'a, Io, Write, u64, &'a mut [u8]>>,
    write: Option<AsyncIoFut<'a, Io, Read, u64, &'a [u8]>>,
}

/// Bridges mfio with futures.
///
/// # Examples
///
/// Read from mfio object through futures traits.
///
/// ```rust
/// # mod sample {
/// #     include!("sample.rs");
/// # }
/// # use sample::SampleIo;
/// # fn work() -> mfio::error::Result<()> {
/// # mfio::linear_types_switch!(
/// #     Linear => { Ok(()) }
/// #     Standard => {{
/// use futures::io::{AsyncReadExt, Cursor};
/// use mfio::backend::*;
/// use mfio::futures_compat::FuturesCompat;
/// use mfio::stdeq::SeekableRef;
///
/// let mem = vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144];
/// let handle = SampleIo::new(mem.clone());
///
/// handle.block_on(async {
///     let mut buf = Cursor::new(vec![0; mem.len()]);
///
///     let handle = SeekableRef::from(&handle);
///     futures::io::copy(handle.compat(), &mut buf).await?;
///     assert_eq!(mem, buf.into_inner());
///
///     Ok(())
/// })
/// # }}
/// # )
/// # }
/// # work().unwrap();
/// ```
///
/// Write using futures traits.
///
/// ```rust
/// # mod sample {
/// #     include!("sample.rs");
/// # }
/// # use sample::SampleIo;
/// # fn work() -> mfio::error::Result<()> {
/// use futures::io::AsyncWriteExt;
/// use mfio::backend::*;
/// use mfio::futures_compat::FuturesCompat;
/// use mfio::stdeq::SeekableRef;
/// use mfio::traits::IoRead;
///
/// let mut mem = vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144];
/// let handle = SampleIo::new(mem.clone());
///
/// handle.block_on(async {
///     let handle = SeekableRef::from(&handle);
///     handle.compat().write_all(&[9, 9, 9]).await?;
///
///     handle.read_all(0, &mut mem[..5]).await.unwrap();
///     assert_eq!(&mem[..5], &[9, 9, 9, 2, 3]);
///
///     Ok(())
/// })
/// # }
/// # work().unwrap();
/// ```
pub trait FuturesCompat {
    fn compat(&self) -> Compat<Self> {
        Compat {
            io: self,
            #[cfg(not(mfio_assume_linear_types))]
            read: None,
            write: None,
        }
    }
}

// StreamPos is needed for all I/O traits, so we use it to make sure rust gives better diagnostics.
impl<Io: ?Sized + stdeq::StreamPos<u64>> FuturesCompat for Io {}

// Currently we cannot guarantee that the user won't swap the buffer when using linear types.
// FIXME: always allocate an intermediary and sync in `Compat`. This way we could also retain the
// buffer, so that's nice.
#[cfg(not(mfio_assume_linear_types))]
#[cfg_attr(docsrs, doc(cfg(not(mfio_assume_linear_types))))]
impl<'a, Io: ?Sized + stdeq::AsyncRead<u64>> AsyncRead for Compat<'a, Io>
where
    u64: PosShift<Io>,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(read) = this.read.as_mut() {
                // Update the sync handle. This is how we hack around the lifetimes of input buffer.
                // SAFETY: AsyncIoFut will only use the sync object if, and only if the buffer is
                // to be written in this poll.
                read.sync = Some(unsafe { &mut *(buf as *mut _) });

                let read = unsafe { Pin::new_unchecked(read) };

                break read.poll(cx).map(|v| {
                    this.read = None;
                    v.map_err(|_| std::io::ErrorKind::Other.into())
                });
            } else {
                // SAFETY: on mfio_assume_linear_types, this is unsafe. Without the switch this is
                // safe, because the buffer is stored in a sync variable that is only used whenever
                // the I/O completes. That is processed in this poll function, and we update the
                // sync at every iteration of the loop.
                let buf = unsafe { &mut *(buf as *mut _) };
                this.read = Some(stdeq::AsyncRead::read(this.io, buf));
            }
        }
    }
}

impl<'a, Io: ?Sized + stdeq::AsyncWrite<u64>> AsyncWrite for Compat<'a, Io>
where
    u64: PosShift<Io>,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(write) = this.write.as_mut() {
                let write = unsafe { Pin::new_unchecked(write) };

                break write.poll(cx).map(|v| {
                    this.write = None;
                    v.map_err(|_| std::io::ErrorKind::Other.into())
                });
            } else {
                // SAFETY: on mfio_assume_linear_types, this is unsafe. Without the switch this is
                // safe, because the buffer is transferred to an intermediate one before this
                // function returns..
                let buf = unsafe { &*(buf as *const _) };
                this.write = Some(stdeq::AsyncWrite::write(this.io, buf));
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        // Completion of every request currently implies we've flushed.
        // TODO: improve semantics
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        // We currently imply that we can just close on drop.
        // TODO: improve semantics
        Poll::Ready(Ok(()))
    }
}

impl<'a, Io: ?Sized + stdeq::StreamPos<u64>> AsyncSeek for Compat<'a, Io> {
    fn poll_seek(self: Pin<&mut Self>, _: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let this = unsafe { self.get_unchecked_mut() };
        Poll::Ready(stdeq::std_seek(this.io, pos))
    }
}
