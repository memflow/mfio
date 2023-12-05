use crate::io::*;
use crate::stdeq::{self, AsyncIoFut};
use crate::util::PosShift;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::io::{AsyncRead, AsyncSeek, AsyncWrite};
use std::io::{Result, SeekFrom};

pub struct Compat<'a, Io: ?Sized> {
    io: &'a Io,
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
/// # }
/// # work().unwrap();
/// ```
pub trait FuturesCompat {
    fn compat(&self) -> Compat<Self> {
        Compat {
            io: self,
            read: None,
            write: None,
        }
    }
}

// StreamPos is needed for all I/O traits, so we use it to make sure rust gives better diagnostics.
impl<'a, Io: ?Sized + stdeq::StreamPos<u64>> FuturesCompat for Io {}

impl<'a, Io: ?Sized + stdeq::AsyncRead<u64>> AsyncRead for Compat<'a, Io>
where
    u64: PosShift<Io>,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(read) = this.read.as_mut() {
                // Update the sync handle. This is how we hack around the lifetimes of input buffer.
                #[cfg(not(mfio_assume_linear_types))]
                {
                    // SAFETY: AsyncIoFut will only use the sync object if, and only if the buffer is
                    // to be written in this poll.
                    read.sync = Some(unsafe { &mut *(buf as *mut _) });
                }

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
