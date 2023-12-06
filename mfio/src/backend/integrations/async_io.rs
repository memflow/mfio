//! `async-io` 2.0 integration.
//!
//! We technically support `async-io` 1, however, the system had a
//! [limitation](https://github.com/smol-rs/async-io/issues/132) that was only resolved in version
//! 2.

use async_io::Async;
use std::os::fd::BorrowedFd;

use super::super::*;
use super::{BorrowingFn, Integration};

/// async-io integration.
///
/// Unlike [`Null`], this integration supports backends with polling handles, however, only
/// async-io based runtimes are supported, such as smol and async_std.
///
/// Internally, this uses async-io's [`Async`] to wait for readiness of the polling FD, which means
/// only unix platforms are supported.
///
/// # Examples
///
/// Using the integration with smol:
///
/// ```
/// # mod sample {
/// #     include!("../../sample.rs");
/// # }
/// # use sample::SampleIo;
/// use mfio::prelude::v1::*;
///
/// # #[cfg(all(unix, not(miri)))]
/// smol::block_on(async {
///     let mut handle = SampleIo::new(vec![1, 2, 3, 4]);
///
///     // Run the integration. Prefer to use `run_with_mut`, so that panics can be avoided.
///     AsyncIo::run_with_mut(&mut handle, |handle| async move {
///         // Read value
///         let val = handle.read(0).await.unwrap();
///         assert_eq!(1u8, val);
///     })
///     .await
/// });
/// # #[cfg(not(all(unix, not(miri))))]
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Default)]
pub struct AsyncIo;

impl Integration for AsyncIo {
    type Impl<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>> =
        AsyncIoImpl<'a, B, Func, Func::Fut<'a>>;

    fn run_with<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>>(
        backend: B,
        func: Func,
    ) -> Self::Impl<'a, B, Func> {
        Self::Impl {
            backend,
            state: AsyncIoState::Initial(func),
        }
    }
}

enum AsyncIoState<'a, B: IoBackend + ?Sized + 'a, Func, F> {
    Initial(Func),
    Loaded(
        WithBackend<'a, B::Backend, F>,
        Option<(Async<BorrowedFd<'a>>, &'a PollingFlags, Waker)>,
    ),
    Finished,
}

#[doc(hidden)]
pub struct AsyncIoImpl<'a, B: LinksIoBackend + 'a, Func, F> {
    backend: B,
    state: AsyncIoState<'a, B::Link, Func, F>,
}

impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>>
    AsyncIoImpl<'a, B, Func, Func::Fut<'a>>
{
    pub async fn run(backend: B, func: Func) -> <Func::Fut<'a> as Future>::Output {
        AsyncIo::run_with(backend, func).await
    }
}

impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>> Future
    for AsyncIoImpl<'a, B, Func, Func::Fut<'a>>
{
    type Output = <Func::Fut<'a> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                AsyncIoState::Initial(_) => {
                    let func = if let AsyncIoState::Initial(func) =
                        core::mem::replace(&mut this.state, AsyncIoState::Finished)
                    {
                        func
                    } else {
                        unreachable!()
                    };
                    // SAFETY: the backend reference is pinned
                    let backend: &'a B::Link =
                        unsafe { &*(this.backend.get_mut() as *const B::Link) };
                    let fut = func.call(backend);
                    let (fut, h) = backend.with_backend(fut);
                    this.state = AsyncIoState::Loaded(
                        fut,
                        h.map(
                            |PollingHandle {
                                 handle,
                                 cur_flags,
                                 waker,
                                 ..
                             }| {
                                let handle = unsafe { BorrowedFd::borrow_raw(handle) };
                                (
                                    Async::new_nonblocking(handle)
                                        .expect("Could not register the IO resource"),
                                    cur_flags,
                                    waker,
                                )
                            },
                        ),
                    );
                }
                AsyncIoState::Loaded(wb, fd) => {
                    break loop {
                        if let Poll::Ready(v) = unsafe { Pin::new_unchecked(&mut *wb) }.poll(cx) {
                            break Poll::Ready(v);
                        }

                        if let Some((fd, p, _)) = fd {
                            let (read, write) = p.get();
                            // TODO: what to do when read = write = false?
                            let mut ret = Some(Poll::Pending);
                            if read && fd.poll_readable(cx).is_ready() {
                                ret = None
                            }
                            if write && fd.poll_writable(cx).is_ready() {
                                ret = None
                            }
                            if let Some(ret) = ret {
                                break ret;
                            }
                        }
                    };
                }
                AsyncIoState::Finished => unreachable!(),
            }
        }
    }
}
