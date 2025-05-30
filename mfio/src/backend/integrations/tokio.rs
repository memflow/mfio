//! `tokio` integration.

use std::os::fd::RawFd;
use tokio::io::{unix::AsyncFd, Interest};

use super::super::*;
use super::{BorrowingFn, Integration};

/// Tokio integration.
///
/// Unlike [`Null`], this integration supports backends with polling handles, however, only tokio
/// runtime is supported.
///
/// Internally, this uses tokio's [`AsyncFd`] to wait for readiness of the polling handle, which
/// means only unix platforms are supported.
///
/// # Examples
///
/// ```
/// # mod sample {
/// #     include!("../../sample.rs");
/// # }
/// # use sample::SampleIo;
/// use mfio::prelude::v1::*;
///
/// #[tokio::main]
/// # #[cfg(all(unix, not(miri)))]
/// async fn main() {
///     let mut handle = SampleIo::new(vec![1, 2, 3, 4]);
///
///     // Run the integration. Prefer to use `run_with_mut`, so that panics can be avoided.
///     Tokio::run_with_mut(&mut handle, |handle| async move {
///         // Read value
///         let val = handle.read(0).await.unwrap();
///         assert_eq!(1u8, val);
///     })
///     .await
/// }
/// # #[cfg(not(all(unix, not(miri))))]
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Default)]
pub struct Tokio;

fn into_tokio(flags: &PollingFlags) -> Interest {
    match flags.get() {
        (true, true) => Interest::READABLE.add(Interest::WRITABLE),
        (false, true) => Interest::WRITABLE,
        (true, false) => Interest::READABLE,
        (false, false) => panic!("Polling flags incompatible!"),
    }
}

impl Integration for Tokio {
    type Impl<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>> =
        TokioImpl<'a, B, Func, Func::Fut<'a>>;

    fn run_with<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>>(
        backend: B,
        func: Func,
    ) -> Self::Impl<'a, B, Func> {
        Self::Impl {
            backend,
            state: TokioState::Initial(func),
        }
    }
}

enum TokioState<'a, Func, F> {
    Initial(Func),
    Loaded(
        WithBackend<'a, F>,
        Option<(AsyncFd<RawFd>, &'a PollingFlags, Waker)>,
    ),
    Finished,
}

#[doc(hidden)]
pub struct TokioImpl<'a, B: LinksIoBackend + 'a, Func, F> {
    backend: B,
    state: TokioState<'a, Func, F>,
}

impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>> TokioImpl<'a, B, Func, Func::Fut<'a>> {
    pub async fn run(backend: B, func: Func) -> <Func::Fut<'a> as Future>::Output {
        Tokio::run_with(backend, func).await
    }
}

impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>> Future
    for TokioImpl<'a, B, Func, Func::Fut<'a>>
{
    type Output = <Func::Fut<'a> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                TokioState::Initial(_) => {
                    let func = if let TokioState::Initial(func) =
                        core::mem::replace(&mut this.state, TokioState::Finished)
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
                    this.state = TokioState::Loaded(
                        fut,
                        h.map(
                            |PollingHandle {
                                 handle,
                                 cur_flags,
                                 waker,
                                 max_flags,
                             }| {
                                (
                                    AsyncFd::with_interest(handle, into_tokio(&max_flags))
                                        .expect("Could not register the IO resource"),
                                    cur_flags,
                                    waker,
                                )
                            },
                        ),
                    );
                }
                TokioState::Loaded(wb, fd) => {
                    break loop {
                        if let Poll::Ready(v) = unsafe { Pin::new_unchecked(&mut *wb) }.poll(cx) {
                            break Poll::Ready(v);
                        }
                        if let Some((fd, p, _)) = fd {
                            let (read, write) = p.get();
                            // TODO: what to do when read = write = false?
                            let mut ret = Some(Poll::Pending);
                            if read {
                                if let Poll::Ready(Ok(mut guard)) = fd.poll_read_ready(cx) {
                                    // We clear the ready flag, because the backend is expected to consume
                                    // all I/O until it blocks without waking anything.
                                    guard.clear_ready();
                                    ret = None;
                                }
                            }
                            if write {
                                if let Poll::Ready(Ok(mut guard)) = fd.poll_write_ready(cx) {
                                    // We clear the ready flag, because the backend is expected to consume
                                    // all I/O until it blocks without waking anything.
                                    guard.clear_ready();
                                    ret = None;
                                }
                            }
                            if let Some(ret) = ret {
                                break ret;
                            }
                        }
                    };
                }
                TokioState::Finished => unreachable!(),
            }
        }
    }
}
