use std::os::fd::RawFd;
use tokio::io::unix::AsyncFd;

use super::super::*;
use super::{BorrowingFn, Integration};

#[derive(Clone, Copy, Default)]
pub struct Tokio;

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

enum TokioState<'a, B: IoBackend + ?Sized + 'a, Func, F> {
    Initial(Func),
    Loaded(
        WithBackend<'a, B::Backend, F>,
        Option<(AsyncFd<RawFd>, &'a PollingFlags, Waker)>,
    ),
    Finished,
}

pub struct TokioImpl<'a, B: LinksIoBackend + 'a, Func, F> {
    backend: B,
    state: TokioState<'a, B::Link, Func, F>,
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
                        h.map(|(h, p, w)| {
                            (
                                AsyncFd::new(h).expect("Could not register the IO resource"),
                                p,
                                w,
                            )
                        }),
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
