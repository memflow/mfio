use async_io::Async;
use std::os::fd::RawFd;

use super::super::*;
use super::{BorrowingFn, Integration};

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
        Option<(Async<RawFd>, Waker)>,
    ),
    Finished,
}

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
                        h.map(|(h, w)| {
                            (
                                // FIXME: we need to make `Async` not set nonblocking mode, as it
                                // is unsupported on kqueues. We should talk with upstream to
                                // enable our usage.
                                // Async::with_nonblocking_mode(h, false)
                                Async::new(h).expect("Could not register the IO resource"),
                                w,
                            )
                        }),
                    );
                }
                AsyncIoState::Loaded(wb, fd) => {
                    if let Some((fd, _)) = fd {
                        // We don't care about the outcome, we only want async-io to wake us up
                        // when the fd is readable, as the backend is responsible for consuming all
                        // outstanding events upon it being polled.
                        let _ = fd.poll_readable(cx);
                    }

                    break unsafe { Pin::new_unchecked(wb) }.poll(cx);
                }
                AsyncIoState::Finished => unreachable!(),
            }
        }
    }
}
