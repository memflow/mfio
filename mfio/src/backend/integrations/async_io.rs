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
        Option<(Async<RawFd>, &'a PollingFlags, Waker)>,
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
                        h.map(
                            |PollingHandle {
                                 handle,
                                 cur_flags,
                                 waker,
                                 ..
                             }| {
                                (
                                    // FIXME: we need to make `Async` not set nonblocking mode, as it
                                    // is unsupported on kqueues. We should talk with upstream to
                                    // enable our usage.
                                    // Async::with_nonblocking_mode(h, false)
                                    Async::new(handle).expect("Could not register the IO resource"),
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
