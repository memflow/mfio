use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};
use tarc::Arc;

pub struct IoScope<B, F> {
    backend_future: B,
    future: F,
    backend_progressed: Arc<AtomicBool>,
}

impl<B: Future, F: Future> Future for IoScope<B, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let backend_future = unsafe { Pin::new_unchecked(&mut this.backend_future) };
            let future = unsafe { Pin::new_unchecked(&mut this.future) };

            match future.poll(cx) {
                Poll::Pending => {
                    let _ = backend_future.poll(cx);

                    if this.backend_progressed.swap(false, Ordering::Relaxed) {
                        continue;
                    }

                    break Poll::Pending;
                }
                Poll::Ready(val) => break Poll::Ready(val),
            }
        }
    }
}

pub trait IoHandle {
    type BackendFuture: Future;
    type Backend;

    fn local_backend(&self) -> (Self::Backend, Self::BackendFuture, Arc<AtomicBool>);

    fn run<Func: FnOnce(Self::Backend) -> Fut, Fut: Future>(
        &self,
        func: Func,
    ) -> IoScope<Self::BackendFuture, Fut> {
        let (backend, backend_future, backend_progressed) = self.local_backend();
        let future = func(backend);
        IoScope {
            backend_future,
            future,
            backend_progressed,
        }
    }
}
