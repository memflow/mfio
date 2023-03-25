use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub struct IoScope<B, F> {
    backend_future: B,
    future: F,
}

impl<B: Future, F: Future> Future for IoScope<B, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let backend_future = unsafe { Pin::new_unchecked(&mut this.backend_future) };
        let future = unsafe { Pin::new_unchecked(&mut this.future) };

        let _ = backend_future.poll(cx);

        future.poll(cx)
    }
}

pub trait IoHandle {
    type BackendFuture: Future;
    type Backend;

    fn local_backend(&self) -> (Self::Backend, Self::BackendFuture);

    fn run<Func: FnOnce(Self::Backend) -> Fut, Fut: Future>(
        &self,
        func: Func,
    ) -> IoScope<Self::BackendFuture, Fut> {
        let (backend, backend_future) = self.local_backend();
        let future = func(backend);
        IoScope {
            backend_future,
            future,
        }
    }
}
