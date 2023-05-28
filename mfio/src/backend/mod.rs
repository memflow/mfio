use core::cell::UnsafeCell;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};

pub mod integrations;

pub use integrations::null::{Null, NullImpl};
pub use integrations::Integration;

#[cfg(unix)]
use std::os::fd::RawFd;
#[cfg(windows)]
use std::os::windows::io::RawHandle;

#[cfg(any(unix, target_os = "wasi"))]
pub mod fd;

#[cfg(windows)]
pub mod windows;

#[cfg(unix)]
pub type DefaultHandle = RawFd;
#[cfg(windows)]
pub type DefaultHandle = RawHandle;

pub type DynBackend = dyn Future<Output = ()> + Send;

pub struct BackendContainer<B: ?Sized> {
    backend: UnsafeCell<Pin<Box<B>>>,
    lock: AtomicBool,
}

unsafe impl<B: ?Sized + Send> Send for BackendContainer<B> {}
unsafe impl<B: ?Sized + Send> Sync for BackendContainer<B> {}

impl<B: ?Sized> BackendContainer<B> {
    pub fn acquire(&self) -> BackendHandle<B> {
        if self.lock.swap(true, Ordering::Acquire) {
            panic!("Tried to acquire backend twice!");
        }

        let backend = unsafe { &mut *self.backend.get() }.as_mut();

        BackendHandle {
            owner: self,
            backend,
        }
    }
}

impl BackendContainer<DynBackend> {
    pub fn new_dyn<T: Future<Output = ()> + Send + 'static>(backend: T) -> Self {
        Self {
            backend: UnsafeCell::new(Box::pin(backend) as Pin<Box<dyn Future<Output = ()> + Send>>),
            lock: Default::default(),
        }
    }
}

pub struct BackendHandle<'a, B: ?Sized> {
    owner: &'a BackendContainer<B>,
    backend: Pin<&'a mut B>,
}

impl<'a, B: ?Sized> Drop for BackendHandle<'a, B> {
    fn drop(&mut self) {
        self.owner.lock.store(false, Ordering::Release);
    }
}

impl<'a, B: ?Sized> core::ops::Deref for BackendHandle<'a, B> {
    type Target = Pin<&'a mut B>;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl<'a, B: ?Sized> core::ops::DerefMut for BackendHandle<'a, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.backend
    }
}

pub struct WithBackend<'a, Backend: ?Sized, Fut: ?Sized> {
    backend: BackendHandle<'a, Backend>,
    future: Fut,
}

impl<'a, Backend: Future + ?Sized, Fut: Future + ?Sized> Future for WithBackend<'a, Backend, Fut> {
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        let backend = this.backend.as_mut();

        match fut.poll(cx) {
            Poll::Ready(v) => Poll::Ready(v),
            Poll::Pending => match backend.poll(cx) {
                Poll::Ready(_) => panic!("Backend future completed"),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

pub type PollingHandle = (DefaultHandle, Waker);

pub trait IoBackend {
    type Backend: Future<Output = ()> + Send + ?Sized;

    /// Gets handle to the backing event system.
    ///
    /// This function returns a handle and a waker. The handle is a `RawFd` on Unix systems, and a
    /// `RawHandle` on Windows. This handle is meant to be polled/waited on by the system.
    ///
    /// The waker is opaque, but should unblock the handle once signaled.
    ///
    /// If the function returns `None`, then it can be assumed that the backend will wake any waker
    /// up with other mechanism (such as auxiliary thread), and that the polling implementation can
    /// simply park the thread.
    fn polling_handle(&self) -> Option<PollingHandle>;

    /// Acquires exclusive handle to IO backend.
    ///
    /// # Panics
    ///
    /// This function panics when multiple backend handles are attempted to be acquired. This
    /// function does not return an `Option`, because such case usually indicates a bug in the
    /// code.
    fn get_backend(&self) -> BackendHandle<Self::Backend>;

    /// Builds a composite future that also polls the backend future.
    ///
    /// If second tuple element is not `None`, then the caller is responsible for registering and
    /// handling read-readiness events.
    fn with_backend<F: Future>(
        &self,
        future: F,
    ) -> (WithBackend<Self::Backend, F>, Option<PollingHandle>) {
        (
            WithBackend {
                backend: self.get_backend(),
                future,
            },
            self.polling_handle(),
        )
    }

    /// Executes a future to completion.
    ///
    /// This function uses mfio's mini-executor that is able to resolve an arbitrary future that is
    /// either awoken externally, or through exported handle's readiness events.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        let backend = self.get_backend();
        let polling = self.polling_handle();
        block_on::<F, Self>(fut, backend, polling)
    }
}

/// Represents types that contain an `IoBackend`.
pub trait LinksIoBackend {
    type Link: IoBackend + ?Sized;

    fn get_mut(&self) -> &Self::Link;
}

impl<T: IoBackend> LinksIoBackend for T {
    type Link = Self;

    fn get_mut(&self) -> &Self::Link {
        self
    }
}

/// `IoBackend` wrapper for references.
pub struct RefLink<'a, T: ?Sized>(&'a T);

impl<'a, T: IoBackend + ?Sized> LinksIoBackend for RefLink<'a, T> {
    type Link = T;

    fn get_mut(&self) -> &Self::Link {
        self.0
    }
}

pub fn block_on<F: Future, B: IoBackend + ?Sized>(
    future: F,
    backend: BackendHandle<B::Backend>,
    polling: Option<PollingHandle>,
) -> F::Output {
    let fut = WithBackend { backend, future };

    if let Some((handle, waker)) = polling {
        block_on_handle(fut, handle, waker)
    } else {
        crate::poller::block_on(fut)
    }
}

#[cfg(miri)]
fn block_on_handle<F: Future>(_: F, _: DefaultHandle, _: Waker) -> F::Output {
    unimplemented!("Polling on miri is unsupported")
}

#[cfg(all(unix, not(miri)))]
fn block_on_handle<F: Future>(mut fut: F, handle: RawFd, waker: Waker) -> F::Output {
    use nix::poll::*;

    let fd = PollFd::new(handle, PollFlags::POLLIN);

    let mut cx = Context::from_waker(&waker);

    loop {
        let fut = unsafe { Pin::new_unchecked(&mut fut) };

        match fut.poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                let _ = poll(&mut [fd], -1);
            }
        }
    }
}

#[cfg(all(windows, not(miri)))]
fn block_on_handle<F: Future>(mut fut: F, handle: RawHandle, waker: Waker) -> F::Output {
    use windows_sys::Win32::System::Threading::{WaitForSingleObject, INFINITE};

    let mut cx = Context::from_waker(&waker);

    loop {
        let fut = unsafe { Pin::new_unchecked(&mut fut) };

        match fut.poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                let _ = unsafe { WaitForSingleObject(handle as _, INFINITE) };
            }
        }
    }
}
