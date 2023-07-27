use core::cell::UnsafeCell;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, Waker};
use tarc::Arc;

pub mod integrations;

pub use integrations::null::{Null, NullImpl};
pub use integrations::Integration;

#[cfg(unix)]
use nix::poll::*;
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
    pub fn acquire(&self, wake_flags: Option<Arc<AtomicU8>>) -> BackendHandle<B> {
        if self.lock.swap(true, Ordering::Acquire) {
            panic!("Tried to acquire backend twice!");
        }

        let backend = unsafe { &mut *self.backend.get() }.as_mut();

        BackendHandle {
            owner: self,
            backend,
            wake_flags,
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
    wake_flags: Option<Arc<AtomicU8>>,
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

        loop {
            this.backend
                .wake_flags
                .as_ref()
                .map(|v| v.fetch_or(0b10, Ordering::AcqRel));
            let fut = unsafe { Pin::new_unchecked(&mut this.future) };
            let backend = this.backend.as_mut();

            match fut.poll(cx) {
                Poll::Ready(v) => {
                    if let Some(v) = this.backend.wake_flags.as_ref() {
                        v.store(0, Ordering::Release);
                    }
                    break Poll::Ready(v);
                }
                Poll::Pending => match backend.poll(cx) {
                    Poll::Ready(_) => panic!("Backend future completed"),
                    Poll::Pending => (),
                },
            }

            if this
                .backend
                .wake_flags
                .as_ref()
                .map(|v| v.fetch_and(0b0, Ordering::AcqRel) & 0b1)
                .unwrap_or(0)
                == 0
            {
                break Poll::Pending;
            }
        }
    }
}

pub struct PollingHandle<'a> {
    pub handle: DefaultHandle,
    pub cur_flags: &'a PollingFlags,
    pub max_flags: PollingFlags,
    pub waker: Waker,
}

#[repr(transparent)]
pub struct PollingFlags {
    flags: AtomicU8,
}

const READ_POLL: u8 = 0b1;
const WRITE_POLL: u8 = 0b10;

impl PollingFlags {
    const fn from_flags(flags: u8) -> Self {
        Self {
            flags: AtomicU8::new(flags),
        }
    }

    pub const fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
        }
    }

    pub const fn all() -> Self {
        Self {
            flags: AtomicU8::new(!0),
        }
    }

    pub const fn read(self, val: bool) -> Self {
        // SAFETY: data layout matches perfectly
        // We need this since AtomicU8::into_inner is not const stable yet.
        let mut flags = unsafe { core::mem::transmute(self) };
        if val {
            flags |= READ_POLL;
        } else {
            flags &= !READ_POLL;
        }
        Self::from_flags(flags)
    }

    pub const fn write(self, val: bool) -> Self {
        // SAFETY: data layout matches perfectly
        let mut flags = unsafe { core::mem::transmute(self) };
        if val {
            flags |= WRITE_POLL;
        } else {
            flags &= !WRITE_POLL;
        }
        Self::from_flags(flags)
    }

    pub fn set_read(&self, val: bool) {
        if val {
            self.flags.fetch_or(READ_POLL, Ordering::Relaxed);
        } else {
            self.flags.fetch_and(!READ_POLL, Ordering::Relaxed);
        }
    }

    pub fn set_write(&self, val: bool) {
        if val {
            self.flags.fetch_or(WRITE_POLL, Ordering::Relaxed);
        } else {
            self.flags.fetch_and(!WRITE_POLL, Ordering::Relaxed);
        }
    }

    pub fn get(&self) -> (bool, bool) {
        let bits = self.flags.load(Ordering::Relaxed);
        (bits & READ_POLL != 0, bits & WRITE_POLL != 0)
    }

    #[cfg(unix)]
    pub fn into_posix(&self) -> PollFlags {
        let mut flags = PollFlags::empty();
        // Relaxed is okay, because flags are meant to be set only by the owner of these flags, who
        // we are going to poll on behalf of.
        let bits = self.flags.load(Ordering::Relaxed);
        if bits & READ_POLL != 0 {
            flags.set(PollFlags::POLLIN, true);
        }
        if bits & WRITE_POLL != 0 {
            flags.set(PollFlags::POLLIN, true);
        }
        flags
    }
}

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

    if let Some(handle) = polling {
        block_on_handle(fut, handle)
    } else {
        crate::poller::block_on(fut)
    }
}

#[cfg(miri)]
fn block_on_handle<F: Future>(_: F, _: PollingHandle) -> F::Output {
    unimplemented!("Polling on miri is unsupported")
}

#[cfg(all(unix, not(miri)))]
fn block_on_handle<F: Future>(mut fut: F, handle: PollingHandle) -> F::Output {
    let PollingHandle {
        handle,
        cur_flags,
        waker,
        ..
    } = handle;

    let mut fd = PollFd::new(handle, PollFlags::empty());

    let mut cx = Context::from_waker(&waker);

    loop {
        let fut = unsafe { Pin::new_unchecked(&mut fut) };

        match fut.poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                fd.set_events(cur_flags.into_posix());
                let _ = poll(&mut [fd], -1);
            }
        }
    }
}

#[cfg(all(windows, not(miri)))]
fn block_on_handle<F: Future>(mut fut: F, handle: PollingHandle) -> F::Output {
    let PollingHandle { handle, waker, .. } = handle;

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
