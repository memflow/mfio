//! Backends for `mfio`.
//!
//! A backend is a stateful object that can be used to resolve a future to completion, either by
//! blocking execution, or, exposing a handle, which can then be integrated into other asynchronous
//! runtimes through [integrations].

#[cfg(not(feature = "std"))]
use crate::std_prelude::*;

use crate::poller::{self, ParkHandle};

use cglue::ext::FutureBox;
use cglue::task::CRefWaker;
use core::cell::UnsafeCell;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, Waker};
use tarc::Arc;

pub mod integrations;

pub use integrations::null::{Null, NullImpl};
pub use integrations::Integration;

#[cfg(all(unix, feature = "std"))]
use nix::poll::*;
#[cfg(all(unix, feature = "std"))]
use std::os::fd::RawFd;
#[cfg(all(windows, feature = "std"))]
use std::os::windows::io::RawHandle;

#[cfg(all(any(unix, target_os = "wasi"), feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(any(unix, target_os = "wasi"), feature = "std"))))]
pub mod fd;

#[cfg(all(windows, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(windows, feature = "std"))))]
pub mod handle;

#[cfg(all(windows, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(windows)))]
pub mod windows;

// TODO: rename DefaultHandle to OsHandle, and get rid of Infallible one.

#[cfg(all(unix, feature = "std"))]
pub type DefaultHandle = RawFd;
#[cfg(all(windows, feature = "std"))]
pub type DefaultHandle = RawHandle;
#[cfg(not(feature = "std"))]
pub type DefaultHandle = core::convert::Infallible;

#[repr(C)]
#[cfg_attr(feature = "abi_stable", derive(::abi_stable::StableAbi))]
struct NestedBackend {
    owner: *const (),
    poll: unsafe extern "C-unwind" fn(*const (), &CRefWaker),
    release: unsafe extern "C-unwind" fn(*const ()),
}

/// Stores a backend.
///
/// This type is always stored on backends, and is acquired by users in [`IoBackend::get_backend`].
/// A backend can only be acquired once at a time, however, it does not matter who does it.
///
/// Once the backend is acquired, it can be used to drive I/O to completion.
#[repr(C)]
#[cfg_attr(feature = "abi_stable", derive(::abi_stable::StableAbi))]
pub struct BackendContainer {
    nest: UnsafeCell<MaybeUninit<NestedBackend>>,
    backend: UnsafeCell<FutureBox<'static, ()>>,
    lock_and_presence: AtomicU8,
}

const LOCK_BIT: u8 = 0b1;
const PRESENCE_BIT: u8 = 0b10;

impl BackendContainer {
    /// SAFETY: must not create 2 aliasing references to underlying data.
    unsafe fn nest_get_locked(&self) -> Option<&mut NestedBackend> {
        if self.lock_and_presence.load(Ordering::Relaxed) & PRESENCE_BIT != 0 {
            Some((*self.nest.get()).assume_init_mut())
        } else {
            None
        }
    }

    /// SAFETY: must hold the lock
    unsafe fn nest_take_locked(&self) -> Option<NestedBackend> {
        if self
            .lock_and_presence
            .fetch_and(!PRESENCE_BIT, Ordering::Relaxed)
            & PRESENCE_BIT
            != 0
        {
            Some(core::ptr::read(self.nest.get()).assume_init())
        } else {
            None
        }
    }

    /// SAFETY: must hold the lock
    unsafe fn nest_store_locked(&self, nest: NestedBackend) {
        if self
            .lock_and_presence
            .fetch_or(PRESENCE_BIT, Ordering::Relaxed)
            & PRESENCE_BIT
            != 0
        {
            (*self.nest.get()).assume_init_drop();
        }

        *self.nest.get() = MaybeUninit::new(nest);
    }
}

unsafe impl Send for BackendContainer {}
unsafe impl Sync for BackendContainer {}

impl Drop for BackendContainer {
    fn drop(&mut self) {
        unsafe { self.nest_take_locked() };
    }
}

impl BackendContainer {
    /// Acquire a backend.
    ///
    /// This function locks the backend and the returned handle keeps it locked, until the handle
    /// gets released.
    ///
    /// # Panics
    ///
    /// Panics if the backend has already been acquired.
    pub fn acquire(&self, wake_flags: Option<Arc<AtomicU8>>) -> BackendHandle {
        if self.lock_and_presence.fetch_or(LOCK_BIT, Ordering::AcqRel) & LOCK_BIT != 0 {
            panic!("Tried to acquire backend twice!");
        }

        let backend = unsafe { Pin::new_unchecked(&mut *self.backend.get()) };

        BackendHandle {
            owner: self,
            backend,
            wake_flags,
        }
    }

    /// Acquires a backend in nested mode.
    ///
    /// This function is useful when layered I/O backends are desirable. When polling, first, this
    /// backend will be polled, and afterwards, the provided handle. The ordering is consistent
    /// with the behavior of first polling the user's future, and then polling the backend. In the
    /// end, backends will be peeled off layer by layer, until the innermost backend is reached.
    pub fn acquire_nested(&self, mut handle: BackendHandle) -> BackendHandle {
        let wake_flags = handle.wake_flags.take();
        let owner = handle.owner;

        let our_handle = self.acquire(wake_flags);

        unsafe extern "C-unwind" fn poll(data: *const (), cx: &CRefWaker) {
            let data = &*(data as *const BackendContainer);
            cx.with_waker(|waker| {
                let mut context = Context::from_waker(waker);
                if Pin::new_unchecked(&mut *data.backend.get())
                    .poll(&mut context)
                    .is_ready()
                {
                    panic!("Backend polled to completion!")
                }
            })
        }

        unsafe extern "C-unwind" fn release(data: *const ()) {
            let data = &*(data as *const BackendContainer);
            data.lock_and_presence
                .fetch_and(!LOCK_BIT, Ordering::Release);
        }

        // We must prevent drop from being called, since we are replacing the release mechanism
        // ourselves.
        core::mem::forget(handle);

        unsafe {
            self.nest_store_locked(NestedBackend {
                owner: owner as *const _ as *const (),
                poll,
                release,
            });
        }

        our_handle
    }
}

impl BackendContainer {
    /// Creates a new container.
    pub fn new_dyn<T: Future<Output = ()> + Send + 'static>(backend: T) -> Self {
        Self {
            backend: UnsafeCell::new(cglue::trait_obj!(backend as Future)),
            nest: UnsafeCell::new(MaybeUninit::uninit()),
            lock_and_presence: Default::default(),
        }
    }
}

/// Handle to a backend.
///
/// This handle can be used to drive arbitrary future to completion by attaching a backend to it.
/// This is typically done using [`WithBackend`] that is constructed in
/// [`IoBackendExt::with_backend`].
///
/// Usually, the user would want to bypass this type and use [`IoBackendExt::block_on`], or an
/// [`Integration`] equivalent.
pub struct BackendHandle<'a> {
    owner: &'a BackendContainer,
    backend: Pin<&'a mut FutureBox<'static, ()>>,
    wake_flags: Option<Arc<AtomicU8>>,
}

impl<'a> Drop for BackendHandle<'a> {
    fn drop(&mut self) {
        // SAFETY: we are still holding the lock to this data
        if let Some(NestedBackend { owner, release, .. }) = unsafe { self.owner.nest_take_locked() }
        {
            // SAFETY: this structure is constructed only in acquire_nested. We assume it
            // constructs the structure correctly.
            unsafe { release(owner) }
        }

        self.owner
            .lock_and_presence
            .fetch_and(!LOCK_BIT, Ordering::Release);
    }
}

impl<'a> core::ops::Deref for BackendHandle<'a> {
    type Target = Pin<&'a mut FutureBox<'static, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl<'a> core::ops::DerefMut for BackendHandle<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.backend
    }
}

/// Future combined with a backend.
///
/// This future can be used to drive arbitrary future to completion by attaching a backend to it.
/// Construct this type using [`IoBackendExt::with_backend`].
///
/// Usually, the user would want to bypass this type and use [`IoBackendExt::block_on`], or an
/// [`Integration`] equivalent.
pub struct WithBackend<'a, Fut: ?Sized> {
    backend: BackendHandle<'a>,
    future: Fut,
}

impl<'a, Fut: Future + ?Sized> Future for WithBackend<'a, Fut> {
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
                    Poll::Pending => {
                        // SAFETY: we are holding the lock to the backend.
                        if let Some(NestedBackend { owner, poll, .. }) =
                            unsafe { this.backend.owner.nest_get_locked() }
                        {
                            let cx = CRefWaker::from(cx.waker());
                            // SAFETY: this structure is constructed only in acquire_nested. We
                            // assume it constructs the structure correctly.
                            unsafe { poll(*owner, &cx) };
                        }
                    }
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

/// Cooperative polling handle.
///
/// This handle contains a handle and necessary metadata needed to cooperatively drive mfio code to
/// completion.
///
/// This handle is typically created on the [`IoBackend`] side.
pub struct PollingHandle<'a, Handle = DefaultHandle> {
    pub handle: Handle,
    pub cur_flags: &'a PollingFlags,
    pub max_flags: PollingFlags,
    pub waker: Waker,
}

/// Represents desired object state flags to poll for.
///
/// Different backends may expose handles with different requirements. Some handles, when polled
/// with incorrect flags may return an error, meaning it is crucial for the caller to pass correct
/// flags in.
///
/// This is an object accessible from IoBackends that describes these flags. The object is designed
/// so that these flags may get modified on the fly. Note that there is no encapsulation, so the
/// caller should take great care in ensuring they do not modify these flags in breaking manner.
/// However, doing so should not result in undefined behavior.
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

    /// Builds a new `PollingFlags` with no bits set.
    pub const fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
        }
    }

    /// Builds a new `PollingFlags` with all bits set.
    pub const fn all() -> Self {
        Self {
            flags: AtomicU8::new(!0),
        }
    }

    /// Consumes and returns a new `PollingFlags` with read bit set to specified value.
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

    /// Consumes and returns a new `PollingFlags` with write bit set to specified value.
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

    /// Updates the read bit in-place.
    pub fn set_read(&self, val: bool) {
        if val {
            self.flags.fetch_or(READ_POLL, Ordering::Relaxed);
        } else {
            self.flags.fetch_and(!READ_POLL, Ordering::Relaxed);
        }
    }

    /// Updates the write bit in-place.
    pub fn set_write(&self, val: bool) {
        if val {
            self.flags.fetch_or(WRITE_POLL, Ordering::Relaxed);
        } else {
            self.flags.fetch_and(!WRITE_POLL, Ordering::Relaxed);
        }
    }

    /// Returns the values of current read and write bits.
    pub fn get(&self) -> (bool, bool) {
        let bits = self.flags.load(Ordering::Relaxed);
        (bits & READ_POLL != 0, bits & WRITE_POLL != 0)
    }

    /// Converts these flags into posix PollFlags.
    #[cfg(all(unix, feature = "std"))]
    pub fn to_posix(&self) -> PollFlags {
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

/// Primary trait describing I/O backends.
///
/// This trait is implemented at the outer-most stateful object of the I/O context. A `IoBackend`
/// has the opportunity to expose efficient ways of driving said backend to completion.
///
/// Users may want to call methods available on [`IoBackendExt`], instead of the ones on this
/// trait.
pub trait IoBackend<Handle: Pollable = DefaultHandle> {
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
    fn get_backend(&self) -> BackendHandle;
}

/// Helpers for [`IoBackend`].
pub trait IoBackendExt<Handle: Pollable>: IoBackend<Handle> {
    /// Builds a composite future that also polls the backend future.
    ///
    /// If second tuple element is not `None`, then the caller is responsible for registering and
    /// handling read-readiness events.
    fn with_backend<F: Future>(&self, future: F) -> (WithBackend<F>, Option<PollingHandle>) {
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
        block_on::<Handle, F>(fut, backend, polling)
    }
}

impl<T: ?Sized + IoBackend<Handle>, Handle: Pollable> IoBackendExt<Handle> for T {}

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

pub fn block_on<H: Pollable, F: Future>(
    future: F,
    backend: BackendHandle,
    polling: Option<PollingHandle>,
) -> F::Output {
    let fut = WithBackend { backend, future };

    if let Some(handle) = polling {
        poller::block_on_handle(fut, &handle, &handle.waker)
    } else {
        poller::block_on(fut)
    }
}

impl<H: Pollable> ParkHandle for PollingHandle<'_, H> {
    fn unpark(&self) {
        self.waker.wake_by_ref();
    }

    fn park(&self) {
        self.handle.poll(self.cur_flags)
    }
}

/// A pollable handle.
///
/// Implementing this trait on a custom type, allows one to build custom cooperation mechanisms for
/// different operating environments.
pub trait Pollable {
    fn poll(&self, flags: &PollingFlags);
}

#[cfg(any(miri, not(feature = "std")))]
impl Pollable for DefaultHandle {
    fn poll(&self, _: &PollingFlags) {
        unimplemented!("Polling on requires std feature, and not be run on miri")
    }
}

#[cfg(all(not(miri), unix, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(not(miri), feature = "std"))))]
impl Pollable for DefaultHandle {
    fn poll(&self, flags: &PollingFlags) {
        let fd = PollFd::new(*self, flags.to_posix());
        let _ = poll(&mut [fd], -1);
    }
}

#[cfg(all(not(miri), windows, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(not(miri), feature = "std"))))]
impl Pollable for DefaultHandle {
    fn poll(&self, _: &PollingFlags) {
        use windows_sys::Win32::System::Threading::{WaitForSingleObject, INFINITE};
        let _ = unsafe { WaitForSingleObject(*self as _, INFINITE) };
    }
}
