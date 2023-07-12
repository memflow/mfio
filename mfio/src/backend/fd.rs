use core::sync::atomic::{AtomicU8, Ordering};
use core::task::{RawWaker, RawWakerVTable, Waker};
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use tarc::{Arc, BaseArc};

/// An eventfd/pipe backed waker.
///
/// This waker simply writes a 8 byte value (little endian 1) to the provided file descriptor upon
/// wakeup. Thus, this waking mechanism is not limited to just eventfd or pipes.
#[repr(transparent)]
pub struct FdWaker<F: AsRawFd>(*const FdWakerInner<F>);

unsafe impl<F: AsRawFd + Send> Send for FdWaker<F> {}
unsafe impl<F: AsRawFd + Send> Sync for FdWaker<F> {}

impl<F: AsRawFd> Clone for FdWaker<F> {
    fn clone(&self) -> Self {
        unsafe {
            BaseArc::increment_strong_count(self.0);
        }
        Self(self.0)
    }
}

impl<F: AsRawFd> Drop for FdWaker<F> {
    fn drop(&mut self) {
        unsafe {
            BaseArc::decrement_strong_count(self.0);
        }
    }
}

impl<F: AsRawFd> From<F> for FdWaker<F> {
    fn from(fd: F) -> Self {
        Self(
            BaseArc::new(FdWakerInner {
                fd,
                flags: Default::default(),
            })
            .into_raw(),
        )
    }
}

impl<F: AsRawFd> FdWaker<F> {
    pub fn flags(&self) -> Arc<AtomicU8> {
        unsafe {
            BaseArc::increment_strong_count(self.0);
        }
        unsafe { BaseArc::from_raw(self.0) }.transpose()
    }

    pub fn wake_by_ref(&self) {
        let inner = unsafe { &*self.0 };
        let flags = inner.flags.fetch_or(0b1, Ordering::AcqRel);
        if flags & 0b11 == 0 {
            let mut f = unsafe { File::from_raw_fd(inner.fd.as_raw_fd()) };
            f.write_all(&1u64.to_ne_bytes())
                .expect("Could not wake the waker up");
            let _ = f.into_raw_fd();
        }
    }

    pub fn into_raw_waker(self) -> RawWaker {
        let data: *const () = unsafe { core::mem::transmute(self) };
        let vtbl = &RawWakerVTable::new(
            Self::raw_clone,
            Self::raw_wake,
            Self::raw_wake_by_ref,
            Self::raw_drop,
        );
        RawWaker::new(data, vtbl)
    }

    pub fn into_waker(self) -> Waker {
        unsafe { Waker::from_raw(self.into_raw_waker()) }
    }

    unsafe fn raw_wake(data: *const ()) {
        let waker = core::ptr::read((&data as *const _) as *const Self);
        waker.wake_by_ref()
    }

    unsafe fn raw_wake_by_ref(data: *const ()) {
        let waker: &Self = &*((&data as *const _) as *const Self);
        waker.wake_by_ref()
    }

    unsafe fn raw_clone(data: *const ()) -> RawWaker {
        let waker: &Self = &*((&data as *const _) as *const Self);
        waker.clone().into_raw_waker()
    }

    unsafe fn raw_drop(data: *const ()) {
        core::ptr::drop_in_place((&data as *const _) as *const Self as *mut Self)
    }
}

struct FdWakerInner<F: AsRawFd> {
    fd: F,
    flags: AtomicU8,
}

impl<F: AsRawFd> AsRef<AtomicU8> for FdWakerInner<F> {
    fn as_ref(&self) -> &AtomicU8 {
        &self.flags
    }
}
