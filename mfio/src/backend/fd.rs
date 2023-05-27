use core::task::{RawWaker, RawWakerVTable};
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use tarc::BaseArc;

/// An eventfd/pipe backed waker.
///
/// This waker simply writes a 8 byte value (little endian 1) to the provided file descriptor upon
/// wakeup. Thus, this waking mechanism is not limited to just eventfd or pipes.
#[repr(transparent)]
pub struct FdWaker(*const OwnedFd);

impl Clone for FdWaker {
    fn clone(&self) -> Self {
        unsafe {
            BaseArc::increment_strong_count(self.0);
        }
        Self(self.0)
    }
}

impl Drop for FdWaker {
    fn drop(&mut self) {
        unsafe {
            BaseArc::decrement_strong_count(self.0);
        }
    }
}

impl From<BaseArc<OwnedFd>> for FdWaker {
    fn from(fd: BaseArc<OwnedFd>) -> Self {
        Self(fd.into_raw())
    }
}

impl FdWaker {
    fn wake_by_ref(&self) {
        let mut f = unsafe { File::from_raw_fd((*self.0).as_raw_fd()) };
        f.write_all(&1u64.to_ne_bytes())
            .expect("Could not wake the waker up");
        let _ = f.into_raw_fd();
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