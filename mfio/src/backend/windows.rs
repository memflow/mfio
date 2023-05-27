use core::task::{RawWaker, RawWakerVTable};
use std::os::windows::io::{AsRawHandle, OwnedHandle};
use tarc::BaseArc;
use windows_sys::Win32::System::Threading::SetEvent;

/// A windows event backed waker.
#[repr(transparent)]
pub struct EventWaker(*const OwnedHandle);

impl Clone for EventWaker {
    fn clone(&self) -> Self {
        unsafe {
            BaseArc::increment_strong_count(self.0);
        }
        Self(self.0)
    }
}

impl Drop for EventWaker {
    fn drop(&mut self) {
        unsafe {
            BaseArc::decrement_strong_count(self.0);
        }
    }
}

impl From<BaseArc<OwnedHandle>> for EventWaker {
    fn from(handle: BaseArc<OwnedHandle>) -> Self {
        Self(handle.into_raw())
    }
}

impl EventWaker {
    fn wake_by_ref(&self) {
        let handle = unsafe { &*self.0 }.as_raw_handle();
        unsafe {
            SetEvent(handle as _);
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
