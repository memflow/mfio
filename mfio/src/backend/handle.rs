use core::sync::atomic::{AtomicU8, Ordering};
use core::task::{RawWaker, RawWakerVTable, Waker};

use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use tarc::{Arc, BaseArc};

use windows_sys::Win32::System::Threading::{CreateEventA, ResetEvent, SetEvent};

#[repr(transparent)]
pub struct EventWakerOwner(EventWaker);

impl Drop for EventWakerOwner {
    fn drop(&mut self) {
        self.0.close()
    }
}

impl EventWakerOwner {
    pub fn new() -> Option<Self> {
        let handle = unsafe { CreateEventA(core::ptr::null(), 1, 0, core::ptr::null()) };

        let handle = if handle == 0 {
            return None;
        } else {
            unsafe { OwnedHandle::from_raw_handle(handle as _) }
        };

        Some(Self(EventWaker(
            BaseArc::new(EventWakerInner {
                handle,
                flags: Default::default(),
            })
            .into_raw(),
        )))
    }

    pub fn clear(&self) -> Option<()> {
        let inner = unsafe { &*self.0 .0 };
        if unsafe { ResetEvent(inner.handle.as_raw_handle() as _) } != 0 {
            Some(())
        } else {
            None
        }
    }
}

impl core::ops::Deref for EventWakerOwner {
    type Target = EventWaker;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An Event backed waker.
///
/// This waker simply signals an event handle, which can be awaited for externally.
#[repr(transparent)]
pub struct EventWaker(*const EventWakerInner);

unsafe impl Send for EventWaker {}
unsafe impl Sync for EventWaker {}

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

impl EventWaker {
    pub fn flags(&self) -> Arc<AtomicU8> {
        unsafe {
            BaseArc::increment_strong_count(self.0);
        }
        unsafe { BaseArc::from_raw(self.0) }.transpose()
    }

    pub fn wake_by_ref(&self) {
        let inner = unsafe { &*self.0 };
        let flags = inner.flags.fetch_or(0b1, Ordering::AcqRel);
        log::trace!("Flags {flags:b}");
        if flags & 0b111 == 0 {
            let ret = unsafe { SetEvent(inner.handle.as_raw_handle() as _) };
            assert_ne!(0, ret);
        }
    }

    pub fn close(&self) {
        let inner = unsafe { &*self.0 };
        inner.flags.fetch_or(0b100, Ordering::AcqRel);
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

impl AsRawHandle for EventWaker {
    fn as_raw_handle(&self) -> RawHandle {
        unsafe { &*self.0 }.handle.as_raw_handle()
    }
}

struct EventWakerInner {
    handle: OwnedHandle,
    flags: AtomicU8,
}

impl AsRef<AtomicU8> for EventWakerInner {
    fn as_ref(&self) -> &AtomicU8 {
        &self.flags
    }
}
