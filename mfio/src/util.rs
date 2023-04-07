use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Waker;
use parking_lot::Mutex;

#[derive(Default)]
pub struct Event {
    waker: Mutex<Option<Waker>>,
    signaled: AtomicBool,
}

impl Event {
    pub fn signal(&self) {
        if !self.signaled.swap(true, Ordering::Release) {
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
        }
    }

    pub async fn wait(&self) {
        let mut yielded = false;
        core::future::poll_fn(|cx| {
            if !yielded && !self.signaled.swap(false, Ordering::Acquire) {
                yielded = true;
                let mut guard = self.waker.lock();
                // Check after locking to avoid race conditions
                if self.signaled.swap(false, Ordering::Acquire) {
                    core::task::Poll::Ready(())
                } else {
                    *guard = Some(cx.waker().clone());
                    core::task::Poll::Pending
                }
            } else {
                core::task::Poll::Ready(())
            }
        })
        .await;
    }
}

#[derive(Default, Debug)]
pub struct ReadOnly<T>(UnsafeCell<T>);

unsafe impl<T: Sync> Sync for ReadOnly<T> {}
unsafe impl<T: Send> Send for ReadOnly<T> {}

impl<T> core::ops::Deref for ReadOnly<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.0.get() as *const _) }
    }
}

impl<T> From<T> for ReadOnly<T> {
    fn from(data: T) -> Self {
        Self(data.into())
    }
}

pub(crate) trait UsizeMath {
    fn add_assign(&mut self, val: usize);
    fn add(self, val: usize) -> Self;
}

impl UsizeMath for usize {
    fn add_assign(&mut self, val: usize) {
        *self += val;
    }

    fn add(self, val: usize) -> Self {
        self + val
    }
}

impl UsizeMath for u64 {
    fn add_assign(&mut self, val: usize) {
        *self += val as u64;
    }

    fn add(self, val: usize) -> Self {
        self + val as u64
    }
}
