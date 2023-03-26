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
