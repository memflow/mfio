use core::cell::UnsafeCell;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use parking_lot::Mutex;
use tarc::BaseArc;

struct ManualLock<F: ?Sized> {
    wakers: Mutex<Vec<Waker>>,
    data: UnsafeCell<F>,
}

unsafe impl<T: ?Sized + Send> Send for ManualLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for ManualLock<T> {}

impl<T> ManualLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            wakers: Mutex::new(vec![]),
            data: UnsafeCell::new(data),
        }
    }
}

impl<T: ?Sized> ManualLock<T> {
    /// Attempt to acquire the lock
    ///
    /// This will attempt to atomically acquire the lock, and if successful, returns reference to
    /// the underlying data. Otherwise, returns `None`.
    ///
    /// # SAFETY
    ///
    /// After acquiring the lock, there are no safeguards in place against releasing the lock and
    /// acquiring it again, leading to mutable aliasing. Thus, users must ensure such scenario does
    /// not occur.
    ///
    unsafe fn acquire(&self, waker: &Waker) -> Option<&mut T> {
        let mut wakers = self.wakers.lock();

        if wakers.is_empty() {
            wakers.push(waker.clone());
            return unsafe { self.data.get().as_mut() };
        } else if !wakers.last().unwrap().will_wake(waker) {
            wakers.push(waker.clone());
        }

        None
    }
}

impl<F> SharedFutureContext<F> {
    unsafe fn clone(this: *const ()) -> RawWaker {
        let this = BaseArc::from_raw(this as *const Self);
        let ret = Self::waker(&this);
        core::mem::forget(this);
        ret
    }

    unsafe fn wake(this: *const ()) {
        let this = BaseArc::from_raw(this as *const Self);

        let mut wakers = this.future.wakers.lock();
        for w in wakers.drain(0..) {
            w.wake();
        }

        core::mem::drop(wakers);
    }

    unsafe fn wake_by_ref(this: *const ()) {
        BaseArc::increment_strong_count(this as *const Self);
        Self::wake(this)
    }

    unsafe fn do_drop(this: *const ()) {
        BaseArc::decrement_strong_count(this as *const Self);
    }

    const fn waker_vtbl() -> &'static RawWakerVTable {
        &RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::do_drop)
    }

    fn waker(this: &BaseArc<Self>) -> RawWaker {
        RawWaker::new(
            BaseArc::into_raw(this.clone()) as *const (),
            Self::waker_vtbl(),
        )
    }
}

struct SharedFutureContext<F> {
    future: ManualLock<Option<F>>,
    finished: AtomicBool,
}

impl<F: Future> From<F> for SharedFutureContext<F> {
    fn from(future: F) -> Self {
        let future = ManualLock::new(Some(future));
        Self {
            future,
            finished: AtomicBool::new(false),
        }
    }
}

pub struct SharedFuture<F> {
    inner: Pin<BaseArc<SharedFutureContext<F>>>,
}

impl<F> Clone for SharedFuture<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<F: Future> From<F> for SharedFuture<F> {
    fn from(inner: F) -> Self {
        let inner = BaseArc::pin(inner.into());
        Self { inner }
    }
}

pub enum SharedFutureOutput<F: Future> {
    AlreadyFinished,
    JustFinished(F::Output),
    Running,
}

impl<F: Future> SharedFuture<F> {
    pub fn try_run_once_sync(&self, cx: &mut Context<'_>) -> Option<SharedFutureOutput<F>> {
        // SAFETY: we do not re-enter this method while borrowing
        // At least we shouldn't, it could happen with self referencing, TODO: look into that
        // TODO: fairness - if there is a starved task that we know will poll this future near
        // immediately, let them poll it!
        let data = unsafe { self.inner.future.acquire(cx.waker()) };
        if let Some(future_cont) = data {
            //println!("Acquired");
            let ret = if let Some(future) = future_cont.as_mut() {
                // Wrap the waker
                let inner = unsafe { Pin::into_inner_unchecked(self.inner.clone()) };
                let waker = unsafe { Waker::from_raw(SharedFutureContext::waker(&inner)) };

                let mut context = Context::from_waker(&waker);

                // Poll the future
                let pin = unsafe { Pin::new_unchecked(&mut *future) };
                let poll = pin.poll(&mut context);
                match poll {
                    Poll::Pending => Some(SharedFutureOutput::Running),
                    Poll::Ready(val) => {
                        *future_cont = None;
                        Some(SharedFutureOutput::JustFinished(val))
                    }
                }
            } else {
                Some(SharedFutureOutput::AlreadyFinished)
            };

            ret
        } else if self.inner.finished.load(Ordering::Acquire) {
            Some(SharedFutureOutput::AlreadyFinished)
        } else {
            None
        }
    }
}
