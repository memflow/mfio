use core::cell::UnsafeCell;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use tarc::BaseArc;

struct ManualLock<F: ?Sized> {
    locked: AtomicBool,
    waker: UnsafeCell<Option<Waker>>,
    data: UnsafeCell<F>,
}

unsafe impl<T: ?Sized + Send> Send for ManualLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for ManualLock<T> {}

impl<T> ManualLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            waker: UnsafeCell::new(None),
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
    unsafe fn acquire(&self) -> Option<&mut T> {
        if !self.locked.swap(true, Ordering::Acquire) {
            self.data.get().as_mut()
        } else {
            None
        }
    }

    /// Release the lock
    fn release(&self) {
        self.locked.store(false, Ordering::Release);
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
        let waker = &mut *this.future.waker.get();
        // TODO: which is better, wake by ref, or always install waker?
        if let Some(waker) = waker {
            waker.wake_by_ref();
        }
        this.future.release();
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

#[derive(Debug, Clone, Copy)]
pub enum SharedFutureOutput<T> {
    AlreadyFinished,
    JustFinished(T),
    Running,
}

impl<F: Future> SharedFuture<F> {
    /*pub async fn run_till_finished(self) {
        loop {
            match SharedFutureWrapper(&self).await {
                SharedFutureOutput::AlreadyFinished | SharedFutureOutput::JustFinished(_) => return,
                _ => {}
            }

            // Yield once to avoid deadlock
            let mut yielded = false;

            core::future::poll_fn(|cx| {
                if !yielded {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
        }
    }*/

    pub fn run_once(&self) -> SharedFutureWrapper<'_, F> {
        SharedFutureWrapper(self)
    }

    pub fn try_run_once_sync(
        &self,
        cx: &mut Context<'_>,
    ) -> Option<Poll<SharedFutureOutput<F::Output>>> {
        // SAFETY: we do not re-enter this method while borrowing
        // At least we shouldn't, it could happen with self referencing, TODO: look into that
        // TODO: fairness - if there is a starved task that we know will poll this future near
        // immediately, let them poll it!
        let data = unsafe { self.inner.future.acquire() };
        if let Some(future_cont) = data {
            if let Some(future) = future_cont.as_mut() {
                // Install the waker
                if match unsafe { &*self.inner.future.waker.get() } {
                    Some(waker) if !waker.will_wake(cx.waker()) => true,
                    None => true,
                    _ => false,
                } {
                    //println!("Install waker");
                    unsafe {
                        *self.inner.future.waker.get() = Some(cx.waker().clone());
                    }
                }

                // Wrap the waker
                let inner = unsafe { Pin::into_inner_unchecked(self.inner.clone()) };
                let waker = unsafe { Waker::from_raw(SharedFutureContext::waker(&inner)) };

                let mut context = Context::from_waker(&waker);

                // Poll the future
                let pin = unsafe { Pin::new_unchecked(&mut *future) };
                let poll = pin.poll(&mut context);
                match poll {
                    Poll::Pending => Some(Poll::Ready(SharedFutureOutput::Running)),
                    Poll::Ready(val) => {
                        *future_cont = None;
                        Some(Poll::Ready(SharedFutureOutput::JustFinished(val)))
                    }
                }
            } else {
                Some(Poll::Ready(SharedFutureOutput::AlreadyFinished))
            }
        } else if self.inner.finished.load(Ordering::Acquire) {
            Some(Poll::Ready(SharedFutureOutput::AlreadyFinished))
        } else {
            None
        }
    }
}

pub struct SharedFutureWrapper<'a, F>(&'a SharedFuture<F>);

impl<F: Future> Future for SharedFutureWrapper<'_, F> {
    type Output = SharedFutureOutput<F::Output>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().0;

        this.try_run_once_sync(cx)
            .unwrap_or(Poll::Ready(SharedFutureOutput::Running))
    }
}
