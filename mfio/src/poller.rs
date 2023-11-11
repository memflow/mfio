use core::future::Future;
use core::mem;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
#[cfg(feature = "std")]
use std::thread::{self, Thread};

pub trait ThreadLocal: Sized {
    /// Get handle to current thread.
    fn current() -> Self;
}

/// Parkable handle.
///
/// This handle allows a thread to potentially be efficiently blocked. This is used in the polling
/// implementation to wait for wakeups.
pub trait ParkHandle: Sized {
    /// Park the current thread.
    fn park(&self);

    /// Unpark specified thread.
    fn unpark(&self);
}

pub trait Wakeable: ParkHandle + Clone {
    /// Convert self into opaque pointer.
    ///
    /// This requires `Self` to either be layout compatible with `*const ()` or heap allocated upon
    /// switch.
    fn into_opaque(self) -> *const ();

    /// Convert opaque pointer into `Self`.
    ///
    /// # Safety
    ///
    /// This function is safe if the `data` argument is a valid park handle created by
    /// `Self::into_opaque`.
    unsafe fn from_opaque(data: *const ()) -> Self;

    /// Create a raw waker out of `self`.
    ///
    /// This will clone self and build a `RawWaker` with vtable built from this trait's waker
    /// functions.
    ///
    /// `ParkHandle::waker` depends on this method building the correct waker, thus overloading
    /// this blanket function needs to be done with great care.
    unsafe fn raw_waker(&self) -> RawWaker {
        let data = self.clone().into_opaque();
        RawWaker::new(
            data,
            &RawWakerVTable::new(
                Self::clone_waker,
                Self::wake,
                Self::wake_by_ref,
                Self::drop_waker,
            ),
        )
    }

    /// Create a waker out of `self`
    ///
    /// This function will clone self and build a `Waker` object.
    ///
    /// The default implementation relies on `Self::raw_waker` method being correct.
    fn waker(&self) -> Waker {
        unsafe { Waker::from_raw(self.raw_waker()) }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let waker = Self::from_opaque(data);
        let ret = waker.raw_waker();
        mem::forget(waker);
        ret
    }

    unsafe fn wake(data: *const ()) {
        let waker = Self::from_opaque(data);
        waker.unpark();
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let waker = Self::from_opaque(data);
        waker.unpark();
        mem::forget(waker);
    }

    unsafe fn drop_waker(data: *const ()) {
        let _ = Self::from_opaque(data);
    }
}

#[cfg(feature = "std")]
impl ThreadLocal for Thread {
    fn current() -> Self {
        thread::current()
    }
}

#[cfg(feature = "std")]
impl ParkHandle for Thread {
    fn park(&self) {
        thread::park();
    }

    fn unpark(&self) {
        Thread::unpark(self);
    }
}

#[cfg(feature = "std")]
impl Wakeable for Thread {
    fn into_opaque(self) -> *const () {
        // SAFETY: `Thread` internal layout is an Arc to inner type, which is represented as a
        // single pointer. The only thing we do with the pointer is transmute it back to
        // ThreadWaker in the waker functions. If for whatever reason Thread layout will change to
        // contain multiple fields, this will still be safe, because the compiler will simply
        // refuse to compile the program.
        unsafe { mem::transmute::<_, *const ()>(self) }
    }

    unsafe fn from_opaque(data: *const ()) -> Self {
        mem::transmute(data)
    }
}

impl ThreadLocal for *const () {
    fn current() -> Self {
        core::ptr::null()
    }
}

impl ParkHandle for *const () {
    fn park(&self) {
        core::hint::spin_loop()
    }

    fn unpark(&self) {}
}

impl Wakeable for *const () {
    fn into_opaque(self) -> *const () {
        self
    }

    unsafe fn from_opaque(data: *const ()) -> Self {
        data
    }
}

/// Block the thread until the future is ready with current thread's parking handle.
///
/// This allows one to use custom thread parking mechanisms in `no_std` environments.
///
/// # Example
///
/// ```no_run
/// use std::thread::Thread;
///
/// let my_fut = async {};
/// //let result = mfio::poller::block_on_t::<Thread, _>(my_fut);
/// ```
pub fn block_on_t<T: ParkHandle + Wakeable + ThreadLocal, F: Future>(fut: F) -> F::Output {
    let handle = T::current();
    let waker = handle.waker();
    block_on_handle(fut, &handle, &waker)
}

/// Block the thread until the future is ready with given parking handle.
///
/// This allows one to use custom thread parking mechanisms in `no_std` environments.
///
/// # Example
///
/// ```no_run
/// use std::thread::Thread;
///
/// let my_fut = async {};
/// //let result = mfio::poller::block_on_handle::<Thread, _>(my_fut);
/// ```
pub fn block_on_handle<T: ParkHandle, F: Future>(
    mut fut: F,
    handle: &T,
    waker: &Waker,
) -> F::Output {
    // Pin the future so that it can be polled.
    // SAFETY: We shadow `fut` so that it cannot be used again. The future is now pinned to the stack and will not be
    // moved until the end of this scope. This is, incidentally, exactly what the `pin_mut!` macro from `pin_utils`
    // does.
    let mut fut = unsafe { core::pin::Pin::new_unchecked(&mut fut) };

    let mut context = Context::from_waker(waker);

    // Poll the future to completion
    loop {
        match fut.as_mut().poll(&mut context) {
            Poll::Pending => handle.park(),
            Poll::Ready(item) => break item,
        }
    }
}

/// Block the thread until the future is ready.
///
/// # Example
///
/// ```no_run
/// let my_fut = async {};
/// //let result = mfio::poller::block_on(my_fut);
/// ```
pub fn block_on<F: Future>(fut: F) -> F::Output {
    #[cfg(feature = "std")]
    return block_on_t::<Thread, _>(fut);
    #[cfg(not(feature = "std"))]
    return block_on_t::<*const (), _>(fut);
}
