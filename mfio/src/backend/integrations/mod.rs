//! Integrates `mfio` backends with other async runtimes.
//!
//! Integrations allow `mfio` backend objects to be used in other runtimes with true cooperation.
//!
//! Note that the current integration use is rather limited, mainly, both `tokio` and `async-io`
//! require unix platforms, since async equivalents for windows raw handle polling is not exposed
//! at the moment.

#[cfg(all(unix, feature = "std", feature = "async-io"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "std", feature = "async-io"))))]
pub mod async_io;
pub mod null;
#[cfg(all(unix, not(miri), feature = "std", feature = "tokio"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "std", feature = "tokio"))))]
pub mod tokio;

use super::{IoBackend, LinksIoBackend, RefLink};
use core::future::Future;
use core::marker::PhantomData;

pub trait BorrowingFn<B: ?Sized> {
    type Fut<'a>: Future
    where
        B: 'a;
    fn call(self, arg: &B) -> Self::Fut<'_>;
}

impl<B: ?Sized, Func: for<'a> FnOnce(&'a B) -> F, F: Future> BorrowingFn<B> for Func {
    type Fut<'a> = F where B: 'a;

    fn call(self, arg: &B) -> Self::Fut<'_> {
        self(arg)
    }
}

/// Wrapper to convert `FnOnce(&'1 _)` to `for<'a> FnOnce(&'a _)`.
///
/// This wrapper is needed to walk around issues with closures not defaulting to using
/// higher-ranked type bounds. See: <https://github.com/rust-lang/rust/issues/70263>.
///
/// This wrapper is used in `run_with_mut` to accept a non-hrtb closure without complex trickery.
pub struct UnsafeHrtb<'a, B: ?Sized, Func: FnOnce(&'a B) -> F, F: Future> {
    func: Func,
    _phantom: PhantomData<fn(&'a B) -> F>,
}

impl<'a, B: ?Sized, Func: FnOnce(&'a B) -> F, F: Future> UnsafeHrtb<'a, B, Func, F> {
    unsafe fn new(func: Func) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }
}

impl<'a, B: ?Sized, Func: FnOnce(&'a B) -> F, F: Future> BorrowingFn<B>
    for UnsafeHrtb<'a, B, Func, F>
{
    type Fut<'b> = F where B: 'b;

    fn call<'b>(self, arg: &'b B) -> Self::Fut<'b> {
        let arg: &'a B = unsafe { &*(arg as *const B) };
        (self.func)(arg)
    }
}

pub trait Integration: Copy + Default {
    type Impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>>: Future<
        Output = <Func::Fut<'a> as Future>::Output,
    >;

    fn run_with<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>>(
        link: B,
        func: Func,
    ) -> Self::Impl<'a, B, Func>;

    fn run_with_mut<'a, B: IoBackend + 'a, Func: FnOnce(&'a B) -> F, F: Future + 'a>(
        link: &'a mut B,
        func: Func,
    ) -> Self::Impl<'a, RefLink<'a, B>, UnsafeHrtb<'a, B, Func, F>> {
        // SAFETY: we know we will be passing the backend link to this closure, therefore we can
        // safely cast the function to be HRTB.
        let func = unsafe { UnsafeHrtb::new(func) };
        Self::run_with(RefLink(link), func)
    }
}
