//! A null integration.
//!
//! This integration assumes external wakers, and no backend dependence on cooperative handles.
//! This works great whenever the I/O backend runs on a separate thread, however, this usually
//! leads to severe latency penalty due to cross-thread synchronization.

use super::super::*;
use super::{BorrowingFn, Integration};

/// Minimal integration.
///
/// This integration works in all async runtimes, however, it does not support the backend's
/// `PollingHandle`. If the backend returns `Some(handle)`, then this integration panics.
///
/// # Examples
///
/// Running with `pollster`:
///
/// ```
/// # mod sample {
/// #     include!("../../sample.rs");
/// # }
/// # use sample::SampleIo;
/// use mfio::prelude::v1::*;
///
/// pollster::block_on(async {
///     let mut handle = SampleIo::new(vec![1, 2, 3, 4]);
///
///     // Run the integration. Prefer to use `run_with_mut`, so that panics can be avoided.
///     Null::run_with_mut(&mut handle, |handle| async move {
///         // Read value
///         let val = handle.read(0).await.unwrap();
///         assert_eq!(1u8, val);
///     })
///     .await
/// });
/// ```
#[derive(Clone, Copy, Default)]
pub struct Null;

impl Integration for Null {
    type Impl<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>> =
        NullImpl<'a, B, Func, Func::Fut<'a>>;

    fn run_with<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>>(
        backend: B,
        func: Func,
    ) -> Self::Impl<'a, B, Func> {
        Self::Impl {
            backend,
            state: NullState::Initial(func),
        }
    }
}

enum NullState<'a, B: IoBackend + ?Sized + 'a, Func, F> {
    Initial(Func),
    Loaded(WithBackend<'a, B::Backend, F>),
    Finished,
}

#[doc(hidden)]
pub struct NullImpl<'a, B: LinksIoBackend + 'a, Func, F> {
    backend: B,
    state: NullState<'a, B::Link, Func, F>,
}

impl<'a, B: LinksIoBackend + 'a, Func: for<'b> BorrowingFn<B::Link>>
    NullImpl<'a, B, Func, Func::Fut<'a>>
{
    pub async fn run(backend: B, func: Func) -> <Func::Fut<'a> as Future>::Output {
        Null::run_with(backend, func).await
    }
}

impl<'a, B: LinksIoBackend + 'a, Func: BorrowingFn<B::Link>> Future
    for NullImpl<'a, B, Func, Func::Fut<'a>>
{
    type Output = <Func::Fut<'a> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                NullState::Initial(_) => {
                    let func = if let NullState::Initial(func) =
                        core::mem::replace(&mut this.state, NullState::Finished)
                    {
                        func
                    } else {
                        unreachable!()
                    };
                    // SAFETY: the backend reference is pinned
                    let backend: &'a B::Link =
                        unsafe { &*(this.backend.get_mut() as *const B::Link) };
                    let fut = func.call(backend);
                    let (fut, h) = backend.with_backend(fut);
                    assert!(
                        h.is_none(),
                        "Null future cannot be used when backend exports a FD!"
                    );
                    this.state = NullState::Loaded(fut);
                }
                NullState::Loaded(wb) => break unsafe { Pin::new_unchecked(wb) }.poll(cx),
                NullState::Finished => unreachable!(),
            }
        }
    }
}
