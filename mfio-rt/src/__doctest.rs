#[cfg(feature = "native")]
use crate::NativeRt;
#[cfg(feature = "native")]
use core::future::Future;
#[cfg(feature = "native")]
use mfio::backend::IoBackend;

#[cfg(feature = "native")]
pub fn run_each<'a, Func: Fn(&'a NativeRt) -> F, F: Future>(func: Func) {
    for (_, fs) in NativeRt::builder().enable_all().build_each() {
        if let Ok(fs) = fs {
            let fs = &fs;
            // SAFETY: there isn't. The doctests shouldn't move the fs handle though.
            let fs: &'a NativeRt = unsafe { &(*(fs as *const _)) };
            fs.block_on(func(fs));
        }
    }
}
