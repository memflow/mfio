#[cfg(all(any(miri, test, feature = "virt"), not(feature = "native")))]
use crate::virt::VirtRt;
#[cfg(feature = "native")]
use crate::NativeRt;
#[cfg(any(miri, test, feature = "native", feature = "virt"))]
use core::future::Future;
#[cfg(any(miri, test, feature = "native", feature = "virt"))]
use mfio::backend::IoBackendExt;

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

#[cfg(all(any(miri, test, feature = "virt"), not(feature = "native")))]
pub fn run_each<'a, Func: Fn(&'a VirtRt) -> F, F: Future>(func: Func) {
    use crate::{DirHandle, Fs, OpenOptions};
    use mfio::traits::*;

    const FILES: &[(&str, &str)] = &[
        ("Cargo.toml", include_str!("../Cargo.toml")),
        ("src/lib.rs", include_str!("lib.rs")),
    ];

    let fs = &VirtRt::new();

    fs.block_on(async {
        let cd = fs.current_dir();
        cd.create_dir("src").await.unwrap();
        for (p, data) in FILES {
            let fh = cd
                .open_file(p, OpenOptions::new().create_new(true).write(true))
                .await
                .unwrap();
            fh.write_all(0, data.as_bytes()).await.unwrap();
        }
    });

    // SAFETY: there isn't. The doctests shouldn't move the fs handle though.
    let fs: &'a VirtRt = unsafe { &(*(fs as *const _)) };
    fs.block_on(func(fs));
}
