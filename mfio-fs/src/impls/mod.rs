cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub mod thread;
        pub use thread::*;
    } else if #[cfg(all(feature = "gpl", target_os = "linux"))] {
        // rio is a GPL3 crate, thus we only enable it if specific feature flag is flipped
        pub mod io_uring;
        pub use io_uring::*;
    } else {
        // Fallback to thread on any unmatched cases
        pub mod thread;
        pub use thread::*;
    }
}
