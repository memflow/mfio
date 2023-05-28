cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub mod thread;
        pub use thread::*;
    } else if #[cfg(all(unix, feature = "mio"))] {
        // mio allows for true async io
        // however, we are relying on file descriptors here, so we can't expose it on non-unix
        // platforms.
        pub mod mio;
        pub use self::mio::*;
    } else {
        // Fallback to thread on any unmatched cases
        pub mod thread;
        pub use thread::*;
    }
}
