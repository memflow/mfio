cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub mod thread;
        pub use thread::*;
    } else {
        // Fallback to thread on any unmatched cases
        pub mod thread;
        pub use thread::*;
    }
}
