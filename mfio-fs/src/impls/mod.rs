use mfio::error::{Error, Location, State, Subject, INTERNAL_ERROR};

fn io_err(state: State) -> Error {
    Error {
        code: INTERNAL_ERROR,
        location: Location::Backend,
        subject: Subject::Io,
        state,
    }
}

cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub mod thread;
        pub use thread::*;
    } else if #[cfg(all(target_os = "linux", feature = "io-uring"))] {
        // io-uring provides true completion I/O, however, it's Linux-only.
        pub mod io_uring;
        pub use self::io_uring::*;
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
