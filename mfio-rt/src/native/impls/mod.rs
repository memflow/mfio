#[cfg(unix)]
mod unix_extra;

pub mod thread;

#[cfg(all(not(miri), target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

#[cfg(all(not(miri), unix, feature = "mio"))]
pub mod mio;

cfg_if::cfg_if! {
    if #[cfg(miri)] {
        // Force use thread impl if on miri
        pub use thread::*;
    } else if #[cfg(all(target_os = "linux", feature = "io-uring"))] {
        // io-uring provides true completion I/O, however, it's Linux-only.
        pub use self::io_uring::*;
    } else if #[cfg(all(unix, feature = "mio"))] {
        // mio allows for true async io
        // however, we are relying on file descriptors here, so we can't expose it on non-unix
        // platforms.
        pub use self::mio::*;
    } else {
        // Fallback to thread on any unmatched cases
        pub use thread::*;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Key {
    File(usize),
    Stream(usize),
    TcpListener(usize),
}

const NUM_KEYS: usize = 3;

impl From<usize> for Key {
    fn from(raw: usize) -> Self {
        let idx = raw / NUM_KEYS;
        match raw % NUM_KEYS {
            0 => Self::File(idx),
            1 => Self::Stream(idx),
            2 => Self::TcpListener(idx),
            _ => unreachable!(),
        }
    }
}

impl Key {
    pub fn idx(self) -> usize {
        match self {
            Self::File(v) => v,
            Self::Stream(v) => v,
            Self::TcpListener(v) => v,
        }
    }

    pub fn key(self) -> usize {
        match self {
            Self::File(v) => v * NUM_KEYS,
            Self::Stream(v) => v * NUM_KEYS + 1,
            Self::TcpListener(v) => v * NUM_KEYS + 2,
        }
    }
}
