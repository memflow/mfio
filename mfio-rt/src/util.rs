#![cfg_attr(not(feature = "std"), allow(dead_code))]

use crate::{Component, Path, PathBuf};
use alloc::{boxed::Box, vec, vec::Vec};
use core::mem::MaybeUninit;
use mfio::error::{Error, Location, State, Subject, INTERNAL_ERROR};
use mfio::io::*;

#[cfg(feature = "std")]
pub mod stream;

/// Compute path difference
///
/// This was taken from `pathdiff` crate, but made compatible with `typed-path` paths.
pub fn diff_paths<P, B>(path: P, base: B) -> Option<PathBuf>
where
    P: AsRef<Path>,
    B: AsRef<Path>,
{
    let path = path.as_ref();
    let base = base.as_ref();

    if path.is_absolute() != base.is_absolute() {
        if path.is_absolute() {
            Some(PathBuf::from(path))
        } else {
            None
        }
    } else {
        let mut ita = path.components();
        let mut itb = base.components();
        let mut comps: Vec<Component> = vec![];
        loop {
            match (ita.next(), itb.next()) {
                (None, None) => break,
                (Some(a), None) => {
                    comps.push(a);
                    comps.extend(ita.by_ref());
                    break;
                }
                (None, _) => comps.push(Component::ParentDir),
                (Some(a), Some(b)) if comps.is_empty() && a == b => (),
                (Some(a), Some(b)) if b == Component::CurDir => comps.push(a),
                (Some(_), Some(b)) if b == Component::ParentDir => return None,
                (Some(a), Some(_)) => {
                    comps.push(Component::ParentDir);
                    for _ in itb {
                        comps.push(Component::ParentDir);
                    }
                    comps.push(a);
                    comps.extend(ita.by_ref());
                    break;
                }
            }
        }

        Some(
            comps
                .iter()
                .map(|c| {
                    #[cfg(feature = "std")]
                    let r = c.as_os_str();
                    #[cfg(not(feature = "std"))]
                    let r: &[u8] = c.as_ref();
                    r
                })
                .collect(),
        )
    }
}

pub fn path_filename_str(path: &Path) -> Option<&str> {
    let filename = path.file_name()?;
    #[cfg(feature = "std")]
    let filename = filename.to_str()?;
    #[cfg(not(feature = "std"))]
    let filename = core::str::from_utf8(filename).ok()?;

    Some(filename)
}

pub fn io_err(state: State) -> Error {
    Error {
        code: INTERNAL_ERROR,
        location: Location::Backend,
        subject: Subject::Io,
        state,
    }
}

#[cfg(feature = "std")]
pub fn from_io_error(err: std::io::Error) -> Error {
    io_err(err.kind().into())
}

#[derive(Default)]
pub struct DeferredPackets {
    packets: Vec<(AnyPacket, Option<Error>)>,
}

impl Drop for DeferredPackets {
    fn drop(&mut self) {
        self.flush();
    }
}

impl DeferredPackets {
    pub fn ok(&mut self, p: impl Into<AnyPacket>) {
        self.packets.push((p.into(), None));
    }

    pub fn error(&mut self, p: impl Into<AnyPacket>, err: Error) {
        self.packets.push((p.into(), Some(err)))
    }

    pub fn flush(&mut self) {
        self.packets
            .drain(0..)
            .filter_map(|(p, e)| Some(p).zip(e))
            .for_each(|(p, e)| p.error(e));
    }
}

#[repr(transparent)]
pub struct RawBox(pub(crate) *mut [MaybeUninit<u8>]);

impl RawBox {
    pub fn null() -> Self {
        Self(unsafe { core::mem::MaybeUninit::zeroed().assume_init() })
    }
}

unsafe impl Send for RawBox {}
unsafe impl Sync for RawBox {}

impl Drop for RawBox {
    fn drop(&mut self) {
        if !self.0.is_null() {
            let _ = unsafe { Box::from_raw(self.0) };
        }
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
