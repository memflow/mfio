use crate::io::NoPos;

pub(crate) trait UsizeMath {
    fn add_assign(&mut self, val: usize);
    fn add(self, val: usize) -> Self;
}

impl UsizeMath for usize {
    fn add_assign(&mut self, val: usize) {
        *self += val;
    }

    fn add(self, val: usize) -> Self {
        self + val
    }
}

impl UsizeMath for u64 {
    fn add_assign(&mut self, val: usize) {
        *self += val as u64;
    }

    fn add(self, val: usize) -> Self {
        self + val as u64
    }
}

impl UsizeMath for NoPos {
    fn add_assign(&mut self, _: usize) {}

    fn add(self, _: usize) -> Self {
        self
    }
}

// FIXME: this trait shouldn't be public
pub trait CopyPos: Sized {
    fn copy_pos(&self) -> Self;
}

// This trait unifies implementations on NoPos (streams) with seekable I/O
pub(crate) trait PosShift<Io>: CopyPos + UsizeMath {
    fn add_pos(&mut self, out: usize, io: &Io);
    fn add_io_pos(io: &Io, out: usize);
}

impl<Param: Copy> CopyPos for Param {
    fn copy_pos(&self) -> Self {
        *self
    }
}

impl CopyPos for NoPos {
    fn copy_pos(&self) -> Self {
        Self::new()
    }
}

impl<Io> PosShift<Io> for NoPos {
    fn add_pos(&mut self, _: usize, _: &Io) {}
    fn add_io_pos(_: &Io, _: usize) {}
}
