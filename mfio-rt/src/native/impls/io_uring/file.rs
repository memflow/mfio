use std::os::fd::AsRawFd;

use io_uring::{opcode, squeue::Entry, types::Fixed};
use parking_lot::Mutex;

use core::mem::MaybeUninit;

use mfio::io::{Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{IoUringState, Key, Operation, RawBox};

trait IntoOp: PacketPerms {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacketView<Self>) -> (Entry, Operation);
}

impl IntoOp for RdPerm {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacketView<Self>) -> (Entry, Operation) {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let (buf, raw_box, pkt) = match pkt {
            Ok(pkt) => (pkt.as_ptr(), RawBox::null(), Ok(pkt)),
            Err(pkt) => {
                let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(len as usize);
                unsafe { buf.set_len(len as usize) };
                let mut buf = buf.into_boxed_slice();
                let buf_ptr = buf.as_mut_ptr();
                let buf = Box::into_raw(buf);
                let pkt = unsafe { pkt.transfer_data(buf_ptr as *mut ()) };
                (buf_ptr as *const u8, RawBox(buf), Err(pkt))
            }
        };

        let entry = opcode::Write::new(Fixed(fd), buf, len as u32)
            .offset(pos)
            .build();

        (entry, Operation::FileWrite(pkt, raw_box))
    }
}

impl IntoOp for WrPerm {
    fn into_op(fd: u32, pos: u64, pkt: BoundPacketView<Self>) -> (Entry, Operation) {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let (buf, raw_box) = match &pkt {
            Ok(pkt) => (pkt.as_ptr().cast(), RawBox::null()),
            Err(_) => {
                let mut buf = Vec::with_capacity(len as usize);
                unsafe { buf.set_len(len as usize) };
                let mut buf = buf.into_boxed_slice();
                let buf_ptr = buf.as_mut_ptr();
                let buf = Box::into_raw(buf);
                (buf_ptr, RawBox(buf))
            }
        };

        let buf: *mut MaybeUninit<u8> = buf;

        let entry = opcode::Read::new(Fixed(fd), buf.cast(), len as u32)
            .offset(pos)
            .build();

        (entry, Operation::FileRead(pkt, raw_box))
    }
}

impl<Perms: IntoOp> PacketIo<Perms, u64> for FileWrapper {
    fn send_io(&self, pos: u64, packet: BoundPacketView<Perms>) {
        let mut state = self.state.lock();
        let state = &mut *state;

        // TODO: handle size limitations???
        let (ring_entry, ops_entry) = Perms::into_op(Key::File(self.idx).key() as _, pos, packet);

        state.all_ssub += 1;
        state.push_handle().try_push_op(ring_entry, ops_entry);
    }
}

pub struct FileWrapper {
    idx: usize,
    state: BaseArc<Mutex<IoUringState>>,
}

impl FileWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<IoUringState>>) -> Self {
        Self { idx, state }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.files.remove(self.idx);

        log::trace!("Dropping {} {}", self.idx, v.as_raw_fd());

        let r = state
            .ring
            .submitter()
            .register_files_update(Key::File(self.idx).key() as _, &[-1])
            .unwrap();

        log::trace!("{r} {}", self.state.strong_count(),);
    }
}
