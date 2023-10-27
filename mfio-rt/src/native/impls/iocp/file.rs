use std::os::windows::io::AsRawHandle;

use parking_lot::Mutex;

use core::mem::MaybeUninit;

use mfio::io::*;
use mfio::tarc::BaseArc;

use ::windows::Win32::Foundation::HANDLE;

use ::windows::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

use super::{IocpState, Operation, OperationHeader, OperationMode, RawBox};

trait IntoOp: PacketPerms {
    fn into_op(pkt: BoundPacketView<Self>) -> OperationMode;
}

impl IntoOp for Read {
    fn into_op(pkt: BoundPacketView<Self>) -> OperationMode {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let (raw_box, pkt) = match pkt {
            Ok(pkt) => (RawBox::null(), Ok(pkt)),
            Err(pkt) => {
                let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(len as usize);
                unsafe { buf.set_len(len as usize) };
                let mut buf = buf.into_boxed_slice();
                let buf_ptr = buf.as_mut_ptr();
                let buf = Box::into_raw(buf);
                let pkt = unsafe { pkt.transfer_data(buf_ptr as *mut ()) };
                (RawBox(buf), Err(pkt))
            }
        };

        OperationMode::FileWrite(pkt, raw_box)
    }
}

impl IntoOp for Write {
    fn into_op(pkt: BoundPacketView<Self>) -> OperationMode {
        let len = pkt.len();
        let pkt = pkt.try_alloc();

        let raw_box = match &pkt {
            Ok(_) => RawBox::null(),
            Err(_) => {
                let mut buf = Vec::with_capacity(len as usize);
                unsafe { buf.set_len(len as usize) };
                let buf = buf.into_boxed_slice();
                let buf = Box::into_raw(buf);
                RawBox(buf)
            }
        };

        OperationMode::FileRead(pkt, raw_box)
    }
}

impl<Perms: IntoOp> PacketIo<Perms, u64> for FileWrapper {
    fn send_io(&self, pos: u64, packet: BoundPacketView<Perms>) {
        log::trace!("Send io @ {pos:x}");
        let mut state = self.state.lock();
        let state = &mut *state;

        let hdr = OperationHeader {
            overlapped: OVERLAPPED {
                Internal: 0,
                InternalHigh: 0,
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: (pos & (!0 >> 32)) as u32,
                        OffsetHigh: (pos >> 32) as u32,
                    },
                },
                hEvent: HANDLE(state.event.as_raw_handle() as _),
            },
            idx: !0,
            handle: HANDLE(state.files.get(self.idx).unwrap().as_raw_handle() as _),
        };

        let operation = Operation {
            header: hdr.into(),
            mode: Perms::into_op(packet),
        };

        let _ = unsafe { state.try_submit_op(operation) };
    }
}

pub struct FileWrapper {
    idx: usize,
    state: BaseArc<Mutex<IocpState>>,
}

impl FileWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<IocpState>>) -> Self {
        Self { idx, state }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let v = state.files.remove(self.idx);
        log::trace!("Dropping {} {:?}", self.idx, v.as_raw_handle());
    }
}
