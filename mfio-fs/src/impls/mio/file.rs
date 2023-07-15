use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};
use parking_lot::Mutex;

use core::mem::MaybeUninit;

use mfio::packet::{FastCWaker, Read as RdPerm, Splittable, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::RawHandleConv;
use super::MioState;
use crate::util::io_err;
use mfio::error::State;

struct ReadOp {
    pkt: MaybeAlloced<'static, WrPerm>,
}

struct WriteOp {
    pkt: AllocedOrTransferred<'static, RdPerm>,
    transferred: Option<(usize, Vec<u8>)>,
}

pub struct FileInner {
    file: File,
    pos: u64,
    read_ops: VecDeque<(u64, ReadOp)>,
    write_ops: VecDeque<(u64, WriteOp)>,
    tmp_buf: Vec<u8>,
}

impl AsRawFd for FileInner {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl Source for FileInner {
    // Required methods
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.register(&mut SourceFd(&self.file.as_raw()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.reregister(&mut SourceFd(&self.file.as_raw()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        registry.deregister(&mut SourceFd(&self.file.as_raw()))
    }
}

impl From<File> for FileInner {
    fn from(file: File) -> Self {
        Self {
            file,
            pos: 0,
            read_ops: Default::default(),
            write_ops: Default::default(),
            tmp_buf: vec![],
        }
    }
}

impl FileInner {
    fn read(mut file: &File, buf: &mut [u8]) -> io::Result<usize> {
        file.read(buf)
    }

    fn write(mut file: &File, buf: &[u8]) -> io::Result<usize> {
        file.write(buf)
    }

    fn seek(&mut self, new_pos: u64) -> io::Result<()> {
        if self.pos == new_pos {
            Ok(())
        } else {
            match self.file.seek(SeekFrom::Start(new_pos)) {
                Ok(p) if p == new_pos => {
                    self.pos = new_pos;
                    Ok(())
                }
                Ok(_) => Err(std::io::ErrorKind::Other.into()),
                Err(e) => Err(e),
            }
        }
    }

    pub fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops file={:?} read={read} write={write} (to read={} to write={})",
            self.file.as_raw(),
            self.read_ops.len(),
            self.write_ops.len()
        );
        if read {
            'outer: while let Some((pos, op)) = self.read_ops.pop_front() {
                match self.seek(pos) {
                    Ok(()) => (),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        self.read_ops.push_front((pos, op));
                        break 'outer;
                    }
                    Err(e) => {
                        let err = io_err(e.kind().into());

                        match op.pkt {
                            Ok(pkt) => pkt.error(err),
                            Err(pkt) => pkt.error(err),
                        }
                        continue;
                    }
                }

                let ReadOp { mut pkt } = op;

                loop {
                    let len = pkt.len();

                    let slice = match &mut pkt {
                        Ok(pkt) => {
                            let buf = pkt.as_ptr() as *mut u8;
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            unsafe { core::slice::from_raw_parts_mut(buf, len) }
                        }
                        Err(_) => {
                            if len > self.tmp_buf.len() {
                                self.tmp_buf.reserve(len - self.tmp_buf.len());
                            }
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            unsafe { self.tmp_buf.set_len(len) }
                            &mut self.tmp_buf[..]
                        }
                    };

                    match Self::read(&self.file, slice) {
                        Ok(l) => {
                            log::trace!("Read {l}/{}", slice.len());
                            self.pos += l as u64;
                            if l == len {
                                if let Err(pkt) = pkt {
                                    unsafe { pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast()) };
                                }
                                break;
                            } else if l > 0 {
                                let (a, b) = pkt.split_at(l);
                                if let Err(pkt) = a {
                                    unsafe { pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast()) };
                                }
                                pkt = b;
                            } else {
                                pkt.error(io_err(State::Nop));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            log::trace!("Would Block");
                            self.read_ops.push_front((self.pos, ReadOp { pkt }));
                            break 'outer;
                        }
                        Err(e) => {
                            log::trace!("Error {e}");
                            pkt.error(io_err(e.kind().into()));
                            break;
                        }
                    }
                }
            }
        }

        if write {
            'outer: while let Some((pos, op)) = self.write_ops.pop_front() {
                match self.seek(pos) {
                    Ok(()) => (),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        self.write_ops.push_front((pos, op));
                        break 'outer;
                    }
                    Err(e) => {
                        let err = io_err(e.kind().into());

                        match op.pkt {
                            Ok(pkt) => pkt.error(err),
                            Err(pkt) => pkt.error(err),
                        }
                        continue;
                    }
                }

                let WriteOp {
                    mut pkt,
                    mut transferred,
                } = op;

                loop {
                    let len = pkt.len();

                    let slice = match &mut pkt {
                        Ok(pkt) => {
                            let buf = pkt.as_ptr();
                            unsafe { core::slice::from_raw_parts(buf, len) }
                        }
                        Err(_) => {
                            let (pos, buf) = transferred.as_ref().unwrap();
                            &buf[*pos..]
                        }
                    };

                    match Self::write(&self.file, slice) {
                        Ok(l) => {
                            log::trace!("Written {l}/{}", slice.len());
                            self.pos += l as u64;
                            if let Some((pos, _)) = &mut transferred {
                                *pos += l;
                            }
                            if l == len {
                                break;
                            } else if l > 0 {
                                pkt = pkt.split_at(l).1;
                            } else {
                                pkt.error(io_err(State::Nop));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            log::trace!("WouldBlock");
                            self.write_ops
                                .push_front((self.pos, WriteOp { pkt, transferred }));
                            break 'outer;
                        }
                        Err(e) => {
                            log::trace!("Error {e}");
                            pkt.error(io_err(e.kind().into()));
                            break;
                        }
                    }
                }
            }
        }
    }
}

trait IntoOp: PacketPerms {
    fn push_op(file: &mut FileInner, pos: u64, alloced: MaybeAlloced<'static, Self>);
}

impl IntoOp for RdPerm {
    fn push_op(file: &mut FileInner, pos: u64, alloced: MaybeAlloced<'static, Self>) {
        let op = match alloced {
            Ok(pkt) => WriteOp {
                pkt: Ok(pkt),
                transferred: None,
            },
            Err(pkt) => {
                let mut new_trans: Vec<MaybeUninit<u8>> = Vec::with_capacity(pkt.len());
                unsafe { new_trans.set_len(pkt.len()) };

                let transferred = unsafe { pkt.transfer_data(new_trans.as_mut_ptr() as *mut ()) };
                // SAFETY: buffer has now been initialized, it is safe to transmute it into [u8].
                let new_trans = unsafe { core::mem::transmute(new_trans) };
                WriteOp {
                    pkt: Err(transferred),
                    transferred: Some((0, new_trans)),
                }
            }
        };

        file.write_ops.push_back((pos, op));

        // If we haven't got any other ops enqueued, then trigger processing!
        if file.write_ops.len() < 2 {
            file.do_ops(false, true);
        }
    }
}

impl IntoOp for WrPerm {
    fn push_op(file: &mut FileInner, pos: u64, pkt: MaybeAlloced<'static, Self>) {
        file.read_ops.push_back((pos, ReadOp { pkt }));
        // If we haven't got any other ops enqueued, then trigger processing!
        if file.read_ops.len() < 2 {
            file.do_ops(true, false);
        }
    }
}

struct FileOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, u64>,
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl<Perms: IntoOp> FileOpsHandle<Perms> {
    fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            idx,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, u64>> for FileOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, u64> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, u64> for FileOpsHandle<Perms> {
    extern "C" fn send_input(&self, pos: u64, packet: BoundPacket<'static, Perms>) {
        let mut state = self.state.lock();

        let file = state.files.get_mut(self.idx).unwrap();

        Perms::push_op(file, pos, packet.try_alloc());
    }
}

pub struct FileWrapper {
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, u64>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, u64>>,
}

impl FileWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        let write_io = BaseArc::new(FileOpsHandle::new(idx, state.clone()));

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_io = BaseArc::new(FileOpsHandle::new(idx, state.clone()));

        let read_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
        });

        Self {
            idx,
            state,
            write_stream,
            read_stream,
        }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let mut file = state.files.remove(self.idx);
        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut file);
    }
}

impl PacketIo<RdPerm, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}
