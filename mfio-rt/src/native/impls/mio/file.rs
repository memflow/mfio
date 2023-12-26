use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};

use core::mem::MaybeUninit;

use mfio::io::{Read as RdPerm, Splittable, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::{BlockTrack, Key, MioState};
use crate::util::io_err;
use mfio::error::State;

struct ReadOp {
    pkt: MaybeAlloced<WrPerm>,
}

struct WriteOp {
    pkt: AllocedOrTransferred<RdPerm>,
    transferred: Option<(usize, Vec<u8>)>,
}

pub struct FileInner {
    file: File,
    track: BlockTrack,
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
        self.track.cur_interests = Some(interests);
        registry.register(&mut SourceFd(&self.file.as_raw_fd()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.track.cur_interests = Some(interests);
        registry.reregister(&mut SourceFd(&self.file.as_raw_fd()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.track.cur_interests = None;
        registry.deregister(&mut SourceFd(&self.file.as_raw_fd()))
    }
}

impl From<File> for FileInner {
    fn from(file: File) -> Self {
        Self {
            file,
            track: Default::default(),
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

    pub fn update_interests(&mut self, key: usize, registry: &Registry) -> std::io::Result<()> {
        let expected_interests = self.track.expected_interests();

        if self.track.cur_interests != expected_interests {
            if let Some(i) = expected_interests {
                if self.track.cur_interests.is_some() {
                    self.reregister(registry, Token(key), i)?;
                } else {
                    self.register(registry, Token(key), i)?;
                }
            } else {
                self.deregister(registry)?;
            }
        }

        Ok(())
    }

    pub fn cancel_all_ops(&mut self) {
        self.read_ops.clear();
        self.write_ops.clear();
    }

    #[tracing::instrument(skip(self))]
    pub fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops file={:?} read={read} write={write} (to read={} to write={})",
            self.file.as_raw_fd(),
            self.read_ops.len(),
            self.write_ops.len()
        );
        let rd_span = tracing::span!(tracing::Level::TRACE, "read", ops = self.read_ops.len());
        let wr_span = tracing::span!(tracing::Level::TRACE, "write", ops = self.write_ops.len());
        if read || !self.track.read_blocked {
            let _guard = rd_span.enter();
            'outer: while let Some((pos, op)) = self.read_ops.pop_front() {
                self.track.read_blocked = false;
                match self.seek(pos) {
                    Ok(()) => (),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        self.track.read_blocked = true;
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
                            unsafe { core::slice::from_raw_parts_mut(buf, len as usize) }
                        }
                        Err(_) => {
                            if len as usize > self.tmp_buf.len() {
                                self.tmp_buf.reserve(len as usize - self.tmp_buf.len());
                            }
                            // SAFETY: assume MaybeUninit<u8> is initialized,
                            // as God intended :upside_down:
                            unsafe { self.tmp_buf.set_len(len as usize) }
                            &mut self.tmp_buf[..]
                        }
                    };

                    match Self::read(&self.file, slice) {
                        Ok(l) => {
                            log::trace!("Read {l}/{}", slice.len());
                            self.pos += l as u64;
                            if l == len as usize {
                                if let Err(pkt) = pkt {
                                    let _ = unsafe {
                                        pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast())
                                    };
                                }
                                break;
                            } else if l > 0 {
                                let (a, b) = pkt.split_at(l);
                                if let Err(pkt) = a {
                                    let _ = unsafe {
                                        pkt.transfer_data(self.tmp_buf.as_mut_ptr().cast())
                                    };
                                }
                                pkt = b;
                            } else {
                                pkt.error(io_err(State::Nop));
                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            log::trace!("Would Block");
                            self.track.read_blocked = true;
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

        if write || !self.track.write_blocked {
            let _guard = wr_span.enter();
            'outer: while let Some((pos, op)) = self.write_ops.pop_front() {
                self.track.write_blocked = false;
                match self.seek(pos) {
                    Ok(()) => (),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        self.track.write_blocked = true;
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
                            unsafe { core::slice::from_raw_parts(buf, len as usize) }
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
                            if l == len as usize {
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
                            self.track.write_blocked = true;
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

    pub fn on_queue(&mut self) {
        self.track.update_queued = false;
        self.do_ops(true, true);
    }
}

trait IntoOp: PacketPerms {
    fn push_op(file: &mut FileInner, pos: u64, alloced: MaybeAlloced<Self>);
}

impl IntoOp for RdPerm {
    fn push_op(file: &mut FileInner, pos: u64, alloced: MaybeAlloced<Self>) {
        let op = match alloced {
            Ok(pkt) => WriteOp {
                pkt: Ok(pkt),
                transferred: None,
            },
            Err(pkt) => {
                let mut new_trans: Vec<MaybeUninit<u8>> = Vec::with_capacity(pkt.len() as _);
                unsafe { new_trans.set_len(pkt.len() as _) };

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
            file.do_ops(false, false);
        }
    }
}

impl IntoOp for WrPerm {
    fn push_op(file: &mut FileInner, pos: u64, pkt: MaybeAlloced<Self>) {
        file.read_ops.push_back((pos, ReadOp { pkt }));
        // If we haven't got any other ops enqueued, then trigger processing!
        if file.read_ops.len() < 2 {
            file.do_ops(false, false);
        }
    }
}

impl<Perms: IntoOp> PacketIo<Perms, u64> for FileWrapper {
    fn send_io(&self, pos: u64, packet: BoundPacketView<Perms>) {
        let files = self.state.files.read();
        let file = files.get(self.idx).unwrap();
        let file = &mut *file.lock();

        Perms::push_op(file, pos, packet.try_alloc());

        // This will trigger change in interests in the mio loop
        if !file.track.update_queued && file.track.expected_interests() != file.track.cur_interests
        {
            file.track.update_queued = true;
            self.state.opqueue.lock().push(Key::File(self.idx));
        }
    }
}

pub struct FileWrapper {
    idx: usize,
    state: BaseArc<MioState>,
}

impl FileWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<MioState>) -> Self {
        Self { idx, state }
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        let mut file = self.state.files.read().take(self.idx).unwrap();
        // TODO: what to do on error?
        let _ = self.state.poll.lock().registry().deregister(file.get_mut());
    }
}
