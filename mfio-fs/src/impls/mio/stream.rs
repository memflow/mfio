use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};
use parking_lot::Mutex;

use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::{unix_extra::StreamBuf, OwnedStreamHandle, StreamBorrow, StreamHandleConv};
use super::{BlockTrack, Key, MioState};
use std::io::{IoSlice, IoSliceMut};

pub struct StreamInner {
    fd: OwnedFd,
    stream: StreamBuf,
    track: BlockTrack,
    read: fn(&OwnedStreamHandle, &mut [IoSliceMut]) -> io::Result<usize>,
    write: fn(&OwnedStreamHandle, &[IoSlice]) -> io::Result<usize>,
}

impl AsRawFd for StreamInner {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Source for StreamInner {
    // Required methods
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        // TODO: do we need to not do this on error?
        self.track.cur_interests = Some(interests);
        registry.register(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.track.cur_interests = Some(interests);
        registry.reregister(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.track.cur_interests = None;
        registry.deregister(&mut SourceFd(&self.fd.as_raw_fd()))
    }
}

impl<T: StreamHandleConv> From<T> for StreamInner {
    fn from(stream: T) -> Self {
        fn read<T: StreamHandleConv>(
            stream: &OwnedStreamHandle,
            iov: &mut [IoSliceMut],
        ) -> io::Result<usize> {
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            stream.read_vectored(iov)
        }

        fn write<T: StreamHandleConv>(
            stream: &OwnedStreamHandle,
            iov: &[IoSlice],
        ) -> io::Result<usize> {
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            stream.write_vectored(iov)
        }

        let fd = stream.into_owned();
        Self {
            fd,
            stream: StreamBuf::default(),
            track: Default::default(),
            read: read::<T>,
            write: write::<T>,
        }
    }
}

impl StreamInner {
    pub fn update_interests(&mut self, key: usize, registry: &Registry) -> std::io::Result<()> {
        let expected_interests = self.track.expected_interests();

        if self.track.cur_interests != expected_interests {
            if let Some(i) = expected_interests {
                self.reregister(registry, Token(key), i)?;
            } else {
                self.deregister(registry)?;
            }
        }

        Ok(())
    }

    pub fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops file={:?} read={read} write={write} (to read={} to write={})",
            self.fd.as_raw_fd(),
            self.stream.read_ops(),
            self.stream.write_ops()
        );
        if read || !self.track.read_blocked {
            self.track.read_blocked = false;
            while self.stream.read_ops() > 0 {
                let queue = self.stream.read_queue();
                if !queue.is_empty() {
                    let res = (self.read)(&self.fd, queue);

                    if res
                        .as_ref()
                        .err()
                        .map(|e| e.kind() != io::ErrorKind::WouldBlock)
                        .unwrap_or(true)
                    {
                        self.stream.on_read(res);
                    } else {
                        self.track.read_blocked = true;
                        break;
                    }
                }
            }
        }

        if write || !self.track.write_blocked {
            while self.stream.write_ops() > 0 {
                self.track.write_blocked = false;
                let queue = self.stream.write_queue();
                if !queue.is_empty() {
                    let res = (self.write)(&self.fd, queue);

                    if res
                        .as_ref()
                        .err()
                        .map(|e| e.kind() != io::ErrorKind::WouldBlock)
                        .unwrap_or(true)
                    {
                        self.stream.on_write(res);
                    } else {
                        self.track.write_blocked = true;
                        break;
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
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>);
}

impl IntoOp for RdPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_write(pkt);
        stream.do_ops(false, false);
    }
}

impl IntoOp for WrPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_read(pkt);
        stream.do_ops(true, false);
    }
}

struct StreamOpsHandle<Perms: IntoOp> {
    handle: PacketIoHandle<'static, Perms, NoPos>,
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
}

impl<Perms: IntoOp> StreamOpsHandle<Perms> {
    fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            idx,
            state,
        }
    }
}

impl<Perms: IntoOp> AsRef<PacketIoHandle<'static, Perms, NoPos>> for StreamOpsHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, NoPos> {
        &self.handle
    }
}

impl<Perms: IntoOp> PacketIoHandleable<'static, Perms, NoPos> for StreamOpsHandle<Perms> {
    extern "C" fn send_input(&self, _: NoPos, packet: BoundPacket<'static, Perms>) {
        let state = &mut *self.state.lock();

        let stream = state.streams.get_mut(self.idx).unwrap();

        Perms::push_op(stream, packet);

        // This will trigger change in interests in the mio loop
        if !stream.track.update_queued
            && stream.track.expected_interests() != stream.track.cur_interests
        {
            stream.track.update_queued = true;
            state.opqueue.push(Key::Stream(self.idx));
        }
    }
}

pub struct StreamWrapper {
    idx: usize,
    state: BaseArc<Mutex<MioState>>,
    read_stream: BaseArc<PacketStream<'static, WrPerm, NoPos>>,
    write_stream: BaseArc<PacketStream<'static, RdPerm, NoPos>>,
}

impl StreamWrapper {
    pub(super) fn new(idx: usize, state: BaseArc<Mutex<MioState>>) -> Self {
        let write_io = BaseArc::new(StreamOpsHandle::new(idx, state.clone()));

        let write_stream = BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        });

        let read_io = BaseArc::new(StreamOpsHandle::new(idx, state.clone()));

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

impl Drop for StreamWrapper {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let mut stream = state.streams.remove(self.idx);
        // TODO: what to do on error?
        let _ = state.poll.registry().deregister(&mut stream);
    }
}

impl PacketIo<RdPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, RdPerm, NoPos>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<WrPerm, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, WrPerm, NoPos>> {
        Some(self.read_stream.new_packet_id())
    }
}
