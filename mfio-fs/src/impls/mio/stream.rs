use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use mio::{event::Source, unix::SourceFd, Interest, Registry, Token};
use parking_lot::Mutex;

use mfio::packet::{FastCWaker, Read as RdPerm, Write as WrPerm, *};
use mfio::tarc::BaseArc;

use super::super::{unix_extra::StreamBuf, OwnedStreamHandle, StreamBorrow, StreamHandleConv};
use super::MioState;
use std::io::{IoSlice, IoSliceMut};

pub struct StreamInner {
    fd: OwnedFd,
    stream: StreamBuf,
    queue_triggered: bool,
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
        registry.register(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.reregister(&mut SourceFd(&self.fd.as_raw_fd()), token, interests)
    }
    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
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
            queue_triggered: false,
            read: read::<T>,
            write: write::<T>,
        }
    }
}

impl StreamInner {
    pub fn do_ops(&mut self, read: bool, write: bool) {
        log::trace!(
            "Do ops file={:?} read={read} write={write} (to read={} to write={})",
            self.fd.as_raw_fd(),
            self.stream.read_ops(),
            self.stream.write_ops()
        );
        if read {
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
                }
            }
        }

        if write {
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
                }
            }
        }
    }

    pub fn on_queue(&mut self) {
        self.queue_triggered = false;
        self.do_ops(true, true);
    }
}

trait IntoOp: PacketPerms {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>);
}

impl IntoOp for RdPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_write(pkt);
    }
}

impl IntoOp for WrPerm {
    fn push_op(stream: &mut StreamInner, pkt: BoundPacket<'static, Self>) {
        stream.stream.queue_read(pkt);
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
    extern "C" fn send_input(&self, _pos: NoPos, packet: BoundPacket<'static, Perms>) {
        let state = &mut *self.state.lock();

        let stream = state.streams.get_mut(self.idx).unwrap();

        if !stream.queue_triggered {
            stream.queue_triggered = true;
            state.opqueue.push(self.idx);
        }

        Perms::push_op(stream, packet);
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
