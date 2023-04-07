use std::fs::File;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use core::mem::ManuallyDrop;
use core::task::Context;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use mfio::packet::*;
use mfio::tarc::BaseArc;

struct IoThreadHandle<Perms: PacketPerms> {
    handle: PacketIoHandle<'static, Perms, u64>,
    tx: Sender<(u64, BoundPacket<'static, Perms>)>,
}

impl<Perms: PacketPerms> IoThreadHandle<Perms> {
    fn new(tx: Sender<(u64, BoundPacket<'static, Perms>)>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            tx,
        }
    }
}

impl<Perms: PacketPerms> AsRef<PacketIoHandle<'static, Perms, u64>> for IoThreadHandle<Perms> {
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, u64> {
        &self.handle
    }
}

impl<Perms: PacketPerms> PacketIoHandleable<'static, Perms, u64> for IoThreadHandle<Perms> {
    extern "C" fn send_input(&self, pos: u64, packet: BoundPacket<'static, Perms>) {
        self.tx.send((pos, packet)).unwrap();
    }
}

fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    return file.read_at(buf, offset);
    #[cfg(windows)]
    return file.seek_read(buf, offset);
}

fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    return file.write_at(buf, offset);
    #[cfg(windows)]
    return file.seek_write(buf, offset);
}

pub struct FileWrapper {
    file: BaseArc<File>,
    read_stream: ManuallyDrop<BaseArc<PacketStream<'static, Write, u64>>>,
    write_stream: ManuallyDrop<BaseArc<PacketStream<'static, Read, u64>>>,
    read_thread: Option<JoinHandle<()>>,
    write_thread: Option<JoinHandle<()>>,
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.read_stream);
            ManuallyDrop::drop(&mut self.write_stream);
        }
        self.read_thread.take().unwrap().join().unwrap();
        self.write_thread.take().unwrap().join().unwrap();
    }
}

impl From<File> for FileWrapper {
    fn from(file: File) -> Self {
        Self::from(BaseArc::from(file))
    }
}

impl From<BaseArc<File>> for FileWrapper {
    fn from(file: BaseArc<File>) -> Self {
        let (read_tx, read_rx) = mpsc::channel();
        let read_io = BaseArc::new(IoThreadHandle::<Write>::new(read_tx));

        let read_thread = Some(thread::spawn({
            let file = file.clone();
            move || {
                for (pos, buf) in read_rx.into_iter() {
                    let mut pkt = buf.get_mut();
                    let buf = &mut pkt[..];
                    if !buf.is_empty() {
                        // SAFETY: assume MaybeUninit<u8> is initialized,
                        // as God intended :upside_down:
                        let buf = unsafe {
                            let ptr = buf.as_mut_ptr();
                            let len = buf.len();
                            core::slice::from_raw_parts_mut(ptr as *mut u8, len)
                        };

                        match read_at(&file, buf, pos) {
                            Ok(read) if read < buf.len() => {
                                let (_, right) = pkt.split_at(read);
                                right.error(Some(()));
                            }
                            Err(_) => pkt.error(Some(())),
                            _ => (),
                        }
                    }
                }
            }
        }));

        let (write_tx, write_rx) = mpsc::channel();
        let write_io = BaseArc::new(IoThreadHandle::new(write_tx));

        let write_thread = Some(thread::spawn({
            let file = file.clone();
            move || {
                for (pos, buf) in write_rx.into_iter() {
                    let pkt = buf.get();
                    if pkt.len() > 0 {
                        match write_at(&file, &pkt, pos) {
                            Ok(written) if written < pkt.len() => {
                                let (_, right) = pkt.split_at(written);
                                right.error(Some(()));
                            }
                            Err(_) => pkt.error(Some(())),
                            _ => (),
                        }
                    }
                }
            }
        }));

        let write_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
            future: None,
        }));

        let read_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
            future: None,
        }));

        Self {
            file,
            read_thread,
            write_thread,
            read_stream,
            write_stream,
        }
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        *self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Read, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        *self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut Context) -> Option<PacketId<'a, Write, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}
