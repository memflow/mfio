use std::fs::File;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use core::future::pending;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::task::Waker;

#[cfg(all(unix, not(miri)))]
use std::os::unix::fs::FileExt;
#[cfg(all(windows, not(miri)))]
use std::os::windows::fs::FileExt;

use mfio::backend::*;
use mfio::packet::*;
use mfio::tarc::BaseArc;

use super::{io_err, State};

#[cfg(miri)]
type FileInner = std::sync::Mutex<File>;
#[cfg(not(miri))]
type FileInner = File;

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

fn read_at(file: &FileInner, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(miri)]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }
    #[cfg(not(miri))]
    {
        #[cfg(unix)]
        return file.read_at(buf, offset);
        #[cfg(windows)]
        return file.seek_read(buf, offset);
    }
}

fn write_at(file: &FileInner, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(miri)]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write(buf)
    }
    #[cfg(not(miri))]
    {
        #[cfg(unix)]
        return file.write_at(buf, offset);
        #[cfg(windows)]
        return file.seek_write(buf, offset);
    }
}

pub struct FileWrapper {
    file: BaseArc<FileInner>,
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
        #[allow(clippy::useless_conversion)]
        Self::from(BaseArc::from(FileInner::from(file)))
    }
}

impl From<BaseArc<FileInner>> for FileWrapper {
    fn from(file: BaseArc<FileInner>) -> Self {
        let (read_tx, read_rx) = mpsc::channel();
        let read_io = BaseArc::new(IoThreadHandle::<Write>::new(read_tx));

        let read_thread = Some(thread::spawn({
            let file = file.clone();
            move || {
                let mut tmp_buf = vec![];
                for (pos, buf) in read_rx {
                    let copy_buf = |buf: &mut [MaybeUninit<u8>]| {
                        // SAFETY: assume MaybeUninit<u8> is initialized,
                        // as God intended :upside_down:
                        let buf = unsafe {
                            let ptr = buf.as_mut_ptr();
                            let len = buf.len();
                            core::slice::from_raw_parts_mut(ptr as *mut u8, len)
                        };

                        read_at(&file, buf, pos)
                    };

                    if !buf.is_empty() {
                        match buf.try_alloc() {
                            Ok(mut alloced) => match copy_buf(&mut alloced[..]) {
                                Ok(read) if read < alloced.len() => {
                                    let (_, right) = alloced.split_at(read);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => alloced.error(io_err(e.kind().into())),
                                _ => (),
                            },
                            Err(buf) => {
                                // TODO: size limit the temp buffer.
                                if tmp_buf.len() < buf.len() {
                                    tmp_buf.reserve(tmp_buf.len() - buf.len());
                                    // SAFETY: assume MaybeUninit<u8> is initialized,
                                    // as God intended :upside_down:
                                    unsafe { tmp_buf.set_len(tmp_buf.capacity()) }
                                }
                                match copy_buf(&mut tmp_buf[..buf.len()]) {
                                    Ok(read) if read < buf.len() => {
                                        let (left, right) = buf.split_at(read);
                                        unsafe { left.transfer_data(tmp_buf.as_ptr().cast()) };
                                        right.error(io_err(State::Nop));
                                    }
                                    Err(e) => buf.error(io_err(e.kind().into())),
                                    _ => {
                                        unsafe { buf.transfer_data(tmp_buf.as_ptr().cast()) };
                                    }
                                }
                            }
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
                let mut tmp_buf: Vec<MaybeUninit<u8>> = vec![];
                for (pos, buf) in write_rx {
                    match buf.try_alloc() {
                        Ok(alloced) => {
                            // For some reason type inference loses itself
                            let alloced: ReadPacketObj = alloced;
                            match write_at(&file, &alloced[..], pos) {
                                Ok(written) if written < alloced.len() => {
                                    let (_, right) = alloced.split_at(written);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => alloced.error(io_err(e.kind().into())),
                                _ => (),
                            }
                        }
                        Err(buf) => {
                            // TODO: size limit the temp buffer.
                            if tmp_buf.len() < buf.len() {
                                tmp_buf.reserve(tmp_buf.len() - buf.len());
                                // SAFETY: assume MaybeUninit<u8> is initialized,
                                // as God intended :upside_down:
                                unsafe { tmp_buf.set_len(buf.len()) }
                            }
                            let buf = unsafe { buf.transfer_data(tmp_buf.as_mut_ptr().cast()) };
                            let tmp_buf = unsafe {
                                &*(&tmp_buf[..] as *const [MaybeUninit<u8>] as *const [u8])
                            };
                            match write_at(&file, tmp_buf, pos) {
                                Ok(written) if written < buf.len() => {
                                    let (_, right) = buf.split_at(written);
                                    right.error(io_err(State::Nop));
                                }
                                Err(e) => buf.error(io_err(e.kind().into())),
                                _ => (),
                            }
                        }
                    }
                }
            }
        }));

        let write_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(write_io).into(),
        }));

        let read_stream = ManuallyDrop::new(BaseArc::from(PacketStream {
            ctx: PacketCtx::new(read_io).into(),
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

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Read, u64>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn separate_thread_state(&mut self) {
        *self = Self::from(self.file.clone());
    }

    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Write, u64>> {
        Some(self.read_stream.new_packet_id())
    }
}

pub struct NativeFs {
    backend: BackendContainer<DynBackend>,
}

impl Default for NativeFs {
    fn default() -> Self {
        Self {
            backend: BackendContainer::new_dyn(pending()),
        }
    }
}

impl IoBackend for NativeFs {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire()
    }
}

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        FileWrapper::from(file)
    }
}
