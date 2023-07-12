use std::fs::File;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use core::future::pending;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::task::Waker;

use super::{OwnedStreamHandle, StreamBorrow, StreamHandleConv};

use std::io;
#[cfg(all(unix, not(miri)))]
use std::os::unix::fs::FileExt;
#[cfg(all(windows, not(miri)))]
use std::os::windows::fs::FileExt;

#[cfg(windows)]
use std::os::windows::io::{
    AsRawHandle, FromRawHandle, IntoRawHandle as IntoRaw, IntoRawSocket, OwnedHandle, OwnedSocked,
};

use mfio::backend::*;
use mfio::packet::*;
use mfio::tarc::BaseArc;

use crate::util::io_err;
use mfio::error::State;

struct IoInner<Handle, Param> {
    handle: Handle,
    read_at: fn(&Handle, &mut [u8], Param) -> io::Result<usize>,
    write_at: fn(&Handle, &[u8], Param) -> io::Result<usize>,
}

impl<Handle, Param> IoInner<Handle, Param> {
    fn read_at(&self, buf: &mut [u8], pos: Param) -> io::Result<usize> {
        (self.read_at)(&self.handle, buf, pos)
    }

    fn write_at(&self, buf: &[u8], pos: Param) -> io::Result<usize> {
        (self.write_at)(&self.handle, buf, pos)
    }
}

impl From<File> for IoInner<File, u64> {
    fn from(handle: File) -> Self {
        fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            #[cfg(miri)]
            {
                use io::{Read, Seek, SeekFrom};
                (&*file).seek(SeekFrom::Start(offset))?;
                return (&*file).read(buf);
            }
            #[cfg(not(miri))]
            {
                #[cfg(unix)]
                return file.read_at(buf, offset);
                #[cfg(windows)]
                return file.seek_read(buf, offset);
            }
        }

        fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
            #[cfg(miri)]
            {
                use io::{Seek, SeekFrom, Write};
                (&*file).seek(SeekFrom::Start(offset))?;
                (&*file).write(buf)
            }
            #[cfg(not(miri))]
            {
                #[cfg(unix)]
                return file.write_at(buf, offset);
                #[cfg(windows)]
                return file.seek_write(buf, offset);
            }
        }

        Self {
            handle,
            read_at,
            write_at,
        }
    }
}

impl<T: StreamHandleConv> From<T> for IoInner<OwnedStreamHandle, NoPos> {
    fn from(stream: T) -> Self {
        fn read_at<T: StreamHandleConv>(
            stream: &OwnedStreamHandle,
            buf: &mut [u8],
            _: NoPos,
        ) -> io::Result<usize> {
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            stream.read(buf)
        }

        fn write_at<T: StreamHandleConv>(
            stream: &OwnedStreamHandle,
            buf: &[u8],
            _: NoPos,
        ) -> io::Result<usize> {
            log::debug!("Write {}", buf.len());
            let mut stream = unsafe { StreamBorrow::<T>::get(stream) };
            let ret = stream.write(buf);
            log::debug!("Written");
            ret
        }

        let handle = stream.into_owned();

        Self {
            handle,
            read_at: read_at::<T>,
            write_at: write_at::<T>,
        }
    }
}

struct IoThreadHandle<Perms: PacketPerms, Param> {
    handle: PacketIoHandle<'static, Perms, Param>,
    tx: Sender<(Param, BoundPacket<'static, Perms>)>,
}

impl<Perms: PacketPerms, Param> IoThreadHandle<Perms, Param> {
    fn new(tx: Sender<(Param, BoundPacket<'static, Perms>)>) -> Self {
        Self {
            handle: PacketIoHandle::new::<Self>(),
            tx,
        }
    }
}

impl<Perms: PacketPerms, Param> AsRef<PacketIoHandle<'static, Perms, Param>>
    for IoThreadHandle<Perms, Param>
{
    fn as_ref(&self) -> &PacketIoHandle<'static, Perms, Param> {
        &self.handle
    }
}

impl<Perms: PacketPerms, Param> PacketIoHandleable<'static, Perms, Param>
    for IoThreadHandle<Perms, Param>
{
    extern "C" fn send_input(&self, pos: Param, packet: BoundPacket<'static, Perms>) {
        self.tx.send((pos, packet)).unwrap();
    }
}

struct IoWrapper<Handle, Param> {
    _file: BaseArc<IoInner<Handle, Param>>,
    read_stream: ManuallyDrop<BaseArc<PacketStream<'static, Write, Param>>>,
    write_stream: ManuallyDrop<BaseArc<PacketStream<'static, Read, Param>>>,
    read_thread: Option<JoinHandle<()>>,
    write_thread: Option<JoinHandle<()>>,
}

impl<Handle, Param> Drop for IoWrapper<Handle, Param> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.read_stream);
            ManuallyDrop::drop(&mut self.write_stream);
        }
        self.read_thread.take().unwrap().join().unwrap();
        self.write_thread.take().unwrap().join().unwrap();
    }
}

impl<Handle: Send + Sync + 'static, Param: Send + 'static> From<BaseArc<IoInner<Handle, Param>>>
    for IoWrapper<Handle, Param>
{
    fn from(file: BaseArc<IoInner<Handle, Param>>) -> Self {
        let (read_tx, read_rx) = mpsc::channel();
        let read_io = BaseArc::new(IoThreadHandle::<Write, Param>::new(read_tx));

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

                        file.read_at(buf, pos)
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
                                    tmp_buf.reserve(buf.len() - tmp_buf.len());
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
                            match file.write_at(&alloced[..], pos) {
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
                            match file.write_at(tmp_buf, pos) {
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
            _file: file,
            read_thread,
            write_thread,
            read_stream,
            write_stream,
        }
    }
}

impl<Handle, Param> PacketIo<Read, Param> for IoWrapper<Handle, Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Read, Param>> {
        Some(self.write_stream.new_packet_id())
    }
}

impl<Handle, Param> PacketIo<Write, Param> for IoWrapper<Handle, Param> {
    fn try_new_id<'a>(&'a self, _: &mut FastCWaker) -> Option<PacketId<'a, Write, Param>> {
        Some(self.read_stream.new_packet_id())
    }
}

impl PacketIo<Read, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, Read, u64>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<Write, u64> for FileWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, Write, u64>> {
        self.0.try_new_id(cx)
    }
}

impl PacketIo<Read, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, w: &mut FastCWaker) -> Option<PacketId<'a, Read, NoPos>> {
        self.0.try_new_id(w)
    }
}

impl PacketIo<Write, NoPos> for StreamWrapper {
    fn try_new_id<'a>(&'a self, cx: &mut FastCWaker) -> Option<PacketId<'a, Write, NoPos>> {
        self.0.try_new_id(cx)
    }
}

pub struct NativeFs {
    backend: BackendContainer<DynBackend>,
}

impl NativeFs {
    pub fn try_new() -> Result<Self, std::convert::Infallible> {
        Ok(Self {
            backend: BackendContainer::new_dyn(pending()),
        })
    }
}

impl IoBackend for NativeFs {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<(DefaultHandle, Waker)> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire(None)
    }
}

pub struct FileWrapper(IoWrapper<File, u64>);
pub struct StreamWrapper(IoWrapper<OwnedStreamHandle, NoPos>);

impl NativeFs {
    pub fn register_file(&self, file: File) -> FileWrapper {
        FileWrapper(IoWrapper::from(BaseArc::from(IoInner::from(file))))
    }

    pub fn register_stream<T: StreamHandleConv>(&self, stream: T) -> StreamWrapper {
        StreamWrapper(IoWrapper::from(BaseArc::from(
            <T as Into<IoInner<_, _>>>::into(stream),
        )))
    }
}
