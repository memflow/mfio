//! `std::io` equivalent Read/Write traits.

use crate as mfio;
use crate::error::Result;
use crate::io::*;
use crate::locks::Mutex;
use crate::std_prelude::*;
use crate::traits::*;
use crate::util::{PosShift, UsizeMath};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use mfio_derive::*;

pub trait StreamPos<Param> {
    fn set_pos(&self, pos: Param);

    fn get_pos(&self) -> Param;

    fn update_pos<F: FnOnce(Param) -> Param>(&self, f: F);

    fn end(&self) -> Option<Param> {
        None
    }
}

impl<Param: Copy + UsizeMath, Io: StreamPos<Param>> PosShift<Io> for Param {
    fn add_pos(&mut self, out: usize, io: &Io) {
        self.add_assign(out);
        io.set_pos(*self);
    }

    fn add_io_pos(io: &Io, out: usize) {
        io.update_pos(|pos| pos.add(out))
    }
}

pub trait AsyncRead<Param: 'static>: IoRead<Param> {
    fn read<'a>(&'a self, buf: &'a mut [u8]) -> AsyncIoFut<'a, Self, Write, Param, &'a mut [u8]>;
    fn read_to_end<'a>(&'a self, buf: &'a mut Vec<u8>) -> StdReadToEndFut<'a, Self, Param>;
}

impl<T: IoRead<Param> + StreamPos<Param>, Param: 'static + Copy> AsyncRead<Param> for T {
    fn read<'a>(&'a self, buf: &'a mut [u8]) -> AsyncIoFut<'a, Self, Write, Param, &'a mut [u8]> {
        let len = buf.len();
        let (pkt, sync) = <&'a mut [u8] as IntoPacket<Write>>::into_packet(buf);
        AsyncIoFut {
            io: self,
            len,
            fut: self.io(self.get_pos(), pkt),
            sync: Some(sync),
        }
    }

    fn read_to_end<'a>(&'a self, buf: &'a mut Vec<u8>) -> StdReadToEndFut<'a, Self, Param> {
        StdReadToEndFut {
            io: self,
            fut: <Self as IoRead<Param>>::read_to_end(self, self.get_pos(), buf),
        }
    }
}

impl<T: IoRead<NoPos>> AsyncRead<NoPos> for T {
    fn read<'a>(&'a self, buf: &'a mut [u8]) -> AsyncIoFut<'a, Self, Write, NoPos, &'a mut [u8]> {
        let len = buf.len();
        let (pkt, sync) = <&'a mut [u8] as IntoPacket<Write>>::into_packet(buf);
        AsyncIoFut {
            io: self,
            len,
            fut: self.io(NoPos::new(), pkt),
            sync: Some(sync),
        }
    }

    fn read_to_end<'a>(&'a self, buf: &'a mut Vec<u8>) -> StdReadToEndFut<'a, Self, NoPos> {
        StdReadToEndFut {
            io: self,
            fut: <Self as IoRead<NoPos>>::read_to_end(self, NoPos::new(), buf),
        }
    }
}

pub trait AsyncWrite<Param>: IoWrite<Param> {
    fn write<'a>(&'a self, buf: &'a [u8]) -> AsyncIoFut<'a, Self, Read, Param, &'a [u8]>;
}

impl<T: IoWrite<Param> + StreamPos<Param>, Param: Copy> AsyncWrite<Param> for T {
    fn write<'a>(&'a self, buf: &'a [u8]) -> AsyncIoFut<'a, Self, Read, Param, &'a [u8]> {
        let len = buf.len();
        let (pkt, sync) = buf.into_packet();
        AsyncIoFut {
            io: self,
            len,
            fut: self.io(self.get_pos(), pkt),
            sync: Some(sync),
        }
    }
}

impl<T: IoWrite<NoPos>> AsyncWrite<NoPos> for T {
    fn write<'a>(&'a self, buf: &'a [u8]) -> AsyncIoFut<'a, Self, Read, NoPos, &'a [u8]> {
        let len = buf.len();
        let (pkt, sync) = buf.into_packet();
        AsyncIoFut {
            io: self,
            len,
            fut: self.io(NoPos::new(), pkt),
            sync: Some(sync),
        }
    }
}

pub struct AsyncIoFut<
    'a,
    Io: PacketIo<Perms, Param>,
    Perms: PacketPerms,
    Param: 'a,
    Obj: IntoPacket<'a, Perms>,
> {
    io: &'a Io,
    fut: IoFut<'a, Io, Perms, Param, Obj::Target>,
    sync: Option<Obj::SyncHandle>,
    len: usize,
}

impl<
        'a,
        Io: PacketIo<Perms, Param>,
        Perms: PacketPerms,
        Param: PosShift<Io>,
        Obj: IntoPacket<'a, Perms>,
    > Future for AsyncIoFut<'a, Io, Perms, Param, Obj>
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let fut = unsafe { Pin::new_unchecked(&mut this.fut) };

        fut.poll(cx).map(|pkt| {
            let hdr = <<Obj as IntoPacket<'a, Perms>>::Target as OpaqueStore>::stack_hdr(&pkt);
            // TODO: put this after error checking
            Obj::sync_back(hdr, this.sync.take().unwrap());
            let progressed = core::cmp::min(hdr.error_clamp() as usize, this.len);
            Param::add_io_pos(this.io, progressed);
            // TODO: actual error checking
            Ok(progressed)
        })
    }
}

pub struct StdReadToEndFut<'a, Io: PacketIo<Write, Param>, Param> {
    io: &'a Io,
    fut: ReadToEndFut<'a, Io, Param>,
}

impl<'a, Io: PacketIo<Write, Param>, Param: PosShift<Io>> Future
    for StdReadToEndFut<'a, Io, Param>
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match unsafe { Pin::new_unchecked(&mut this.fut) }.poll(cx) {
            Poll::Ready(Ok(r)) => {
                Param::add_io_pos(this.io, r);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[macro_export]
/// Implements `Read`+`Write`+`Seek` traits on compatible type.
///
/// Implements `io::Seek` on type implementing `StreamPos<u64>`, `io::Write` on type implementing
/// `AsyncWrite<u64>` and `io::Read` on type implementing `AsyncRead<u64>`.
macro_rules! stdio_impl {
    (<$($ty2:ident),*> $t:ident <$($ty:ident),*> @ $($tt:tt)*) => {
        impl<$($ty2),*> std::io::Seek for $t<$($ty),*> where $($tt)* {
            fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
                match pos {
                    std::io::SeekFrom::Start(val) => {
                        self.set_pos(val);
                        Ok(val)
                    }
                    std::io::SeekFrom::End(val) => {
                        if let Some(end) = $crate::stdeq::StreamPos::<u64>::end(self) {
                            let pos = if val < 0 {
                                end.checked_sub((-val) as u64)
                                    .ok_or_else(|| std::io::ErrorKind::InvalidInput)?
                            } else {
                                end + val as u64
                            };
                            self.set_pos(pos);
                            Ok(pos)
                        } else {
                            Err(std::io::ErrorKind::Unsupported.into())
                        }
                    }
                    std::io::SeekFrom::Current(val) => {
                        let pos = $crate::stdeq::StreamPos::<u64>::get_pos(self);
                        let pos = if val < 0 {
                            pos.checked_sub((-val) as u64)
                                .ok_or_else(|| std::io::ErrorKind::InvalidInput)?
                        } else {
                            pos + val as u64
                        };
                        self.set_pos(pos);
                        Ok(pos)
                    }
                }
            }

            fn stream_position(&mut self) -> std::io::Result<u64> {
                Ok(self.get_pos())
            }

            fn rewind(&mut self) -> std::io::Result<()> {
                self.set_pos(0);
                Ok(())
            }
        }

        impl<$($ty2),*> std::io::Read for $t<$($ty),*> where $t<$($ty),*>: $crate::stdeq::AsyncRead<u64> + $crate::backend::IoBackend, $($tt)* {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                use $crate::backend::IoBackend;
                self.block_on($crate::stdeq::AsyncRead::read(self, buf)).map_err(|_| std::io::ErrorKind::Other.into())
            }

            fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
                use $crate::backend::IoBackend;
                let len = buf.len();
                self.block_on($crate::stdeq::AsyncRead::read_to_end(self, buf)).map_err(|_| std::io::ErrorKind::Other)?;
                Ok(buf.len() - len)
            }
        }

        impl<$($ty2),*> std::io::Write for $t<$($ty),*> where $t<$($ty),*>: $crate::stdeq::AsyncWrite<u64> + $crate::backend::IoBackend, $($tt)* {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                use $crate::backend::IoBackend;
                self.block_on(AsyncWrite::write(self, buf)).map_err(|_| std::io::ErrorKind::Other.into())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
    };
    ($t:ident @ $($tt:tt)*) => {
        $crate::stdio_impl!($t<> @ $($tt)*);
    }
}

#[derive(SyncIoWrite, SyncIoRead)]
pub struct Seekable<T, Param> {
    pos: Mutex<Param>,
    handle: T,
}

impl<T, Param: Default> From<T> for Seekable<T, Param> {
    fn from(handle: T) -> Self {
        Self {
            pos: Default::default(),
            handle,
        }
    }
}

impl<T: PacketIo<Perms, Param>, Perms: PacketPerms, Param> PacketIo<Perms, Param>
    for Seekable<T, Param>
{
    fn send_io(&self, param: Param, view: BoundPacketView<Perms>) {
        self.handle.send_io(param, view)
    }
}

impl<T, Param: Copy> StreamPos<Param> for Seekable<T, Param> {
    fn get_pos(&self) -> Param {
        *self.pos.lock()
    }

    fn set_pos(&self, pos: Param) {
        *self.pos.lock() = pos;
    }

    fn update_pos<F: FnOnce(Param) -> Param>(&self, f: F) {
        let mut pos = self.pos.lock();
        *pos = f(*pos);
    }
}

#[cfg(feature = "std")]
stdio_impl!(<T> Seekable<T, u64> @);

#[derive(SyncIoWrite, SyncIoRead)]
pub struct FakeSeek<T> {
    handle: T,
}

impl<T> From<T> for FakeSeek<T> {
    fn from(handle: T) -> Self {
        Self { handle }
    }
}

impl<T: PacketIo<Perms, Param>, Perms: PacketPerms, Param> PacketIo<Perms, Param> for FakeSeek<T> {
    fn send_io(&self, param: Param, view: BoundPacketView<Perms>) {
        self.handle.send_io(param, view)
    }
}

impl<T, Param: Default + core::ops::Not<Output = Param>> StreamPos<Param> for FakeSeek<T> {
    fn get_pos(&self) -> Param {
        !Param::default()
    }

    fn set_pos(&self, _: Param) {}

    fn update_pos<F: FnOnce(Param) -> Param>(&self, _: F) {}
}

#[cfg(feature = "std")]
stdio_impl!(<T> FakeSeek<T> @);
