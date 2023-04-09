//! `std::io` equivalent Read/Write traits.

use crate::packet::*;
use crate::traits::*;
use crate::util::UsizeMath;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;
use std::io;

pub trait StreamPos<Param> {
    fn set_pos(&mut self, pos: Param);

    fn get_pos(&self) -> Param;

    fn end(&self) -> Option<Param> {
        None
    }
}

pub trait AsyncRead<Param>: IoRead<Param> + StreamPos<Param> {
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> AsyncIoFut<'a, Self, Write, Param> {
        AsyncIoFut {
            io: self as *mut Self,
            pos: self.get_pos(),
            len: buf.len(),
            state: AsyncIoFutState::NewId(buf.into(), self.new_id()),
            _phantom: PhantomData,
        }
    }

    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> StdReadToEndFut<'a, Self, Param> {
        StdReadToEndFut {
            io: self as *mut Self,
            fut: <Self as IoRead<Param>>::read_to_end(self, self.get_pos(), buf),
        }
    }
}

impl<T: IoRead<Param> + StreamPos<Param>, Param> AsyncRead<Param> for T {}

pub trait AsyncWrite<Param>: IoWrite<Param> + StreamPos<Param> {
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> AsyncIoFut<'a, Self, Read, Param> {
        AsyncIoFut {
            io: self as *mut Self,
            pos: self.get_pos(),
            len: buf.len(),
            state: AsyncIoFutState::NewId(buf.into(), self.new_id()),
            _phantom: PhantomData,
        }
    }
}

impl<T: IoWrite<Param> + StreamPos<Param>, Param> AsyncWrite<Param> for T {}

pub struct AsyncIoFut<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param> {
    io: *mut Io,
    pos: Param,
    len: usize,
    state: AsyncIoFutState<'a, Io, Perms, Param>,
    _phantom: PhantomData<&'a mut [u8]>,
}

pub enum AsyncIoFutState<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param: 'a> {
    NewId(Packet<'a, Perms>, NewIdFut<'a, Io, Perms, Param>),
    Read(
        Option<usize>,
        Option<io::Error>,
        <NewIdFut<'a, Io, Perms, Param> as Future>::Output,
    ),
    Finished,
}

impl<
        'a,
        Io: PacketIo<Perms, Param> + StreamPos<Param>,
        Perms: PacketPerms,
        Param: Copy + UsizeMath,
    > Future for AsyncIoFut<'a, Io, Perms, Param>
{
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                AsyncIoFutState::NewId(_, new_id) => {
                    let new_id = unsafe { Pin::new_unchecked(new_id) };

                    if let Poll::Ready(id) = new_id.poll(cx) {
                        let prev = core::mem::replace(
                            &mut this.state,
                            AsyncIoFutState::Read(None, None, id),
                        );
                        match (prev, &this.state) {
                            (
                                AsyncIoFutState::NewId(packet, _),
                                AsyncIoFutState::Read(_, _, id),
                            ) => {
                                unsafe { Pin::new_unchecked(id) }.send_io(this.pos, packet);
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        break Poll::Pending;
                    }
                }
                AsyncIoFutState::Read(failed_pos, err, id) => {
                    match unsafe { Pin::new_unchecked(&mut *id) }.poll_next(cx) {
                        Poll::Ready(Some((pkt, err))) => {
                            // We failed, thus cap the output length,
                            // but we still need to complete outstanding reads.
                            if err.is_some() {
                                let new_end = pkt.end();
                                let end = failed_pos.get_or_insert(new_end);
                                *end = core::cmp::min(*end, new_end);
                            }
                        }
                        Poll::Ready(None) => {
                            let out = failed_pos.unwrap_or(this.len);

                            if let Some(err) = err.take() {
                                if out == 0 {
                                    break Poll::Ready(Err(err));
                                }
                            }

                            this.pos.add_assign(out);

                            // SAFETY: there are no more shared references to io.
                            unsafe {
                                (*this.io).set_pos(this.pos);
                            }

                            break Poll::Ready(Ok(out));
                        }
                        _ => break Poll::Pending,
                    }
                }
                AsyncIoFutState::Finished => unreachable!(),
            }
        }
    }
}

pub struct StdReadToEndFut<'a, Io: PacketIo<Write, Param>, Param> {
    io: *mut Io,
    fut: ReadToEndFut<'a, Io, Param>,
}

impl<'a, Io: PacketIo<Write, Param> + StreamPos<Param>, Param: Copy + UsizeMath> Future
    for StdReadToEndFut<'a, Io, Param>
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match unsafe { Pin::new_unchecked(&mut this.fut) }.poll(cx) {
            Poll::Ready(Some(r)) => {
                // SAFETY: there are no more shared references to io.
                unsafe {
                    let io = &mut *this.io;
                    io.set_pos(io.get_pos().add(r));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Err(io::ErrorKind::Other.into())),
            //Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[macro_export]
/// Implement io::Seek on type implementing `StreamPos<u64>`
macro_rules! seek_impl_on_pos {
    (<$($ty2:ident),*> $t:ident <$($ty:ident),*> @ $($tt:tt)*) => {
        impl<$($ty2),*> std::io::Seek for $t<$($ty),*> $($tt)* {
            fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
                match pos {
                    std::io::SeekFrom::Start(val) => {
                        self.set_pos(val);
                        Ok(val)
                    }
                    std::io::SeekFrom::End(val) => {
                        if let Some(end) = self.end() {
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
                        let pos = self.get_pos();
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
    };
    ($t:ident @ $($tt:tt)*) => {
        $crate::seek_impl!($t<> @ $($tt)*);
    }
}

pub struct Seekable<T, Param> {
    pos: Param,
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
    fn separate_thread_state(&mut self) {
        self.handle.separate_thread_state();
    }

    fn try_new_id<'a>(&'a self, context: &mut Context) -> Option<PacketId<'a, Perms, Param>> {
        self.handle.try_new_id(context)
    }
}

impl<T, Param: Copy> StreamPos<Param> for Seekable<T, Param> {
    fn get_pos(&self) -> Param {
        self.pos
    }

    fn set_pos(&mut self, pos: Param) {
        self.pos = pos;
    }
}

seek_impl_on_pos!(<T> Seekable<T, u64> @);