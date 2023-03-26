use crate::packet::*;
use bytemuck::Pod;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;

pub trait IoRead<Pos>: PacketIo<Write, Pos> {
    fn read_raw<'a>(
        &'a self,
        pos: Pos,
        packet: impl Into<Packet<'a, Write>>,
    ) -> IoFut<'a, Self, Write, Pos> {
        self.io(pos, packet)
    }

    fn read_all<'a>(
        &'a self,
        pos: Pos,
        packet: impl Into<Packet<'a, Write>>,
    ) -> IoFullFut<'a, Self, Write, Pos> {
        IoFullFut::AllocStream(pos, packet.into(), self.alloc_stream())
    }

    fn read_into<'a, T: Pod>(
        &'a self,
        pos: Pos,
        data: &'a mut MaybeUninit<T>,
    ) -> IoFullFut<'a, Self, Write, Pos> {
        let buf = unsafe {
            core::slice::from_raw_parts_mut(
                data as *mut MaybeUninit<T> as *mut MaybeUninit<u8>,
                core::mem::size_of::<T>(),
            )
        };
        self.read_all(pos, buf)
    }

    /// # Notes
    ///
    /// This function may break rust stacked borrows rules. If you wish to not do that, please use
    /// [`read_into`](Self::read_into) function.
    ///
    /// This may be fixed once const generics are able to instantiate `[u8; mem::size_of::<T>()]`.
    fn read<T: Pod>(&self, pos: Pos) -> IoReadFut<Self, Pos, T> {
        IoReadFut::AllocStream(pos, self.alloc_stream())
    }

    fn read_to_end<'a>(&'a self, pos: Pos, buf: &'a mut Vec<u8>) -> ReadToEndFut<'a, Self, Pos> {
        ReadToEndFut {
            pos,
            buf,
            state: ReadToEndFutState::AllocStream(self.alloc_stream()),
        }
    }
}

impl<T: PacketIo<Write, Pos>, Pos> IoRead<Pos> for T {}

pub trait IoWrite<Pos>: PacketIo<Read, Pos> {
    fn write_raw<'a>(
        &'a self,
        pos: Pos,
        packet: impl Into<Packet<'a, Read>>,
    ) -> IoFut<'a, Self, Read, Pos> {
        self.io(pos, packet)
    }

    fn write_all<'a>(
        &'a self,
        pos: Pos,
        packet: impl Into<Packet<'a, Read>>,
    ) -> IoFullFut<'a, Self, Read, Pos> {
        IoFullFut::AllocStream(pos, packet.into(), self.alloc_stream())
    }

    fn write<'a, T>(&'a self, pos: Pos, data: &'a T) -> IoFullFut<'a, Self, Read, Pos> {
        let buf = unsafe {
            core::slice::from_raw_parts(data as *const T as *const u8, core::mem::size_of::<T>())
        };
        self.write_all(pos, buf)
    }
}

impl<T: PacketIo<Read, Pos>, Pos> IoWrite<Pos> for T {}

pub enum IoFullFut<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param> {
    AllocStream(
        Param,
        Packet<'a, Perms>,
        AllocStreamFut<'a, Io, Perms, Param>,
    ),
    Read(
        Option<()>,
        <AllocStreamFut<'a, Io, Perms, Param> as Future>::Output,
    ),
    Finished,
}

impl<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param> Future
    for IoFullFut<'a, Io, Perms, Param>
{
    type Output = Option<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match this {
                Self::AllocStream(_, _, alloc) => {
                    let alloc = unsafe { Pin::new_unchecked(alloc) };

                    if let Poll::Ready(stream) = alloc.poll(cx) {
                        let prev = core::mem::replace(this, Self::Read(None, stream));
                        match (prev, &mut *this) {
                            (Self::AllocStream(param, packet, _), Self::Read(_, stream)) => {
                                stream.send_io(param, packet)
                            }
                            _ => unreachable!(),
                        }
                        // Poll again to force processing of the stream
                        continue;
                    } else {
                        break Poll::Pending;
                    }
                }
                Self::Read(err, stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(None) => {
                        let prev = core::mem::replace(this, Self::Finished);

                        match prev {
                            Self::Read(err, _) => break Poll::Ready(err),
                            _ => unreachable!(),
                        }
                    }
                    Poll::Ready(Some((_, nerr))) => {
                        if let Some(nerr) = nerr {
                            *err = Some(nerr);
                        }
                        continue;
                    }
                    _ => break Poll::Pending,
                },
                Self::Finished => unreachable!(),
            }
        }
    }
}

pub struct ReadToEndFut<'a, Io, Param> {
    pos: Param,
    buf: &'a mut Vec<u8>,
    state: ReadToEndFutState<'a, Io, Param>,
}

pub enum ReadToEndFutState<'a, Io, Param> {
    AllocStream(AllocStreamFut<'a, Io, Write, Param>),
    Read(
        usize,
        usize,
        Option<usize>,
        <AllocStreamFut<'a, Io, Write, Param> as Future>::Output,
    ),
    Finished,
}

impl<'a, Io: PacketIo<Write, Param>, Param: Copy + core::ops::AddAssign<usize>> Future
    for ReadToEndFut<'a, Io, Param>
{
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match &mut this.state {
            ReadToEndFutState::AllocStream(alloc) => {
                let alloc = unsafe { Pin::new_unchecked(alloc) };

                if let Poll::Ready(stream) = alloc.poll(cx) {
                    let start_len = this.buf.len();
                    let start_cap = this.buf.capacity();

                    // Reserve enough for 32 bytes of data initially
                    if start_cap - start_len < 32 {
                        this.buf.reserve(start_cap - start_len);
                    }

                    // Issue a read
                    let data = this.buf.as_mut_ptr() as *mut MaybeUninit<u8>;
                    // SAFETY: the data here is uninitialized, and we are getting exclusive access
                    // to it.
                    let data = unsafe {
                        core::slice::from_raw_parts_mut(
                            data.add(start_len),
                            this.buf.capacity() - start_len,
                        )
                    };
                    stream.send_io(this.pos, data);

                    this.state = ReadToEndFutState::Read(start_len, start_cap, None, stream);
                    // SAFETY: self is already pinned
                    unsafe { Pin::new_unchecked(this) }.poll(cx)
                } else {
                    Poll::Pending
                }
            }
            ReadToEndFutState::Read(start_len, start_cap, final_cap, stream) => {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some((pkt, err))) => {
                        // We failed, thus cap the buffer length, complete queued I/O, but do not
                        // perform any further reads.
                        if err.is_some() {
                            let new_end = pkt.end();
                            let end = final_cap.get_or_insert(new_end);
                            *end = core::cmp::min(*end, new_end);
                        }
                        // SAFETY: self is already pinned
                        unsafe { Pin::new_unchecked(this) }.poll(cx)
                    }
                    Poll::Ready(None) => {
                        // If we read all bytes successfully, grow the buffer and keep going.
                        // Otherwise, return finished state.
                        match final_cap {
                            Some(cap) => {
                                // SAFETY: these bytes have been successfully read
                                unsafe { this.buf.set_len(*start_len + *cap) };
                                Poll::Ready(*cap)
                            }
                            _ => {
                                // SAFETY: all these bytes have been successfully read
                                unsafe { this.buf.set_len(this.buf.capacity()) };

                                // Double read size, but cap it to 2MB
                                let reserve_len =
                                    core::cmp::min(this.buf.capacity() - *start_cap, 0x20000);
                                this.buf.reserve(reserve_len);

                                // Issue a read
                                let data = this.buf.as_mut_ptr() as *mut MaybeUninit<u8>;
                                // SAFETY: the data here is uninitialized, and we are getting exclusive access
                                // to it.
                                let data = unsafe {
                                    core::slice::from_raw_parts_mut(
                                        data.add(this.buf.len()),
                                        this.buf.capacity() - this.buf.len(),
                                    )
                                };

                                this.pos += this.buf.len() - *start_len;

                                stream.send_io(this.pos, data);

                                // SAFETY: self is already pinned
                                unsafe { Pin::new_unchecked(this) }.poll(cx)
                            }
                        }
                    }
                    _ => Poll::Pending,
                }
            }
            ReadToEndFutState::Finished => unreachable!(),
        }
    }
}

pub enum IoReadFut<'a, Io: PacketIo<Write, Param>, Param, T> {
    AllocStream(Param, AllocStreamFut<'a, Io, Write, Param>),
    Read(
        MaybeUninit<T>,
        <AllocStreamFut<'a, Io, Write, Param> as Future>::Output,
    ),
    Finished,
}

impl<'a, Io: PacketIo<Write, Param>, Param, T> Future for IoReadFut<'a, Io, Param, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let f = move || {
            let this = unsafe { self.get_unchecked_mut() };

            loop {
                match this {
                    Self::AllocStream(_, alloc) => {
                        let alloc = unsafe { Pin::new_unchecked(alloc) };

                        if let Poll::Ready(stream) = alloc.poll(cx) {
                            let prev =
                                core::mem::replace(this, Self::Read(MaybeUninit::uninit(), stream));
                            match (prev, &mut *this) {
                                (Self::AllocStream(param, _), Self::Read(data, stream)) => {
                                    //let data = data.get_mut();
                                    let buf = unsafe {
                                        core::slice::from_raw_parts_mut(
                                            data as *mut MaybeUninit<_> as *mut MaybeUninit<u8>,
                                            core::mem::size_of::<T>(),
                                        )
                                    };
                                    stream.send_io(param, buf)
                                }
                                _ => unreachable!(),
                            }
                            // Poll again to force processing of the stream
                            continue;
                        } else {
                            break Poll::Pending;
                        }
                    }
                    Self::Read(_, stream) => match stream.as_mut().poll_next(cx) {
                        Poll::Ready(None) => {
                            let prev = core::mem::replace(this, Self::Finished);

                            match prev {
                                Self::Read(data, _) => {
                                    break Poll::Ready(unsafe {
                                        data /*.into_inner()*/
                                            .assume_init()
                                    });
                                }
                                _ => unreachable!(),
                            }
                        }
                        Poll::Ready(_) => {
                            continue;
                        }
                        _ => break Poll::Pending,
                    },
                    Self::Finished => unreachable!(),
                }
            }
        };
        f()
    }
}
