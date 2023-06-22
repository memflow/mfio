use crate::packet::*;

use crate::backend::IoBackend;
use crate::util::UsizeMath;
use bytemuck::Pod;
use cglue::prelude::v1::*;
use core::future::Future;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;

pub trait IoRead<Pos: 'static>: PacketIo<Write, Pos> {
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
        IoFullFut::NewId(pos, packet.into(), self.new_id())
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
        IoReadFut::NewId(pos, self.new_id())
    }

    fn read_to_end<'a>(&'a self, pos: Pos, buf: &'a mut Vec<u8>) -> ReadToEndFut<'a, Self, Pos> {
        ReadToEndFut {
            pos,
            buf,
            state: ReadToEndFutState::NewId(self.new_id()),
        }
    }
}

impl<Pos: 'static, T> IoRead<Pos> for T where T: PacketIo<Write, Pos> {}

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
        IoFullFut::NewId(pos, packet.into(), self.new_id())
    }

    fn write<'a, T>(&'a self, pos: Pos, data: &'a T) -> IoFullFut<'a, Self, Read, Pos> {
        let buf = unsafe {
            core::slice::from_raw_parts(data as *const T as *const u8, core::mem::size_of::<T>())
        };
        self.write_all(pos, buf)
    }
}

impl<Pos: 'static, T> IoWrite<Pos> for T where T: PacketIo<Read, Pos> {}

pub enum IoFullFut<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param: 'a> {
    NewId(Param, Packet<'a, Perms>, NewIdFut<'a, Io, Perms, Param>),
    Read(
        Option<()>,
        ManuallyDrop<<NewIdFut<'a, Io, Perms, Param> as Future>::Output>,
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
                Self::NewId(_, _, alloc) => {
                    let alloc = unsafe { Pin::new_unchecked(alloc) };

                    if let Poll::Ready(id) = alloc.poll(cx) {
                        let prev =
                            core::mem::replace(this, Self::Read(None, ManuallyDrop::new(id)));
                        match (prev, &mut *this) {
                            (Self::NewId(param, packet, _), Self::Read(_, id)) => {
                                unsafe { Pin::new_unchecked(&**id) }.send_io(param, packet)
                            }
                            _ => unreachable!(),
                        }
                        // Poll again to force processing of the stream
                        continue;
                    } else {
                        break Poll::Pending;
                    }
                }
                Self::Read(err, id) => match unsafe { Pin::new_unchecked(&mut **id) }.poll_next(cx)
                {
                    Poll::Ready(None) => {
                        let err = err.take();
                        unsafe { ManuallyDrop::drop(id) };
                        *this = Self::Finished;
                        break Poll::Ready(err);
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

pub struct ReadToEndFut<'a, Io: PacketIo<Write, Param>, Param> {
    pos: Param,
    buf: &'a mut Vec<u8>,
    state: ReadToEndFutState<'a, Io, Param>,
}

pub enum ReadToEndFutState<'a, Io: PacketIo<Write, Param>, Param: 'a> {
    NewId(NewIdFut<'a, Io, Write, Param>),
    // TODO: change this to a struct
    Read(
        usize,
        usize,
        Option<usize>,
        usize,
        Option<()>,
        ManuallyDrop<<NewIdFut<'a, Io, Write, Param> as Future>::Output>,
    ),
    Finished,
}

impl<'a, Io: PacketIo<Write, Param>, Param: Copy + UsizeMath> Future
    for ReadToEndFut<'a, Io, Param>
{
    type Output = Option<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                ReadToEndFutState::NewId(alloc) => {
                    let alloc = unsafe { Pin::new_unchecked(alloc) };

                    if let Poll::Ready(stream) = alloc.poll(cx) {
                        let start_len = this.buf.len();
                        let start_cap = this.buf.capacity();

                        // Reserve enough for 32 bytes of data initially
                        if start_cap - start_len < 32 {
                            this.buf.reserve(32 - (start_cap - start_len));
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
                        this.state = ReadToEndFutState::Read(
                            start_len,
                            start_cap,
                            None,
                            0,
                            None,
                            ManuallyDrop::new(stream),
                        );

                        match &mut this.state {
                            ReadToEndFutState::Read(_, _, _, _, _, stream) => {
                                unsafe { Pin::new_unchecked(&**stream) }.send_io(this.pos, data);
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        break Poll::Pending;
                    }
                }
                ReadToEndFutState::Read(
                    start_len,
                    start_cap,
                    final_cap,
                    max_cap,
                    error,
                    stream,
                ) => {
                    match unsafe { Pin::new_unchecked(&mut **stream) }.poll_next(cx) {
                        Poll::Ready(Some((pkt, err))) => {
                            // We failed, thus cap the buffer length, complete queued I/O, but do not
                            // perform any further reads.
                            if err.is_some() {
                                let new_end = pkt.start() + (this.buf.len() - *start_len);
                                let end = final_cap.get_or_insert(new_end);
                                *error = err;
                                *end = core::cmp::min(*end, new_end);
                            } else {
                                let new_end = pkt.end() + (this.buf.len() - *start_len);
                                *max_cap = core::cmp::max(*max_cap, new_end);
                            }
                        }
                        Poll::Ready(None) => {
                            // If we got no successful output, cap the capacity to 0
                            // so that we don't end up in a deadlock if the backend does no I/O
                            // processing.
                            if *max_cap == 0 {
                                *final_cap = Some(0);
                            }
                            // If we read all bytes successfully, grow the buffer and keep going.
                            // Otherwise, return finished state.
                            match final_cap {
                                Some(cap) => {
                                    let cap = core::cmp::min(cap, max_cap);
                                    unsafe { ManuallyDrop::drop(stream) };
                                    // SAFETY: these bytes have been successfully read
                                    unsafe { this.buf.set_len(*start_len + *cap) };
                                    this.pos.add_assign(this.buf.len() - *start_len);
                                    if error.is_some() && *cap == 0 {
                                        break Poll::Ready(None);
                                    } else {
                                        break Poll::Ready(Some(*cap));
                                    }
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

                                    unsafe { Pin::new_unchecked(&**stream) }
                                        .send_io(this.pos.add(this.buf.len()), data);
                                }
                            }
                        }
                        _ => break Poll::Pending,
                    }
                }
                ReadToEndFutState::Finished => unreachable!(),
            }
        }
    }
}

pub enum IoReadFut<'a, Io: PacketIo<Write, Param>, Param: 'a, T> {
    NewId(Param, NewIdFut<'a, Io, Write, Param>),
    Read(
        MaybeUninit<T>,
        ManuallyDrop<<NewIdFut<'a, Io, Write, Param> as Future>::Output>,
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
                    Self::NewId(_, alloc) => {
                        let alloc = unsafe { Pin::new_unchecked(alloc) };

                        if let Poll::Ready(stream) = alloc.poll(cx) {
                            let prev = core::mem::replace(
                                this,
                                Self::Read(MaybeUninit::uninit(), ManuallyDrop::new(stream)),
                            );
                            match (prev, &mut *this) {
                                (Self::NewId(param, _), Self::Read(data, stream)) => {
                                    //let data = data.get_mut();
                                    let buf = unsafe {
                                        core::slice::from_raw_parts_mut(
                                            data as *mut MaybeUninit<_> as *mut MaybeUninit<u8>,
                                            core::mem::size_of::<T>(),
                                        )
                                    };
                                    unsafe { Pin::new_unchecked(&**stream) }.send_io(param, buf)
                                }
                                _ => unreachable!(),
                            }
                            // Poll again to force processing of the stream
                            continue;
                        } else {
                            break Poll::Pending;
                        }
                    }
                    Self::Read(_, stream) => {
                        match unsafe { Pin::new_unchecked(&mut **stream) }.poll_next(cx) {
                            Poll::Ready(None) => {
                                unsafe { ManuallyDrop::drop(stream) };
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
                        }
                    }
                    Self::Finished => unreachable!(),
                }
            }
        };
        f()
    }
}

pub mod sync {
    use super::*;

    #[cglue_trait]
    pub trait SyncIoRead<Pos: 'static>: IoRead<Pos> + IoBackend {
        fn read_all<'a>(&'a self, pos: Pos, packet: impl Into<Packet<'a, Write>>) -> Option<()> {
            self.block_on(IoRead::read_all(self, pos, packet))
        }

        fn read_into<'a, T: Pod>(&'a self, pos: Pos, data: &'a mut MaybeUninit<T>) -> Option<()> {
            self.block_on(IoRead::read_into(self, pos, data))
        }

        fn read<T: Pod>(&self, pos: Pos) -> T {
            self.block_on(IoRead::read(self, pos))
        }

        #[skip_func]
        fn read_to_end<'a>(&'a self, pos: Pos, buf: &'a mut Vec<u8>) -> Option<usize>
        where
            ReadToEndFut<'a, Self, Pos>: Future<Output = Option<usize>>,
        {
            self.block_on(IoRead::read_to_end(self, pos, buf))
        }
    }

    #[cglue_trait]
    pub trait SyncIoWrite<Pos: 'static>: IoWrite<Pos> + IoBackend {
        fn write_all<'a>(&'a self, pos: Pos, packet: impl Into<Packet<'a, Read>>) -> Option<()> {
            self.block_on(IoWrite::write_all(self, pos, packet))
        }

        fn write<'a, T>(&'a self, pos: Pos, data: &'a T) -> Option<()> {
            self.block_on(IoWrite::write(self, pos, data))
        }
    }
}
