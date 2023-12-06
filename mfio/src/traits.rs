//! Helper traits

use crate::std_prelude::*;

use crate::io::*;

use crate::backend::{IoBackend, IoBackendExt};
use crate::error::Error;
use crate::util::{CopyPos, UsizeMath};
use bytemuck::Pod;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll};

/// I/O read operations.
pub trait IoRead<Pos: 'static>: PacketIo<Write, Pos> {
    /// Forwards a read request to the I/O object.
    ///
    /// This is equivalent to `PacketIo::io`, but disambiguates packet permissions.
    fn read_raw<'a, T: PacketStore<'a, Write>>(
        &'a self,
        pos: Pos,
        packet: T,
    ) -> IoFut<'a, Self, Write, Pos, T> {
        self.io(pos, packet)
    }

    /// Read all data into the given object.
    fn read_all<'a, T: IntoPacket<'a, Write>>(
        &'a self,
        pos: Pos,
        packet: T,
    ) -> IoFullFut<'a, Self, Write, Pos, T> {
        let (packet, sync) = packet.into_packet();
        IoFullFut {
            fut: self.io(pos, packet),
            sync: Some(sync),
        }
    }

    /// Reads data into a `Pod` struct.
    fn read_into<'a, T: Pod>(
        &'a self,
        pos: Pos,
        data: &'a mut MaybeUninit<T>,
    ) -> IoFullFut<'a, Self, Write, Pos, &'a mut [MaybeUninit<u8>]> {
        let buf = unsafe {
            core::slice::from_raw_parts_mut(
                data as *mut MaybeUninit<T> as *mut MaybeUninit<u8>,
                core::mem::size_of::<T>(),
            )
        };
        self.read_all(pos, buf)
    }

    /// Reads data into a new `Pod` struct.
    fn read<T: Pod>(&self, pos: Pos) -> IoReadFut<Self, Pos, T> {
        let pkt = FullPacket::<_, Write>::new_uninit();
        IoReadFut(self.io(pos, pkt))
    }

    /// Reads data into given buffer until a gap is reached.
    fn read_to_end<'a>(&'a self, pos: Pos, buf: &'a mut Vec<u8>) -> ReadToEndFut<'a, Self, Pos>
    where
        Pos: CopyPos,
    {
        let start_len = buf.len();
        let start_cap = buf.capacity();

        // Reserve enough for 32 bytes of data initially
        if start_cap - start_len < 32 {
            buf.reserve(32 - (start_cap - start_len));
        }

        // Issue a read
        let data = buf.as_mut_ptr() as *mut MaybeUninit<u8>;
        // SAFETY: the data here is uninitialized, and we are getting exclusive access
        // to it.
        let data = unsafe {
            core::slice::from_raw_parts_mut(data.add(start_len), buf.capacity() - start_len)
        };

        let fut = Some(data.into_packet()).map(|(pkt, sync)| (self.io(pos.copy_pos(), pkt), sync));

        ReadToEndFut {
            io: self,
            pos,
            buf,
            fut,
            start_len,
            start_cap,
        }
    }
}

impl<Pos: 'static, T> IoRead<Pos> for T where T: PacketIo<Write, Pos> {}

/// I/O write operations.
pub trait IoWrite<Pos>: PacketIo<Read, Pos> {
    /// Forwards a write request to the I/O object.
    ///
    /// This is equivalent to `PacketIo::io`, but disambiguates packet permissions.
    fn write_raw<'a, T: PacketStore<'a, Read>>(
        &'a self,
        pos: Pos,
        packet: T,
    ) -> IoFut<'a, Self, Read, Pos, T> {
        self.io(pos, packet)
    }

    /// Writes all data in the given packet to destination.
    fn write_all<'a, T: IntoPacket<'a, Read>>(
        &'a self,
        pos: Pos,
        packet: T,
    ) -> IoFullFut<'a, Self, Read, Pos, T> {
        let (packet, sync) = packet.into_packet();
        IoFullFut {
            fut: self.io(pos, packet),
            sync: Some(sync),
        }
    }

    /// Writes a pod object into to destination.
    fn write<'a, T>(&'a self, pos: Pos, data: &'a T) -> IoFullFut<'a, Self, Read, Pos, &'a [u8]> {
        let buf = unsafe {
            core::slice::from_raw_parts(data as *const T as *const u8, core::mem::size_of::<T>())
        };
        self.write_all(pos, buf)
    }
}

impl<Pos: 'static, T> IoWrite<Pos> for T where T: PacketIo<Read, Pos> {}

pub struct IoFullFut<
    'a,
    Io: PacketIo<Perms, Param>,
    Perms: PacketPerms,
    Param: 'a,
    Obj: IntoPacket<'a, Perms>,
> {
    fut: IoFut<'a, Io, Perms, Param, Obj::Target>,
    sync: Option<Obj::SyncHandle>,
}

impl<'a, Io: PacketIo<Perms, Param>, Perms: PacketPerms, Param, Obj: IntoPacket<'a, Perms>> Future
    for IoFullFut<'a, Io, Perms, Param, Obj>
{
    type Output = Result<<Obj::Target as OpaqueStore>::StackReq<'a>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let fut = unsafe { Pin::new_unchecked(&mut this.fut) };

        fut.poll(cx).map(|pkt| {
            let hdr = <<Obj as IntoPacket<'a, Perms>>::Target as OpaqueStore>::stack_hdr(&pkt);
            // TODO: put this after error checking
            Obj::sync_back(hdr, this.sync.take().unwrap());
            hdr.err_on_zero().map(|_| pkt)
        })
    }
}

type UninitSlice<'a> = &'a mut [MaybeUninit<u8>];

#[allow(clippy::type_complexity)]
pub struct ReadToEndFut<'a, Io: PacketIo<Write, Param>, Param> {
    io: &'a Io,
    pos: Param,
    buf: &'a mut Vec<u8>,
    fut: Option<(
        IoFut<'a, Io, Write, Param, <UninitSlice<'a> as IntoPacket<'a, Write>>::Target>,
        <UninitSlice<'a> as IntoPacket<'a, Write>>::SyncHandle,
    )>,
    start_len: usize,
    start_cap: usize,
}

impl<'a, Io: PacketIo<Write, Param>, Param: CopyPos + UsizeMath> Future
    for ReadToEndFut<'a, Io, Param>
{
    type Output = Result<usize, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let (fut, _) = this.fut.as_mut().expect("Poll called in invalid state");
            let fut = unsafe { Pin::new_unchecked(fut) };

            match fut.poll(cx) {
                Poll::Ready(pkt) => {
                    // TODO: check into safety of this. We are technically unpinning a previously
                    // pinned object.
                    let (_, sync) = this.fut.take().unwrap();

                    let hdr = <<UninitSlice<'a> as IntoPacket<'a, Write>>::Target as OpaqueStore>::stack_hdr(&pkt);
                    let len = Write::len(hdr);
                    let clamp = hdr.error_clamp();

                    <UninitSlice<'a> as IntoPacket<'a, Write>>::sync_back(hdr, sync);
                    // SAFETY: all these bytes have been successfully read
                    unsafe {
                        this.buf
                            .set_len(this.buf.len() + core::cmp::min(clamp, len) as usize)
                    };

                    // We reached the end
                    if clamp < len || clamp == 0 {
                        let total_len = this.buf.len() - this.start_len;
                        // TODO: figure out how to extract error on 0 read
                        break Poll::Ready(Ok(total_len));
                    } else {
                        // Double read size, but cap it to 2MB
                        let reserve_len =
                            core::cmp::min(this.buf.capacity() - this.start_cap, 0x20000);
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

                        this.fut = Some(data.into_packet()).map(|(pkt, sync)| {
                            (
                                this.io.io(
                                    this.pos.copy_pos().add(this.buf.len() - this.start_len),
                                    pkt,
                                ),
                                sync,
                            )
                        });
                    }
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

pub struct IoReadFut<'a, Io: PacketIo<Write, Param>, Param: 'a, T: 'static>(
    IoFut<'a, Io, Write, Param, FullPacket<MaybeUninit<T>, Write>>,
);

impl<'a, Io: PacketIo<Write, Param>, Param, T: 'a> Future for IoReadFut<'a, Io, Param, T> {
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let fut = unsafe { Pin::new_unchecked(&mut this.0) };

        fut.poll(cx).map(|pkt| {
            pkt.err_on_zero()
                .map(|_| unsafe { core::ptr::read(pkt.simple_data_ptr().cast::<T>()) })
        })
    }
}

pub mod sync {
    //! Synchronous I/O wrappers
    use super::*;

    // TODO: figure out how to expose these over cglue

    /// Synchronous I/O read operations.
    ///
    /// This trait simply wraps `PacketIo + IoBackend` types in order to not subject the user to
    /// async code.
    pub trait SyncIoRead<Pos: 'static>: IoRead<Pos> + IoBackend {
        fn read_all<'a>(
            &'a self,
            pos: Pos,
            packet: impl IntoPacket<'a, Write>,
        ) -> Result<(), Error> {
            self.block_on(IoRead::read_all(self, pos, packet))
                .map(|_| ())
        }

        fn read_into<'a, T: Pod>(
            &'a self,
            pos: Pos,
            data: &'a mut MaybeUninit<T>,
        ) -> Result<(), Error> {
            self.block_on(IoRead::read_into(self, pos, data))
                .map(|_| ())
        }

        fn read<T: Pod>(&self, pos: Pos) -> Result<T, Error> {
            self.block_on(IoRead::read(self, pos))
        }

        fn read_to_end<'a>(&'a self, pos: Pos, buf: &'a mut Vec<u8>) -> Option<usize>
        where
            ReadToEndFut<'a, Self, Pos>: Future<Output = Option<usize>>,
            Pos: CopyPos,
        {
            self.block_on(IoRead::read_to_end(self, pos, buf))
        }
    }

    /// Synchronous I/O write operations.
    ///
    /// This trait simply wraps `PacketIo + IoBackend` types in order to not subject the user to
    /// async code.
    pub trait SyncIoWrite<Pos: 'static>: IoWrite<Pos> + IoBackend {
        fn write_all<'a>(
            &'a self,
            pos: Pos,
            packet: impl IntoPacket<'a, Read>,
        ) -> Result<(), Error> {
            self.block_on(IoWrite::write_all(self, pos, packet))
                .map(|_| ())
        }

        fn write<'a, T>(&'a self, pos: Pos, data: &'a T) -> Result<(), Error> {
            self.block_on(IoWrite::write(self, pos, data)).map(|_| ())
        }
    }
}
