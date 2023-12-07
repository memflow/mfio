//! Desribes abstract I/O operations.
//!
//! # Introduction
//!
//! I/O is performed on packets - fungible units of operation. A packet is paired with an address
//! and then is transferred throughout the I/O chain, until it reaches the final I/O backend,
//! completing the operation. Along the way, the packet may be split into smaller ones, parts of it
//! may be rejected, while other parts may get forwarded, with potentially diverging addresses. In
//! the end, all parts of the packet are collected back into one place.
//!
//! A packet represents an abstract source or destination for I/O operations. It is always
//! parameterized with [`PacketPerms`], describing how the underlying data of the packet can be
//! accessed. Accessing the data means that the packet will no longer be forwarded, but before that
//! point, the packet may be split up into smaller chunks, and sent to different I/O subsystems.
//!
//! # Lifecycle
//!
//! Most packet interactions can be traced back to [`PacketIo`] - a trait enabling the user to send
//! packets to the I/O system. [`PacketIo::send_io`] is used to pass a bound packet to the
//! front-facing I/O backend. It is the entrypoint for packets. Upon completion of each segment,
//! output function is notified (if it has been assigned), and once all segments have completed
//! operation, the original packet's waker is signaled. Complete flow is as follows:
//!
//! 1. A packet is bound to a stack or heap location through [`PacketStore`] trait.
//!
//! 2. Caller stores reference to the packet's header locally.
//!
//! 3. Bound packet is passed to [`PacketIo::send_io`].
//!
//! 4. The I/O backend processes the packet.
//!
//! 5. Result is fed back to [`OutputRef`], if it has been assigned during binding process.
//!
//! 6. Caller may choose to process returning packets as they come, based on the epecific
//!    `OutputRef` being attached, or await for total completion by calling
//!    [`Future::poll`] on the [`Packet`] reference.
//!
//! Steps 1-3 may be abstracted using [`PacketIoExt`] trait. Entire flow may be abstracted using
//! [`IoRead`](crate::traits::IoRead), and [`IoWrite`](crate::traits::IoWrite) traits. However, if
//! custom packet permissions are needed, the standard traits may not be sufficient.
//!
//! # Copy constraint negotiation
//!
//! I/O systems have various constraints on kinds of I/O operations possible. Some systems work by
//! exposing a publicly accessible byte buffer, while in other systems those buffers are opaque and
//! hidden behind hardware mechanisms or OS APIs. The simplest way to tackle varying requirements
//! is to allocate intermediary buffers on the endpoints and expose those for I/O. However, that
//! can be highly inefficient, because multiple copies may be involved, before data ends up at the
//! final destination. Ideal scenario, for any I/O system, is to have only one copy per operation.
//! And in I/O system where data is generated on-the-fly, ideal scenario would be to write output
//! directly to the destination.
//!
//! To achieve this in mfio, we attach constraints to various parts of the I/O chain, and allocate
//! temporary buffers only when needed. For any I/O end, we have the following constraint options:
//!
//! 1. Publicly exposed aligned byte-addressable buffer - this is the lower constraint tier, as
//!    individual bytes can be modified at neglibible cost.
//! 2. Accepts byte-addressable input - this is more constarined, because the caller must provide a
//!    byte buffer, and cannot generate data on the fly. The callee takes this buffer and processes
//!    it internally using opaque mechanisms.
//!
//! I/O has 2 ends - input and output. These constraint levels are similar on both ends. See how
//! these levels are described in the context of input (caller):
//!
//! 1. Sends byte-addressable buffer - this is the lower constraint tier, because the callee can
//!    process the input in any way possible.
//! 2. Fills a byte-addressable buffer - this is more constrained, because the callee needs to
//!    provide a buffer to write to. However, this may also mean that the caller generates data on
//!    the fly, thus memory usage is lower.
//!
//! This is not exhaustive, but generally sufficient for most I/O cases. In practice, a backend
//! that is able to access byte-addressable buffer directly will simply provide it to the packet,
//! which will then process it. If the backend instead needs a buffer from the packet, it will
//! call [`BoundPacketView::try_alloc`] with desired alignment parameters. If the allocation is not
//! successful, it will then fall back to allocating an intermediary buffer.

use crate::error::Error;
#[cfg(feature = "cglue-trait")]
use cglue::prelude::v1::*;
use core::cell::UnsafeCell;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

mod packet;
pub use packet::*;
mod opaque;
pub use opaque::*;

/// The primary trait enabling I/O.
///
/// This trait is generic in as many aspects as possible. The typical input is a parameter,
/// typically representing a location, while view contains a slice of a packet. These 2 are
/// disjoint, because the parameter may change on multiple hops of I/O chain, without data on the
/// view changing.
///
/// `Perms` represents access permissions the packets sent through this trait have. This typically
/// means reversed meaning when it comes to desired I/O operation. For instance, `Write` packets
/// allow read operations to be performced, because data is read into the packet. Meanwhile, `Read`
/// permission implies the packet holds the data, and that data can be transferred to the internal
/// data store of the I/O backend.
///
/// You may want to check [`PacketIoExt`] trait and the [`traits`](crate::traits) module for easier
/// to use abstractions.
///
/// # Example
///
/// Request-response handler:
///
/// ```
/// use mfio::io::*;
/// use mfio::mferr;
/// use mfio::traits::*;
///
/// enum Request {
///     Hi,
///     Ho,
/// }
///
/// impl Request {
///     fn response(self) -> &'static [u8] {
///         match self {
///             Self::Hi => b"Hi",
///             Self::Ho => b"Hoooooo",
///         }
///     }
/// }
///
/// struct Backend;
///
/// impl PacketIo<Write, Request> for Backend {
///     fn send_io(&self, param: Request, view: BoundPacketView<Write>) {
///         let resp = param.response();
///
///         let view = if view.len() > resp.len() as u64 {
///             let (a, b) = view.split_at(resp.len() as u64);
///             b.error(mferr!(Memory, Outside, Backend));
///             a
///         } else {
///             view
///         };
///
///         // SAFETY: we have verified the packet view is at most the size of the response.
///         unsafe {
///             let _ = view.transfer_data(resp.as_ptr().cast());
///         }
///     }
/// }
///
/// # pollster::block_on(async {
/// let backend = Backend;
///
/// let mut buf = [0u8; 64];
/// let _ = backend.read_all(Request::Ho, &mut buf[..]).await;
/// assert_eq!(&buf[..7], b"Hoooooo");
/// let _ = backend.read_all(Request::Hi, &mut buf[..]).await;
/// assert_eq!(&buf[..2], b"Hi");
/// assert_eq!(&buf[..10], b"Hiooooo\0\0\0");
/// # });
/// ```
#[cfg_attr(feature = "cglue-trait", cglue_trait)]
pub trait PacketIo<Perms: PacketPerms, Param>: Sized {
    /// Send I/O request to the backend.
    ///
    /// This is a low level function for sending I/O to the backend. Typically, you'd use
    /// [`PacketIoExt`], and [`StreamIoExt`] traits as second level abstractions. Third level
    /// abstractions include [`IoRead`](crate::traits::IoRead), [`IoWrite`](crate::traits::IoWrite),
    /// [`AsyncRead`](crate::stdeq::AsyncRead), and [`AsyncWrite`](crate::stdeq::AsyncWrite). Use
    /// of these traits is highly encouraged.
    ///
    /// TODO: make this a sink (<https://github.com/memflow/mfio/issues/3>)
    ///
    /// # Example
    ///
    /// Manually sending I/O and awaiting it:
    ///
    /// ```
    /// # mod sample {
    /// #     include!("../sample.rs");
    /// # }
    /// # use sample::SampleIo;
    /// use mfio::backend::*;
    /// use mfio::io::*;
    /// use mfio::mferr;
    ///
    /// let handle = SampleIo::new(vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]);
    ///
    /// handle.block_on(async {
    ///     // Create a "simple" packet on the stack. Note that use of this must be careful - such
    ///     // usage can be considered to fall within the description of `mfio_assume_linear_types`
    ///     let packet = FullPacket::<_, Write>::new([0u8; 5]);
    ///
    ///     let view = PacketView::from_ref(&packet, 0);
    ///     // SAFETY: through block_on, we ensure the packet will be waited until completion, and
    ///     // not be moved from the original location.
    ///     let view = unsafe { view.bind(None) };
    ///
    ///     handle.send_io(3, view);
    ///
    ///     // Packet is awaitable, and blocks until there are no more in-flight segments.
    ///     let _ = (&*packet).await;
    ///
    ///     assert_eq!(packet.simple_contiguous_slice().unwrap(), &[2, 3, 5, 8, 13]);
    /// })
    /// ```
    fn send_io(&self, param: Param, view: BoundPacketView<Perms>);
}

/// I/O helpers.
///
/// Use these helpers to simplify the usage of [`PacketIo`].
pub trait PacketIoExt<Perms: PacketPerms, Param>: PacketIo<Perms, Param> {
    fn io<'a, T: PacketStore<'a, Perms>>(
        &'a self,
        param: Param,
        packet: T,
    ) -> IoFut<'a, Self, Perms, Param, T> {
        IoFut {
            pkt: UnsafeCell::new(Some(packet.stack())),
            initial_state: UnsafeCell::new(Some((self, param))),
            _phantom: PhantomData,
        }
    }

    fn io_to<'a, T: PacketStore<'a, Perms>, O: OutputStore<'a, Perms>>(
        &'a self,
        param: Param,
        packet: T,
        output: O,
    ) -> IoToFut<'a, Self, Perms, Param, T, O> {
        IoToFut {
            pkt_out: UnsafeCell::new(Some((packet.stack(), output.stack()))),
            initial_state: UnsafeCell::new(Some((self, param))),
            _phantom: PhantomData,
        }
    }

    fn io_to_stream<'a, T: PacketStore<'a, Perms> + 'a, O: PushPop<Output<'a, Perms>> + 'a>(
        &'a self,
        param: Param,
        packet: T,
        container: O,
    ) -> IoToFut<'a, Self, Perms, Param, T, PacketStream<O, Perms>> {
        self.io_to(param, packet, PacketStream::new(container))
    }

    fn io_to_fn<
        'a,
        T: PacketStore<'a, Perms>,
        F: Fn(PacketView<'a, Perms>, Option<Error>) + Send + Sync + 'a,
    >(
        &'a self,
        param: Param,
        packet: T,
        func: F,
    ) -> IoToFut<'a, Self, Perms, Param, T, OutputFunction<F, Perms>> {
        self.io_to(param, packet, OutputFunction::new(func))
    }
}

impl<T: PacketIo<Perms, Param>, Perms: PacketPerms, Param> PacketIoExt<Perms, Param> for T {}

/// Helpers for Stream I/O.
///
/// This is mainly meant for cases where I/O does not have a position parameter, such as TCP
/// streams.
pub trait StreamIoExt<Perms: PacketPerms>: PacketIo<Perms, NoPos> {
    fn stream_io<'a, T: PacketStore<'a, Perms>>(
        &'a self,
        packet: T,
    ) -> IoFut<'a, Self, Perms, NoPos, T> {
        self.io(NoPos::new(), packet)
    }

    fn stream_io_to<'a, T: PacketStore<'a, Perms>, O: OutputStore<'a, Perms>>(
        &'a self,
        packet: T,
        output: O,
    ) -> IoToFut<'a, Self, Perms, NoPos, T, O> {
        self.io_to(NoPos::new(), packet, output)
    }
}

impl<T: PacketIo<Perms, NoPos>, Perms: PacketPerms> StreamIoExt<Perms> for T {}

/// Describes lack of position.
///
/// This type is used in streams to signify that I/O is sequential. The convention is that I/O is
/// processed on first-come, first-served basis
#[repr(transparent)]
#[derive(Clone)]
pub struct NoPos(core::marker::PhantomData<()>);

impl NoPos {
    pub const fn new() -> Self {
        Self(core::marker::PhantomData)
    }
}

/// The simplest I/O future.
///
/// This future will drive an operation on the packet to completion, and then return `Poll::Ready`.
///
/// To perform more complex actions on partial results, please look at [`IoToFut`].
pub struct IoFut<'a, T: ?Sized, Perms: PacketPerms, Param, Packet: PacketStore<'a, Perms>> {
    pkt: UnsafeCell<Option<Packet::StackReq<'a>>>,
    initial_state: UnsafeCell<Option<(&'a T, Param)>>,
    _phantom: PhantomData<Perms>,
}

impl<
        'a,
        T: PacketIo<Perms, Param> + ?Sized,
        Perms: PacketPerms,
        Param,
        Pkt: PacketStore<'a, Perms>,
    > Future for IoFut<'a, T, Perms, Param, Pkt>
{
    type Output = Pkt::StackReq<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let state = self.into_ref().get_ref();

        loop {
            match unsafe { (*state.initial_state.get()).take() } {
                Some((io, param)) => {
                    // SAFETY: this packet's existence is tied to 'a lifetime, meaning it will be valid
                    // throughout 'a.
                    let pkt: &'a Pkt::StackReq<'a> =
                        unsafe { (*state.pkt.get()).as_ref().unwrap() };

                    let view: PacketView<'a, Perms> = Pkt::stack_opaque(pkt);

                    // SAFETY: PacketView's lifetime is a marker, and we are using the marker lifetime to guide
                    // assumptions about type's validity. A sound implementation would put a 'static object
                    // here regardless, making the object 'static, while non-'static implementations are out of
                    // our hand, therefore we assume the caller is giving us correct info.
                    let bound = unsafe { view.bind(None) };
                    io.send_io(param, bound)
                }
                None => {
                    let pkt: &'a Pkt::StackReq<'a> =
                        unsafe { (*state.pkt.get()).as_ref().unwrap() };

                    let mut pkt: &'a Packet<Perms> = Pkt::stack_hdr(pkt);
                    let pkt = Pin::new(&mut pkt);
                    break pkt
                        .poll(cx)
                        .map(|_| unsafe { (*state.pkt.get()).take().unwrap() });
                }
            }
        }
    }
}

/// I/O future with custom actions per returned packet segment.
///
/// This future allows customizing behavior upon each completed packet segment. This may include
/// logging, storing segments in a collection, or processing them in a stream. Please see
/// appropriate output modules for more details.
pub struct IoToFut<
    'a,
    T: ?Sized,
    Perms: PacketPerms,
    Param,
    Packet: PacketStore<'a, Perms>,
    Output: OutputStore<'a, Perms>,
> {
    pkt_out: UnsafeCell<Option<(Packet::StackReq<'a>, Output::StackReq<'a>)>>,
    initial_state: UnsafeCell<Option<(&'a T, Param)>>,
    _phantom: PhantomData<Perms>,
}

impl<
        'a,
        T: PacketIo<Perms, Param> + ?Sized,
        Perms: PacketPerms,
        Param,
        Pkt: PacketStore<'a, Perms>,
        Out: OutputStore<'a, Perms>,
    > IoToFut<'a, T, Perms, Param, Pkt, Out>
{
    pub fn submit(self: Pin<&mut Self>) -> &Out::StackReq<'a> {
        let this = self.into_ref();
        if let Some((io, param)) = unsafe { (*this.initial_state.get()).take() } {
            // SAFETY: this packet's existence is tied to 'a lifetime, meaning it will be valid
            // throughout 'a.
            let (pkt, out): &'a mut (Pkt::StackReq<'a>, Out::StackReq<'a>) =
                unsafe { (*this.pkt_out.get()).as_mut().unwrap() };
            let view: PacketView<'a, Perms> = Pkt::stack_opaque(pkt);
            // SAFETY: PacketView's lifetime is a marker, and we are using the marker lifetime to guide
            // assumptions about type's validity. A sound implementation would put a 'static object
            // here regardless, making the object 'static, while non-'static implementations are out of
            // our hand, therefore we assume the caller is giving us correct info.
            let bound = unsafe { view.bind(Some(Out::stack_opaque(out))) };
            io.send_io(param, bound)
        }

        unsafe { (*this.pkt_out.get()).as_ref().map(|(_, out)| out).unwrap() }
    }
}

impl<
        'a,
        T: PacketIo<Perms, Param>,
        Perms: PacketPerms,
        Param,
        Pkt: PacketStore<'a, Perms>,
        Out: OutputStore<'a, Perms>,
    > Future for IoToFut<'a, T, Perms, Param, Pkt, Out>
{
    type Output = (Pkt::StackReq<'a>, Out::StackReq<'a>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.as_mut().submit();

        let state = self.into_ref();

        let pkt: &'a Pkt::StackReq<'a> = unsafe { &(*state.pkt_out.get()).as_ref().unwrap().0 };
        let mut pkt: &'a Packet<Perms> = Pkt::stack_hdr(pkt);
        let pkt = Pin::new(&mut pkt);
        pkt.poll(cx)
            .map(|_| unsafe { (*state.pkt_out.get()).take().unwrap() })
    }
}
