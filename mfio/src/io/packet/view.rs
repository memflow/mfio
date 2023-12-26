use super::{
    Errorable, MaybeAlloced, NumBounds, OutputRef, Packet, PacketPerms, Splittable,
    TransferredPacket,
};
use crate::error::Error;
use cglue::prelude::v1::*;
use core::marker::PhantomData;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr::NonNull;
use core::sync::atomic::*;
use num::{NumCast, ToPrimitive};
use tarc::BaseArc;

/// Bound Packet View.
///
/// Views may be bound to an output reference before being sent to the I/O backend. If the packet
/// is bound to such output ref, then, upon completion of processing, every resulting packet view
/// is passed to the given output ref.
#[repr(C)]
pub struct BoundPacketView<Perms: PacketPerms> {
    pub(crate) view: ManuallyDrop<PacketView<'static, Perms>>,
    pub(crate) has_output: bool,
    pub(crate) output: MaybeUninit<OutputRef<'static, Perms>>,
}

impl<Perms: PacketPerms> Drop for BoundPacketView<Perms> {
    fn drop(&mut self) {
        // FIXME: output failure by default.
        // SAFETY: we are calling this from Drop, therefore it is fine.
        unsafe { self.output(None) }
    }
}

impl<T: PacketPerms> Splittable for BoundPacketView<T> {
    type Bounds = T::Bounds;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        let mut this = ManuallyDrop::new(self);
        let view = unsafe { ManuallyDrop::take(&mut this.view) };
        let (v1, v2) = view.split_local(NumCast::from(len).expect("Input bound out of range"));

        //this.id.size.fetch_add(1, Ordering::Release);

        let output = if this.has_output {
            unsafe {
                let output = this.output.assume_init_mut();
                output.bound_views.fetch_add(1, Ordering::Release);
                MaybeUninit::new(output.clone())
            }
        } else {
            MaybeUninit::uninit()
        };

        (
            Self {
                view: ManuallyDrop::new(v1),
                has_output: this.has_output,
                output,
            },
            Self {
                view: ManuallyDrop::new(v2),
                has_output: this.has_output,
                output: unsafe { core::ptr::read(&this.output) },
            },
        )
    }

    fn len(&self) -> T::Bounds {
        self.view.len()
    }
}

impl<T: PacketPerms> Errorable for BoundPacketView<T> {
    fn error(self, err: Error) {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: this function is safe, because we are consuming self and making sure `drop` is
        // not invoked.
        unsafe {
            this.output(Some(err));
        }
        // Manually drop all the fields, without invoking our actual drop implementation.
        // Note that `buffer` is being taken out by the `output` function.
        // SAFETY: this function is consuming `self`, thus the data will not be touched again.
        unsafe {
            core::ptr::drop_in_place(&mut this.output);
        }
    }
}

impl<Perms: PacketPerms> BoundPacketView<Perms> {
    /// Forget the packet
    ///
    /// # Safety
    ///
    /// This packet must have originally been the receiver of [`BoundPacketView::extract_packet`]
    /// call. There must be one extracted packet split left.
    pub unsafe fn forget(mut self) {
        let pkt = ManuallyDrop::take(&mut self.view);

        let waker = pkt.pkt().on_output(None);

        debug_assert!(waker.is_none());

        if let Some(waker) = waker {
            waker.wake();
        }

        core::mem::forget(self)
    }

    /// Extract a sub-packet from this packet
    ///
    /// # Safety
    ///
    /// To avoid undefined behavior, the caller must ensure the following conditions are met:
    ///
    /// 1. Extracted packets do not overlap each other.
    ///
    /// 2. Entire range of `self` must be extracted.
    ///
    /// 3. Before the last extracted packet is dropped, `self` must be released with
    ///    [`BoundPacketView::forget`].
    pub unsafe fn extract_packet(&self, pos: Perms::Bounds, len: Perms::Bounds) -> Self {
        let b = self.view.extract_packet(pos, len);

        Self {
            view: ManuallyDrop::new(b),
            has_output: self.has_output,
            output: unsafe { core::ptr::read(&self.output) },
        }
    }

    /// Output the packet view.
    ///
    /// # Safety
    ///
    /// This function may only be invoked once, and only upon the object being consumed. After
    /// calling this function, the object must be explicitly forgotten using `mem::forget`.
    unsafe fn output(&mut self, error: Option<Error>) {
        let pkt = ManuallyDrop::take(&mut self.view);
        let error = error.map(IntError::into_int_err);

        let waker = pkt.pkt().on_output(error.map(|v| (self.view.start, v)));

        if self.has_output {
            let output = self.output.assume_init_read();
            (output.vtbl.output)(output, pkt, error);
        }

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Tries allocating data for the packet view with 1 byte alignment.
    pub fn try_alloc(self) -> MaybeAlloced<Perms> {
        Perms::try_alloc(self, 1)
    }

    /// Get an unbound `Packet`.
    ///
    /// This function allows the packet to be rebound to a different I/O backend and have their
    /// results intercepted.
    ///
    /// # Safety
    ///
    /// Please see [`PacketView::extract_packet`] documentation for details.
    pub unsafe fn unbound(&self) -> PacketView<'static, Perms> {
        self.view
            .extract_packet(Default::default(), self.view.len())
    }

    /// Transfers data between the packet and the `input`.
    ///
    /// If packet has [`Read`](super::Read) permissions, `input` will be written to. If packet has
    /// [`Write`](super::Write) permissions, `input` will be read.
    ///
    /// If packet has [`ReadWrite`](super::ReadWrite) permissions, behavior is not fully specified,
    /// but currently buffers perform memory swap operation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `input` pointer is sufficient for the bounds of the packet. In
    /// addition, the data pointed by `input` must be properly initialized and byte-addressible.
    pub unsafe fn transfer_data(
        mut self,
        input: Perms::ReverseDataType,
    ) -> TransferredPacket<Perms> {
        if let Some(vtable) = self.view.pkt().vtbl.vtbl() {
            (vtable.transfer_data_fn())(&mut self.view, input);
        } else {
            Perms::transfer_data_simple(&mut self.view, input);
        }
        TransferredPacket(self)
    }

    pub fn ptr(&self) -> *const u8 {
        if self.view.pkt().vtbl.vtbl().is_some() {
            core::ptr::null()
        } else {
            unsafe {
                self.view
                    .pkt()
                    .simple_data_ptr()
                    .add(self.view.start.to_usize().unwrap())
            }
        }
    }
}

/// Describes a view to a given packet.
///
/// A bound packet view is sent to an I/O backend, where it can be split into multiple, strictly
/// non-overlapping views. This structure contains necessary data for describing the bounds of each
/// sub-view.
#[repr(C)]
pub struct PacketView<'a, Perms: PacketPerms> {
    pub(crate) pkt: NonNull<Packet<Perms>>,
    /// Right-most bit indicates whether packet is an Arc or a ref. The rest is user-defined.
    pub(crate) tag: u64,
    pub(crate) start: Perms::Bounds,
    pub(crate) end: Perms::Bounds,
    phantom: PhantomData<&'a Packet<Perms>>,
}

unsafe impl<Perms: PacketPerms> Send for PacketView<'_, Perms> {}
unsafe impl<Perms: PacketPerms> Sync for PacketView<'_, Perms> {}

impl<Perms: PacketPerms> Drop for PacketView<'_, Perms> {
    fn drop(&mut self) {
        if self.tag & 1 != 0 {
            unsafe {
                BaseArc::decrement_strong_count(self.pkt.as_ptr());
            }
        }
    }
}

impl<'a, Perms: PacketPerms> PacketView<'a, Perms> {
    /// Create `PacketView` from packet arc.
    ///
    /// # Panics
    ///
    /// This function panics if the tag value requires more than 63 bits to fit, or when input
    /// packet has existing unprocessed views.
    pub fn from_arc(pkt: BaseArc<Packet<Perms>>, tag: u64) -> Self {
        Self::from_arc_ref(&pkt, tag)
    }

    /// Create `PacketView` from packet arc.
    ///
    /// # Panics
    ///
    /// This function panics if the tag value requires more than 63 bits to fit, or when input
    /// packet reference has existing unprocessed views.
    pub fn from_arc_ref(pkt: &BaseArc<Packet<Perms>>, tag: u64) -> Self {
        assert!(tag.leading_zeros() > 0);
        let end = Perms::len(pkt);

        // SAFETY: we are modifying atomic value on shared reference, and then asserting that we
        // indeed have no shared views to the packet.
        unsafe { pkt.on_add_to_view() };

        let pkt = NonNull::new(pkt.as_ptr().cast_mut()).unwrap();

        // SAFETY: we are using a pointer source from an arc, and we are correctly decrementing the
        // strong reference count upon drop.
        unsafe { BaseArc::increment_strong_count(pkt.as_ptr()) };
        Self {
            pkt,
            tag: tag << 1 | 1,
            start: Default::default(),
            end,
            phantom: PhantomData,
        }
    }

    /// Binds the packet to a given output, and returns a bound view.
    ///
    /// # Safety
    ///
    /// Passing `'static` `self`, and `'static` `output` to this function is safe.
    ///
    /// This function accepts an [`OutputRef`](crate::io::packet::output::OutputRef) of any
    /// lifetime `'a`. This is to allow for the possibility of binding stack allocated output
    /// structure to a stack allocated packet. However, binding such way comes with its set of
    /// challenges, most notably, having to manually ensure that the returned [`BoundPacketView`]
    /// does not outlive the provided lifetime.
    ///
    /// Usually, implementations of [`OpaqueStore`](super::OpaqueStore) govern how this is to be
    /// handled, and I/O abstractions generally guarantee the required invariants. However, in
    /// async contexts, the task issuing async operations may be cancelled, breaking the invariant.
    /// Thus, `OpaqueStore` implementations generally default to heap allocating both heap and
    /// stack preferences, and only opting for stack variables if
    /// `#[cfg(mfio_assume_linear_types)]` config switch is enabled.
    pub unsafe fn bind(self, output: Option<OutputRef<'a, Perms>>) -> BoundPacketView<Perms> {
        if let Some(output) = &output {
            output.bound_views.fetch_add(1, Ordering::Release);
        }

        let (has_output, output) = if let Some(output) = output {
            (true, MaybeUninit::new(core::mem::transmute(output)))
        } else {
            (false, MaybeUninit::uninit())
        };

        BoundPacketView {
            view: ManuallyDrop::new(core::mem::transmute(self)),
            has_output,
            output,
        }
    }
}

impl<'a, Perms: PacketPerms> PacketView<'a, Perms> {
    /// Create `PacketView` from packet reference.
    ///
    /// # Panics
    ///
    /// This function panics if the tag value requires more than 63 bits to fit, or whenever this
    /// packet already has existing views.
    ///
    /// # Safety
    ///
    /// TODO: this section should be moved elsewhere
    ///
    /// Caller must ensure that the returned view does not outlive the provided packet. This
    /// function is meant to be used as an optimization to avoid heap allocations, but it may only
    /// be used under assumption of type linearity. However, in practice, async tasks may get
    /// cancelled leading to undefined behavior. Since this function is useless without linear type
    /// assumption, it is only made available when `unsound_assume_linear_types` flag is enabled.
    pub fn from_ref(pkt: &'a Packet<Perms>, tag: u64) -> Self {
        assert!(tag.leading_zeros() > 0);
        let end = Perms::len(pkt);

        // SAFETY: we are modifying atomic value on shared reference, and then asserting that we
        // indeed have no shared views to the packet.
        unsafe { (*pkt).on_add_to_view() };

        Self {
            pkt: NonNull::new((pkt as *const Packet<Perms>).cast_mut()).unwrap(),
            tag: tag << 1,
            start: Default::default(),
            end,
            phantom: PhantomData,
        }
    }

    /// Gets a reference to the underlying [`Packet`](crate::io::Packet) header.
    pub fn pkt(&self) -> &Packet<Perms> {
        unsafe { &*self.pkt.as_ptr().cast_const() }
    }

    /// Gets a mutable reference to the underlying [`Packet`](crate::io::Packet) header.
    pub fn pkt_mut(&mut self) -> &mut Packet<Perms> {
        unsafe { &mut *self.pkt.as_ptr() }
    }

    /// Gets current tag value of the packet.
    pub fn tag(&self) -> u64 {
        self.tag >> 1
    }

    /// Sets a new tag value of the packet.
    ///
    /// # Panics
    ///
    /// This function will panic if the tag uses more than the first 63 bits of data.
    pub fn set_tag(&mut self, tag: u64) {
        assert!(tag.leading_zeros() > 0);
        self.tag = (tag << 1) | (self.tag & 1);
    }

    /// Returns the length this packet view covers.
    pub fn len(&self) -> Perms::Bounds {
        self.end - self.start
    }

    /// Returns the starting offset within the packet that this view represents.
    pub fn start(&self) -> Perms::Bounds {
        self.start
    }

    /// Returns the ending position (+1) within the packet that this view represents.
    pub fn end(&self) -> Perms::Bounds {
        self.end
    }

    /// Returns true if packet length is 0.
    pub fn is_empty(&self) -> bool {
        self.len() == Default::default()
    }

    /// Split the packet view into 2 at given offset.
    pub fn split_local(self, pos: Perms::Bounds) -> (Self, Self) {
        assert!(pos < self.len());

        // TODO: maybe relaxed is enough here?
        self.pkt().rc_and_waker.inc_rc();

        let Self {
            pkt,
            tag,
            start,
            end,
            phantom,
        } = self;

        if tag & 1 != 0 {
            unsafe {
                BaseArc::increment_strong_count(pkt.as_ptr());
            }
        }

        let ret = (
            Self {
                pkt,
                tag,
                start,
                end: start + pos,
                phantom,
            },
            Self {
                pkt,
                tag,
                start: start + pos,
                end,
                phantom,
            },
        );

        core::mem::forget(self);

        ret
    }

    /// Extract a sub-packet from this packet.
    ///
    /// # Safety
    ///
    /// Please see [`BoundPacketView::extract_packet`] documentation for details.
    pub unsafe fn extract_packet(&self, offset: Perms::Bounds, len: Perms::Bounds) -> Self {
        self.pkt().rc_and_waker.inc_rc();

        let Self {
            pkt, tag, start, ..
        } = self;
        assert!(offset <= self.len());
        assert!(offset + len <= self.len());

        if self.tag & 1 != 0 {
            unsafe {
                BaseArc::increment_strong_count(self.pkt.as_ptr());
            }
        }

        Self {
            pkt: *pkt,
            tag: *tag,
            start: *start + offset,
            end: *start + offset + len,
            phantom: PhantomData,
        }
    }
}
