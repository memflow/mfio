use crate::std_prelude::*;

use super::OpaqueStore;
use crate::error::Error;
use atomic_traits::{fetch::Min, Atomic};
pub use cglue::task::{CWaker, FastCWaker};
use core::cell::UnsafeCell;
use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::num::NonZeroI32;
use core::pin::Pin;
use core::sync::atomic::*;
use core::task::{Context, Poll};
use num::{Integer, NumCast, One, Saturating, ToPrimitive};
use rangemap::RangeSet;
use tarc::BaseArc;

mod output;
pub use output::*;
mod view;
pub use view::*;

const LOCK_BIT: u64 = 1 << 63;
const HAS_WAKER_BIT: u64 = 1 << 62;
const FINALIZED_BIT: u64 = 1 << 61;
const ALL_BITS: u64 = LOCK_BIT | HAS_WAKER_BIT | FINALIZED_BIT;

struct RcAndWaker {
    rc_and_flags: AtomicU64,
    waker: UnsafeCell<MaybeUninit<CWaker>>,
}

impl Default for RcAndWaker {
    fn default() -> Self {
        Self {
            rc_and_flags: 0.into(),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl core::fmt::Debug for RcAndWaker {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(
            fmt,
            "{}",
            (self.rc_and_flags.load(Ordering::Relaxed) & HAS_WAKER_BIT) != 0
        )
    }
}

impl RcAndWaker {
    fn acquire(&self) -> bool {
        (loop {
            let flags = self.rc_and_flags.fetch_or(LOCK_BIT, Ordering::AcqRel);
            if (flags & LOCK_BIT) == 0 {
                break flags;
            }
            while self.rc_and_flags.load(Ordering::Relaxed) & LOCK_BIT != 0 {
                core::hint::spin_loop();
            }
        } & HAS_WAKER_BIT)
            != 0
    }

    pub fn take(&self) -> Option<CWaker> {
        let ret = if self.acquire() {
            Some(unsafe { (*self.waker.get()).assume_init_read() })
        } else {
            None
        };
        self.rc_and_flags
            .fetch_and(!(LOCK_BIT | HAS_WAKER_BIT), Ordering::Release);
        ret
    }

    pub fn write(&self, waker: CWaker) -> u64 {
        if self.acquire() {
            unsafe { core::ptr::drop_in_place((*self.waker.get()).as_mut_ptr()) }
        }

        unsafe { *self.waker.get() = MaybeUninit::new(waker) };

        self.rc_and_flags.fetch_or(HAS_WAKER_BIT, Ordering::Relaxed);

        self.rc_and_flags.fetch_and(!LOCK_BIT, Ordering::AcqRel) & !ALL_BITS
    }

    pub fn acquire_rc(&self) -> u64 {
        self.rc_and_flags.load(Ordering::Acquire) & !ALL_BITS
    }

    pub fn dec_rc(&self) -> (u64, bool) {
        let ret = self.rc_and_flags.fetch_sub(1, Ordering::AcqRel);
        (ret & !ALL_BITS, (ret & HAS_WAKER_BIT) != 0)
    }

    pub fn inc_rc(&self) -> u64 {
        self.rc_and_flags.fetch_add(1, Ordering::AcqRel) & !ALL_BITS
    }

    pub fn finalize(&self) {
        self.rc_and_flags.fetch_or(FINALIZED_BIT, Ordering::Release);
    }

    pub fn wait_finalize(&self) {
        while (self.rc_and_flags.load(Ordering::Acquire) & FINALIZED_BIT) == 0 {
            core::hint::spin_loop();
        }
    }
}

/// Describes a full packet.
///
/// This packet is considered simple.
///
/// This packet cant be stored and modified on the stack. If `#[cfg(mfio_assume_linear_types)]` is
/// enabled, then the packet can also be sent to I/O backend as reference. Otherwise, it first
/// needs to be converted to an arc.
#[repr(C)]
pub struct FullPacket<T, Perms: PacketPerms> {
    header: Packet<Perms>,
    data: PackedLenData<T>,
}

impl<T, Perms: PacketPerms> FullPacket<T, Perms> {
    /// Creates a new [`FullPacket`] with POD data.
    pub fn new(val: T) -> Self
    where
        T: bytemuck::Pod,
    {
        unsafe { Self::new_unchecked(val) }
    }

    /// Creates a new `FullPacket` with `T` as backing byte storage.
    ///
    /// # Safety
    ///
    /// It must be okay to interpret `T` as raw bytes. If `T` is a complex type, undefined behavior
    /// may be reached.
    pub unsafe fn new_unchecked(data: T) -> Self {
        FullPacket {
            header: Packet::new_hdr(PacketVtblRef {
                tag: PacketVtblTag::SimpleDirect as _,
            }),
            data: PackedLenData {
                len: core::mem::size_of::<T>(),
                data,
            },
        }
    }
}

impl<T: bytemuck::Pod> FullPacket<MaybeUninit<T>, Write> {
    /// Builds a new packet with uninitialized data of `T`.
    pub fn new_uninit() -> Self {
        unsafe { Self::new_unchecked(MaybeUninit::uninit()) }
    }
}

impl<T, Perms: PacketPerms> AsRef<Packet<Perms>> for FullPacket<T, Perms> {
    fn as_ref(&self) -> &Packet<Perms> {
        &self.header
    }
}

impl<T, Perms: PacketPerms> core::ops::Deref for FullPacket<T, Perms> {
    type Target = Packet<Perms>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Describes a vector packet.
///
/// This packet is considered simple.
///
/// This is a convenience structure to allow passing existing vectors to I/O system, and retrieving
/// them afterwards. Only [`Vec::len`](Vec::len) amount of data is used for the packet.
#[repr(C)]
pub struct VecPacket<Perms: PacketPerms> {
    header: Packet<Perms>,
    data: PackedLenData<Perms::DataType>,
    capacity: usize,
    drop: unsafe extern "C" fn(Perms::DataType, usize, usize),
}

unsafe impl<Perms: PacketPerms> Send for VecPacket<Perms> {}
unsafe impl<Perms: PacketPerms> Sync for VecPacket<Perms> {}

impl<Perms: PacketPerms> AsRef<Packet<Perms>> for VecPacket<Perms> {
    fn as_ref(&self) -> &Packet<Perms> {
        &self.header
    }
}

impl<Perms: PacketPerms> core::ops::Deref for VecPacket<Perms> {
    type Target = Packet<Perms>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl VecPacket<Read> {
    /// Convert this `VecPacket` into a `Vec`.
    pub fn take(self) -> Vec<u8> {
        let vec = unsafe {
            Vec::from_raw_parts(
                self.data.data.cast_mut().cast(),
                self.data.len,
                self.capacity,
            )
        };
        core::mem::forget(self);
        vec
    }
}

impl VecPacket<Write> {
    /// Convert this `VecPacket` into a `Vec`.
    pub fn take(self) -> Vec<MaybeUninit<u8>> {
        let vec =
            unsafe { Vec::from_raw_parts(self.data.data.cast(), self.data.len, self.capacity) };
        core::mem::forget(self);
        vec
    }
}

impl VecPacket<ReadWrite> {
    /// Convert this `VecPacket` into a `Vec`.
    pub fn take(self) -> Vec<u8> {
        let vec =
            unsafe { Vec::from_raw_parts(self.data.data.cast(), self.data.len, self.capacity) };
        core::mem::forget(self);
        vec
    }
}

impl<Perms: PacketPerms> Drop for VecPacket<Perms> {
    fn drop(&mut self) {
        unsafe {
            (self.drop)(self.data.data, self.data.len, self.capacity);
        }
    }
}

impl From<Vec<u8>> for VecPacket<Read> {
    fn from(mut vec: Vec<u8>) -> Self {
        unsafe extern "C" fn drop(
            data: <Read as PacketPerms>::DataType,
            len: usize,
            capacity: usize,
        ) {
            let _ = Vec::from_raw_parts(data.cast_mut().cast::<u8>(), len, capacity);
        }

        let data = vec.as_mut_ptr();
        let len = vec.len();
        let capacity = vec.capacity();
        core::mem::forget(vec);

        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len,
                data: data.cast(),
            },
            capacity,
            drop,
        }
    }
}

impl<T: AnyBytes> From<Vec<T>> for VecPacket<Write> {
    fn from(mut vec: Vec<T>) -> Self {
        unsafe extern "C" fn drop(
            data: <Write as PacketPerms>::DataType,
            len: usize,
            capacity: usize,
        ) {
            let _ = Vec::from_raw_parts(data.cast::<u8>(), len, capacity);
        }

        let data = vec.as_mut_ptr();
        let len = vec.len();
        let capacity = vec.capacity();
        core::mem::forget(vec);

        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len,
                data: data.cast(),
            },
            capacity,
            drop,
        }
    }
}

/// Represents a packet that owns a `Box<[u8]>`.
///
/// This packet is considered simple.
///
/// Once the packet is dropped, the underlying box is dropped as well.
#[repr(C)]
pub struct OwnedPacket<Perms: PacketPerms> {
    header: Packet<Perms>,
    data: PackedLenData<Perms::DataType>,
    drop: unsafe extern "C" fn(Perms::DataType, usize),
}

unsafe impl<Perms: PacketPerms> Send for OwnedPacket<Perms> {}
unsafe impl<Perms: PacketPerms> Sync for OwnedPacket<Perms> {}

impl<Perms: PacketPerms> Drop for OwnedPacket<Perms> {
    fn drop(&mut self) {
        unsafe {
            (self.drop)(self.data.data, self.data.len);
        }
    }
}

impl From<Box<[u8]>> for OwnedPacket<Read> {
    fn from(slc: Box<[u8]>) -> Self {
        unsafe extern "C" fn drop(data: <Read as PacketPerms>::DataType, len: usize) {
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(
                data.cast_mut().cast::<u8>(),
                len,
            ));
        }

        let data = Box::leak(slc);

        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: data.len(),
                data: data as *const [u8] as *const (),
            },
            drop,
        }
    }
}

impl From<Box<[u8]>> for OwnedPacket<ReadWrite> {
    fn from(slc: Box<[u8]>) -> Self {
        unsafe extern "C" fn drop(data: <ReadWrite as PacketPerms>::DataType, len: usize) {
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(data.cast::<u8>(), len));
        }

        let data = Box::leak(slc);

        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: data.len(),
                data: data as *mut [u8] as *mut (),
            },
            drop,
        }
    }
}

impl<T: AnyBytes> From<Box<[T]>> for OwnedPacket<Write> {
    fn from(slc: Box<[T]>) -> Self {
        unsafe extern "C" fn drop(data: <Write as PacketPerms>::DataType, len: usize) {
            let _ = Box::from_raw(core::slice::from_raw_parts_mut(
                data.cast::<MaybeUninit<u8>>(),
                len,
            ));
        }

        let data = Box::leak(slc);

        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: data.len(),
                data: data as *mut [T] as *mut (),
            },
            drop,
        }
    }
}

impl<Perms: PacketPerms> AsRef<Packet<Perms>> for OwnedPacket<Perms> {
    fn as_ref(&self) -> &Packet<Perms> {
        &self.header
    }
}

impl<Perms: PacketPerms> core::ops::Deref for OwnedPacket<Perms> {
    type Target = Packet<Perms>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Represents a borrowed byte buffer packet.
///
/// This packet is considered simple.
///
/// A non-static `RefPacket` may be sent to the I/O backend, if `#[cfg(mfio_assume_linear_types)]`
/// config switch is enabled. Otherwise, only static `RefPacket`s may be sent, and only when they
/// are wrapped in an arc.
#[repr(C)]
pub struct RefPacket<'a, Perms: PacketPerms> {
    header: Packet<Perms>,
    data: PackedLenData<Perms::DataType>,
    // mut because it is more constrained than const.
    _phantom: PhantomData<&'a mut u8>,
}

#[cfg(mfio_assume_linear_types)]
impl<'a> From<&'a [u8]> for RefPacket<'a, Read> {
    fn from(slc: &'a [u8]) -> Self {
        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: slc.len(),
                data: slc.as_ptr().cast(),
            },
            _phantom: PhantomData,
        }
    }
}

#[cfg(mfio_assume_linear_types)]
impl<'a, T: AnyBytes> From<&'a mut [T]> for RefPacket<'a, Write> {
    fn from(slc: &'a mut [T]) -> Self {
        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: slc.len(),
                data: slc.as_mut_ptr().cast(),
            },
            _phantom: PhantomData,
        }
    }
}

#[cfg(mfio_assume_linear_types)]
impl<'a> From<&'a mut [u8]> for RefPacket<'a, ReadWrite> {
    fn from(slc: &'a mut [u8]) -> Self {
        Self {
            header: unsafe {
                Packet::new_hdr(PacketVtblRef {
                    tag: PacketVtblTag::SimpleIndirect as _,
                })
            },
            data: PackedLenData {
                len: slc.len(),
                data: slc.as_mut_ptr().cast(),
            },
            _phantom: PhantomData,
        }
    }
}

impl<Perms: PacketPerms> AsRef<Packet<Perms>> for RefPacket<'_, Perms> {
    fn as_ref(&self) -> &Packet<Perms> {
        &self.header
    }
}

impl<Perms: PacketPerms> core::ops::Deref for RefPacket<'_, Perms> {
    type Target = Packet<Perms>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Packed length + data pair for simple packets.
#[repr(C, packed)]
struct PackedLenData<T> {
    len: usize,
    data: T,
}

/// Represents different packet modes.
#[repr(usize)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum PacketVtblTag {
    /// Simple packet where after header we have length+buffer with no indirection.
    SimpleDirect = 0,
    /// Simple packet where after header we have length+pointer to a byte buffer with given length.
    SimpleIndirect = 1,
    /// Complex packeet.
    ///
    /// This is a placeholder value - inside the packet the true value in this case would be a
    /// valid vtable pointer.
    Complex,
}

/// Describes packet type.
///
/// If the value within this union is less than [`PacketVtblTag::Complex`], then the packet is
/// simple, and `tag` should be accessed. Meanwhile, any other value implies the packet is a
/// complex one, and `vtable` should be accessed instead.
#[derive(Clone, Copy)]
pub union PacketVtblRef<Perms: PacketPerms> {
    pub tag: usize,
    pub vtbl: &'static Perms,
}

impl<Perms: PacketPerms> core::fmt::Debug for PacketVtblRef<Perms> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self.tag() {
            PacketVtblTag::Complex => core::fmt::Debug::fmt(unsafe { self.vtbl }, f),
            v => core::fmt::Debug::fmt(&v, f),
        }
    }
}

impl<Perms: PacketPerms> PacketVtblRef<Perms> {
    pub fn tag(&self) -> PacketVtblTag {
        match unsafe { self.tag } {
            0 => PacketVtblTag::SimpleDirect,
            1 => PacketVtblTag::SimpleIndirect,
            _ => PacketVtblTag::Complex,
        }
    }

    pub fn vtbl(&self) -> Option<&'static Perms> {
        if self.tag() == PacketVtblTag::Complex {
            unsafe { Some(self.vtbl) }
        } else {
            None
        }
    }
}

/// Main packet header.
///
/// This header is at the start of every packet. In practice, all valid packet references may be
/// opaquable to references to [`Packet`].
///
/// This header primarily describes the current state of the packet (how many references in flight,
/// presence of a waker, etc.), but it also describes functionality of the packet.
///
/// ## Simple packets
///
/// A packet may be considered "simple", if what follows the header is either data buffer, or
/// reference to a data buffer. If that is not the case, then the packet is considered "complex".
///
/// ## Complex packets
///
/// A packet is considered "complex", if behaviors of various low level operations (such as
/// allocation, data transfer, etc.) are governed by a vtable. This extra level of indirection may
/// cost some performance overhead, but it allows to build more complex structures, such as lazily
/// allocated outputs.
#[repr(C)]
#[derive(Debug)]
pub struct Packet<Perms: PacketPerms> {
    /// If `None`, then we have a len + byte buffer after the header
    vtbl: PacketVtblRef<Perms>,
    /// Number of packet fragments currently in-flight (-1), and waker flags.
    ///
    /// The waker flags are encoded in the 2 highest bits. The left-most bit is a "start writing"
    /// bit. The second left-most bit is a "end writing" bit.
    ///
    /// Setting of waker should look as follows:
    ///
    /// ```ignore
    /// // Clear the flag bits, because we want the end writing bit be properly set
    /// rc_and_flags.fetch_and(!(0b11 << 62), Ordering::AcqRel);
    /// // Load in the start writing bit
    /// let loaded = rc_and_flags.fetch_or(0b1 << 63, Ordering::AcqRel);
    ///
    /// if !(loaded | (0b11 << 62)) == 0 {
    ///     // no more packets left, we don't need to write anything
    ///     return false
    /// }
    ///
    /// unsafe {
    ///     *waker.get() = in_waker;
    /// }
    ///
    /// // Load in the end writing bit.
    /// let loaded = rc_and_flags.fetch_or(0b1 << 62, Ordering::AcqRel);
    ///
    /// if !(loaded | (0b11 << 62)) == 0 {
    ///     // no more packets left, we wrote uselessly
    ///     return false
    /// }
    ///
    /// // true indicates the waker was installed and we can go to sleep.
    /// return true
    /// ```
    ///
    /// Acquiring the waker is done as follows:
    ///
    /// ```ignore
    /// let loaded = rc_and_flags.fetch_sub(1, Ordering::AcqRel);
    ///
    /// // we are not the last packet here, do nothing.
    /// if (loaded & !(0b11 << 62)) != 0 {
    ///     return false
    /// }
    ///
    /// // if the packet was not fully written yet, then we will do nothing, because the polling
    /// // thread will catch this and handle it appropriately.
    /// if loaded >> 62 != 0b11 {
    ///     return false
    /// }
    ///
    /// unsafe {
    ///     *waker.get()
    /// }.wake();
    ///
    /// return true
    /// ```
    rc_and_waker: RcAndWaker,
    /// What was the smallest position that resulted in an error.
    ///
    /// This value is initialized to !0, and upon each errored packet segment, is minned
    /// atomically. Upon I/O is complete, this allows the caller to check for the size of the
    /// contiguous memory region being successfully processed without gaps.
    error_clamp: <Perms::Bounds as NumBounds>::Atomic,
    /// Note that this may be raced against so it should not be relied as "the minimum error".
    min_error: AtomicI32,
    // We need miri to treat packets magically. Without marking this type as !Unpin, miri would
    // detect UB in situations where a packet is shared across threads, even though we are not
    // performing any mutable aliasing.
    //
    // See:
    // https://github.com/RalfJung/rfcs/blob/9881c94c5a9b7b24b12aaa07541c8b25e64ad5ae/text/0000-unsafe-aliased.md
    _phantom: PhantomPinned,
    // data afterwards
}

impl<Perms: PacketPerms> AsRef<Self> for Packet<Perms> {
    fn as_ref(&self) -> &Self {
        self
    }
}

unsafe impl<Perms: PacketPerms> Send for Packet<Perms> {}
unsafe impl<Perms: PacketPerms> Sync for Packet<Perms> {}

impl<Perms: PacketPerms> Drop for Packet<Perms> {
    fn drop(&mut self) {
        let loaded = self.rc_and_waker.acquire_rc();
        assert_eq!(loaded, 0, "The packet has in-flight segments.");
    }
}

impl<'a, Perms: PacketPerms> Future for &'a Packet<Perms> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = Pin::into_inner(self);

        let rc = this.rc_and_waker.write(cx.waker().clone().into());

        if rc == 0 {
            // Synchronize the thread that last decremented the refcount.
            // If we don't, we risk a race condition where we drop the packet, while the packet
            // reference is still being used to take the waker.
            this.rc_and_waker.wait_finalize();

            // no more packets left, we don't need to write anything
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

impl<Perms: PacketPerms> Packet<Perms> {
    /// Current reference count of the packet.
    pub fn rc(&self) -> usize {
        (self.rc_and_waker.acquire_rc()) as usize
    }

    unsafe fn on_output(&self, error: Option<(Perms::Bounds, NonZeroI32)>) -> Option<CWaker> {
        if let Some((start, error)) = error {
            if self.error_clamp.fetch_min(start, Ordering::AcqRel) > start {
                self.min_error.store(error.into(), Ordering::Relaxed);
            }
        }

        let (prev, has_waker) = self.rc_and_waker.dec_rc();

        // Do nothing, because we are not the last packet
        if prev != 1 {
            return None;
        }

        let ret = if has_waker {
            self.rc_and_waker.take()
        } else {
            None
        };

        self.rc_and_waker.finalize();

        ret
    }

    unsafe fn on_add_to_view(&self) {
        let rc = self.rc_and_waker.inc_rc();
        if rc != 0 {
            self.rc_and_waker.dec_rc();
            assert_eq!(rc, 0);
        }
    }

    /// Resets error state of the packet.
    ///
    /// # Safety
    ///
    /// This function is safe to call only when all packet operations have concluded.
    pub unsafe fn reset_err(&self) {
        self.error_clamp
            .store(!<Perms::Bounds as Default>::default(), Ordering::Release);
        self.min_error.store(0, Ordering::Release);
    }

    /// Creates a new `Packet` header.
    ///
    /// # Safety
    ///
    /// The caller must ensure the resulting header is placed into an appropriate packet structure
    /// that the provided vtable is meant to use. Failure to do so leads to undefined behavior.
    pub unsafe fn new_hdr(vtbl: PacketVtblRef<Perms>) -> Self {
        Packet {
            vtbl,
            rc_and_waker: Default::default(),
            error_clamp: (!<Perms::Bounds as Default>::default()).into(),
            min_error: 0.into(),
            _phantom: PhantomPinned,
        }
    }

    /// Gets pointer to length of a simple packet.
    ///
    /// # Safety
    ///
    /// `this` must point to a valid packet header for a simple packet. I.e. after the header what
    /// must proceed is a single usize field representing the length of the following byte data.
    pub unsafe fn simple_len(this: *const Self) -> *const usize {
        this.add(1).cast()
    }

    /// Gets pointer to data of a simple packet.
    ///
    /// # Safety
    ///
    /// `this` must point to a valid packet header for a simple packet. I.e. after the header what
    /// must proceed is a single usize field representing the length of the following byte data.
    /// This function will return a `*const u8`, which may be cast to `*const *const u8` in
    /// indirect packet scenarios.
    pub unsafe fn simple_data(this: *const Self) -> *const u8 {
        this.add(1).cast::<usize>().add(1).cast()
    }

    /// Gets pointer to data of a simple packet.
    ///
    /// # Safety
    ///
    /// `this` must point to a valid packet header for a simple packet. I.e. after the header what
    /// must proceed is a single usize field representing the length of the following byte data.
    /// This function will return a `*mut u8`, which may be cast to `*const *mut u8` in
    /// indirect packet scenarios.
    pub unsafe fn simple_data_mut(this: *const Self) -> *mut u8 {
        this.add(1).cast::<usize>().add(1).cast_mut().cast()
    }

    /// Gets slice to the data of the simple packet.
    ///
    /// This will return all packet bytes as `MaybeUninit<u8>`. If you want to get only initialized
    /// bytes (in read scenario), then use
    /// [`simple_contiguous_slice`](Self::simple_contiguous_slice) function.
    pub fn simple_slice(&self) -> Option<&[MaybeUninit<u8>]> {
        if self.vtbl.tag() == PacketVtblTag::Complex {
            None
        } else {
            Some(unsafe {
                core::slice::from_raw_parts(self.simple_data_ptr().cast(), *Self::simple_len(self))
            })
        }
    }

    /// Gets a mutable slice to the data of the simple packet.
    ///
    /// # Safety
    ///
    /// Only a single instance of the mutable data must be active at a given time, with no constant
    /// references.
    pub unsafe fn simple_slice_mut(&self) -> Option<&mut [MaybeUninit<u8>]> {
        if self.vtbl.tag() == PacketVtblTag::Complex {
            None
        } else {
            Some(unsafe {
                core::slice::from_raw_parts_mut(
                    self.simple_data_ptr().cast_mut().cast(),
                    *Self::simple_len(self),
                )
            })
        }
    }

    /// Gets slice to the first segment of contiguously processed bytes.
    pub fn simple_contiguous_slice(&self) -> Option<&[u8]> {
        if self.vtbl.tag() == PacketVtblTag::Complex {
            None
        } else {
            Some(unsafe {
                core::slice::from_raw_parts(
                    self.simple_data_ptr(),
                    core::cmp::min(
                        self.error_clamp.load(Ordering::Acquire).to_usize().unwrap(),
                        *Self::simple_len(self),
                    ),
                )
            })
        }
    }

    /// Gets pointer to data of a simple packet.
    ///
    /// # Panics
    ///
    /// This function will panic if called on a complex packet.
    pub fn simple_data_ptr(&self) -> *const u8 {
        match self.vtbl.tag() {
            PacketVtblTag::SimpleDirect => unsafe { Self::simple_data(self) },
            PacketVtblTag::SimpleIndirect => unsafe {
                *Self::simple_data(self).cast::<*const u8>()
            },
            PacketVtblTag::Complex => panic!("simple_data_ptr called on complex Packet"),
        }
    }

    /// Gets mutable pointer to data of a simple packet.
    ///
    /// # Panics
    ///
    /// This function will panic if called on a complex packet.
    pub fn simple_data_ptr_mut(&mut self) -> *mut u8 {
        match self.vtbl.tag() {
            PacketVtblTag::SimpleDirect => unsafe { Self::simple_data_mut(self) },
            PacketVtblTag::SimpleIndirect => unsafe { *Self::simple_data(self).cast::<*mut u8>() },
            PacketVtblTag::Complex => panic!("simple_data_ptr called on complex Packet"),
        }
    }

    /// Gets the error value at the lowest packet offset that was erroneous.
    pub fn min_error(&self) -> Option<Error> {
        NonZeroI32::new(self.min_error.load(Ordering::Relaxed)).map(Error::from_int_err)
    }

    /// Gets the length of the leading contiguous segment, or `!0`.
    ///
    /// If there was an error case, this function will return the length of the first contiguous
    /// segment. However, if the packet encountered no error cases, `!0` will be returned.
    pub fn error_clamp(&self) -> Perms::Bounds {
        self.error_clamp.load(Ordering::Relaxed)
    }

    /// Returns [`min_error`](Self::min_error) if 0 leading bytes were processed.
    pub fn err_on_zero(&self) -> Result<(), Error> {
        if self.error_clamp() > Default::default() {
            Ok(())
        } else {
            Err(self.min_error().expect("No error when error_clamp is 0"))
        }
    }

    /// Returns [`min_erorr`](Self::min_error) if any parts of the packet had errors.
    pub fn err_any(&self) -> Result<(), Error> {
        if let Some(err) = self.min_error() {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<Perms: PacketPerms> Packet<Perms> {
    /// Builds a new packet with dynamically sized buffer.
    ///
    /// The returned packet will be simple, and wrapped in an arc.
    pub fn new_buf(len: usize) -> BaseArc<Packet<Perms>> {
        // TODO: feature gate this
        use std::alloc::Layout;

        let size = core::mem::size_of::<FullPacket<PhantomData<()>, Perms>>() + len;
        let align = core::mem::align_of::<FullPacket<PhantomData<()>, Perms>>();

        unsafe extern "C" fn drop_pkt<Perms: PacketPerms>(data: *mut ()) {
            core::ptr::drop_in_place(data.cast::<Packet<Perms>>())
        }

        // SAFETY: we are creating a packet that is sufficient for our needed amount of data.
        // In addition, we do not need a cleanup routine, because, because the only object that
        // would need freeing is the `CWaker`. The polling implementation ensures that the
        // `CWaker` is not left without any packets left to process it, and any outputted packets
        // will trigger the `CWaker`. Any other fields are POD.
        let packet = unsafe {
            BaseArc::custom(
                Layout::from_size_align_unchecked(size, align),
                Some(drop_pkt::<Perms>),
            )
        };

        // SAFETY: we are initializing the packet here, hence the transmute is valid
        unsafe {
            // first we need to write the packet data to make it valid
            (packet.exclusive_ptr().unwrap().as_ptr() as *mut FullPacket<PhantomData<()>, Perms>)
                .write(FullPacket {
                    header: Self::new_hdr(PacketVtblRef {
                        tag: PacketVtblTag::SimpleDirect as _,
                    }),
                    data: PackedLenData {
                        len,
                        data: PhantomData,
                    },
                });

            core::mem::transmute(packet)
        }
    }
}

impl Packet<Read> {
    /// Creates a new packet filled with data from the given slice.
    pub fn copy_from_slice(buf: &[u8]) -> BaseArc<Packet<Read>> {
        let pkt = Self::new_buf(buf.len());
        unsafe {
            core::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                pkt.simple_data_ptr().cast_mut(),
                buf.len(),
            )
        };
        pkt
    }
}

impl Packet<ReadWrite> {
    /// Creates a new packet filled with data from the given slice.
    pub fn copy_from_slice(buf: &[u8]) -> BaseArc<Packet<ReadWrite>> {
        let pkt = Self::new_buf(buf.len());
        unsafe {
            core::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                pkt.simple_data_ptr().cast_mut(),
                buf.len(),
            )
        };
        pkt
    }
}

unsafe impl<Perms: PacketPerms> OpaqueStore for BaseArc<Packet<Perms>> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = Self where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_arc_ref(stack, 0)
    }
}

unsafe impl<'c, Perms: PacketPerms> OpaqueStore for &'c BaseArc<Packet<Perms>> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = BaseArc<Packet<Perms>> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.clone()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_arc_ref(*stack, 0)
    }
}

unsafe impl<T: 'static, Perms: PacketPerms> OpaqueStore for FullPacket<T, Perms> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;

    crate::linear_types_switch! {
        Standard => {
            type StackReq<'a> = BaseArc<Self> where Self: 'a;
        }
        Linear => {
            type StackReq<'a> = Self where Self: 'a;
        }
    }

    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        #[allow(clippy::useless_conversion)]
        self.into()
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        crate::linear_types_switch! {
            Standard => {
                PacketView::from_arc_ref(
                    // SAFETY: we know the header is at the start of the fullpacket struct, therefore
                    // reinterpreting the reference is okay.
                    unsafe { &*(stack as *const BaseArc<FullPacket<T, Perms>>).cast() },
                    0,
                )
            }
            Linear => {
                PacketView::from_ref(
                    // SAFETY: we know the header is at the start of the fullpacket struct, therefore
                    // reinterpreting the reference is okay.
                    unsafe { &*(stack as *const FullPacket<T, Perms>).cast() },
                    0,
                )
            }
        }
    }
}

unsafe impl<T: 'static, Perms: PacketPerms> OpaqueStore for BaseArc<FullPacket<T, Perms>> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = Self where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_arc_ref(
            // SAFETY: we know the header is at the start of the fullpacket struct, therefore
            // reinterpreting the reference is okay.
            unsafe { &*(stack as *const BaseArc<FullPacket<T, Perms>>).cast() },
            0,
        )
    }
}

unsafe impl<Perms: PacketPerms> OpaqueStore for crate::linear_types_switch! { Standard => { RefPacket::<'static, Perms> } Linear => { RefPacket::<'_, Perms> } } {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    // TODO: do we need an arc here?
    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_ref(
            // SAFETY: we know the header is at the start of the fullpacket struct, therefore
            // reinterpreting the reference is okay.
            unsafe { &*(stack as *const Self).cast() },
            0,
        )
    }
}

unsafe impl<Perms: PacketPerms> OpaqueStore for VecPacket<Perms> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_ref(
            // SAFETY: we know the header is at the start of the fullpacket struct, therefore
            // reinterpreting the reference is okay.
            unsafe { &*(stack as *const Self).cast() },
            0,
        )
    }
}

unsafe impl<Perms: PacketPerms> OpaqueStore for OwnedPacket<Perms> {
    type ConstHdr = Packet<Perms>;
    type Opaque<'a> = PacketView<'a, Perms> where Self: 'a;
    type StackReq<'a> = Self where Self: 'a;
    type HeapReq = BaseArc<Self> where Self: 'static;

    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a,
    {
        self
    }

    fn heap(self) -> Self::HeapReq
    where
        Self: 'static,
    {
        self.into()
    }

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr
    where
        Self: 'a,
    {
        stack
    }

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a> {
        // TODO: deal with tags
        PacketView::from_ref(
            // SAFETY: we know the header is at the start of the fullpacket struct, therefore
            // reinterpreting the reference is okay.
            unsafe { &*(stack as *const Self).cast() },
            0,
        )
    }
}

impl<T: AsRef<Packet<Perms>>, Perms: PacketPerms> From<BaseArc<T>> for PacketView<'static, Perms> {
    fn from(pkt: BaseArc<T>) -> Self {
        // TODO: deal with tags
        Self::from_arc(pkt.transpose().into_base().unwrap(), 0)
    }
}

pub trait PacketStore<'a, Perms: PacketPerms>:
    'a + OpaqueStore<Opaque<'a> = PacketView<'a, Perms>, ConstHdr = Packet<Perms>>
{
}

impl<'a, Perms: PacketPerms, T> PacketStore<'a, Perms> for T where
    T: 'a + OpaqueStore<Opaque<'a> = PacketView<'a, Perms>, ConstHdr = Packet<Perms>>
{
}

trait AnyBytes {}

impl AnyBytes for u8 {}
impl AnyBytes for MaybeUninit<u8> {}

/// Object convertable into a packet.
///
/// This is an extension of [`OpaqueStore`], where an object may have additional synchronization
/// steps taken upon the end of I/O processing.
pub trait IntoPacket<'a, Perms: PacketPerms> {
    type Target: PacketStore<'a, Perms>;
    type SyncHandle;

    /// Converts `Self` into a packet.
    fn into_packet(self) -> (Self::Target, Self::SyncHandle);

    /// Sync data back from the packet.
    ///
    /// The blanket implementation of this method is a no-op, however, some types may need to have
    /// the I/O results be synchronized back from the packets that were sent out (this is due to
    /// necessary heap indirections needed to appease the borrow checker). Therefore, any I/O
    /// abstraction should call this function before returning data back to the user.
    fn sync_back(_hdr: &<Self::Target as OpaqueStore>::ConstHdr, _handle: Self::SyncHandle) {}
}

impl<'a, T: PacketStore<'a, Perms>, Perms: PacketPerms> IntoPacket<'a, Perms> for T {
    type Target = Self;
    type SyncHandle = ();

    fn into_packet(self) -> (Self, ()) {
        (self, ())
    }
}

impl<'a, 'b: 'a> IntoPacket<'a, Read> for &'b [u8] {
    type Target = BaseArc<Packet<Read>>;
    type SyncHandle = ();

    fn into_packet(self) -> (Self::Target, ()) {
        (Packet::<Read>::copy_from_slice(self), ())
    }
}

impl<'a, 'b: 'a, T: AnyBytes> IntoPacket<'a, Write> for &'b mut [T] {
    crate::linear_types_switch! {
        Standard => {
            type SyncHandle = Self;
            type Target = BaseArc<Packet<Write>>;

            fn into_packet(self) -> (Self::Target, Self) {
                (Packet::<Write>::new_buf(self.len()), self)
            }

            fn sync_back(hdr: &<Self::Target as OpaqueStore>::ConstHdr, handle: Self::SyncHandle) {
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        hdr.simple_data_ptr(),
                        handle.as_mut_ptr().cast(),
                        core::cmp::min(handle.len(), hdr.error_clamp() as usize),
                    );
                }
            }
        }
        Linear => {
            type SyncHandle = ();
            type Target = RefPacket<'a, Write>;

            fn into_packet(self) -> (Self::Target, Self::SyncHandle) {
                (self.into(), ())
            }
        }
    }
}

impl<'a, 'b: 'a> IntoPacket<'a, ReadWrite> for &'b mut [u8] {
    type Target = BaseArc<Packet<ReadWrite>>;
    type SyncHandle = Self;

    fn into_packet(self) -> (Self::Target, Self) {
        (Packet::<ReadWrite>::copy_from_slice(&*self), self)
    }

    fn sync_back(hdr: &<Self::Target as OpaqueStore>::ConstHdr, handle: Self::SyncHandle) {
        unsafe {
            core::ptr::copy_nonoverlapping(
                hdr.simple_data_ptr(),
                handle.as_mut_ptr(),
                core::cmp::min(handle.len(), hdr.error_clamp() as usize),
            );
        }
    }
}

use cglue::prelude::v1::*;

/// Consumes `BoundPacketObj` and returns allocated version of it.
///
/// If the `alignment` constraint is met, this function consumes `packet`, fills `out_aligned`, and
/// returns `true`. If the constraint cannot be satisfied, `false` is returned.
///
/// If this function returns `false`, the caller should consider allocating an intermediate buffer,
/// and invoking [`TransferDataFn`](TransferDataFn) instead.
///
/// # Remarks
///
/// The caller should prefer [`TransferDataFn`](TransferDataFn), as opposed to this function, if
/// they can access their internal buffer with zero allocations.
pub type AllocFn<T> = for<'a> unsafe extern "C" fn(
    packet: &'a mut ManuallyDrop<BoundPacketView<T>>,
    alignment: usize,
    out_alloced: &'a mut MaybeUninit<<T as PacketPerms>::Alloced>,
) -> bool;

/// Transfers data between a `BoundPacketObj` and reverse data type.
///
/// This function consumes `packet`, and transfers data between it and `input`. The caller must
/// ensure that `input` bounds are sufficient for the size of the packet.
///
/// TODO: decide on whether this function can fail.
pub type TransferDataFn<T> = for<'a> unsafe extern "C" fn(
    packet: &'a mut PacketView<T>,
    input: <T as PacketPerms>::ReverseDataType,
);

/// Retrieves total length of the packet.
pub type LenFn<T> = unsafe extern "C" fn(packet: &Packet<T>) -> <T as PacketPerms>::Bounds;

/// Packet that may be alloced.
///
/// `Ok` represents alloced, `Err` represents unalloced (bound packet view).
pub type MaybeAlloced<T> = Result<<T as PacketPerms>::Alloced, BoundPacketView<T>>;

/// Represents the state where packet is either alloced or transferred.
///
/// `Ok` represents alloced, `Err` represents transferred.
pub type AllocedOrTransferred<T> = Result<<T as PacketPerms>::Alloced, TransferredPacket<T>>;

/// Describes type constraints on packet operations.
pub trait PacketPerms: 'static + core::fmt::Debug + Clone + Copy {
    type DataType: Clone + Copy + core::fmt::Debug;
    type ReverseDataType: Clone + Copy + core::fmt::Debug;
    type Alloced: AllocatedPacket<Perms = Self>;
    type Bounds: NumBounds;

    /// Returns vtable function for getting packet length.
    fn len_fn(&self) -> LenFn<Self>;

    /// Returns packet length.
    fn len(packet: &Packet<Self>) -> Self::Bounds {
        if let Some(vtbl) = packet.vtbl.vtbl() {
            unsafe { (vtbl.len_fn())(packet) }
        } else {
            unsafe {
                NumCast::from(*Packet::simple_len(packet)).expect("Packet larger than bounds")
            }
        }
    }

    /// Returns vtable function for getting
    fn alloc_fn(&self) -> AllocFn<Self>;

    /// A buffer for simple packet.
    ///
    /// # Safety
    ///
    /// The given packet view must be simple.
    unsafe fn alloced_simple(packet: BoundPacketView<Self>) -> Self::Alloced;

    /// Tries allocating packet buffer with given alignment.
    fn try_alloc(packet: BoundPacketView<Self>, alignment: usize) -> MaybeAlloced<Self> {
        if let Some(vtbl) = packet.view.pkt().vtbl.vtbl() {
            let mut view = ManuallyDrop::new(packet);
            let mut out = MaybeUninit::uninit();
            let ret = unsafe { (vtbl.alloc_fn())(&mut view, alignment, &mut out) };
            if ret {
                Ok(unsafe { out.assume_init() })
            } else {
                Err(ManuallyDrop::into_inner(view))
            }
        } else {
            let data = unsafe {
                packet
                    .view
                    .pkt()
                    .simple_data_ptr()
                    // This should never panic
                    .add(packet.view.start.to_usize().unwrap())
            };
            if data.align_offset(alignment) == 0 {
                Ok(unsafe { Self::alloced_simple(packet) })
            } else {
                Err(packet)
            }
        }
    }

    /// Returns vtable function for transfering data.
    fn transfer_data_fn(&self) -> TransferDataFn<Self>;

    /// Transfers data between a simple packet and a byte buffer.
    ///
    /// # Safety
    ///
    /// The provided input buffer must be valid and cover the length of the packet view.
    ///
    /// In addition, the provided packet must be simple.
    unsafe fn transfer_data_simple(packet: &mut PacketView<Self>, input: Self::ReverseDataType);

    /// Transfers data between a packet and a byte buffer.
    ///
    /// # Safety
    ///
    /// The provided input buffer must be valid and cover the length of the packet view.
    unsafe fn transfer_data(packet: &mut PacketView<Self>, input: Self::ReverseDataType) {
        if let Some(vtbl) = packet.pkt().vtbl.vtbl() {
            (vtbl.transfer_data_fn())(packet, input)
        } else {
            Self::transfer_data_simple(packet, input)
        }
    }
}

/// ReadWrite permissions.
///
/// The behavior of ReadWrite is not well defined, but for simple buffers, it implies the swapping
/// of data between the 2 buffers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReadWrite<Bounds: NumBounds = u64> {
    pub len: unsafe extern "C" fn(&Packet<Self>) -> Bounds,
    pub get_mut: for<'a> unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketView<Self>>,
        usize,
        &mut MaybeUninit<ReadWritePacketObj<Bounds>>,
    ) -> bool,
    pub transfer_data: for<'a, 'b> unsafe extern "C" fn(&'a mut PacketView<Self>, *mut ()),
}

impl<Bounds: NumBounds> core::fmt::Debug for ReadWrite<Bounds> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl<Bounds: NumBounds> PacketPerms for ReadWrite<Bounds> {
    type DataType = *mut ();
    type ReverseDataType = *mut ();
    type Alloced = ReadWritePacketObj<Bounds>;
    type Bounds = Bounds;

    fn len_fn(&self) -> LenFn<Self> {
        self.len
    }

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get_mut
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }

    unsafe fn alloced_simple(packet: BoundPacketView<Self>) -> Self::Alloced {
        let data = packet.view.pkt().simple_data_ptr().cast_mut();
        ReadWritePacketObj {
            alloced_packet: unsafe { data.add(packet.view.start.to_usize().unwrap()) },
            buffer: packet,
        }
    }

    unsafe fn transfer_data_simple(view: &mut PacketView<Self>, data: *mut ()) {
        let dst = Packet::simple_data_ptr_mut(view.pkt_mut());
        // TODO: does this operation even make sense?
        core::ptr::swap_nonoverlapping(
            data.cast(),
            dst.add(view.start.to_usize().unwrap()),
            view.len().to_usize().unwrap(),
        );
    }
}

/// Write permissions.
///
/// This implies the packet is writeable and may not have valid data beforehand.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Write<Bounds: NumBounds = u64> {
    pub len: unsafe extern "C" fn(&Packet<Self>) -> Bounds,
    pub get_mut: for<'a> unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketView<Self>>,
        usize,
        &mut MaybeUninit<WritePacketObj<Bounds>>,
    ) -> bool,
    pub transfer_data: for<'a, 'b> unsafe extern "C" fn(&'a mut PacketView<Self>, *const ()),
}

impl<Bounds: NumBounds> core::fmt::Debug for Write<Bounds> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get_mut as *const ())
    }
}

impl<Bounds: NumBounds> PacketPerms for Write<Bounds> {
    type DataType = *mut ();
    type ReverseDataType = *const ();
    type Alloced = WritePacketObj<Bounds>;
    type Bounds = Bounds;

    fn len_fn(&self) -> LenFn<Self> {
        self.len
    }

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get_mut
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }

    unsafe fn alloced_simple(packet: BoundPacketView<Self>) -> Self::Alloced {
        let data = packet
            .view
            .pkt()
            .simple_data_ptr()
            .cast_mut()
            .cast::<MaybeUninit<u8>>();
        WritePacketObj {
            alloced_packet: unsafe { data.add(packet.view.start.to_usize().unwrap()) },
            buffer: packet,
        }
    }

    unsafe fn transfer_data_simple(view: &mut PacketView<Self>, data: *const ()) {
        let dst = Packet::simple_data_ptr_mut(view.pkt_mut());
        core::ptr::copy(
            data.cast(),
            dst.add(view.start.to_usize().unwrap()),
            view.len().to_usize().unwrap(),
        );
    }
}

/// Read permissions.
///
/// This implies this packet contains valid data and it can be read.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Read<Bounds: NumBounds = u64> {
    pub len: unsafe extern "C" fn(&Packet<Self>) -> Bounds,
    pub get: unsafe extern "C" fn(
        &mut ManuallyDrop<BoundPacketView<Self>>,
        usize,
        &mut MaybeUninit<ReadPacketObj<Bounds>>,
    ) -> bool,
    pub transfer_data: for<'a> unsafe extern "C" fn(&'a mut PacketView<Self>, *mut ()),
}

impl<Bounds: NumBounds> core::fmt::Debug for Read<Bounds> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(fmt, "{:?}", self.get as *const ())
    }
}

impl<Bounds: NumBounds> PacketPerms for Read<Bounds> {
    type DataType = *const ();
    type ReverseDataType = *mut ();
    type Alloced = ReadPacketObj<Bounds>;
    type Bounds = Bounds;

    fn len_fn(&self) -> LenFn<Self> {
        self.len
    }

    fn alloc_fn(&self) -> AllocFn<Self> {
        self.get
    }

    fn transfer_data_fn(&self) -> TransferDataFn<Self> {
        self.transfer_data
    }

    unsafe fn alloced_simple(packet: BoundPacketView<Self>) -> Self::Alloced {
        let data = packet.view.pkt().simple_data_ptr().cast::<u8>();
        ReadPacketObj {
            alloced_packet: unsafe { data.add(packet.view.start.to_usize().unwrap()) },
            buffer: packet,
        }
    }

    unsafe fn transfer_data_simple(view: &mut PacketView<Self>, data: *mut ()) {
        let src = view.pkt().simple_data_ptr();
        core::ptr::copy(
            src,
            data.cast::<u8>().add(view.start.to_usize().unwrap()),
            view.len().to_usize().unwrap(),
        );
    }
}

pub trait NumBounds:
    Integer
    + NumCast
    + Copy
    + Send
    + Default
    + core::ops::Not<Output = Self>
    + Saturating
    + core::fmt::Debug
    + Into<Self::Atomic>
    + 'static
{
    type Atomic: atomic_traits::Atomic<Type = Self> + atomic_traits::NumOps + core::fmt::Debug;
}

macro_rules! num_bounds {
    ($($ty1:ident => $ty2:ident),*) => {
        $(
        impl NumBounds for $ty1 {
            type Atomic = core::sync::atomic::$ty2;
        }
        )*
    }
}

num_bounds!(u8 => AtomicU8, u16 => AtomicU16, u32 => AtomicU32, u64 => AtomicU64, usize => AtomicUsize);

//impl<T: Integer + NumCast + Copy + Default + Send + Saturating + 'static> NumBounds for T {}

/// Objects that can be split.
///
/// This trait enables splitting objects into non-overlapping parts.
pub trait Splittable: Sized {
    type Bounds: NumBounds;

    /// Splits an object at given position.
    ///
    /// # Panics
    ///
    /// This function may panic if len is outside the bounds of the given object.
    fn split_at(self, len: impl NumBounds) -> (Self, Self);
    fn len(&self) -> Self::Bounds;
    fn is_empty(&self) -> bool {
        self.len() == Default::default()
    }
}

impl<T: NumBounds, A: Splittable<Bounds = T>, B: Splittable<Bounds = T>> Splittable
    for Result<A, B>
{
    type Bounds = T;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        match self {
            Ok(v) => {
                let (a, b) = v.split_at(len);
                (Ok(a), Ok(b))
            }
            Err(v) => {
                let (a, b) = v.split_at(len);
                (Err(a), Err(b))
            }
        }
    }

    fn len(&self) -> T {
        match self {
            Ok(v) => v.len(),
            Err(v) => v.len(),
        }
    }
}

/// Object that can be returned with an error.
///
/// This is meant for all types of packets - allow backend to easily return them to the user with
/// an appropriate error condition attached.
pub trait Errorable: Sized {
    fn error(self, err: Error);
}

impl<A: Errorable, B: Errorable> Errorable for Result<A, B> {
    fn error(self, err: Error) {
        match self {
            Ok(v) => v.error(err),
            Err(v) => v.error(err),
        }
    }
}

/// Packet which has been allocated.
///
/// Allocated packets expose direct access to the underlying buffer.
pub trait AllocatedPacket: Splittable + Errorable {
    type Perms: PacketPerms;
    type Pointer: Copy;

    fn as_ptr(&self) -> Self::Pointer;
}

/// Represents a simple allocated packet with write permissions.
#[repr(C)]
pub struct ReadWritePacketObj<Bounds: NumBounds = u64> {
    alloced_packet: *mut u8,
    buffer: BoundPacketView<ReadWrite<Bounds>>,
}

impl<Bounds: NumBounds> Splittable for ReadWritePacketObj<Bounds> {
    type Bounds = Bounds;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        let len_usize = len.to_usize().expect("Input out of range");
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len_usize) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> Bounds {
        self.buffer.view.len()
    }
}

impl<Bounds: NumBounds> Errorable for ReadWritePacketObj<Bounds> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<Bounds: NumBounds> AllocatedPacket for ReadWritePacketObj<Bounds> {
    type Perms = ReadWrite<Bounds>;
    type Pointer = *mut u8;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }
}

unsafe impl<Bounds: NumBounds> Send for ReadWritePacketObj<Bounds> {}
unsafe impl<Bounds: NumBounds> Sync for ReadWritePacketObj<Bounds> {}

impl<Bounds: NumBounds> core::ops::Deref for ReadWritePacketObj<Bounds> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.alloced_packet,
                self.buffer.view.len().to_usize().unwrap(),
            )
        }
    }
}

impl<Bounds: NumBounds> core::ops::DerefMut for ReadWritePacketObj<Bounds> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            core::slice::from_raw_parts_mut(
                self.alloced_packet,
                self.buffer.view.len().to_usize().unwrap(),
            )
        }
    }
}

/// Represents a simple allocated packet with write permissions.
///
/// The data inside may not be initialized, therefore, this packet should only be written to.
#[repr(C)]
pub struct WritePacketObj<Bounds: NumBounds = u64> {
    alloced_packet: *mut MaybeUninit<u8>,
    buffer: BoundPacketView<Write<Bounds>>,
}

impl<Bounds: NumBounds> Splittable for WritePacketObj<Bounds> {
    type Bounds = Bounds;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        let len_usize = len.to_usize().expect("Input out of range");
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len_usize) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> Bounds {
        self.buffer.view.len()
    }
}

impl<Bounds: NumBounds> Errorable for WritePacketObj<Bounds> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<Bounds: NumBounds> AllocatedPacket for WritePacketObj<Bounds> {
    type Perms = Write<Bounds>;
    type Pointer = *mut MaybeUninit<u8>;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }
}

unsafe impl<Bounds: NumBounds> Send for WritePacketObj<Bounds> {}
unsafe impl<Bounds: NumBounds> Sync for WritePacketObj<Bounds> {}

impl<Bounds: NumBounds> core::ops::Deref for WritePacketObj<Bounds> {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.alloced_packet,
                self.buffer.view.len().to_usize().unwrap(),
            )
        }
    }
}

impl<Bounds: NumBounds> core::ops::DerefMut for WritePacketObj<Bounds> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            core::slice::from_raw_parts_mut(
                self.alloced_packet,
                self.buffer.view.len().to_usize().unwrap(),
            )
        }
    }
}

/// Represents a simple allocated packet with read permissions.
#[repr(C)]
pub struct ReadPacketObj<Bounds: NumBounds = u64> {
    alloced_packet: *const u8,
    buffer: BoundPacketView<Read<Bounds>>,
}

impl<Bounds: NumBounds> Splittable for ReadPacketObj<Bounds> {
    type Bounds = Bounds;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        let len_usize = len.to_usize().expect("Input out of range");
        let (b1, b2) = self.buffer.split_at(len);

        (
            Self {
                alloced_packet: self.alloced_packet,
                buffer: b1,
            },
            Self {
                alloced_packet: unsafe { self.alloced_packet.add(len_usize) },
                buffer: b2,
            },
        )
    }

    fn len(&self) -> Bounds {
        self.buffer.view.len()
    }
}

impl<Bounds: NumBounds> Errorable for ReadPacketObj<Bounds> {
    fn error(self, err: Error) {
        self.buffer.error(err)
    }
}

impl<Bounds: NumBounds> AllocatedPacket for ReadPacketObj<Bounds> {
    type Perms = Read<Bounds>;
    type Pointer = *const u8;

    fn as_ptr(&self) -> Self::Pointer {
        self.alloced_packet
    }
}

unsafe impl<Bounds: NumBounds> Send for ReadPacketObj<Bounds> {}
unsafe impl<Bounds: NumBounds> Sync for ReadPacketObj<Bounds> {}

impl<Bounds: NumBounds> core::ops::Deref for ReadPacketObj<Bounds> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.alloced_packet,
                self.buffer.view.len().to_usize().unwrap(),
            )
        }
    }
}

/// Represents the state of the packet that has already had data transferred to.
///
/// This struct is marked as `must_use`, because the intention is for the backend to be explicit
/// about when the packet is dropped and released to the user. Since bound packets may call a
/// function that generates more I/O requests, it is important to drop the packet outside critical
/// sections.
#[repr(transparent)]
#[must_use = "please handle point of drop intentionally"]
pub struct TransferredPacket<T: PacketPerms>(BoundPacketView<T>);

impl<T: PacketPerms> Splittable for TransferredPacket<T> {
    type Bounds = T::Bounds;

    fn split_at(self, len: impl NumBounds) -> (Self, Self) {
        let (b1, b2) = self.0.split_at(len);

        (Self(b1), Self(b2))
    }

    fn len(&self) -> T::Bounds {
        self.0.view.len()
    }
}

impl<T: PacketPerms> Errorable for TransferredPacket<T> {
    fn error(self, err: Error) {
        self.0.error(err)
    }
}

/// Standard packet variations.
///
/// This is a small helper used to enable easy storage of different packet types in one place.
pub enum StandardPktVariations<Perms: PacketPerms> {
    BoundPacketView(BoundPacketView<Perms>),
    Alloced(<Perms as PacketPerms>::Alloced),
    TransferredPacket(TransferredPacket<Perms>),
}

impl<P: AllocatedPacket<Perms = PP>, PP: PacketPerms<Alloced = P>> From<P>
    for StandardPktVariations<P::Perms>
{
    fn from(p: P) -> Self {
        Self::Alloced(p)
    }
}

impl<Perms: PacketPerms> From<BoundPacketView<Perms>> for StandardPktVariations<Perms> {
    fn from(p: BoundPacketView<Perms>) -> Self {
        Self::BoundPacketView(p)
    }
}

impl<Perms: PacketPerms> From<MaybeAlloced<Perms>> for StandardPktVariations<Perms> {
    fn from(p: MaybeAlloced<Perms>) -> Self {
        match p {
            Ok(p) => Self::Alloced(p),
            Err(p) => Self::BoundPacketView(p),
        }
    }
}

impl<Perms: PacketPerms> From<AllocedOrTransferred<Perms>> for StandardPktVariations<Perms> {
    fn from(p: AllocedOrTransferred<Perms>) -> Self {
        match p {
            Ok(p) => Self::Alloced(p),
            Err(p) => Self::TransferredPacket(p),
        }
    }
}

impl<Perms: PacketPerms> From<TransferredPacket<Perms>> for StandardPktVariations<Perms> {
    fn from(p: TransferredPacket<Perms>) -> Self {
        Self::TransferredPacket(p)
    }
}

impl<Perms: PacketPerms> Errorable for StandardPktVariations<Perms> {
    fn error(self, err: Error) {
        match self {
            Self::BoundPacketView(p) => p.error(err),
            Self::Alloced(p) => p.error(err),
            Self::TransferredPacket(p) => p.error(err),
        }
    }
}

macro_rules! packet_combos {
    ($name:ident, $($perms:ident),*) => {
        /// Packet combinations.
        ///
        /// This is a small helper used to enable easy storage of different packet types in one place.
        pub enum $name {
            $($perms(StandardPktVariations<$perms>)),*
        }

        impl<P: AllocatedPacket<Perms = PP>, PP: PacketPerms<Alloced = P>> From<P> for $name where StandardPktVariations<PP>: Into<AnyPacket> {
            fn from(p: P) -> Self {
                StandardPktVariations::<PP>::from(p).into()
            }
        }

        $(
            impl From<StandardPktVariations<$perms>> for $name {
                fn from(p: StandardPktVariations<$perms>) -> Self {
                    Self::$perms(p)
                }
            }

            impl From<BoundPacketView<$perms>> for $name {
                fn from(p: BoundPacketView<$perms>) -> Self {
                    Self::$perms(p.into())
                }
            }

            impl From<MaybeAlloced<$perms>> for $name {
                fn from(p: MaybeAlloced<$perms>) -> Self {
                    Self::$perms(p.into())
                }
            }

            impl From<AllocedOrTransferred<$perms>> for $name {
                fn from(p: AllocedOrTransferred<$perms>) -> Self {
                    Self::$perms(p.into())
                }
            }

            impl From<TransferredPacket<$perms>> for $name {
                fn from(p: TransferredPacket<$perms>) -> Self {
                    Self::$perms(p.into())
                }
            }
        )*

        impl Errorable for $name {
            fn error(self, err: Error) {
                match self {
                    $(Self::$perms(p) => p.error(err),)*
                }
            }
        }
    }
}

packet_combos!(AnyPacket, Read, Write, ReadWrite);

/*macro_rules! downgrade_packet {
    ($from:expr, $to:expr) => {
        impl<'a> From<Packet<'a, { $from }>> for Packet<'a, { $to }> {
            fn from(
                Packet {
                    data,
                    start,
                    end,
                    _phantom,
                }: Packet<'a, { $from }>,
            ) -> Self {
                Self {
                    data,
                    start,
                    end,
                    _phantom,
                }
            }
        }

        impl<'a> AsRef<Packet<'a, { $to }>> for Packet<'a, { $from }> {
            fn as_ref(&self) -> &Packet<'a, { $to }> {
                unsafe { &*(self as *const Self as *const _) }
            }
        }

        impl<'a> AsMut<Packet<'a, { $to }>> for Packet<'a, { $from }> {
            fn as_mut(&mut self) -> &mut Packet<'a, { $to }> {
                unsafe { &mut *(self as *mut Self as *mut _) }
            }
        }
    };
}*/

/*downgrade_packet!(perms::READ, perms::NONE);
downgrade_packet!(perms::WRITE, perms::NONE);
downgrade_packet!(perms::READ_WRITE, perms::NONE);
downgrade_packet!(perms::READ_WRITE, perms::READ);
downgrade_packet!(perms::READ_WRITE, perms::WRITE);
*/
/*impl<'a> Packet<'a, {perms::READ}> {

}*/

/// Allows to intercept a packet between its origin packet ID.
///
/// `ReboundPacket` allows a packet to be read or written, before returning its result to the
/// origin. The packet segments which have finished their core I/O operation can be re-split into
/// smaller parts and returned to the origin with partial successes or failures.
///
/// ## Background
///
/// All parts of a bound packet must end up in their original starting position - the first place
/// they were bound. Failure to uphold this invariant may lead to deadlocks, or other unexpected
/// behavior. In addition, packet usually reaches an I/O backend, where it gets processed, before
/// being returned back to the caller. However, it is often desired to be able to do some
/// intermediary processing, or apply custom routing/splitting rules on the packet.
///
/// ### Client-server example
///
/// Let's take networked I/O backend as an example. To perform a read request, the following flow
/// happens:
///
/// 1. Caller sends an I/O packet to the client, acting as a I/O backend. The packet gets accepted,
///    read request is sent to the server.
///
/// 2. The server performs the read request, and for every segment of the packet writes the
///    response header with the packet bytes itself.
///
/// 3. The client receives individual packet segments as `(header, bytes)` pairs. For each pair the
///    client outputs the segment back to the original caller.
///
/// This is a very simple flow - client sends a request, server sends a response, client processes
/// the response, but how do you make this efficient?
///
/// For starters, the client needs to hold on to the original packet until all responses arrive.
/// Upon each response, the client needs to split a segment out, read data into it, and then return
/// it to the sender. This can actually be relatively easily solved with a simple sharded wrapper
/// around packet. Something equivalent to `BTreeMap<usize, BoundPacketObj>`. However, the
/// difficulty appears when working on the send side.
///
/// A naive way to transfer the packet over the wire is to use regular
/// [`IoWrite`](crate::traits::IoWrite) interface and await for each write to complete. However,
/// the process of awaiting for each request to finish makes the client extremely bottlenecked by
/// the time it takes to write each individual request. As far as client is concerned, completion
/// of each write operation is not important, what matters is order of operations.
///
/// [`NoPos`](crate::io::NoPos) I/O is by convention processed in the packet send order. Therefore,
/// all a client needs to do is send packets to the same packet ID, while awaiting for
/// confirmations in a separate async task, polled concurrently.
///
/// For a client write request, the one missing piece is awaiting for actual response from the
/// other end - which segment of the packet was successful, which was not. The problem is that
/// the regular [`IoWrite`](crate::traits::IoWrite) interface consumes the packet and does not let
/// you fail parts of it after the fact. A client needs to intercept the packet, and fail parts of
/// it after the fact. This is where [`ReboundPacket`] comes into play.
///
/// Upon arrival at a client, the write request would be rebound to the client's session packet ID,
/// and sent for processing through the I/O stream. At some point, the server returns back the
/// result of individual segments, which the client would then translate to actual response.
///
/// `ReboundPacket` helps in this situation, because it facilitates this rebind process safely.
pub struct ReboundPacket<T: PacketPerms> {
    ranges: RangeSet<T::Bounds>,
    orig: ManuallyDrop<BoundPacketView<T>>,
    unbound: AtomicBool,
}

impl<T: PacketPerms> ReboundPacket<T> {
    pub fn packets_in_flight(&self) -> usize {
        self.orig.view.pkt().rc() - if self.ranges.is_empty() { 0 } else { 1 }
    }

    pub fn unbound(&self) -> PacketView<'static, T> {
        assert!(!self.unbound.swap(true, Ordering::Acquire));
        // SAFETY: we are ensuring only a single unbound packet instance is created. In addition,
        // the rest of this struct ensures that all required invariants of the packet system are
        // upheld.
        unsafe { self.orig.unbound() }
    }

    /// Inform that this packet view was fully sent out.
    ///
    /// Call whenever a packet view is about to be dropped, usually in on_output handler. This will
    /// mark the packet's range as intercepted, allowing the true result of the operation to be
    /// later propagated.
    pub fn on_processed(&mut self, pkt: PacketView<'static, T>, err: Option<Error>) {
        match err {
            Some(err) => {
                let pkt = unsafe {
                    self.orig
                        .extract_packet(pkt.start - self.orig.view.start, pkt.len())
                };
                pkt.error(err);
            }
            None => {
                let start = pkt.start - self.orig.view.start;
                let end = pkt.end - self.orig.view.start;
                self.ranges.insert(start..end);
            }
        }
    }

    /// Finalize result of intercepted operation.
    ///
    /// Whenever a packet view's range is inserted in the intercepted range, it is possible to call
    /// this function to finalize the range's results.
    ///
    /// # Panics
    ///
    /// Whenever an invalid range is provided.
    pub fn range_result(&mut self, start: T::Bounds, len: T::Bounds, err: Option<Error>) {
        let range = start..(start + len);
        let mut o = self.ranges.overlapping(&range);
        let o = o.next().unwrap();
        assert!(o.contains(&start));
        assert!(o.contains(&(start + len.saturating_sub(One::one()))));
        self.ranges.remove(range);

        // SAFETY: we verified uniqueness of the range.
        let pkt = unsafe { self.orig.extract_packet(start, len) };

        // We just extracted the last packet from the range, we are not going to do it again, as
        // per the API contract, we must forget this packet right now.
        if self.ranges.is_empty() {
            let orig = unsafe { ManuallyDrop::take(&mut self.orig) };
            unsafe { orig.forget() };
        }

        if let Some(err) = err {
            pkt.error(err)
        }
    }

    pub fn ranges(&self) -> &RangeSet<T::Bounds> {
        &self.ranges
    }
}

impl<T: PacketPerms> From<BoundPacketView<T>> for ReboundPacket<T> {
    fn from(orig: BoundPacketView<T>) -> Self {
        Self {
            ranges: Default::default(),
            orig: ManuallyDrop::new(orig),
            unbound: false.into(),
        }
    }
}

impl<T: PacketPerms> Drop for ReboundPacket<T> {
    fn drop(&mut self) {
        if *self.unbound.get_mut() {
            let mut prev = None;

            for range in self.ranges.iter() {
                prev = Some(unsafe {
                    self.orig
                        .extract_packet(range.start, range.end - range.start)
                });
            }

            // Only forget if we haven't been taken before
            if prev.is_some() {
                unsafe { ManuallyDrop::take(&mut self.orig).forget() };
            }

            core::mem::drop(prev);
        } else {
            unsafe { ManuallyDrop::drop(&mut self.orig) };
        }
    }
}
