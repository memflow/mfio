use super::DeferredPackets;
use core::ops::Range;
use mfio::error::*;
use mfio::io::*;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::io::{IoSlice, IoSliceMut};

const EOF: Error = Error {
    code: Code::from_http_const(503),
    subject: Subject::Io,
    state: State::Nop,
    location: Location::Other,
};

// 2MB
const DEFAULT_SIZE: usize = 0x200000;

trait StaticConv {
    type This<'a>: 'a;

    unsafe fn static_conv_ref<'a, 'b>(inp: &'b Self::This<'static>) -> &'b Self::This<'a> {
        core::mem::transmute(inp)
    }

    unsafe fn static_conv_mut<'a, 'b>(inp: &'b mut Self::This<'static>) -> &'b mut Self::This<'a> {
        core::mem::transmute(inp)
    }
}

impl StaticConv for IoSlice<'static> {
    type This<'b> = IoSlice<'b>;
}

impl StaticConv for IoSliceMut<'static> {
    type This<'b> = IoSliceMut<'b>;
}

impl<T: StaticConv> StaticConv for Vec<T> {
    type This<'a> = Vec<T::This<'a>>;
}

struct CircularBuf {
    buf: *mut u8,
    layout: Layout,
    head: usize,
    len: usize,
}

unsafe impl Send for CircularBuf {}
unsafe impl Sync for CircularBuf {}

impl Drop for CircularBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.buf, self.layout) }
    }
}

impl CircularBuf {
    // Returns the buffers after the tail and before the head
    pub fn spare_bufs(&mut self) -> (&mut [u8], &mut [u8]) {
        let (a_range, b_range) = self.spare_slice_ranges();
        // SAFETY: `slice_ranges` always returns valid ranges into
        // the physical buffer.
        unsafe {
            (
                &mut *self.buffer_range(a_range),
                &mut *self.buffer_range(b_range),
            )
        }
    }

    // Returns the buffers that have been fully reserved
    pub fn bufs(&mut self) -> (&mut [u8], &mut [u8]) {
        let (a_range, b_range) = self.slice_ranges();
        // SAFETY: `slice_ranges` always returns valid ranges into
        // the physical buffer.
        unsafe {
            (
                &mut *self.buffer_range(a_range),
                &mut *self.buffer_range(b_range),
            )
        }
    }

    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    fn ptr(&self) -> *mut u8 {
        self.buf
    }

    /// Returns the index in the underlying buffer for a given logical element index.
    #[inline]
    fn wrap_index(logical_index: usize, capacity: usize) -> usize {
        debug_assert!(
            (logical_index == 0 && capacity == 0)
                || logical_index < capacity
                || (logical_index - capacity) < capacity
        );
        if logical_index >= capacity {
            logical_index - capacity
        } else {
            logical_index
        }
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        Self::wrap_index(idx.wrapping_add(addend), self.capacity())
    }

    /// Returns a slice pointer into the buffer.
    /// `range` must lie inside `0..self.capacity()`.
    #[inline]
    unsafe fn buffer_range(&self, range: Range<usize>) -> *mut [u8] {
        unsafe {
            std::ptr::slice_from_raw_parts_mut(self.ptr().add(range.start), range.end - range.start)
        }
    }

    #[inline]
    fn to_physical_idx(&self, idx: usize) -> usize {
        self.wrap_add(self.head, idx)
    }

    /// Given a range into the logical buffer of the deque, this function
    /// return two ranges into the physical buffer that correspond to
    /// the given range. The `len` parameter should usually just be `self.len`;
    /// the reason it's passed explicitly is that if the deque is wrapped in
    /// a `Drain`, then `self.len` is not actually the length of the deque.
    ///
    /// # Safety
    ///
    /// This function is always safe to call. For the resulting ranges to be valid
    /// ranges into the physical buffer, the caller must ensure that the result of
    /// calling `slice::range(range, ..len)` represents a valid range into the
    /// logical buffer, and that all elements in that range are initialized.
    pub fn slice_ranges(&self) -> (Range<usize>, Range<usize>) {
        if self.len == 0 {
            (0..0, 0..0)
        } else {
            let wrapped_start = self.to_physical_idx(0);

            let head_len = self.capacity() - wrapped_start;

            if head_len >= self.len {
                (wrapped_start..wrapped_start + self.len, 0..0)
            } else {
                // can't overflow because of the if condition
                let tail_len = self.len - head_len;
                (wrapped_start..self.capacity(), 0..tail_len)
            }
        }
    }

    pub fn spare_slice_ranges(&self) -> (Range<usize>, Range<usize>) {
        if self.len == 0 {
            (0..self.capacity(), 0..0)
        } else if self.head + self.len < self.capacity() {
            ((self.head + self.len)..self.capacity(), 0..self.head)
        } else {
            (0..self.head, 0..0)
        }
    }

    pub fn reserve(&mut self, len: usize) {
        if self.len + len > self.capacity() {
            panic!(
                "Not enough space for the reservation {:x} {len:x}",
                self.len
            );
        }
        self.len += len;
    }

    pub fn release(&mut self, len: usize) {
        if len == 0 {
            panic!();
        }

        if self.len < len {
            panic!("Too much memory wanted to be freed");
        }

        self.len -= len;
        if self.len == 0 {
            // NOTE: this is very important! If len == 0, then spare_slice_ranges returns the whole
            // buffer. If head is not 0 in such condition, then you may perform reads to the wrong
            // offset.
            self.head = 0;
        } else {
            self.head = (self.head + len) % self.capacity();
        }
    }
}

impl Default for CircularBuf {
    fn default() -> Self {
        let layout = Layout::from_size_align(DEFAULT_SIZE, 64).unwrap();
        let buf = unsafe { alloc(layout) };
        Self {
            buf,
            layout,
            head: 0,
            len: 0,
        }
    }
}

enum WriteOp {
    Alloced(<Read as PacketPerms>::Alloced),
    Unalloced {
        transferred: VecDeque<TransferredPacket<Read>>,
        queued: Option<BoundPacketView<Read>>,
    },
}

#[derive(Default)]
pub struct StreamBuf {
    read_buf: CircularBuf,
    read_cached: usize,
    read_ops1: VecDeque<MaybeAlloced<Write>>,
    read_ops2: VecDeque<MaybeAlloced<Write>>,
    read_queue: Vec<IoSliceMut<'static>>,
    write_ops: VecDeque<WriteOp>,
    write_ops_cache: Vec<VecDeque<TransferredPacket<Read>>>,
    write_buf: CircularBuf,
    write_queue: Vec<IoSlice<'static>>,
    eof_read: bool,
    eof_write: bool,
}

impl StreamBuf {
    pub fn queue_read(
        &mut self,
        mut packet: BoundPacketView<Write>,
        mut deferred_pkts: Option<&mut DeferredPackets>,
    ) {
        // Ignore 0 byte requests. Letting such request in leads to livelock
        if packet.is_empty() {
            if let Some(v) = deferred_pkts {
                v.ok(packet)
            }
            return;
        }

        if self.eof_read {
            log::trace!("Reached eof read");
            if let Some(d) = deferred_pkts {
                d.error(packet, EOF);
            } else {
                packet.error(EOF);
            }
            return;
        }

        log::trace!("Queue read {:x}", packet.len());
        // first try to clear out cached reads
        while self.read_cached > 0 {
            let spare = self.read_buf.bufs().0;
            let spare_len = core::cmp::min(spare.len(), self.read_cached);

            if (spare_len as u64) < packet.len() {
                let (a, b) = packet.split_at(spare_len as u64);
                let transferred = unsafe { a.transfer_data(spare.as_mut_ptr().cast()) };
                self.read_buf.release(transferred.len() as usize);
                self.read_cached -= transferred.len() as usize;
                if let Some(v) = deferred_pkts.as_mut() {
                    v.ok(transferred)
                }
                packet = b;
            } else {
                let transferred = unsafe { packet.transfer_data(spare.as_mut_ptr().cast()) };
                self.read_buf.release(transferred.len() as usize);
                self.read_cached -= transferred.len() as usize;
                if let Some(v) = deferred_pkts.as_mut() {
                    v.ok(transferred)
                }
                log::trace!("Cached read done :)");
                return;
            }
        }

        self.read_ops1.push_back(packet.try_alloc());
    }

    pub fn read_ops(&self) -> usize {
        self.read_ops1.len() + self.read_ops2.len()
    }

    pub fn write_ops(&self) -> usize {
        self.write_ops.len()
    }

    pub fn queue_write(
        &mut self,
        packet: BoundPacketView<Read>,
        deferred_pkts: Option<&mut DeferredPackets>,
    ) {
        if packet.is_empty() {
            if let Some(v) = deferred_pkts {
                v.ok(packet)
            }
            return;
        }

        if self.eof_write {
            log::trace!("Reached eof write");
            if let Some(d) = deferred_pkts {
                d.error(packet, EOF);
            } else {
                packet.error(EOF);
            }
            return;
        }

        log::trace!("Queue write {:x}", packet.len());
        match packet.try_alloc() {
            Ok(alloced) => self.write_ops.push_back(WriteOp::Alloced(alloced)),
            Err(packet) => {
                let transferred = self.write_ops_cache.pop().unwrap_or_default();
                assert!(transferred.is_empty());
                self.write_ops.push_back(WriteOp::Unalloced {
                    transferred,
                    queued: Some(packet),
                });
            }
        }
    }

    pub fn on_read(
        &mut self,
        res: std::io::Result<usize>,
        mut deferred_pkts: Option<&mut DeferredPackets>,
    ) {
        match res {
            Ok(0) => {
                log::trace!("Read EOF");
                self.eof_read = true;

                for v in self.read_ops2.drain(..) {
                    if let Some(ref mut d) = deferred_pkts {
                        d.error(v, EOF);
                    } else {
                        v.error(EOF);
                    }
                }

                for v in self.read_ops1.drain(..) {
                    if let Some(ref mut d) = deferred_pkts {
                        d.error(v, EOF);
                    } else {
                        v.error(EOF);
                    }
                }
            }
            Ok(mut len) => {
                log::trace!("Read {len:x}");
                while len > 0 {
                    if let Some(packet) = self.read_ops2.pop_front() {
                        let packet = if len as u64 >= packet.len() {
                            packet
                        } else {
                            let (a, b) = packet.split_at(len as u64);
                            self.read_ops2.push_front(b);
                            a
                        };

                        len -= packet.len() as usize;

                        match packet {
                            Ok(v) => {
                                if let Some(d) = deferred_pkts.as_mut() {
                                    d.ok(v)
                                }
                            }
                            Err(v) => {
                                let buf = self.read_buf.bufs().0;
                                assert!(buf.len() as u64 >= v.len());
                                let transferred = unsafe { v.transfer_data(buf.as_ptr().cast()) };
                                self.read_buf.release(transferred.len() as usize);
                                if let Some(d) = deferred_pkts.as_mut() {
                                    d.ok(transferred)
                                }
                            }
                        }
                    } else {
                        self.read_buf.reserve(len);
                        self.read_cached += len;
                        len = 0;
                    }
                }
            }
            Err(e) => {
                let e = Error::from(e);
                // TODO: decide on whether we want to fail just one request, or all of them
                while let Some(packet) = self.read_ops2.pop_front() {
                    if let Some(ref mut d) = deferred_pkts {
                        d.error(packet, e);
                    } else {
                        packet.error(e);
                    }
                }

                while let Some(packet) = self.read_ops1.pop_front() {
                    if let Some(ref mut d) = deferred_pkts {
                        d.error(packet, e);
                    } else {
                        packet.error(e);
                    }
                }
            }
        }
    }

    pub fn on_write(
        &mut self,
        res: std::io::Result<usize>,
        mut deferred_pkts: Option<&mut DeferredPackets>,
    ) {
        match res {
            Ok(0) => {
                self.eof_write = true;
                for v in self.write_ops.drain(..) {
                    match v {
                        WriteOp::Alloced(v) => {
                            if let Some(ref mut d) = deferred_pkts {
                                d.error(v, EOF);
                            } else {
                                v.error(EOF);
                            }
                        }
                        WriteOp::Unalloced {
                            mut transferred,
                            queued,
                        } => {
                            for v in transferred.drain(..) {
                                if let Some(ref mut d) = deferred_pkts {
                                    d.error(v, EOF);
                                } else {
                                    v.error(EOF);
                                }
                            }
                            self.write_ops_cache.push(transferred);
                            if let Some(v) = queued {
                                if let Some(ref mut d) = deferred_pkts {
                                    d.error(v, EOF);
                                } else {
                                    v.error(EOF);
                                }
                            }
                        }
                    }
                }
            }
            Ok(mut len) => {
                while len > 0 {
                    match self
                        .write_ops
                        .pop_front()
                        .expect("written more than total length of ops!")
                    {
                        WriteOp::Alloced(pkt) => {
                            let pkt_read = core::cmp::min(pkt.len(), len as u64);
                            len -= pkt_read as usize;

                            if pkt_read < pkt.len() {
                                let (p, b) = pkt.split_at(pkt_read);
                                if let Some(v) = deferred_pkts.as_mut() {
                                    v.ok(p)
                                }
                                self.write_ops.push_front(WriteOp::Alloced(b));
                                break;
                            }
                        }
                        WriteOp::Unalloced {
                            mut transferred,
                            queued,
                        } => {
                            while let Some(pkt) = transferred.pop_front() {
                                let pkt_read = core::cmp::min(pkt.len(), len as u64);
                                len -= pkt_read as usize;
                                self.write_buf.release(pkt_read as usize);

                                if pkt_read < pkt.len() {
                                    let (p, b) = pkt.split_at(pkt_read);
                                    if let Some(v) = deferred_pkts.as_mut() {
                                        v.ok(p)
                                    }
                                    transferred.push_front(b);
                                    break;
                                }
                            }

                            if len > 0 && queued.is_some() {
                                panic!("overflow into queued ops!");
                            }

                            if !transferred.is_empty() || queued.is_some() {
                                self.write_ops.push_front(WriteOp::Unalloced {
                                    transferred,
                                    queued,
                                });
                                break;
                            } else {
                                self.write_ops_cache.push(transferred);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let e = Error::from(e);
                // TODO: decide on whether we want to fail just one request, or all of them
                while let Some(packet) = self.write_ops.pop_front() {
                    match packet {
                        WriteOp::Alloced(v) => {
                            if let Some(ref mut d) = deferred_pkts {
                                d.error(v, e);
                            } else {
                                v.error(e);
                            }
                        }
                        WriteOp::Unalloced {
                            mut transferred,
                            queued,
                        } => {
                            while let Some(pkt) = transferred.pop_front() {
                                self.write_buf.release(pkt.len() as usize);
                                if let Some(ref mut d) = deferred_pkts {
                                    d.error(pkt, e);
                                } else {
                                    pkt.error(e);
                                }
                            }

                            self.write_ops_cache.push(transferred);

                            if let Some(pkt) = queued {
                                if let Some(ref mut d) = deferred_pkts {
                                    d.error(pkt, e);
                                } else {
                                    pkt.error(e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn read_queue(&mut self) -> &mut [IoSliceMut<'_>] {
        let queue = &mut self.read_queue;
        queue.clear();
        // SAFETY: we have cleared the queue - old elements do not escape their lifetime bounds
        // In addition, we do not use this vec anywhere else.
        let queue = unsafe { Vec::<IoSliceMut>::static_conv_mut(queue) };

        let mut cur_head = self.read_buf.head;

        // first, reserve enough space for operations within the cache
        while let Some(pkt) = self.read_ops1.pop_front() {
            match pkt {
                Ok(pkt) => self.read_ops2.push_back(Ok(pkt)),
                Err(mut pkt) => loop {
                    let spare = self.read_buf.spare_bufs().0;
                    let spare_len = spare.len();

                    if spare.is_empty() {
                        self.read_ops1.push_front(Err(pkt));
                        break;
                    } else if (spare_len as u64) < pkt.len() {
                        let (a, b) = pkt.split_at(spare_len as u64);
                        self.read_buf.reserve(spare_len);
                        self.read_ops2.push_back(Err(a));
                        pkt = b;
                    } else {
                        self.read_buf.reserve(pkt.len() as usize);
                        self.read_ops2.push_back(Err(pkt));
                        break;
                    }
                },
            }
        }

        // second, queue up all non-cache reads
        for pkt in self.read_ops2.iter_mut() {
            let buf = match pkt {
                Ok(pkt) => {
                    // SAFETY: assume MaybeUninit<u8> is initialized,
                    // as God intended :upside_down:
                    unsafe {
                        let ptr = pkt.as_mut_ptr();
                        let len = pkt.len();
                        core::slice::from_raw_parts_mut(ptr as *mut u8, len as usize)
                    }
                }
                Err(pkt) => {
                    let range = (cur_head)..(cur_head + pkt.len() as usize);
                    cur_head = (cur_head + pkt.len() as usize) % self.read_buf.capacity();
                    // SAFETY: we are grabbing exclusive access to the range
                    unsafe { &mut *self.read_buf.buffer_range(range) }
                }
            };
            queue.push(IoSliceMut::new(buf));
        }

        // then, queue up cache reads
        // TODO: make use of this configurable
        let (a, b) = self.read_buf.spare_bufs();
        if !a.is_empty() {
            queue.push(IoSliceMut::new(a));
            if !b.is_empty() {
                queue.push(IoSliceMut::new(b));
            }
        }

        // TODO: add clear guard
        queue
    }

    pub fn write_queue(&mut self) -> &[IoSlice] {
        let queue = &mut self.write_queue;
        queue.clear();
        // SAFETY: we have cleared the queue - old elements do not escape their lifetime bounds
        // In addition, we do not use this vec anywhere else.
        let queue = unsafe { Vec::<IoSlice>::static_conv_mut(queue) };

        let mut cur_head = self.write_buf.head;

        for op in self.write_ops.iter_mut() {
            match op {
                WriteOp::Alloced(pkt) => {
                    queue.push(IoSlice::new(pkt));
                }
                WriteOp::Unalloced {
                    transferred,
                    queued,
                } => {
                    while let Some(pkt) = queued.take() {
                        let spare = self.write_buf.spare_bufs().0;
                        let spare_len = spare.len();

                        if spare.is_empty() {
                            *queued = Some(pkt);
                            break;
                        } else if (spare_len as u64) < pkt.len() {
                            let (a, b) = pkt.split_at(spare_len as u64);
                            let pkt = unsafe { a.transfer_data(spare.as_mut_ptr().cast()) };
                            self.write_buf.reserve(spare_len);
                            transferred.push_back(pkt);
                            *queued = Some(b);
                        } else {
                            let pkt = unsafe { pkt.transfer_data(spare.as_mut_ptr().cast()) };
                            self.write_buf.reserve(transferred.len());
                            transferred.push_back(pkt);
                        }
                    }

                    for pkt in transferred {
                        let range = (cur_head)..(cur_head + pkt.len() as usize);
                        cur_head = (cur_head + pkt.len() as usize) % self.write_buf.capacity();
                        // SAFETY: we are grabbing exclusive access to the range
                        let buf = unsafe { &*self.write_buf.buffer_range(range) };
                        queue.push(IoSlice::new(buf))
                    }

                    if queued.is_some() {
                        break;
                    }
                }
            };
        }

        // TODO: add clear guard
        queue
    }
}
