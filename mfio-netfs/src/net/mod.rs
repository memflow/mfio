pub mod client;
pub mod server;

pub use client::NetworkFs;

use serde::{Deserialize, Serialize};

use bytemuck::{Pod, Zeroable};
use core::num::NonZeroI32;
use mfio_fs::OpenOptions;

unsafe impl Zeroable for Request {}
unsafe impl Pod for Request {}
unsafe impl Zeroable for Response {}
unsafe impl Pod for Response {}

#[repr(u8, C)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum Request {
    Read {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        len: usize,
    },
    Write {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        len: usize,
    },
    FileOpen {
        req_id: u32,
        options: OpenOptions,
        path_len: u32,
    },
    FileClose {
        file_id: u32,
    },
}

#[repr(u8, C)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum Response {
    Read {
        packet_id: u32,
        idx: usize,
        len: usize,
        err: Option<NonZeroI32>,
    },
    Write {
        packet_id: u32,
        idx: usize,
        len: usize,
        err: Option<NonZeroI32>,
    },
    FileOpen {
        req_id: u32,
        err: Option<NonZeroI32>,
        file_id: u32,
    },
}

use core::pin::Pin;
use futures::{pin_mut, stream::StreamExt};
use mfio::error::Error;
use mfio::heap::{AllocHandle, PinHeap, Release};
use mfio::packet::{BoundPacket, NoPos, Packet, PacketId, PacketPerms, Read, ReboundPacket};
use mfio::tarc::Arc;
use parking_lot::Mutex;
use slab::Slab;

#[derive(Clone, Copy, Debug)]
enum Key {
    Header(usize),
    Obj(usize),
}

impl From<usize> for Key {
    fn from(raw: usize) -> Self {
        if raw & 1 != 0 {
            Self::Obj(raw >> 1)
        } else {
            Self::Header(raw >> 1)
        }
    }
}

impl Key {
    fn idx(self) -> usize {
        match self {
            Self::Header(v) => v,
            Self::Obj(v) => v,
        }
    }

    fn key(self) -> usize {
        match self {
            Self::Header(v) => v << 1,
            Self::Obj(v) => (v << 1) | 1,
        }
    }
}

enum RoutedData<'a, Perms: PacketPerms> {
    Pkt(ReboundPacket<'a, Perms>),
    Bytes { buffer: Box<[u8]>, processed: usize },
    None,
}

struct RoutedObj<'a, T: Copy, Perms: PacketPerms> {
    idx: usize,
    obj: RoutedData<'a, Perms>,
    header: T,
}

impl<'a, T: Copy, Perms: PacketPerms> Release for RoutedObj<'a, T, Perms> {
    fn release(&mut self) {
        self.obj = RoutedData::None;
    }
}

impl<'a, T: bytemuck::Pod> RoutedObj<'a, T, Read> {
    fn header_pkt(self: Pin<&'a Self>) -> Packet<'a, Read> {
        let this = self.get_ref();
        Packet::from(bytemuck::bytes_of(&this.header)).tag(Key::Header(this.idx).key())
    }

    fn data_pkt(self: Pin<&'a Self>) -> Option<Packet<'a, Read>> {
        let this = self.get_ref();
        let pkt = match &this.obj {
            RoutedData::Pkt(p) => Some(p.unbound()),
            RoutedData::Bytes { buffer, .. } => Some(buffer.into()),
            RoutedData::None => None,
        };
        pkt.map(|p| p.tag(Key::Obj(this.idx).key()))
    }
}

struct RoutedHandle<'a, T: Copy, Perms: PacketPerms> {
    obj: Pin<AllocHandle<RoutedObj<'a, T, Perms>>>,
    got_header: usize,
}

impl<'a, T: Copy, Perms: PacketPerms> RoutedHandle<'a, T, Perms> {
    pub fn is_fully_processed(&self) -> bool {
        if self.got_header < core::mem::size_of::<T>() {
            return false;
        }

        let obj = &self.obj.as_ref().obj;
        match obj {
            RoutedData::Pkt(p) => p.ranges().is_empty(),
            RoutedData::Bytes { processed, buffer } => *processed >= buffer.len(),
            RoutedData::None => true,
        }
    }
}

pub struct HeaderRouter<'a, T: Copy, Perms: PacketPerms> {
    packets: Arc<PinHeap<RoutedObj<'a, T, Perms>>>,
    handles: Mutex<Slab<RoutedHandle<'a, T, Perms>>>,
    //id: PacketId<'a, Perms, Param>,
}

impl<'a, T: bytemuck::Pod> HeaderRouter<'a, T, Read> {
    pub fn new() -> Self {
        Self {
            packets: PinHeap::new(0).into(),
            handles: Default::default(),
        }
    }

    fn send(
        &self,
        id: Pin<&PacketId<'a, Read, NoPos>>,
        header: impl FnOnce(usize) -> T,
        obj: RoutedData<'a, Read>,
    ) -> usize {
        let mut handles = self.handles.lock();
        let entry = handles.vacant_entry();
        let idx = entry.key();
        let header = header(idx);

        let obj = PinHeap::alloc_pin(self.packets.clone(), RoutedObj { idx, header, obj });

        {
            unsafe fn extend_lifetime<'b, T>(inp: Pin<&T>) -> Pin<&'b T> {
                core::mem::transmute(inp)
            }

            let obj = obj.as_ref();
            // SAFETY: ownership is tracked manually by the entry flags
            let obj = unsafe { extend_lifetime(obj) };
            id.send_io(NoPos::new(), RoutedObj::header_pkt(obj));
            if let Some(pkt) = RoutedObj::data_pkt(obj) {
                id.send_io(NoPos::new(), pkt);
            }
        }

        let handle = RoutedHandle { obj, got_header: 0 };

        entry.insert(handle);

        idx
    }

    /// Sends a packet, returns key for completion processing.
    ///
    /// The returned key needs to be used once results are received from server side marking state
    /// of success of various ranges of packets.
    pub fn send_pkt(
        &self,
        id: Pin<&PacketId<'a, Read, NoPos>>,
        header: impl FnOnce(usize) -> T,
        pkt: BoundPacket<'static, Read>,
    ) -> usize {
        // SAFETY: bound packet is safe to have its lifetime shrunk
        let pkt = unsafe { core::mem::transmute::<_, BoundPacket<'a, Read>>(pkt) };
        self.send(id, header, RoutedData::Pkt(pkt.into()))
    }

    pub fn send_bytes(
        &self,
        id: Pin<&PacketId<'a, Read, NoPos>>,
        header: impl FnOnce(usize) -> T,
        buffer: Box<[u8]>,
    ) {
        self.send(
            id,
            header,
            RoutedData::Bytes {
                buffer,
                processed: 0,
            },
        );
    }

    pub fn send_hdr(&self, id: Pin<&PacketId<'a, Read, NoPos>>, header: impl FnOnce(usize) -> T) {
        self.send(id, header, RoutedData::None);
    }

    pub fn pkt_result(&self, idx: usize, start: usize, len: usize, res: Option<Error>) -> bool {
        let mut handles = self.handles.lock();
        let handle = if let Some(v) = handles.get_mut(idx) {
            v
        } else {
            return true;
        };

        // SAFETY: we are not moving the data
        let obj = unsafe { &mut handle.obj.as_mut().get_unchecked_mut().obj };

        // TODO: do we warn if this condition doesn't match?
        if let RoutedData::Pkt(p) = obj {
            p.range_result(start, len, res);
        }

        if handle.is_fully_processed() {
            handles.remove(idx);
            true
        } else {
            false
        }
    }

    pub async fn process_loop(&self, id: Pin<&PacketId<'a, Read, NoPos>>) {
        // Hack: we bind a packet to never receive Ready(None) from the stream. This packet will
        // never be sent out to the I/O backend.
        let bound_guard = id.bind_packet(&[]);
        let id = id.get_ref();
        pin_mut!(id);

        while let Some((pkt, err)) = id.next().await {
            let key = Key::from(pkt.tag);
            let mut handles = self.handles.lock();
            let handle = handles.get_mut(key.idx()).unwrap();
            match key {
                Key::Header(_) => {
                    handle.got_header += pkt.len();
                }
                Key::Obj(_) => {
                    // SAFETY: we are not moving the data
                    let obj = unsafe { &mut handle.obj.as_mut().get_unchecked_mut().obj };
                    match obj {
                        RoutedData::Pkt(p) => p.on_processed(pkt, err),
                        RoutedData::Bytes { processed, .. } => *processed += pkt.len(),
                        RoutedData::None => (),
                    }
                }
            }

            if handle.is_fully_processed() {
                handles.remove(key.idx());
            }
        }
        core::mem::drop(bound_guard)
    }
}
