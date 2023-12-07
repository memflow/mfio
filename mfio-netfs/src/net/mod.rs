pub mod client;
pub mod server;

use serde::{Deserialize, Serialize};

use bytemuck::{Pod, Zeroable};
use core::mem::MaybeUninit;
use core::num::{NonZeroI32, NonZeroU16};
use mfio::io::{
    BoundPacketView, FullPacket, OpaqueStore, OutputFunction, PacketIo, PacketOutput, PacketView,
};
use mfio::tarc::BaseArc;
use mfio_rt::{DirEntry, DirOp, Metadata, OpenOptions};

// SAFETY: memunsafe
// We cannot have safe implementation of this, because malformed data may lead to invalid tag.
// This may lead to incorrect jumps in pattern matching.
unsafe impl Zeroable for Request {}
unsafe impl Pod for Request {}
unsafe impl Zeroable for Response {}
unsafe impl Pod for Response {}

#[repr(u8, C)]
#[derive(Debug, Clone, Copy)]
enum Request {
    Read {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        len: u64,
    },
    Write {
        file_id: u32,
        packet_id: u32,
        pos: u64,
        len: u64,
    },
    FileClose {
        file_id: u32,
    },
    ReadDir {
        // If OpenDir results in 16-bit value there is no reason to have 32-bits for read streams.
        // If we end up bottlenecked, we will first be bottlenecked by OpenDir.
        stream_id: u16,
        count: u16,
    },
    Fs {
        req_id: u32,
        dir_id: u16,
        req_len: u16,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum FsRequest {
    Path,
    ReadDir,
    OpenDir { path: String },
    OpenFile { path: String, options: OpenOptions },
    Metadata { path: String },
    DirOp(DirOp<String>),
}

#[repr(u8, C)]
#[derive(Debug, Clone, Copy)]
enum Response {
    Read {
        err: Option<NonZeroI32>,
        packet_id: u32,
        idx: u64,
        len: u64,
    },
    Write {
        err: Option<NonZeroI32>,
        packet_id: u32,
        idx: u64,
        len: u64,
    },
    ReadDir {
        stream_id: u16,
        // Highest bit being set means the stream is closing.
        len: u32,
    },
    Fs {
        req_id: u32,
        resp_len: u16,
    },
}

type ReadDirResponse = Result<DirEntry, NonZeroI32>;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum FsResponse {
    Path {
        path: Result<String, NonZeroI32>,
    },
    ReadDir {
        stream_id: Result<u16, NonZeroI32>,
    },
    OpenDir {
        dir_id: Result<NonZeroU16, NonZeroI32>,
    },
    OpenFile {
        file_id: Result<u32, NonZeroI32>,
    },
    Metadata {
        metadata: Result<Metadata, NonZeroI32>,
    },
    DirOp(Option<NonZeroI32>),
}

use mfio::error::Error;
use mfio::io::{NoPos, Packet, PacketPerms, Read, ReboundPacket};
use parking_lot::Mutex;
use slab::Slab;

#[derive(Clone, Copy, Debug)]
enum Key {
    Header(u64),
    Obj(u64),
}

impl From<u64> for Key {
    fn from(raw: u64) -> Self {
        if raw & 1 != 0 {
            Self::Obj(raw >> 1)
        } else {
            Self::Header(raw >> 1)
        }
    }
}

impl Key {
    fn idx(self) -> u64 {
        match self {
            Self::Header(v) => v,
            Self::Obj(v) => v,
        }
    }

    fn key(self) -> u64 {
        match self {
            Self::Header(v) => v << 1,
            Self::Obj(v) => (v << 1) | 1,
        }
    }
}

enum RoutedData<Perms: PacketPerms> {
    Pkt(ReboundPacket<Perms>),
    Bytes { buffer: BaseArc<Packet<Perms>> },
    None,
}

struct RoutedObj<T: Copy, Perms: PacketPerms> {
    obj: RoutedData<Perms>,
    header: BaseArc<FullPacket<T, Perms>>,
}

impl<T: bytemuck::Pod> RoutedObj<T, Read> {
    fn header_pkt(&self) -> BaseArc<Packet<Read>> {
        self.header.clone().transpose().into_base().unwrap()
        //Packet::from(bytemuck::bytes_of(&this.header)).tag(Key::Header(this.idx).key())
    }

    fn data_pkt(&self) -> Option<PacketView<'static, Read>> {
        match &self.obj {
            RoutedData::Pkt(p) => Some(p.unbound()),
            RoutedData::Bytes { buffer, .. } => Some(PacketView::from_arc(buffer.clone(), 0)),
            RoutedData::None => None,
        }
    }

    pub fn is_fully_processed(&self) -> bool {
        if self.header.rc() > 0 {
            return false;
        }

        let obj = &self.obj;
        match obj {
            RoutedData::Pkt(p) => p.ranges().is_empty() && p.packets_in_flight() == 0,
            RoutedData::Bytes { buffer } => {
                let rc = buffer.rc();
                rc == 0
            }
            RoutedData::None => true,
        }
    }
}

pub struct HeaderRouterState<T: Copy, Perms: PacketPerms> {
    handles: Slab<RoutedObj<T, Perms>>,
    spares: Vec<RoutedObj<T, Perms>>,
}

impl<T: Copy, Perms: PacketPerms> Default for HeaderRouterState<T, Perms> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            spares: vec![],
        }
    }
}

pub struct HeaderRouter<'a, T: Copy, Perms: PacketPerms, Io> {
    io: &'a Io,
    state: BaseArc<Mutex<HeaderRouterState<T, Perms>>>,
    output: BaseArc<PacketOutput<Perms>>,
}

impl<'a, T: bytemuck::Pod + Send + Sync, Io: PacketIo<Read, NoPos>> HeaderRouter<'a, T, Read, Io> {
    pub fn new(io: &'a Io) -> Self {
        let state = BaseArc::new(Mutex::new(HeaderRouterState::default()));

        let output = BaseArc::new(OutputFunction::new({
            let state = state.clone();
            move |view, err| {
                let key = Key::from(view.tag());
                let mut state = state.lock();
                let handle = state.handles.get_mut(key.idx() as usize).unwrap();
                match key {
                    Key::Header(_) => {}
                    Key::Obj(_) => {
                        // SAFETY: we are not moving the data
                        let obj = &mut handle.obj;
                        if let RoutedData::Pkt(p) = obj {
                            p.on_processed(view, err);
                        }
                    }
                }

                if handle.is_fully_processed() {
                    state.handles.remove(key.idx() as usize);
                }
            }
        }))
        .transpose()
        .into_base()
        .unwrap();

        Self { io, state, output }
    }

    fn send(&self, header: impl FnOnce(usize) -> T, in_obj: RoutedData<Read>) -> usize {
        let state = &mut *self.state.lock();
        let entry = state.handles.vacant_entry();
        let idx = entry.key();
        let header = header(idx);

        let mut obj = state
            .spares
            .pop()
            .map(|v| {
                let hdr = v.header;

                unsafe { hdr.reset_err() };
                let header = bytemuck::bytes_of(&header);
                // SAFETY: we are converting a slice of initialized bytes to a slice of
                // uninitialized bytes.
                let header = unsafe { core::mem::transmute::<&[u8], &[MaybeUninit<u8>]>(header) };
                unsafe { hdr.simple_slice_mut().unwrap() }.copy_from_slice(header);

                RoutedObj {
                    obj: v.obj,
                    header: hdr,
                }
            })
            .unwrap_or_else(|| RoutedObj {
                obj: RoutedData::None,
                header: FullPacket::new(header).into(),
            });

        obj.obj = in_obj;

        {
            let pkt = obj.header_pkt();
            let pv = PacketView::from_arc(pkt, Key::Header(idx as u64).key());
            let bpv = unsafe { pv.bind(Some(self.output.clone().into())) };
            self.io.send_io(NoPos::new(), bpv);

            if let Some(mut pv) = obj.data_pkt() {
                pv.set_tag(Key::Obj(idx as u64).key());
                let bpv = unsafe { pv.bind(Some(self.output.clone().into())) };
                self.io.send_io(NoPos::new(), bpv);
            }
        }

        entry.insert(obj);

        idx
    }

    /// Sends a packet, returns key for completion processing.
    ///
    /// The returned key needs to be used once results are received from server side marking state
    /// of success of various ranges of packets.
    pub fn send_pkt(&self, header: impl FnOnce(usize) -> T, pkt: BoundPacketView<Read>) -> usize {
        self.send(header, RoutedData::Pkt(pkt.into()))
    }

    pub fn send_bytes<P: 'static + OpaqueStore<HeapReq = BaseArc<impl AsRef<Packet<Read>>>>>(
        &self,
        header: impl FnOnce(usize) -> T,
        buffer: P,
    ) {
        self.send(
            header,
            RoutedData::Bytes {
                buffer: buffer.heap().transpose().into_base().unwrap(),
            },
        );
    }

    pub fn send_hdr(&self, header: impl FnOnce(usize) -> T) {
        self.send(header, RoutedData::None);
    }

    pub fn pkt_result(&self, idx: usize, start: u64, len: u64, res: Option<Error>) -> bool {
        let mut state = self.state.lock();
        let handle = if let Some(v) = state.handles.get_mut(idx) {
            v
        } else {
            return true;
        };

        let obj = &mut handle.obj;

        // TODO: do we warn if this condition doesn't match?
        if let RoutedData::Pkt(p) = obj {
            p.range_result(start, len, res);
        }

        if handle.is_fully_processed() {
            let mut obj = state.handles.remove(idx);
            obj.obj = RoutedData::None;
            state.spares.push(obj);
            true
        } else {
            false
        }
    }
}
