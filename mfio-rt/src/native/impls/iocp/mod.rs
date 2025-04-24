use core::cell::UnsafeCell;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::Poll;
use core::task::Waker;
use force_send_sync::SendSync;
use log::*;
use mfio::backend::handle::{EventWaker, EventWakerOwner};
use mfio::backend::*;
use mfio::error::State;
use mfio::io::{Read as RdPerm, Write as WrPerm, *};
use mfio::mferr;
use mfio::tarc::BaseArc;
use parking_lot::Mutex;
use slab::Slab;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::windows::io::{
    AsRawHandle, AsRawSocket, FromRawHandle, FromRawSocket, IntoRawHandle, IntoRawSocket,
    OwnedHandle, OwnedSocket,
};

use super::windows_extra::{bind_any, connect_ex, new_ip_socket, CSockAddr};
use crate::util::{from_io_error, io_err, DeferredPackets, Key, RawBox};

use ::windows::core::Error;
use ::windows::Win32::Foundation::{
    CloseHandle, ERROR_HANDLE_EOF, ERROR_INVALID_USER_BUFFER, ERROR_IO_INCOMPLETE,
    ERROR_IO_PENDING, ERROR_NOT_ENOUGH_MEMORY, ERROR_NO_DATA, HANDLE, INVALID_HANDLE_VALUE, TRUE,
    WIN32_ERROR,
};
use ::windows::Win32::Networking::WinSock::{
    setsockopt, AcceptEx, WSAGetLastError, WSARecv, WSASend, SOCKADDR, SOCKET, SOCK_STREAM,
    SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, WSABUF,
};
use ::windows::Win32::Storage::FileSystem::{
    ReadFile, SetFileCompletionNotificationModes, WriteFile,
};
use ::windows::Win32::System::WindowsProgramming::{
    FILE_SKIP_COMPLETION_PORT_ON_SUCCESS, FILE_SKIP_SET_EVENT_ON_HANDLE,
};
use ::windows::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatusEx,
    OVERLAPPED, OVERLAPPED_ENTRY,
};

mod file;
mod tcp_listener;
mod tcp_stream;

use tcp_listener::{ListenerInner, TcpAccept};
use tcp_stream::{StreamInner, TcpConnect};

pub use file::FileWrapper;
pub use tcp_listener::TcpListener;
pub use tcp_stream::{TcpConnectFuture, TcpStream};

fn expand_error(e: Error) -> WIN32_ERROR {
    let code = if e.code().0 == 0 {
        Error::from_win32().code().0
    } else {
        e.code().0
    };

    WIN32_ERROR((code & 0xFFFF) as _)
}

fn socket_accepted(
    s: usize,
    l: SOCKET,
    streams: &mut Slab<StreamInner>,
) -> Result<usize, mfio::error::Error> {
    if unsafe {
        setsockopt(
            SOCKET(streams.get_mut(s).unwrap().socket.as_raw_socket() as _),
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            Some(&l.0.to_ne_bytes()),
        )
    } != 0
    {
        Err(mferr!(500, Io, Other, Network))
    } else {
        Ok(s)
    }
}

fn socket_connected(
    s: usize,
    streams: &mut Slab<StreamInner>,
) -> Result<usize, mfio::error::Error> {
    if unsafe {
        setsockopt(
            SOCKET(streams.get_mut(s).unwrap().socket.as_raw_socket() as _),
            SOL_SOCKET,
            SO_UPDATE_CONNECT_CONTEXT,
            None,
        )
    } != 0
    {
        Err(mferr!(500, Io, Other, Network))
    } else {
        Ok(s)
    }
}

#[repr(C)]
struct OperationHeader {
    overlapped: OVERLAPPED,
    idx: usize,
    handle: HANDLE,
}

unsafe impl Send for OperationHeader {}
unsafe impl Sync for OperationHeader {}

#[repr(C)]
struct WsaOp {
    bufs: *const [WSABUF],
    transferred: u32,
    flags: u32,
    sock_idx: usize,
}

unsafe impl Send for WsaOp {}
unsafe impl Sync for WsaOp {}

enum OperationMode {
    FileRead(MaybeAlloced<WrPerm>, RawBox),
    FileWrite(AllocedOrTransferred<RdPerm>, RawBox),
    StreamRead(WsaOp),
    StreamWrite(WsaOp),
    TcpConnect(TcpConnect),
    TcpAccept(TcpAccept),
}

impl OperationMode {
    pub unsafe fn submit_op(
        &mut self,
        header: &mut OperationHeader,
    ) -> ::windows::core::Result<Option<u32>> {
        match self {
            Self::FileRead(p, b) => {
                let buf: *mut u8 = match p {
                    Ok(p) => p.as_mut_ptr().cast(),
                    Err(_) => b.0.cast(),
                };
                log::trace!("FileRead({:?}, {})", header.handle, p.len());
                ReadFile(
                    header.handle,
                    Some(unsafe { core::slice::from_raw_parts_mut(buf, p.len() as usize) }),
                    None,
                    Some(&mut header.overlapped),
                )
                .map(|_| None)
            }
            Self::FileWrite(p, b) => {
                let buf: *const u8 = match p {
                    Ok(p) => p.as_ptr().cast(),
                    Err(_) => b.0.cast(),
                };
                log::trace!("FileWrite({:?}, {})", header.handle, p.len());
                WriteFile(
                    header.handle,
                    Some(unsafe { core::slice::from_raw_parts(buf, p.len() as usize) }),
                    None,
                    Some(&mut header.overlapped),
                )
                .map(|_| None)
            }
            Self::StreamRead(WsaOp {
                transferred,
                bufs,
                flags,
                ..
            }) => {
                log::trace!("StreamRead({:?} {transferred})", bufs);

                let ret = unsafe {
                    WSARecv(
                        SOCKET(header.handle.0 as _),
                        &**bufs,
                        Some(transferred),
                        flags,
                        Some(&mut header.overlapped),
                        None,
                    )
                };

                if ret == 0 {
                    Ok(Some(*transferred))
                } else {
                    Err(WIN32_ERROR(WSAGetLastError().0 as _).into())
                }
            }
            Self::StreamWrite(WsaOp {
                transferred,
                bufs,
                flags,
                ..
            }) => {
                log::trace!("StreamWrite({:?} {transferred})", bufs);

                let ret = unsafe {
                    WSASend(
                        SOCKET(header.handle.0 as _),
                        &**bufs,
                        Some(transferred),
                        *flags,
                        Some(&mut header.overlapped),
                        None,
                    )
                };

                if ret == 0 {
                    Ok(Some(*transferred))
                } else {
                    Err(WIN32_ERROR(WSAGetLastError().0 as _).into())
                }
            }
            Self::TcpConnect(TcpConnect { addr, conn_id }) => {
                log::trace!("TcpConnect({:?}, {})", addr.to_socket_addr(), conn_id);

                connect_ex(
                    SOCKET(header.handle.0 as _),
                    addr as *const _ as *const SOCKADDR,
                    addr.addr_len() as _,
                    core::ptr::null(),
                    0,
                    core::ptr::null_mut(),
                    &mut header.overlapped,
                )
                .map(|_| None)
            }
            Self::TcpAccept(TcpAccept {
                in_sock,
                conn_id,
                tmp_addr,
            }) => {
                log::trace!("TcpAccept({:?}, {})", in_sock, conn_id);

                let mut transferred = 0;

                if unsafe {
                    AcceptEx(
                        SOCKET(header.handle.0 as _),
                        *in_sock,
                        tmp_addr.addr.as_mut_ptr().cast(),
                        0,
                        TmpAddr::ADDR_LENGTH as _,
                        TmpAddr::ADDR_LENGTH as _,
                        &mut transferred,
                        &mut header.overlapped,
                    ) == TRUE
                } {
                    Ok(Some(transferred))
                } else {
                    Err(WIN32_ERROR(WSAGetLastError().0 as _).into())
                }
            }
        }
    }

    pub fn on_processed(
        self,
        handle: HANDLE,
        res: std::io::Result<usize>,
        connections: &mut Slab<TcpGetSock>,
        streams: &mut Slab<StreamInner>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        log::trace!("On processed");
        match self {
            Self::FileRead(pkt, buf) => match res {
                Ok(read) if (read as u64) < pkt.len() => {
                    let (left, right) = pkt.split_at(read);
                    if let Err(pkt) = left {
                        assert!(!buf.0.is_null());
                        let buf = unsafe { &*buf.0 };
                        deferred_pkts.ok(unsafe { pkt.transfer_data(buf.as_ptr().cast()) });
                    }
                    deferred_pkts.error(right, io_err(State::Nop));
                }
                Ok(0) => {
                    deferred_pkts.error(pkt, io_err(State::Nop));
                }
                Err(e) => deferred_pkts.error(pkt, io_err(e.kind().into())),
                _ => match pkt {
                    Ok(pkt) => {
                        deferred_pkts.ok(pkt);
                    }
                    Err(pkt) => {
                        assert!(!buf.0.is_null());
                        let buf = unsafe { &*buf.0 };
                        deferred_pkts.ok(unsafe { pkt.transfer_data(buf.as_ptr().cast()) });
                    }
                },
            },
            Self::FileWrite(pkt, _) => match res {
                Ok(read) if (read as u64) < pkt.len() => {
                    let (left, right) = pkt.split_at(read);
                    deferred_pkts.ok(left);
                    right.error(io_err(State::Nop));
                }
                Ok(0) => {
                    deferred_pkts.error(pkt, io_err(State::Nop));
                }
                Err(e) => deferred_pkts.error(pkt, io_err(e.kind().into())),
                _ => {
                    deferred_pkts.ok(pkt);
                }
            },
            Self::StreamRead(WsaOp { sock_idx, .. }) => {
                let sock = streams.get_mut(sock_idx).unwrap();
                sock.on_read(res, deferred_pkts);
            }
            Self::StreamWrite(WsaOp { sock_idx, .. }) => {
                let sock = streams.get_mut(sock_idx).unwrap();
                sock.on_write(res, deferred_pkts);
            }
            Self::TcpConnect(TcpConnect { conn_id, .. }) => {
                let conn = connections.get_mut(conn_id).unwrap();

                match res {
                    Ok(_) => {
                        conn.res = Some(
                            Ok(conn.socket_idx.take().unwrap())
                                .and_then(|v| socket_connected(v, streams)),
                        )
                    }
                    Err(e) => {
                        conn.res = {
                            streams.remove(conn.socket_idx.take().unwrap());
                            Some(Err(from_io_error(e)))
                        }
                    }
                };

                if let Some(waker) = conn.waker.take() {
                    waker.wake();
                }
            }
            Self::TcpAccept(TcpAccept {
                conn_id, tmp_addr, ..
            }) => {
                let conn = connections.get_mut(conn_id).unwrap();
                conn.tmp_addr = Some(tmp_addr);

                match res {
                    Ok(_) => {
                        conn.res = Some(
                            Ok(conn.socket_idx.take().unwrap())
                                .and_then(|v| socket_accepted(v, SOCKET(handle.0 as _), streams)),
                        )
                    }
                    Err(e) => {
                        conn.res = {
                            streams.remove(conn.socket_idx.take().unwrap());
                            Some(Err(from_io_error(e)))
                        }
                    }
                };

                if let Some(waker) = conn.waker.take() {
                    waker.wake();
                }
            }
        }
    }
}

#[repr(C)]
struct Operation {
    header: UnsafeCell<OperationHeader>,
    mode: OperationMode,
}

impl Operation {
    pub fn on_complete(
        mut self,
        transferred_override: Option<u32>,
        connections: &mut Slab<TcpGetSock>,
        streams: &mut Slab<StreamInner>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        log::trace!("On complete {transferred_override:?}");
        let header = self.header.get_mut();

        let mut transferred = 0;

        let res = unsafe {
            GetOverlappedResult(
                header.handle,
                &mut header.overlapped,
                &mut transferred,
                false,
            )
        };

        // Allow overriding this in case we are getting the result from OVERLAPPED_ENTRY
        if let Some(transferred_override) = transferred_override {
            transferred = transferred_override;
        }

        let res = if res.is_ok() {
            Ok(transferred as usize)
        } else {
            match expand_error(Error::OK) {
                ERROR_IO_INCOMPLETE | ERROR_HANDLE_EOF | ERROR_NO_DATA => Ok(transferred as usize),
                _ => Err(std::io::Error::from(std::io::ErrorKind::Other)),
            }
        };

        self.mode
            .on_processed(header.handle, res, connections, streams, deferred_pkts)
    }

    pub fn on_error(
        mut self,
        connections: &mut Slab<TcpGetSock>,
        streams: &mut Slab<StreamInner>,
        deferred_pkts: &mut DeferredPackets,
    ) {
        log::trace!("On error");
        self.mode.on_processed(
            self.header.get_mut().handle,
            Err(std::io::ErrorKind::Other.into()),
            connections,
            streams,
            deferred_pkts,
        )
    }
}

struct TmpAddr {
    // TODO: find a better way to pin this
    addr: Pin<Box<[u8; Self::ADDR_LENGTH * 2]>>,
}

impl Default for TmpAddr {
    fn default() -> Self {
        Self {
            addr: Box::pin([0; Self::ADDR_LENGTH * 2]),
        }
    }
}

impl TmpAddr {
    const ADDR_LENGTH: usize = core::mem::size_of::<CSockAddr>() + 16;
}

struct TcpGetSock {
    waker: Option<Waker>,
    res: Option<mfio::error::Result<usize>>,
    socket_idx: Option<usize>,
    tmp_addr: Option<TmpAddr>,
}

impl TcpGetSock {
    pub fn new_for_connect(
        addr: SocketAddr,
        state: &mut IocpState,
        waker: Waker,
    ) -> std::io::Result<Self> {
        let socket = new_ip_socket(addr, SOCK_STREAM)?;
        bind_any(socket, addr)?;
        let socket = unsafe { OwnedSocket::from_raw_socket(socket.0 as _) };
        let socket_idx = Some(unsafe { state.register_stream(socket) }.idx());

        Ok(Self {
            waker: Some(waker),
            res: None,
            socket_idx,
            tmp_addr: None,
        })
    }

    pub fn new_for_accept(
        listen_addr: SocketAddr,
        state: &mut IocpState,
        waker: Waker,
    ) -> std::io::Result<Self> {
        let socket = new_ip_socket(listen_addr, SOCK_STREAM)?;
        let socket = unsafe { OwnedSocket::from_raw_socket(socket.0 as _) };
        let socket_idx = Some(unsafe { state.register_stream(socket) }.idx());

        Ok(Self {
            waker: Some(waker),
            res: None,
            socket_idx,
            tmp_addr: None,
        })
    }
}

struct IocpState {
    iocp: HANDLE,
    event: EventWakerOwner,
    files: Slab<OwnedHandle>,
    streams: Slab<StreamInner>,
    listeners: Slab<ListenerInner>,
    ops: Slab<Operation>,
    pending_ops: VecDeque<Result<usize, Operation>>,
    connections: Slab<TcpGetSock>,
    deferred_pkts: DeferredPackets,
}

impl Drop for IocpState {
    fn drop(&mut self) {
        if let Err(e) = unsafe { CloseHandle(self.iocp) } {
            log::error!("CloseHandle: {e}");
        }
    }
}

impl IocpState {
    pub fn try_new() -> std::io::Result<Self> {
        let event = EventWakerOwner::new().ok_or(std::io::ErrorKind::Other)?;

        // TODO: should we set number of threads to 1?
        let iocp = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, None, 0, 0)? };

        Ok(Self {
            iocp,
            event,
            files: Default::default(),
            streams: Default::default(),
            listeners: Default::default(),
            ops: Slab::with_capacity(1024),
            pending_ops: Default::default(),
            connections: Default::default(),
            deferred_pkts: Default::default(),
        })
    }

    fn register_handle(iocp: HANDLE, handle: HANDLE, key: usize) -> std::io::Result<()> {
        log::trace!("Register handle {handle:?} on {key}");
        unsafe {
            CreateIoCompletionPort(handle, iocp, key, 0)?;
            SetFileCompletionNotificationModes(
                handle,
                (FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE) as _,
            )?;
        }
        Ok(())
    }

    fn register_file(&mut self, file: impl IntoRawHandle) -> Key {
        let file = file.into_raw_handle();
        let file = unsafe { OwnedHandle::from_raw_handle(file) };
        let file_handle = file.as_raw_handle();
        let key = Key::File(self.files.insert(file));
        Self::register_handle(self.iocp, HANDLE(file_handle as _), key.key())
            .expect("Unable to register file");
        key
    }

    /// Registers a TCP stream
    ///
    /// # Safety
    ///
    /// The provided handle must be a TCP stream handle.
    unsafe fn register_stream(&mut self, socket: impl IntoRawSocket) -> Key {
        let socket = socket.into_raw_socket();
        let stream = unsafe { net::TcpStream::from_raw_socket(socket) };
        let socket = stream.as_raw_socket();
        let key = Key::Stream(self.streams.insert(stream.into()));
        Self::register_handle(self.iocp, HANDLE(socket as _), key.key())
            .expect("Unable to register socket");
        key
    }

    unsafe fn submit_op(&mut self, idx: usize, queue_back: bool) -> bool {
        log::trace!("Submit op {idx} {queue_back}");
        let entry = self.ops.get_mut(idx).unwrap();
        let header = entry.header.get_mut();
        header.overlapped.Internal = 0;
        header.overlapped.InternalHigh = 0;
        header.idx = idx;
        match entry.mode.submit_op(header) {
            Err(e) => {
                match expand_error(e) {
                    ERROR_INVALID_USER_BUFFER | ERROR_NOT_ENOUGH_MEMORY => {
                        if queue_back {
                            self.pending_ops.push_back(Ok(idx))
                        } else {
                            self.pending_ops.push_front(Ok(idx))
                        }
                        return false;
                    }
                    ERROR_IO_PENDING => (),
                    // TODO: Handle ERROR_HANDLE_EOF specially
                    v => {
                        error!("E {v:?}");
                        let entry = self.ops.remove(idx);
                        // TODO: we don't really have to defer this, because we can just drop the state.
                        entry.on_error(
                            &mut self.connections,
                            &mut self.streams,
                            &mut self.deferred_pkts,
                        );
                    }
                }
            }
            Ok(tx) => {
                let entry = self.ops.remove(idx);
                // TODO: we don't really have to defer this, because we can just drop the state.
                entry.on_complete(
                    tx,
                    &mut self.connections,
                    &mut self.streams,
                    &mut self.deferred_pkts,
                );
            }
        }
        true
    }

    fn submit_pending_ops(&mut self) {
        // Submit all pending ops, without changing order if the queue gets
        // full again.
        while let Some(op) = self.pending_ops.pop_front() {
            let idx = match op {
                Ok(idx) => idx,
                Err(op) => {
                    if self.ops.len() < self.ops.capacity() {
                        self.ops.insert(op)
                    } else {
                        self.pending_ops.push_front(Err(op));
                        break;
                    }
                }
            };

            if !unsafe { self.submit_op(idx, false) } {
                break;
            }
        }
    }

    unsafe fn try_submit_op(&mut self, operation: Operation) -> Result<(), Option<usize>> {
        self.submit_pending_ops();

        if self.ops.len() < self.ops.capacity() {
            let idx = self.ops.insert(operation);
            match self.submit_op(idx, true) {
                false => Err(Some(idx)),
                true => Ok(()),
            }
        } else {
            self.pending_ops.push(Err(operation));
            Err(None)
        }
    }
}

pub struct Runtime {
    // NOTE: this must be before `state`, because `backend` contains references to data, owned by
    // `state`.
    backend: BackendContainer,
    state: BaseArc<Mutex<IocpState>>,
    waker: EventWaker,
}

impl Runtime {
    pub fn try_new() -> std::io::Result<Self> {
        let state = IocpState::try_new()?;
        let waker = state.event.clone();

        let state = BaseArc::new(Mutex::new(state));

        let backend = {
            let state = state.clone();

            async move {
                let mut old_deferred_pkts = DeferredPackets::default();
                // SAFETY: overlapped entries are just plain old data.
                let mut completions =
                    unsafe { SendSync::new(vec![OVERLAPPED_ENTRY::default(); 512]) };
                let mut stream_ops = vec![];

                loop {
                    {
                        let state = &mut *state.lock();

                        state.event.clear();

                        // TODO: limit the number of operations dequed
                        loop {
                            // Submit all pending stream ops
                            for (key, stream) in state.streams.iter_mut() {
                                stream.on_queue(
                                    key,
                                    &mut stream_ops,
                                    &mut state.deferred_pkts,
                                    state.event.as_raw_handle(),
                                );
                            }

                            for op in stream_ops.drain(0..) {
                                let _ = unsafe { state.try_submit_op(op) };
                            }

                            let mut count = completions.len() as u32;
                            if unsafe {
                                GetQueuedCompletionStatusEx(
                                    state.iocp,
                                    &mut completions,
                                    &mut count,
                                    0,
                                    false,
                                )
                            }
                            .is_ok()
                            {
                                for e in completions[..(count as usize)].iter() {
                                    if e.lpOverlapped.is_null() {
                                        // This is unexpected from our end.
                                        todo!("Handle null responses");
                                    } else {
                                        let idx = unsafe {
                                            (*e.lpOverlapped.cast::<OperationHeader>()).idx
                                        };
                                        let entry = state.ops.remove(idx);
                                        entry.on_complete(
                                            Some(e.dwNumberOfBytesTransferred),
                                            &mut state.connections,
                                            &mut state.streams,
                                            &mut state.deferred_pkts,
                                        );
                                    }
                                }
                            } else if state.pending_ops.is_empty() {
                                break;
                            }

                            state.submit_pending_ops();
                        }

                        core::mem::swap(&mut old_deferred_pkts, &mut state.deferred_pkts);
                    }

                    old_deferred_pkts.flush();

                    let mut signaled = false;

                    poll_fn(|_| {
                        if signaled {
                            Poll::Ready(())
                        } else {
                            signaled = true;
                            Poll::Pending
                        }
                    })
                    .await;
                }
            }
        };

        Ok(Self {
            state,
            backend: BackendContainer::new_dyn(backend),
            waker,
        })
    }
}

impl IoBackend for Runtime {
    fn polling_handle(&self) -> Option<PollingHandle> {
        static FLAGS: PollingFlags = PollingFlags::new();
        Some(PollingHandle {
            handle: self.waker.as_raw_handle(),
            cur_flags: &FLAGS,
            max_flags: PollingFlags::new(),
            waker: self.waker.clone().into_waker(),
        })
    }

    fn get_backend(&self) -> BackendHandle {
        self.backend.acquire(Some(self.waker.flags()))
    }
}

impl Runtime {
    pub fn register_file(&self, file: File) -> FileWrapper {
        let mut state = self.state.lock();
        let key = state.register_file(file);
        FileWrapper::new(key.idx(), self.state.clone())
    }

    pub fn register_stream(&self, stream: std::net::TcpStream) -> TcpStream {
        let state = &mut *self.state.lock();
        let key = unsafe { state.register_stream(stream) };
        TcpStream::new(key.idx(), self.state.clone())
    }

    pub fn cancel_all_ops(&self) {
        let mut state = self.state.lock();
        for (_, f) in state.files.iter_mut() {
            let _ = unsafe { CancelIoEx(HANDLE(f.as_raw_handle() as _), None) };
        }
        for (_, l) in state.listeners.iter_mut() {
            let _ = unsafe { CancelIoEx(HANDLE(l.socket.as_raw_socket() as _), None) };
        }
        for (_, s) in state.streams.iter_mut() {
            s.cancel_all_ops();
        }
    }

    pub fn register_listener(&self, listener: std::net::TcpListener) -> TcpListener {
        TcpListener::register_listener(&self.state, listener)
    }

    pub fn tcp_connect<'a, A: ToSocketAddrs + Send + 'a>(
        &'a self,
        addrs: A,
    ) -> TcpConnectFuture<'a, A> {
        TcpStream::tcp_connect(&self.state, addrs)
    }
}

pub fn map_options(mut open_options: OpenOptions) -> OpenOptions {
    use ::windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
    use std::os::windows::prelude::OpenOptionsExt;
    open_options.custom_flags(FILE_FLAG_OVERLAPPED.0);
    open_options
}
