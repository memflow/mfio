use core::ffi::c_void;
use once_cell::sync::OnceCell;
use std::io;

use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Once;

use windows::core::GUID;
use windows::Win32::Foundation::{TRUE, WIN32_ERROR};
use windows::Win32::Networking::WinSock::{
    bind, closesocket, ioctlsocket, socket, WSAGetLastError, WSAIoctl, AF_INET, AF_INET6, FIONBIO,
    LPFN_CONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6,
    SOCKET, WINSOCK_SOCKET_TYPE, WSAID_CONNECTEX,
};
use windows::Win32::System::IO::OVERLAPPED;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) union CSockAddr {
    pub generic: SOCKADDR,
    pub ipv4: SOCKADDR_IN,
    pub ipv6: SOCKADDR_IN6,
}

impl CSockAddr {
    pub fn addr_len(&self) -> usize {
        match unsafe { self.generic.sa_family } {
            AF_INET => core::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => core::mem::size_of::<SOCKADDR_IN6>(),
            _ => unreachable!(),
        }
    }

    pub fn to_socket_addr(self) -> SocketAddr {
        match unsafe { self.generic.sa_family } {
            AF_INET => SocketAddr::V4(SocketAddrV4::new(
                u32::from_be(unsafe { self.ipv4.sin_addr.S_un.S_addr }).into(),
                u16::from_be(unsafe { self.ipv4.sin_port }),
            )),
            AF_INET6 => SocketAddr::V6(SocketAddrV6::new(
                unsafe { self.ipv6.sin6_addr.u.Word }.into(),
                u16::from_be(unsafe { self.ipv6.sin6_port }),
                u32::from_be(unsafe { self.ipv6.sin6_flowinfo }),
                unsafe { self.ipv6.Anonymous.sin6_scope_id },
            )),
            _ => unreachable!(),
        }
    }
}

impl From<SocketAddr> for CSockAddr {
    fn from(a: SocketAddr) -> Self {
        match a {
            SocketAddr::V4(a) => Self { ipv4: a.into() },
            SocketAddr::V6(a) => Self { ipv6: a.into() },
        }
    }
}

/// Initialise the network stack for Windows.
fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Let standard library call `WSAStartup` for us, we can't do it
        // ourselves because otherwise using any type in `std::net` would panic
        // when it tries to call `WSAStartup` a second time.
        drop(std::net::UdpSocket::bind("127.0.0.1:0"));
    });
}

/// Create a new non-blocking socket.
pub(crate) fn new_ip_socket(
    addr: SocketAddr,
    socket_type: WINSOCK_SOCKET_TYPE,
) -> io::Result<SOCKET> {
    let domain = match addr {
        SocketAddr::V4(..) => AF_INET,
        SocketAddr::V6(..) => AF_INET6,
    };

    new_socket(domain.0.into(), socket_type)
}

pub(crate) fn new_socket(domain: u32, socket_type: WINSOCK_SOCKET_TYPE) -> io::Result<SOCKET> {
    init();

    let socket = unsafe { socket(domain as i32, socket_type, 0) };

    let ret = unsafe { ioctlsocket(socket, FIONBIO, &mut 1) };

    if ret != 0 {
        let _ = unsafe { closesocket(socket) };
        return Err(io::Error::from_raw_os_error(ret));
    }

    Ok(socket as SOCKET)
}

unsafe fn get_wsa_fn(socket: SOCKET, guid: GUID) -> ::windows::core::Result<*const ()> {
    init();

    let mut ptr = core::ptr::null();
    let mut ret = 0;
    let r = WSAIoctl(
        socket,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        Some((&guid as *const GUID).cast()),
        core::mem::size_of::<GUID>() as _,
        Some(&mut ptr as *mut _ as *mut c_void),
        core::mem::size_of_val(&ptr) as _,
        &mut ret,
        None,
        None,
    );

    if r != 0 {
        // TODO: is this correct?
        Err(WIN32_ERROR(WSAGetLastError().0 as _).into())
    } else {
        Ok(ptr)
    }
}

pub(crate) fn bind_any(socket: SOCKET, dst_addr_type: SocketAddr) -> ::windows::core::Result<()> {
    let addr = match dst_addr_type {
        SocketAddr::V4(..) => CSockAddr {
            ipv4: SOCKADDR_IN {
                sin_family: AF_INET,
                // INADDR_ANY is just zeroes
                ..Default::default()
            },
        },
        SocketAddr::V6(..) => CSockAddr {
            ipv6: SOCKADDR_IN6 {
                sin6_family: AF_INET6,
                // in6addr_any is just zeroes
                ..Default::default()
            },
        },
    };

    if unsafe {
        bind(
            socket,
            (&addr as *const CSockAddr).cast(),
            addr.addr_len() as _,
        )
    } != 0
    {
        // TODO: is this correct?
        Err(WIN32_ERROR(unsafe { WSAGetLastError() }.0 as _).into())
    } else {
        Ok(())
    }
}

pub(crate) unsafe fn connect_ex(
    socket: SOCKET,
    name: *const SOCKADDR,
    name_len: i32,
    send_buffer: *const c_void,
    send_data_length: u32,
    bytes_sent: *mut u32,
    overlapped: *mut OVERLAPPED,
) -> ::windows::core::Result<()> {
    static CONNECT_EX: OnceCell<LPFN_CONNECTEX> = OnceCell::new();

    let connect_ex = CONNECT_EX.get_or_try_init(|| unsafe {
        get_wsa_fn(socket, WSAID_CONNECTEX).map(|v| core::mem::transmute(v))
    })?;

    let connect_ex = connect_ex.unwrap();

    log::trace!("ConnectEx @ {connect_ex:?} ({socket:?}, {name:?}, {name_len}, {send_buffer:?}, {send_data_length}, {bytes_sent:?}, {overlapped:?})");

    if connect_ex(
        socket,
        name,
        name_len,
        send_buffer,
        send_data_length,
        bytes_sent,
        overlapped,
    ) == TRUE
    {
        Ok(())
    } else {
        // TODO: is this correct?
        Err(WIN32_ERROR(WSAGetLastError().0 as _).into())
    }
}
