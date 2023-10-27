use nix::libc;
use nix::sys::socket::*;
use std::net::SocketAddr;
use std::os::fd::RawFd;

// From mio
pub(crate) fn new_for_addr(
    address: SocketAddr,
    nonblock: bool,
) -> std::io::Result<(AddressFamily, RawFd)> {
    let domain = match address {
        SocketAddr::V4(_) => AddressFamily::Inet,
        SocketAddr::V6(_) => AddressFamily::Inet6,
    };
    new_socket(domain, SockType::Stream, nonblock).map(|v| (domain, v))
}

/// Create a new non-blocking socket.
pub(crate) fn new_socket(
    domain: AddressFamily,
    socket_type: SockType,
    nonblock: bool,
) -> std::io::Result<RawFd> {
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
    ))]
    let flags = SockFlag::SOCK_CLOEXEC.union(SockFlag::from_bits_truncate(if nonblock {
        libc::SOCK_NONBLOCK
    } else {
        0
    }));
    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
    )))]
    let flags = SockFlag::empty();

    let fd = socket(domain, socket_type, flags, None)?;

    // Mimick `libstd` and set `SO_NOSIGPIPE` on apple systems.
    #[cfg(any(
        target_os = "ios",
        target_os = "macos",
        target_os = "tvos",
        target_os = "watchos",
    ))]
    if let Err(e) = nix::errno::Errno::result(unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &1 as *const libc::c_int as *const libc::c_void,
            core::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    }) {
        let _ = nix::unistd::close(fd);
        return Err(e.into());
    }

    // Darwin doesn't have SOCK_NONBLOCK or SOCK_CLOEXEC.
    #[cfg(any(
        target_os = "ios",
        target_os = "macos",
        target_os = "tvos",
        target_os = "watchos",
    ))]
    if nonblock {
        use nix::fcntl::*;

        if let Err(e) = set_nonblock(fd) {
            let _ = nix::unistd::close(fd);
            return Err(e.into());
        }

        if let Err(e) = fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)) {
            let _ = nix::unistd::close(fd);
            return Err(e.into());
        }
    }

    Ok(fd)
}

pub(crate) fn set_nonblock(fd: RawFd) -> std::result::Result<(), nix::errno::Errno> {
    use nix::fcntl::*;

    let flags = fcntl(fd, FcntlArg::F_GETFL)?;
    fcntl(
        fd,
        FcntlArg::F_SETFL(OFlag::from_bits_truncate(flags).union(OFlag::O_NONBLOCK)),
    )?;

    Ok(())
}
