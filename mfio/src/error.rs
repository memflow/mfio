//! mfio's error types
//!
//! Errors in mfio area meant to be both descriptive and easy to pass across FFI-boundary. Hence we
//! opt to an integer describing many states.

use cglue::result::IntError;
use core::num::{NonZeroI32, NonZeroU8};

pub type Result<T> = core::result::Result<T, Error>;

/// Error code
///
/// This code represents an HTTP client/server error, shifted by 399. This means, that `Code(1)`
/// represents `HTTP` code `400`. If `http` feature is enabled, you can freely transform from
/// `Code` to `http::StatusCode`.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Code(NonZeroU8);

#[cfg(not(feature = "http"))]
impl core::fmt::Display for Code {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

const HTTP_SHIFT: usize = 399;

pub const INTERNAL_ERROR: Code =
    Code(unsafe { NonZeroU8::new_unchecked((500 - HTTP_SHIFT) as u8) });

impl Code {
    pub const fn http_code(&self) -> usize {
        self.0.get() as usize + HTTP_SHIFT
    }

    pub const fn from_http_const(code: usize) -> Self {
        if code >= 400 && code < 600 {
            Code(unsafe { NonZeroU8::new_unchecked((code - HTTP_SHIFT) as u8) })
        } else {
            panic!("Invalid code provided")
        }
    }

    pub fn from_http(code: usize) -> Option<Self> {
        if (400..600).contains(&code) {
            NonZeroU8::new((code - HTTP_SHIFT) as u8).map(Code)
        } else {
            None
        }
    }
}

#[cfg(feature = "http")]
mod http {
    use super::*;
    use ::http::StatusCode;

    impl core::fmt::Display for Code {
        fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
            write!(f, "{}", StatusCode::from(*self))
        }
    }

    impl core::convert::TryFrom<StatusCode> for Code {
        type Error = ();
        fn try_from(code: StatusCode) -> core::result::Result<Self, Self::Error> {
            Self::from_http(code.as_u16() as usize).ok_or(())
        }
    }

    impl core::convert::TryFrom<StatusCode> for Error {
        type Error = ();
        fn try_from(code: StatusCode) -> core::result::Result<Self, Self::Error> {
            Code::try_from(code).map(|code| Error {
                code,
                subject: Subject::Other,
                state: State::Other,
                location: Location::Other,
            })
        }
    }

    impl From<Code> for StatusCode {
        fn from(code: Code) -> Self {
            Self::from_u16(code.0.get() as u16 + 399).unwrap()
        }
    }

    impl From<Error> for StatusCode {
        fn from(Error { code, .. }: Error) -> Self {
            Self::from(code)
        }
    }
}

/// mfio's error type.
///
/// This type consists of 4 distinct pieces:
///
/// - `code`, representing equivalent HTTP status code, which may not be descriptive, and often
/// falls back to `INTERNAL_ERROR`, representing HTTP code 500.
/// - `subject`, represents what errored out.
/// - `state`, represents what kind of error state was reached.
/// - `location`, where in the program the error occured.
#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Error {
    pub code: Code,
    pub subject: Subject,
    pub state: State,
    pub location: Location,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(
            f,
            "{}: {} in {} state at {}",
            self.code, self.subject, self.state, self.location
        )
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

impl IntError for Error {
    fn into_int_err(self) -> NonZeroI32 {
        NonZeroI32::new(i32::from_ne_bytes([
            self.code.0.get(),
            self.subject as u8,
            self.state as u8,
            self.location as u8,
        ]))
        .unwrap()
    }

    fn from_int_err(err: NonZeroI32) -> Self {
        let [code, subject, state, location] = err.get().to_ne_bytes();

        let code = Code(NonZeroU8::new(code).unwrap());

        Self {
            code,
            subject: subject.into(),
            state: state.into(),
            location: location.into(),
        }
    }
}

/// Shorthand for building mfio error structure.
///
/// All enum variants act as if they are imported, therefore in this macro they are to be used
/// without specifying the type.
#[macro_export]
macro_rules! mferr {
    ($code:expr, $subject:ident, $state:ident, $location:ident) => {
        $crate::error::Error {
            code: {
                const CODE: $crate::error::Code = $crate::error::Code::from_http_const($code);
                CODE
            },
            subject: $crate::error::Subject::$subject,
            state: $crate::error::State::$state,
            location: $crate::error::Location::$location,
        }
    };
    ($subject:ident, $state:ident, $location:ident) => {
        $crate::mferr!(500, $subject, $state, $location)
    };
}

macro_rules! ienum {
    (
        $(#[$meta:meta])*
        pub enum $ident:ident {
            $($variant:ident,)*
        }
    ) => {
        $(#[$meta])*
        #[repr(u8)]
        #[non_exhaustive]
        #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
        pub enum $ident {
            $($variant),*
        }

        impl From<u8> for $ident {
            fn from(val: u8) -> Self {
                if val < $ident::Other as u8 {
                    unsafe { core::mem::transmute(val) }
                } else {
                    $ident::Other
                }
            }
        }

        impl $ident {
            pub const fn to_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => stringify!($variant),)*
                }
            }
        }

        impl AsRef<str> for $ident {
            fn as_ref(&self) -> &str {
                self.to_str()
            }
        }

        impl core::ops::Deref for $ident {
            type Target = str;

            fn deref(&self) -> &str {
                self.to_str()
            }
        }

        impl core::fmt::Display for $ident {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(f, "{}", self.to_str())
            }
        }
    };
}

ienum! {
    pub enum Subject {
        Argument,
        Data,
        Path,
        File,
        Io,
        Directory,
        Memory,
        Size,
        Bounds,
        Position,
        Offset,
        Address,
        Connection,
        Architecture,
        Response,
        Abi,
        Api,
        Process,
        Value,
        Library,
        Binary,
        Input,
        Output,
        Plugin,
        Target,
        Feature,
        Module,
        Export,
        Import,
        Section,
        Backend,
        Entry,
        Operation,
        Other,
    }
}

ienum! {
    pub enum State {
        Invalid,
        Unreadable,
        Uninitialized,
        Unsupported,
        Unavailable,
        NotImplemented,
        Partial,
        Outside,
        Exhausted,
        Read,
        Write,
        Create,
        Append,
        Seek,
        Map,
        Load,
        AlreadyExists,
        NotFound,
        PermissionDenied,
        Interrupted,
        Rejected,
        Refused,
        NotReady,
        Aborted,
        NotConnected,
        BrokenPipe,
        Timeout,
        Nop,
        UnexpectedEof,
        InUse,
        Corrupted,
        Removed,
        Other,
    }
}

ienum! {
    pub enum Location {
        Backend,
        Memory,
        Client,
        Core,
        Filesystem,
        Application,
        ThirdParty,
        Network,
        Ffi,
        Plugin,
        Library,
        Stdlib,
        Other,
    }
}

pub struct ErrorConstLocation<const N: u8>(pub Code, pub Subject, pub State);

impl<const N: u8> From<ErrorConstLocation<N>> for Error {
    fn from(ErrorConstLocation(code, subject, state): ErrorConstLocation<N>) -> Self {
        Self {
            code,
            subject,
            state,
            location: N.into(),
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            code: INTERNAL_ERROR,
            subject: Subject::Io,
            state: err.kind().into(),
            location: Location::Other,
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::ErrorKind> for State {
    fn from(kind: std::io::ErrorKind) -> Self {
        use std::io::ErrorKind::*;
        match kind {
            NotFound => State::NotFound,
            PermissionDenied => State::PermissionDenied,
            ConnectionRefused => State::Refused,
            ConnectionReset => State::Interrupted,
            ConnectionAborted => State::Aborted,
            NotConnected => State::NotConnected,
            AddrInUse => State::InUse,
            AddrNotAvailable => State::Unavailable,
            BrokenPipe => State::BrokenPipe,
            AlreadyExists => State::AlreadyExists,
            WouldBlock => State::NotReady,
            InvalidInput => State::Invalid,
            InvalidData => State::Invalid,
            TimedOut => State::Timeout,
            WriteZero => State::Nop,
            Interrupted => State::Interrupted,
            Unsupported => State::Unsupported,
            UnexpectedEof => State::UnexpectedEof,
            OutOfMemory => State::Exhausted,
            Other => State::Other,
            _ => State::Other,
        }
    }
}

#[cfg(feature = "std")]
impl<const N: u8> From<std::io::ErrorKind> for ErrorConstLocation<N> {
    fn from(kind: std::io::ErrorKind) -> Self {
        ErrorConstLocation(INTERNAL_ERROR, Subject::Io, State::from(kind))
    }
}

#[cfg(feature = "std")]
impl From<std::io::ErrorKind> for Error {
    fn from(kind: std::io::ErrorKind) -> Self {
        ErrorConstLocation::<{ Location::Other as u8 }>::from(kind).into()
    }
}

impl From<core::convert::Infallible> for Error {
    fn from(_: core::convert::Infallible) -> Self {
        unreachable!()
    }
}

pub trait ErrorSpecify: Sized {
    fn code(self, code: Code) -> Self;
    fn subject(self, subject: Subject) -> Self;
    fn state(self, state: State) -> Self;
    fn location(self, location: Location) -> Self;
}

impl ErrorSpecify for Error {
    fn code(self, code: Code) -> Self {
        Self { code, ..self }
    }

    fn subject(self, subject: Subject) -> Self {
        Self { subject, ..self }
    }

    fn state(self, state: State) -> Self {
        Self { state, ..self }
    }

    fn location(self, location: Location) -> Self {
        Self { location, ..self }
    }
}

impl<T> ErrorSpecify for Result<T> {
    fn code(self, code: Code) -> Self {
        match self {
            Err(e) => Err(e.code(code)),
            v => v,
        }
    }

    fn subject(self, subject: Subject) -> Self {
        match self {
            Err(e) => Err(e.subject(subject)),
            v => v,
        }
    }

    fn state(self, state: State) -> Self {
        match self {
            Err(e) => Err(e.state(state)),
            v => v,
        }
    }

    fn location(self, location: Location) -> Self {
        match self {
            Err(e) => Err(e.location(location)),
            v => v,
        }
    }
}
