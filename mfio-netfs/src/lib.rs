#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(feature = "std")]
mod net;

#[cfg(feature = "std")]
pub use net::client::*;
#[cfg(feature = "std")]
pub use net::server::*;
