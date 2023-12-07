//! # mfio-netfs
//!
//! # Network filesystem sample for mfio
//!
//! This crate is currently just an example showing how a relatively simple filesystem proxy could
//! be implemented using mfio's TCP streams.
//!
//! Please do not use this in production, because the library does close to no error checking, so
//! data corruption is likely to happen.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(feature = "std")]
mod net;

#[cfg(feature = "std")]
pub use net::client::*;
#[cfg(feature = "std")]
pub use net::server::*;
