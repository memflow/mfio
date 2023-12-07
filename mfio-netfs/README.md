# mfio-netfs

# Network filesystem sample for mfio

This crate is currently just an example showing how a relatively simple filesystem proxy could
be implemented using mfio's TCP streams.

Please do not use this in production, because the library does close to no error checking, so
data corruption is likely to happen.
