# mfio-rt

## mfio Backed Runtime

This crate aims to provide building blocks for mfio backed asynchronous runtimes. The traits
have the option to not rely on the standard library. This makes the system great for `no_std`
embedded environments or kernel-side code.

`native` feature (depends on `std`) enables native implementations of the runtime through
`NativeRt` structure.

`virt` feature enables a virtual in-memory runtime through `VirtRt` structure.

Custom runtimes may be implemented by implementing `IoBackend`, and any of the runtime
traits, such as `Fs` or `Tcp`.
