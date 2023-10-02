/// Represents types that can be made opaque.
///
/// This trait enables optimal object storage outcomes, based on the promises and assumptions the
/// caller is able to make. What does this mean?
///
/// Say the client needs to send a packet reference to the I/O processor. In regular async, the
/// caller is not able to know how long the packet reference will have been sent for to the
/// processor. Therefore, in order to avoid undefined behavior, the reference must be `'static`,
/// i.e. heap allocated.
///
/// However, this ignores the assumptions the client has made to their stack. What if the caller
/// could prove, that the sent out packet will not outlive the caller's stack? In this case, the
/// caller could skip heap allocation and send out a reference to a packet being stored on the
/// stack instead. Of course, it is currently impossible to prove this, because rust futures are
/// inert, and they may be cancelled at arbitrary points, making the caller unable to be
/// deterministically sure that the stack location will be valid throughout the call. However, if a
/// client were to assume this was the case, there would then be a significant performance uplift
/// possible.
///
/// This assumption of type linearity is less of a low level decision, rather than a high level one
/// made by the programmer looking at the whole program. Therefore, this trait does not allow for
/// definite promises of "this will 100% be on the stack, and the stack will be valid throughout",
/// instead, it allows the programmer to describe a promise going something like the following:
///
/// "We can promise, that under type linearity assumption, we are able to store a packet on the
/// stack, and give out a reference that will be valid until the I/O processor manually
/// relinquishes ownership of said references."
///
/// The decision, whether type linearity is being assumed or not, is left at a project-wide level,
/// usually controlled by `#[cfg(...)]` switches. Implementors of this trait are then able to
/// define 2 different codepaths, one for the "100% safe Rust" way, and the other, for the more
/// expanded, albeit potentially unsound, lifetime rules.
///
/// In the end, the implementor of an abstraction would promise that an object can be stored on the
/// stack, if it can be done this way, but still have the flexibility of performing fully static
/// heap allocations.
///
/// # Safety
///
/// Implementation should adhere to the lifetime requirements, most notably, the fact that
/// both `StackReq` and `Opaque` types must be valid for `'static` lifetime, if
/// `#[cfg(mfio_assume_linear_types)]` config switch is not enabled.
pub unsafe trait OpaqueStore {
    type ConstHdr;
    type Opaque<'a>: 'a
    where
        Self: 'a;
    type StackReq<'a>: 'a
    where
        Self: 'a;
    type HeapReq: Into<Self::Opaque<'static>>
    where
        Self: 'static;

    /// Request for object to be stored on the stack.
    ///
    /// A sound implementation will not actually store the object on the stack, however, lifetime
    /// bounds are attached to introduce limits necessary for a more efficient, albeit unsound,
    /// implementation that assumes type linearity.
    ///
    /// Taking the returned object, and storing it on stack allows to take a mutable reference to
    /// the stack location and convert it into opaque object.
    fn stack<'a>(self) -> Self::StackReq<'a>
    where
        Self: 'a;

    /// Request for object to be stored on the heap.
    ///
    /// The returned object may be turned into `Opaque<'static>` by calling [`Into::into`] on it.
    ///
    /// TODO: can we just directly go to `Opaque<'static>`?
    fn heap(self) -> Self::HeapReq
    where
        Self: 'static;

    fn stack_hdr<'a: 'b, 'b>(stack: &'b Self::StackReq<'a>) -> &'b Self::ConstHdr;

    fn stack_opaque<'a>(stack: &'a Self::StackReq<'a>) -> Self::Opaque<'a>;
}
