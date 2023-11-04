use crate::std_prelude::*;

use crate::locks::Mutex;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::VecDeque;
use tarc::Arc;

#[allow(clippy::type_complexity)]
pub struct PinHeap<T: Release> {
    used: [UnsafeCell<Option<Box<[AtomicBool]>>>; 32],
    data: [UnsafeCell<Option<Box<[MaybeUninit<T>]>>>; 32],
    shards: Mutex<usize>,
    cached_elems_len: AtomicUsize,
    cached_elems: Mutex<VecDeque<(usize, usize)>>,
    free_elems: Mutex<VecDeque<(usize, usize)>>,
    cache_size: usize,
}

unsafe impl<T: Release> Send for PinHeap<T> {}
unsafe impl<T: Release> Sync for PinHeap<T> {}

impl<T: Release> Default for PinHeap<T> {
    fn default() -> Self {
        Self::new(4096)
    }
}

impl<T: Release> Drop for PinHeap<T> {
    fn drop(&mut self) {
        if core::mem::needs_drop::<T>() {
            let mut guard = self.cached_elems.lock();
            while let Some((shard_id, idx)) = guard.pop_front() {
                // SAFETY: we have exclusive access to the element on the shard
                // that has been fully allocated.
                unsafe {
                    let shard = self.data[shard_id].get();
                    // Here we would have an intermediary step of taking &mut Option and unwrapping it,
                    // but that would cause data race in miri. Since we are certain that the value is
                    // Some(_), and *mut Option<Box<T>> is equivalent to *mut *mut T (because
                    // Option<Box<T>> is equivalent to *mut T), we can ignore the unwrap and instead cast
                    // `*mut Option<Box<T>>` directly into `*mut *mut T`.
                    let shard = shard as *mut Box<MaybeUninit<T>> as *mut *mut MaybeUninit<T>;
                    // In addition, do manual pointer addition to drop slice metadata, in order to avoid
                    // pointer retagging happening in MIR.
                    let elem = (*shard).add(idx);
                    core::ptr::drop_in_place(elem as *mut T);
                };
            }
        }
    }
}

impl<T: Release> PinHeap<T> {
    #[allow(clippy::uninit_assumed_init)]
    pub fn new(cache_size: usize) -> Self {
        Self {
            used: [(); 32].map(|_| UnsafeCell::new(None)),
            data: [(); 32].map(|_| UnsafeCell::new(None)),
            shards: Mutex::new(0),
            cached_elems_len: Default::default(),
            cached_elems: Default::default(),
            free_elems: Default::default(),
            cache_size,
        }
    }

    unsafe fn mark_used(&self, used: bool, shard_id: usize, idx: usize) {
        let shard = self.used[shard_id].get();
        // Here we would have an intermediary step of taking &mut Option and unwrapping it,
        // but that would cause data race in miri. Since we are certain that the value is
        // Some(_), and *mut Option<Box<T>> is equivalent to *mut *mut T (because
        // Option<Box<T>> is equivalent to *mut T), we can ignore the unwrap and instead cast
        // `*mut Option<Box<T>>` directly into `*mut *mut T`.
        let shard = shard as *mut Box<AtomicBool> as *mut *mut AtomicBool;
        // In addition, do manual pointer addition to drop slice metadata, in order to avoid
        // pointer retagging happening in MIR.
        let elem = (*shard).add(idx);
        (*elem).store(used, Ordering::Release);
    }

    pub fn alloc_pin(this: Arc<Self>, value: T) -> Pin<AllocHandle<T>> {
        Self::alloc(this, value).into()
    }

    pub fn alloc_or_cached(
        this: Arc<Self>,
        init: impl FnOnce() -> T,
        clear: impl FnOnce(&mut T),
    ) -> AllocHandle<T> {
        let elem = this.cached_elems.lock().pop_front();
        if let Some((shard_id, idx)) = elem {
            this.cached_elems_len.fetch_sub(1, Ordering::Relaxed);
            let data = unsafe {
                let shard = this.data[shard_id].get();
                // Here we would have an intermediary step of taking &mut Option and unwrapping it,
                // but that would cause data race in miri. Since we are certain that the value is
                // Some(_), and *mut Option<Box<T>> is equivalent to *mut *mut T (because
                // Option<Box<T>> is equivalent to *mut T), we can ignore the unwrap and instead cast
                // `*mut Option<Box<T>>` directly into `*mut *mut T`.
                let shard = shard as *mut Box<MaybeUninit<T>> as *mut *mut MaybeUninit<_>;
                // In addition, do manual pointer addition to drop slice metadata, in order to avoid
                // pointer retagging happening in MIR.
                let elem = (*shard).add(idx);
                (*elem).assume_init_mut()
            };

            clear(data);

            let data = data as *mut _;

            unsafe { this.mark_used(true, shard_id, idx) };

            AllocHandle {
                data,
                location: (shard_id, idx),
                store: this,
            }
        } else {
            Self::alloc(this, init())
        }
    }

    pub fn alloc(this: Arc<Self>, value: T) -> AllocHandle<T> {
        Self::alloc_init(this, |_| value)
    }

    pub fn alloc_init(
        this: Arc<Self>,
        initializer: impl FnOnce((usize, usize)) -> T,
    ) -> AllocHandle<T> {
        let elem = this.free_elems.lock().pop_front();
        let (shard_id, idx) = match elem {
            Some(elem) => elem,
            _ => {
                let mut shards = this.shards.lock();

                let shard = *shards;

                // First shard has 2 elems, each additional shard doubles the elems.
                let alloc_size = 2 << shard;

                // Reinterpret Vec<T> with capacity N as Vec<MaybeUninit<T>> with len N
                let mut alloc = Vec::<T>::with_capacity(alloc_size);
                let p = alloc.as_mut_ptr();
                let len = alloc.len();
                let cap = alloc.capacity();
                core::mem::forget(alloc);
                debug_assert!(len == 0);

                let alloc = unsafe { Vec::<MaybeUninit<T>>::from_raw_parts(p as *mut _, cap, cap) }
                    .into_boxed_slice();

                // SAFETY: we are holding the alloc lock and are the only ones writing to the shard
                unsafe {
                    this.data[shard].get().write(Some(alloc));
                }

                let alloc = (0..alloc_size)
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>();

                // SAFETY: we are holding the alloc lock and are the only ones writing to the shard
                unsafe {
                    this.used[shard].get().write(Some(alloc.into_boxed_slice()));
                }

                *shards += 1;
                core::mem::drop(shards);

                // Push all other elements onto the queue
                this.free_elems
                    .lock()
                    .extend((1..alloc_size).map(|i| (shard, i)));

                (shard, 0)
            }
        };

        // SAFETY: we have exclusive access to the element on the shard that has been fully
        // allocated.
        let data = unsafe {
            let shard = this.data[shard_id].get();
            // Here we would have an intermediary step of taking &mut Option and unwrapping it,
            // but that would cause data race in miri. Since we are certain that the value is
            // Some(_), and *mut Option<Box<T>> is equivalent to *mut *mut T (because
            // Option<Box<T>> is equivalent to *mut T), we can ignore the unwrap and instead cast
            // `*mut Option<Box<T>>` directly into `*mut *mut T`.
            let shard = shard as *mut Box<MaybeUninit<T>> as *mut *mut MaybeUninit<_>;
            // In addition, do manual pointer addition to drop slice metadata, in order to avoid
            // pointer retagging happening in MIR.
            let elem = (*shard).add(idx);
            (*elem).write(initializer((shard_id, idx))) as *mut _
        };

        unsafe { this.mark_used(true, shard_id, idx) };

        AllocHandle {
            data,
            location: (shard_id, idx),
            store: this,
        }
    }

    fn release(this: Arc<Self>, location: (usize, usize), data: *mut T) {
        unsafe { this.mark_used(false, location.0, location.1) };

        if this.cached_elems_len.load(Ordering::Relaxed) >= this.cache_size {
            unsafe { core::ptr::drop_in_place(data) };
            this.free_elems.lock().push_back(location);
        } else {
            this.cached_elems_len.fetch_add(1, Ordering::Relaxed);
            this.cached_elems.lock().push_back(location);
        }
    }
}

pub struct AllocHandle<T: Release> {
    data: *mut T,
    location: (usize, usize),
    store: Arc<PinHeap<T>>,
}

unsafe impl<T: Send + Release> Send for AllocHandle<T> {}
unsafe impl<T: Sync + Release> Sync for AllocHandle<T> {}

impl<T: Release> Drop for AllocHandle<T> {
    fn drop(&mut self) {
        (**self).release();
        PinHeap::release(self.store.clone(), self.location, self.data);
    }
}

impl<T: Release> core::ops::Deref for AllocHandle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl<T: Release> core::ops::DerefMut for AllocHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.data }
    }
}

impl<T: Release> From<AllocHandle<T>> for Pin<AllocHandle<T>> {
    fn from(handle: AllocHandle<T>) -> Self {
        // SAFETY: It's not possible to move or replace the insides of a
        // `Pin<AllocHandle<T>>` when `T: !Unpin`, so it's safe to pin it
        // directly without any additional requirements.
        unsafe { Pin::new_unchecked(handle) }
    }
}

pub trait Release {
    fn release(&mut self);
}
