use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::pin::Pin;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tarc::Arc;

#[allow(clippy::type_complexity)]
pub struct PinHeap<T> {
    data: [UnsafeCell<Option<Box<[MaybeUninit<T>]>>>; 32],
    shards: Mutex<usize>,
    cached_elems_len: AtomicUsize,
    cached_elems: Mutex<VecDeque<(usize, usize)>>,
    free_elems: Mutex<VecDeque<(usize, usize)>>,
}

unsafe impl<T> Send for PinHeap<T> {}
unsafe impl<T> Sync for PinHeap<T> {}

impl<T> Default for PinHeap<T> {
    #[allow(clippy::uninit_assumed_init)]
    fn default() -> Self {
        Self {
            data: [(); 32].map(|_| UnsafeCell::new(None)),
            shards: Mutex::new(0),
            cached_elems_len: Default::default(),
            cached_elems: Default::default(),
            free_elems: Default::default(),
        }
    }
}

impl<T> Drop for PinHeap<T> {
    fn drop(&mut self) {
        if core::mem::needs_drop::<T>() {
            let mut guard = self.cached_elems.lock();
            while let Some((shard_id, idx)) = guard.pop_front() {
                // SAFETY: we have exclusive access to the element on the shard
                // that has been fully allocated.
                unsafe {
                    let shard = &mut *self.data[shard_id].get();
                    let shard = shard.as_mut().unwrap_unchecked();
                    // Intentionally drop the slice metadata so that there is no pointer retagging
                    // happening in MIR
                    let shard = shard as *mut Box<_> as *mut *mut MaybeUninit<T>;
                    let elem = (*shard).add(idx);
                    core::ptr::drop_in_place(elem as *mut T);
                };
            }
        }
    }
}

impl<T> PinHeap<T> {
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
                let shard = &mut *this.data[shard_id].get();
                let shard = shard.as_mut().unwrap_unchecked();
                // Intentionally drop the slice metadata so that there is no pointer retagging
                // happening in MIR
                let shard = shard as *mut Box<_> as *mut *mut MaybeUninit<_>;
                let elem = (*shard).add(idx);
                (*elem).assume_init_mut()
            };

            clear(data);

            let data = data as *mut _;

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
            let shard = &mut *this.data[shard_id].get();
            let shard = shard.as_mut().unwrap_unchecked();
            // Intentionally drop the slice metadata so that there is no pointer retagging
            // happening in MIR
            let shard = shard as *mut Box<_> as *mut *mut MaybeUninit<_>;
            let elem = (*shard).add(idx);
            (*elem).write(value) as *mut _
        };

        AllocHandle {
            data,
            location: (shard_id, idx),
            store: this,
        }
    }

    fn release(this: Arc<Self>, location: (usize, usize), data: *mut T) {
        if this.cached_elems_len.load(Ordering::Relaxed) >= 4096 {
            unsafe { core::ptr::drop_in_place(data) };
            this.free_elems.lock().push_back(location);
        } else {
            this.cached_elems_len.fetch_add(1, Ordering::Relaxed);
            this.cached_elems.lock().push_back(location);
        }
    }
}

pub struct AllocHandle<T> {
    data: *mut T,
    location: (usize, usize),
    store: Arc<PinHeap<T>>,
}

unsafe impl<T: Send> Send for AllocHandle<T> {}
unsafe impl<T: Sync> Sync for AllocHandle<T> {}

impl<T> Drop for AllocHandle<T> {
    fn drop(&mut self) {
        PinHeap::release(self.store.clone(), self.location, self.data);
    }
}

impl<T> core::ops::Deref for AllocHandle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl<T> core::ops::DerefMut for AllocHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.data }
    }
}

impl<T> From<AllocHandle<T>> for Pin<AllocHandle<T>> {
    fn from(handle: AllocHandle<T>) -> Self {
        // SAFETY: It's not possible to move or replace the insides of a
        // `Pin<AllocHandle<T>>` when `T: !Unpin`, so it's safe to pin it
        // directly without any additional requirements.
        unsafe { Pin::new_unchecked(handle) }
    }
}
