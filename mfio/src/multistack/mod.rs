use core::cell::UnsafeCell;
use core::mem::MaybeUninit;

#[derive(Debug)]
struct StackNode<T> {
    // Previous element in the stack
    prev: usize,
    elem: MaybeUninit<T>,
}

impl<T> StackNode<T> {
    unsafe fn take(&self) -> T {
        self.elem.assume_init_read()
    }

    unsafe fn as_ref(&self) -> &T {
        self.elem.assume_init_ref()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StackHandleInner {
    head: usize,
}

#[derive(Debug)]
pub struct StackHandle {
    inner: UnsafeCell<StackHandleInner>,
    stack: *const (),
}

unsafe impl Send for StackHandle {}
unsafe impl Sync for StackHandle {}

#[derive(Debug)]
pub struct MultiStack<T> {
    elems: Vec<StackNode<T>>,
    free_list: StackHandleInner,
}

impl<T> Default for MultiStack<T> {
    fn default() -> Self {
        Self {
            elems: vec![],
            free_list: Default::default(),
        }
    }
}

impl<T> MultiStack<T> {
    pub fn new_stack(&mut self) -> StackHandle {
        StackHandle {
            inner: UnsafeCell::new(Default::default()),
            stack: self as *mut Self as *const (),
        }
    }

    pub fn free_stack(&mut self, id: &StackHandle) {
        while self.pop(id).is_some() {}
    }

    unsafe fn get_inner<'a>(&mut self, id: &mut &'a StackHandle) -> &'a mut StackHandleInner {
        debug_assert_eq!(self as *const Self as *const (), id.stack);
        &mut *id.inner.get()
    }

    unsafe fn get_inner_ref(&self, id: &StackHandle) -> &StackHandleInner {
        debug_assert_eq!(self as *const Self as *const (), id.stack);
        &*id.inner.get()
    }

    pub fn push(&mut self, mut id: &StackHandle, elem: T) {
        let id = unsafe { self.get_inner(&mut id) };

        let prev = id.head;

        let new_node = StackNode {
            elem: MaybeUninit::new(elem),
            prev,
        };

        let new_head = self.free_list.head;

        let new_head = if new_head == 0 {
            self.elems.push(new_node);
            self.elems.len()
        } else {
            let elem = &mut self.elems[new_head - 1];
            self.free_list.head = elem.prev;
            *elem = new_node;
            new_head
        };

        id.head = new_head;
    }

    pub fn pop(&mut self, mut id: &StackHandle) -> Option<T> {
        let id = unsafe { self.get_inner(&mut id) };

        let cur = id.head;

        if cur == 0 {
            // No elements
            return None;
        }

        let idx = cur - 1;

        let elem = &mut self.elems[idx];

        // SAFETY: we know that this is an initialized element
        let ret = unsafe { elem.take() };
        id.head = elem.prev;

        if cur == self.elems.len() {
            // This is the last element on the stack
            let _ = self.elems.pop();
        } else {
            // Put current element to free elems list
            self.elems[idx].prev = self.free_list.head;
            self.free_list.head = cur;
        }

        Some(ret)
    }

    pub fn iter(&self, id: &StackHandle) -> MultiStackIterator<T> {
        let id = unsafe { self.get_inner_ref(id) };

        MultiStackIterator {
            stack: self,
            idx: id.head,
        }
    }
}

pub struct MultiStackIterator<'a, T> {
    stack: &'a MultiStack<T>,
    idx: usize,
}

impl<'a, T> Iterator for MultiStackIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == 0 {
            return None;
        }
        let elem = &self.stack.elems[self.idx - 1];
        self.idx = elem.prev;
        Some(unsafe { elem.as_ref() })
    }
}
