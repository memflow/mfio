#[derive(Clone, Debug)]
struct StackNode<T> {
    // Previous element in the stack
    prev: usize,
    // Next element in the stack, or stack ID, if the last
    next: usize,
    elem: T,
}

#[derive(Debug, Clone)]
pub struct MultiStack<T> {
    elems: Vec<StackNode<T>>,
    stacks: Vec<usize>,
    free_stacks: Vec<usize>,
}

impl<T> Default for MultiStack<T> {
    fn default() -> Self {
        Self {
            elems: vec![],
            stacks: vec![],
            free_stacks: vec![],
        }
    }
}

impl<T> MultiStack<T> {
    pub fn new_stack(&mut self) -> usize {
        if let Some(id) = self.free_stacks.pop() {
            id
        } else {
            self.stacks.push(0);
            self.stacks.len() - 1
        }
    }

    pub fn free_stack(&mut self, id: usize) {
        while self.pop(id).is_some() {}
        self.free_stacks.push(id);
    }

    pub fn push(&mut self, id: usize, elem: T) {
        let prev = self.stacks[id];
        self.elems.push(StackNode {
            elem,
            prev,
            next: id,
        });
        self.stacks[id] = self.elems.len();
        if prev > 0 {
            self.elems[prev - 1].next = self.elems.len();
        }
    }

    pub fn pop(&mut self, id: usize) -> Option<T> {
        let cur = self.stacks[id];
        if cur == 0 {
            // No elements
            return None;
        } else if cur == self.elems.len() {
            // This is the last element on the stack
            let ret = self.elems.pop().unwrap();
            if ret.prev != 0 {
                self.elems[ret.prev - 1].next = id;
            }
            self.stacks[id] = ret.prev;
            return Some(ret.elem);
        }
        // Swap with the top element
        let idx = cur - 1;
        let ret = self.elems.swap_remove(idx);
        // Patch the link to the previous top element
        let swapped_prev = self.elems[idx].prev;
        if swapped_prev != 0 {
            self.elems[swapped_prev - 1].next = idx + 1;
        }
        self.stacks[self.elems[idx].next] = idx + 1;
        self.stacks[id] = ret.prev;
        Some(ret.elem)
    }

    pub fn iter(&self, id: usize) -> MultiStackIterator<T> {
        MultiStackIterator {
            stack: self,
            idx: *self.stacks.get(id).unwrap_or(&0),
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
        Some(&elem.elem)
    }
}
