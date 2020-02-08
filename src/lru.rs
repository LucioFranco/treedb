use std::collections::LinkedList;

pub struct Lru<T> {
    list: LinkedList<T>,
}

impl<T> Lru<T> {
    pub fn new() -> Self {
        Lru {
            list: LinkedList::new(),
        }
    }

    pub fn push_back(&mut self, item: T) {
        self.list.push_back(item);
    }
}
