use lru::LruCache;
use std::{fmt, hash::Hash};

pub struct Lru<T> {
    inner: LruCache<T, usize>,
}

impl<T: Hash + Eq> Lru<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: LruCache::new(cap),
        }
    }

    pub fn bump(&mut self, key: &T) {
        self.inner.get(key);
    }

    /// Insert a new key and its index, if the cache is full this will
    /// return the least recently used index.
    pub fn insert(&mut self, key: T, index: usize) -> Option<usize> {
        let least_used_item = if self.inner.len() == self.inner.cap() {
            self.inner.pop_lru()
        } else {
            None
        };

        self.inner.put(key, index);

        least_used_item.map(|(_, index)| index)
    }

    /// Pop the least recently used item
    pub fn pop_lru(&mut self) -> Option<(T, usize)> {
        self.inner.pop_lru()
    }
}

impl<T: fmt::Debug + std::hash::Hash + Eq> fmt::Debug for Lru<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}
