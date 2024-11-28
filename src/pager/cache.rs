//! This is a really funk weird cache that doesn't actually cache. It just allows you to evict and
//! insert/get. At the moment, this just uses FIFO as the eviciton algorithim. In the future, this
//! should be extended to use `s3-fifo`.

use std::{collections::HashMap, hash::Hash, ptr::NonNull};

pub struct Cache<K: Hash + Eq + Copy, V> {
    index: HashMap<K, NonNull<Node<K, V>>>,
    head: Option<NonNull<Node<K, V>>>,
    tail: Option<NonNull<Node<K, V>>>,
}

struct Node<K, V> {
    key: K,
    val: V,
    prev: Option<NonNull<Node<K, V>>>,
}

impl<K: Hash + Eq + Copy, V> Cache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Cache {
            index: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.index
            .get_mut(key)
            .map(|node| unsafe { &(*node.as_ptr()).val })
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.index
            .get_mut(key)
            .map(|node| unsafe { &mut (*node.as_ptr()).val })
    }

    pub fn insert(&mut self, key: K, val: V) -> Option<(K, V)> {
        unsafe {
            let new = NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                key,
                val,
                prev: None,
            })));

            if let Some(head) = &mut self.head {
                head.as_mut().prev = Some(new);
                self.head = Some(new);
            } else {
                self.head = Some(new);
                self.tail = Some(new);
            }

            if let Some(_) = self.index.insert(key, new) {
                todo!("inserted the same key over another");
            }

            None
        }
    }

    pub fn evict(&mut self) -> Option<(K, V)> {
        unsafe {
            if let Some(tail) = &mut self.tail {
                let prev = tail.as_mut().prev;
                let key = tail.as_mut().key;

                let node = self.index.remove(&key).unwrap();

                if prev.is_none() {
                    self.head = None;
                    self.tail = None;
                } else {
                    self.tail = prev;
                }

                let node = Box::from_raw(node.as_ptr());

                Some((node.key, node.val))
            } else {
                None
            }
        }
    }
}

impl<K: Hash + Eq + Copy, V> Drop for Cache<K, V> {
    fn drop(&mut self) {
        while let Some(_node) = self.evict() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = Cache::new(3);

        // Test insertion
        assert_eq!(cache.insert(1, "one"), None);
        assert_eq!(cache.insert(2, "two"), None);

        // Test get
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&2), Some(&"two"));

        assert_eq!(cache.get(&3), None);
        assert_eq!(cache.insert(3, "three"), None);

        // Test eviction
        assert!(cache.evict().is_some());

        // Verify state after eviction
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_update_existing() {
        let mut cache = Cache::new(2);

        for i in 0..4 {
            cache.insert(i, format!("{}", i));
        }

        for i in 0..4 {
            assert!(cache.evict().is_some(), "{}", i);
        }

        assert!(cache.evict().is_none());

        for i in 0..4 {
            cache.insert(i, format!("{}", i));
        }
    }
}
