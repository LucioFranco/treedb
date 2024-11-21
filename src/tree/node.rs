use bincode::{deserialize, serialize};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{cmp::Ordering, io, sync::Arc};

const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

#[derive(Debug)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
    pub dirty: bool,
}

// Abstract page cache interface
pub trait PageCache: Send + Sync {
    fn get_page(&self, page_id: PageId) -> io::Result<Arc<Page>>;
    fn flush_page(&self, page_id: PageId) -> io::Result<()>;
    fn allocate_page(&self) -> io::Result<PageId>;
    fn mark_dirty(&self, page_id: PageId) -> io::Result<()>;
    fn flush_all(&self) -> io::Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NodeMetadata {
    is_leaf: bool,
    num_keys: u16,
    next_leaf_pos: PageId,
}

#[derive(Debug, Clone)]
struct Node<K, V> {
    metadata: NodeMetadata,
    keys: Vec<K>,
    values: Vec<Option<V>>,
    children: Vec<PageId>,
}

impl<K, V> Node<K, V> {
    fn encode(&self) -> Vec<u8> {
        todo!()
    }

    fn decode(_: &Vec<u8>) -> io::Result<Node<K, V>> {
        todo!()
    }
}

impl<K: Ord + Serialize + for<'a> Deserialize<'a>, V: Serialize + for<'a> Deserialize<'a>>
    Node<K, V>
{
    fn new_leaf() -> Self {
        Self {
            metadata: NodeMetadata {
                is_leaf: true,
                num_keys: 0,
                next_leaf_pos: PageId(0),
            },
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    fn new_internal() -> Self {
        Self {
            metadata: NodeMetadata {
                is_leaf: false,
                num_keys: 0,
                next_leaf_pos: PageId(0),
            },
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    fn find_child(&self, key: &K) -> usize {
        match self.keys.binary_search(key) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        }
    }

    fn insert_into_leaf(&mut self, key: K, value: V) {
        let pos = match self.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        self.keys.insert(pos, key);
        self.values.insert(pos, Some(value));
        self.metadata.num_keys += 1;
    }
}

pub struct BTree<K, V, C>
where
    K: Ord + Serialize + for<'a> Deserialize<'a>,
    V: Serialize + for<'a> Deserialize<'a>,
    C: PageCache,
{
    cache: C,
    root_id: PageId,
    max_keys: usize,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V, C> BTree<K, V, C>
where
    K: Ord + Serialize + for<'a> Deserialize<'a> + Clone,
    V: Serialize + for<'a> Deserialize<'a> + Clone,
    C: PageCache,
{
    pub fn new(cache: C, root_id: PageId, max_keys: usize) -> Self {
        Self {
            cache,
            root_id,
            max_keys,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        let root_page = self.cache.get_page(self.root_id)?;

        let root: Node<K, V> = Node::decode(&root_page.data)?;

        if root.keys.len() >= self.max_keys {
            self.split_root(key, value)?;
        } else {
            self.insert_internal(self.root_id, key, value)?;
        }

        Ok(())
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        let mut current_id = self.root_id;

        loop {
            let page = self.cache.get_page(current_id)?;

            let node: Node<K, V> = Node::decode(&page.data)?;

            if node.metadata.is_leaf {
                match node.keys.binary_search(key) {
                    Ok(idx) => return Ok(node.values[idx].clone()),
                    Err(_) => return Ok(None),
                }
            } else {
                let child_idx = node.find_child(key);
                current_id = node.children[child_idx];
            }
        }
    }

    fn insert_internal(
        &mut self,
        node_id: PageId,
        key: K,
        value: V,
    ) -> io::Result<Option<(K, PageId)>> {
        let page = self.cache.get_page(node_id)?;
        let mut node: Node<K, V> = Node::decode(&page.data)?;

        if node.metadata.is_leaf {
            node.insert_into_leaf(key, value);
            self.write_node(node_id, &node)?;

            if node.keys.len() > self.max_keys {
                Ok(Some(self.split_leaf(node_id, &mut node)?))
            } else {
                Ok(None)
            }
        } else {
            let child_idx = node.find_child(&key);
            let child_id = node.children[child_idx];

            if let Some((split_key, new_page_id)) = self.insert_internal(child_id, key, value)? {
                node.keys.insert(child_idx, split_key);
                node.children.insert(child_idx + 1, new_page_id);
                node.metadata.num_keys += 1;
                self.write_node(node_id, &node)?;

                if node.keys.len() > self.max_keys {
                    Ok(Some(self.split_internal(node_id, &mut node)?))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }

    fn split_leaf(&mut self, node_id: PageId, node: &mut Node<K, V>) -> io::Result<(K, PageId)> {
        let split_point = (self.max_keys + 1) / 2;
        let mut new_node = Node::new_leaf();

        // Move half of the keys and values to the new node
        new_node.keys = node.keys.split_off(split_point);
        new_node.values = node.values.split_off(split_point);
        new_node.metadata.num_keys = new_node.keys.len() as u16;
        node.metadata.num_keys = node.keys.len() as u16;

        // Update leaf node links
        new_node.metadata.next_leaf_pos = node.metadata.next_leaf_pos;
        let new_page_id = self.cache.allocate_page()?;
        node.metadata.next_leaf_pos = new_page_id;

        self.write_node(new_page_id, &new_node)?;
        self.write_node(node_id, node)?;

        Ok((new_node.keys[0].clone(), new_page_id))
    }

    fn split_internal(
        &mut self,
        node_id: PageId,
        node: &mut Node<K, V>,
    ) -> io::Result<(K, PageId)> {
        let split_point = self.max_keys / 2;
        let mut new_node = Node::new_internal();

        // Move half of the keys and children to the new node
        new_node.keys = node.keys.split_off(split_point + 1);
        new_node.children = node.children.split_off(split_point + 1);
        new_node.metadata.num_keys = new_node.keys.len() as u16;
        node.metadata.num_keys = node.keys.len() as u16;

        let split_key = node.keys.pop().unwrap();
        node.metadata.num_keys -= 1;

        let new_page_id = self.cache.allocate_page()?;
        self.write_node(new_page_id, &new_node)?;
        self.write_node(node_id, node)?;

        Ok((split_key, new_page_id))
    }

    fn split_root(&mut self, key: K, value: V) -> io::Result<()> {
        let old_root_id = self.root_id;
        let new_root_id = self.cache.allocate_page()?;

        let mut new_root = Node::new_internal();
        new_root.children.push(old_root_id);

        self.root_id = new_root_id;
        self.write_node(new_root_id, &new_root)?;

        if let Some((split_key, new_page_id)) = self.insert_internal(old_root_id, key, value)? {
            new_root.keys.push(split_key);
            new_root.children.push(new_page_id);
            new_root.metadata.num_keys += 1;
            self.write_node(new_root_id, &new_root)?;
        }

        Ok(())
    }

    fn write_node(&mut self, page_id: PageId, node: &Node<K, V>) -> io::Result<()> {
        let data = node.encode();
        let mut page = self.cache.get_page(page_id)?;
        let page = Arc::get_mut(&mut page).unwrap();
        page.data = data;
        page.dirty = true;
        self.cache.mark_dirty(page_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    pub struct MemoryPageCache {
        pages: RwLock<HashMap<PageId, Arc<Page>>>,
        next_page_id: RwLock<u64>,
    }

    impl MemoryPageCache {
        pub fn new() -> Self {
            Self {
                pages: RwLock::new(HashMap::new()),
                next_page_id: RwLock::new(0),
            }
        }
    }

    impl PageCache for MemoryPageCache {
        fn get_page(&self, page_id: PageId) -> io::Result<Arc<Page>> {
            let pages = self.pages.read().unwrap();
            Ok(Arc::clone(pages.get(&page_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Page not found")
            })?))
        }

        fn flush_page(&self, _page_id: PageId) -> io::Result<()> {
            Ok(())
        }

        fn allocate_page(&self) -> io::Result<PageId> {
            let mut next_id = self.next_page_id.write().unwrap();
            let page_id = PageId(*next_id);
            *next_id += 1;

            let page = Arc::new(Page {
                id: page_id,
                data: vec![0; PAGE_SIZE],
                dirty: false,
            });

            self.pages.write().unwrap().insert(page_id, page);
            Ok(page_id)
        }

        fn mark_dirty(&self, page_id: PageId) -> io::Result<()> {
            // let pages = self.pages.read().unwrap();
            // if let Some(page) = pages.get_mut(&page_id) {
            //     let mut page = Arc::get_mut(&mut page).unwrap();
            //     page.dirty = true;
            // }
            // Ok(())
            todo!()
        }

        fn flush_all(&self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    #[ignore]
    fn test_btree_operations() {
        let cache = MemoryPageCache::new();
        let root_id = cache.allocate_page().unwrap();
        let mut tree = BTree::<i32, String, MemoryPageCache>::new(cache, root_id, 4);

        // Initialize root node
        let root = Node::new_leaf();
        tree.write_node(root_id, &root).unwrap();

        // Test insertions
        tree.insert(1, "one".to_string()).unwrap();
        tree.insert(2, "two".to_string()).unwrap();
        tree.insert(3, "three".to_string()).unwrap();

        // Test retrieval
        assert_eq!(tree.get(&1).unwrap(), Some("one".to_string()));
        assert_eq!(tree.get(&2).unwrap(), Some("two".to_string()));
        assert_eq!(tree.get(&3).unwrap(), Some("three".to_string()));
        assert_eq!(tree.get(&4).unwrap(), None);
    }
}
