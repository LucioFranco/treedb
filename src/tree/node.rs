use crate::Result;
use serde::{Deserialize, Serialize};

type Key = Vec<u8>;
type Value = Vec<u8>;
type PageId = usize;

#[derive(Debug)]
pub struct Node {
    pub kind: Data,
}

#[derive(Debug)]
pub enum Data {
    Leaf(Leaf),
    Internal(Internal),
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Leaf {
    keys: Vec<Key>,
    values: Vec<Value>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Internal {
    keys: Vec<Key>,
    pointers: Vec<PageId>,
}

impl Node {
    // pub fn new_leaf(page_size: usize) -> Self {
    //     let kind = Data::Leaf(Leaf::default());

    //     Self { page_size, kind }
    // }

    // pub fn deserialize(page_size: usize, buf: Vec<u8>) -> Result<Self> {
    //     let leaf: Leaf = bincode::deserialize(&buf[..])?;

    //     let kind = Data::Leaf(leaf);

    //     Ok(Self { page_size, kind })
    // }

    // pub fn serialize(&self) -> crate::Result<Vec<u8>> {
    //     let buf = match &self.kind {
    //         Data::Leaf(leaf) => bincode::serialize(&leaf)?,
    //     };

    //     assert!(buf.len() < self.page_size, "page size too big!");

    //     Ok(buf)
    // }
}

impl Leaf {
    // pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
    //     self.nodes.push((key, value));
    //     self.nodes.sort_by(|n1, n2| n1.0.cmp(&n2.0));
    // }

    // pub fn get(&self, key: &Vec<u8>) -> Option<&(Vec<u8>, Vec<u8>)> {
    //     match self.nodes.binary_search_by(|n| n.0.cmp(&key)) {
    //         Ok(idx) => self.nodes.get(idx),
    //         Err(_) => None,
    //     }
    // }
}
