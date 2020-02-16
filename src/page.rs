use crate::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Page {
    page_size: usize,
    // data: Box<[u8]>,
    pub kind: Kind,
}

#[derive(Debug)]
pub enum Kind {
    Leaf(Leaf),
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Leaf {
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Page {
    pub fn new_leaf(page_size: usize) -> Self {
        let kind = Kind::Leaf(Leaf::default());

        Self { page_size, kind }
    }

    pub fn deserialize(page_size: usize, buf: Vec<u8>) -> Result<Self> {
        let leaf: Leaf = bincode::deserialize(&buf[..])?;

        let kind = Kind::Leaf(leaf);

        Ok(Self { page_size, kind })
    }

    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        let buf = match &self.kind {
            Kind::Leaf(leaf) => bincode::serialize(&leaf)?,
        };

        assert!(buf.len() < self.page_size, "page size too big!");

        Ok(buf)
    }
}

impl Leaf {
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.nodes.push((key, value));
        self.nodes.sort_by(|n1, n2| n1.0.cmp(&n2.0));
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&(Vec<u8>, Vec<u8>)> {
        match self.nodes.binary_search_by(|n| n.0.cmp(&key)) {
            Ok(idx) => self.nodes.get(idx),
            Err(_) => None,
        }
    }
}
