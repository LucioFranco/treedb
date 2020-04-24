use crate::{
    page::{Kind, Page},
    pager::{PageId, Pager},
    Result,
};
use std::fs::OpenOptions;
use std::path::Path;

const PAGE_SIZE: usize = 4096;
const CACHE_SIZE: usize = 1024;

#[derive(Debug)]
pub struct Tree {
    pager: Pager,
    root_page_id: PageId,
}

impl Tree {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        let mut pager = Pager::new(PAGE_SIZE, file, CACHE_SIZE);

        let page = Page::new_leaf(PAGE_SIZE);
        let root_page_id = pager.alloc_page(page)?.id();

        Ok(Self {
            pager,
            root_page_id,
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let root_page = self.pager.get(self.root_page_id)?;
        let root = root_page.page_mut();

        match &mut root.kind {
            Kind::Leaf(leaf) => leaf.put(key, value),
        }

        Ok(())
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Result<Option<&(Vec<u8>, Vec<u8>)>> {
        let root_page = self.pager.get(self.root_page_id)?;
        let root = root_page.page();

        let val = match &root.kind {
            Kind::Leaf(leaf) => leaf.get(&key),
        };

        Ok(val)
    }

    pub fn flush_all(&mut self) -> Result<()> {
        self.pager.flush_all()?;

        Ok(())
    }
}
