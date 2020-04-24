#![allow(dead_code)]

mod page;
mod queue;
#[cfg(test)]
mod test;

use crate::{Error, Result};
use page::{OwnedPage, SharedPage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

/// First version of this!
const VERSION: u16 = 1;
/// 4kb page
const PAGE_SIZE: usize = 4 * 1024;

/// A versioned pager that uses Cow semantics.
///
/// This pager will attempt to copy and remap pages in favor using locks. This
/// is achieved by collecting a linked lists of remapped pages and free pages.
#[derive(Debug)]
pub struct VersionedPager {
    file: File,
    header: Header,

    // Remapped pages go into this queue to allow us to
    // undo remapps at the next commit. So when a snapshot no longer
    // requires a page it will append it to this queue which we can start to
    // undo at commit time.
    // remap_queue: VecDeque<LogicalPageId>,
    remapped_pages: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,

    next_page_id: usize,
}

impl VersionedPager {
    pub fn from_file(file: File) -> Result<Self> {
        let header = Header {
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            // Start with 1, we could add a backup here.
            page_count: 1,
            commited_version: 1,
            oldest_version: 1,
        };

        let mut pager = Self {
            file,
            header,
            remapped_pages: HashMap::new(),
            // One because header page
            next_page_id: 1,
        };

        pager.write_header()?;

        Ok(pager)
    }

    fn write_header(&mut self) -> Result<()> {
        let header = bincode::serialize(&self.header)?;

        assert!(header.len() < PAGE_SIZE, "header must be below PAGE_SIZE");

        self.file.write_at(&header[..], 0)?;

        Ok(())
    }

    /// Allocate a new unused page id, this may get a page from the freelist or
    /// actually allocate a new page.
    pub fn new_page_id(&mut self) -> Result<LogicalPageId> {
        let id = self.next_page_id;
        self.next_page_id += 1;

        let logical_id = LogicalPageId(id);

        Ok(logical_id)
    }

    pub fn new_page_buffer(&mut self) -> Result<Page> {
        let page = Page {
            id: self.new_page_id()?,
            version: self.current_version(),
            buf: Buf::Owned(vec![0u8; self.header.page_size as usize]),
        };

        Ok(page)
    }

    pub fn write_page3(&mut self, page: OwnedPage) -> Result<SharedPage> {
        // TODO: this should take a buf and write it into memory then
        // clone it.
        self.write_page(page.id().0, &page[..])?;
        Ok(page.freeze())
    }

    pub fn write_page2(&mut self, page: Page) -> Result<()> {
        // TODO: this should take a buf and write it into memory then
        // clone it.
        self.write_page(page.id.0, &page.buf[..])
    }

    fn write_page(&mut self, page_id: usize, data: &[u8]) -> Result<()> {
        let offset = page_id * self.header.page_size as usize;
        self.file.write_at(data, offset as u64)?;
        Ok(())
    }

    /// Atomically update the page by creating a new page for the specified
    /// version.
    // TODO: add `update` which adds the page to the cache and writes it to the
    // block device.
    pub fn atomic_update(
        &mut self,
        page_id: LogicalPageId,
        version: Version,
        data: Vec<u8>,
    ) -> Result<LogicalPageId> {
        // Copy page
        let new_page_id = self.new_page_id()?;

        let versions = self
            .remapped_pages
            .entry(page_id)
            .or_insert(BTreeMap::new());

        versions.insert(version, PhysicalPageId(new_page_id.0));

        self.write_page(new_page_id.0, &data[..])?;

        Ok(new_page_id)
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, id: LogicalPageId, version: Version) -> Result<Page> {
        // Get remapped page!
        let page_id = if let Some(remapped_pages) = self.remapped_pages.get(&id) {
            let (_, page) = remapped_pages
                .range(..)
                .next_back()
                .expect("there should be atleast one entry");

            *page
        } else {
            PhysicalPageId(id.0)
        };

        let buf = self.read_page(page_id)?;

        Ok(Page { id, version, buf })
    }

    fn read_page(&mut self, page_id: PhysicalPageId) -> Result<Buf> {
        let mut buf = vec![0u8; PAGE_SIZE];
        let offset = page_id.0 * PAGE_SIZE;
        self.file.read_at(&mut buf[..], offset as u64)?;

        Ok(Buf::Shared(Arc::new(buf)))
    }

    /// Free a page at the specified version.
    pub fn free(&mut self, page_id: LogicalPageId, version: Version) {
        // First check if this page id matches any "originally" remapped pages
        // from the remapped_pages map. If it is an original page then add
        // it to the back of the `remap_queue`. If the version is older than
        // the last effective version we can add it to the freelist page,
        // otherwise add it to the delayed free list queue.
        todo!()
    }

    pub fn commit(&mut self) -> Result<()> {
        self.header.commited_version += 1;

        self.write_header()?;
        self.file.sync_data()?;

        Ok(())
    }

    /// Get the effective last version which can be more than the last commited
    /// last version.
    pub fn effective_last_version(&self) -> Version {
        todo!()
    }

    fn current_version(&self) -> Version {
        Version(self.header.commited_version + 1)
    }

    // fn next_page(&mut self) ->
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PhysicalPageId(usize);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct LogicalPageId(usize);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version(u64);

#[derive(Debug, Clone)]
pub struct Page {
    id: LogicalPageId,
    version: Version,
    buf: Buf,
}

#[derive(Debug, Clone)]
pub enum Buf {
    Shared(Arc<Vec<u8>>),
    Owned(Vec<u8>),
}

impl Buf {
    /// If this page is shared, this will make a copy.
    pub fn to_mut(&mut self) -> &mut [u8] {
        match *self {
            Buf::Shared(ref buf) => {
                *self = Buf::Owned(Vec::clone(&buf));

                match *self {
                    Buf::Shared(_) => unreachable!(),
                    Buf::Owned(ref mut b) => b,
                }
            }

            Buf::Owned(ref mut buf) => &mut buf[..],
        }
    }
}

impl std::ops::Deref for Buf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self {
            Buf::Owned(b) => &b[..],
            Buf::Shared(b) => &b[..],
        }
    }
}

impl fmt::Display for LogicalPageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<LogicalPageId> for usize {
    fn from(t: LogicalPageId) -> Self {
        t.0 as usize
    }
}

impl From<usize> for LogicalPageId {
    fn from(t: usize) -> Self {
        LogicalPageId(t)
    }
}

impl From<&LogicalPageId> for LogicalPageId {
    fn from(t: &LogicalPageId) -> Self {
        *t
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    version: u16,
    page_size: u32,
    page_count: u64,
    commited_version: u64,
    oldest_version: u64,
}
