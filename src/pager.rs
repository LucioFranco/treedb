#![allow(dead_code)]

#[cfg(test)]
mod test;

mod page;
mod queue;

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    num::NonZeroUsize,
    sync::Arc,
};

use bytes::BytesMut;
use clru::CLruCache;
use serde::{Deserialize, Serialize};

use crate::Result;

use self::{page::Page, queue::Queue};

/// First version of this!
const VERSION: u16 = 1;
/// 4kb page
const PAGE_SIZE: usize = 4 * 1024;

type PageCache = CLruCache<LogicalPageId, PageCacheEntry>;

pub trait File {
    fn len(&self) -> Result<usize>;
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;
    fn sync_data(&self) -> Result<()>;
}

pub struct Pager {
    file: Box<dyn File>,
    header: Header,
    cache: PageCache,
    page_table: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,
    next_page_id: usize,

    remap_queue: Queue<RemappedPage>,
}

impl Pager {
    /// Recover a `VersionedPager`, if the file is empty it will create a new
    /// pager.
    pub fn recover(file: impl File + 'static) -> Result<Self> {
        let file_size = file.len()?;

        let cache = CLruCache::with_memory(NonZeroUsize::new(1024).unwrap(), 1024);
        let page_table = HashMap::new();
        let remap_queue = Queue::create(PhysicalPageId(0), 0)?;

        let file = Box::new(file) as Box<dyn File>;

        let header = if file_size > PAGE_SIZE {
            let mut header_buf = BytesMut::zeroed(PAGE_SIZE);
            // TODO: Probably need to make this read_exact?
            file.read_at(&mut header_buf[..], 0)?;
            bincode::deserialize::<Header>(&header_buf[..])?
        } else {
            Header {
                version: VERSION,
                page_size: PAGE_SIZE as u32,
                // Start with 1, we could add a backup here.
                page_count: 1,
                commited_version: 1,
                oldest_version: 1,
            }
        };

        let mut pager = Self {
            file,
            header,
            cache,
            page_table,
            remap_queue,
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

    pub fn new_page_id(&mut self) -> LogicalPageId {
        // TODO: re-use freemap etc
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        LogicalPageId(page_id)
    }

    fn new_page_buffer(&mut self) -> Arc<Page> {
        Arc::new(Page::new())
    }

    fn read_page(&mut self, page_id: PhysicalPageId) -> Result<Arc<Page>> {
        let logical_page_id = LogicalPageId(page_id.0);

        if let Some(entry) = self.cache.get(&logical_page_id) {
            Ok(entry.page.clone())
        } else {
            let mut page = Arc::new(Page::new());

            self.read_physical_page(page_id, Arc::get_mut(&mut page).unwrap())?;

            let entry = PageCacheEntry { page: page.clone() };

            self.cache.put(logical_page_id, entry);

            Ok(page)
        }
    }

    fn read_physical_page(&self, page_id: PhysicalPageId, page: &mut Page) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.file.read_at(page.buf_mut(), offset as u64)?;

        Ok(())
    }

    fn write_page(&mut self, page_id: PhysicalPageId, page: Arc<Page>) -> Result<()> {
        let offset = page_id.0 * self.header.page_size as usize;
        self.file.write_at(page.buf(), offset as u64)?;

        Ok(())
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, id: LogicalPageId, _version: Version) -> Result<Arc<Page>> {
        // Get remapped page!
        let page_id = if let Some(remapped_pages) = self.page_table.get(&id) {
            let (_, page) = remapped_pages
                .range(..)
                .next_back()
                .expect("there should be atleast one entry");

            *page
        } else {
            PhysicalPageId(id.0)
        };

        let page = self.read_page(page_id)?;

        Ok(page)
    }

    fn update_page(&mut self, page_id: LogicalPageId, page: Arc<Page>) -> Result<()> {
        let entry = self
            .cache
            .get_mut(&page_id)
            .expect("Update page failed on cache lookup");

        entry.page = page.clone();

        self.write_page(PhysicalPageId(page_id.0), page)?;

        Ok(())
    }

    /// Atomically update the page by crekating a new page for the specified
    /// version.
    // TODO: add `update` which adds the page to the cache and writes it to the
    // block device.
    pub fn atomic_update(
        &mut self,
        page_id: LogicalPageId,
        version: Version,
        page: Arc<Page>,
    ) -> Result<LogicalPageId> {
        // Copy page
        let new_page_id = self.new_page_id();

        self.update_page(page_id, page);

        let versions = self.page_table.entry(page_id).or_insert(BTreeMap::new());

        // Pushed into the queue to be un-mapped later
        // self.remap_queue.push_back(RemappedPage {
        //     version,
        //     original_page_id: page_id,
        //     new_page_id,
        // });

        versions.insert(version, PhysicalPageId(new_page_id.0));

        Ok(new_page_id)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.header.commited_version += 1;

        self.write_header()?;
        self.file.sync_data()?;

        Ok(())
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

    fn current_version(&self) -> Version {
        Version(self.header.commited_version + 1)
    }
}

struct PageCacheEntry {
    page: Arc<Page>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPageId(usize);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogicalPageId(usize);

#[derive(Debug)]
struct DelayedFreePage {
    version: Version,
    page_id: LogicalPageId,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemappedPage {
    version: Version,
    original_page_id: LogicalPageId,
    new_page_id: LogicalPageId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Version(u64);

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

#[cfg(test)]
mod tests {}
