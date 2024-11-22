#![allow(dead_code)]

#[cfg(test)]
mod test;

mod cache;
mod page;
mod queue;

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use bytes::BytesMut;
use cache::SieveCache;
use serde::{Deserialize, Serialize};
use typed_arena::Arena;
use zerocopy::{
    little_endian::{U16, U32, U64},
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
};

use crate::Result;

use self::{page::Page, queue::Queue};

/// First version of this!
const VERSION: u16 = 1;
/// 4kb page
const PAGE_SIZE: usize = 4 * 1024;

type PageCache = SieveCache<LogicalPageId, PageCacheEntry>;

pub trait File {
    fn len(&self) -> Result<usize>;
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;
    fn sync_data(&self) -> Result<()>;
}

#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C)]
struct Header {
    version: U16,
    page_size: U32,
    page_count: U64,
    commited_version: U64,
    oldest_version: U64,
}

pub struct Pager {
    file: Box<dyn File>,
    header: Header,
    cache: PageCache,
    page_arena: Arena<Page>,
    page_table: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,
    next_page_id: usize,

    remap_queue: Queue<RemappedPage>,
}

impl Pager {
    /// Recover a `VersionedPager`, if the file is empty it will create a new
    /// pager.
    pub fn recover(file: impl File + 'static) -> Result<Self> {
        let file_size = file.len()?;

        let cache = SieveCache::new(1024).unwrap();
        let page_table = HashMap::new();
        let remap_queue = Queue::create(PhysicalPageId(0), 0)?;

        let file = Box::new(file) as Box<dyn File>;

        let header = if file_size > PAGE_SIZE {
            let mut header_buf = BytesMut::zeroed(PAGE_SIZE);
            // TODO: Probably need to make this read_exact?
            file.read_at(&mut header_buf[..], 0)?;
            let header_size = std::mem::size_of::<Header>();
            Header::read_from_bytes(&header_buf[..header_size]).unwrap()
        } else {
            Header {
                version: VERSION.into(),
                page_size: (PAGE_SIZE as u32).into(),
                // Start with 1, we could add a backup here.
                page_count: 1.into(),
                commited_version: 1.into(),
                oldest_version: 1.into(),
            }
        };

        let pager = Self {
            file,
            header,
            page_arena: Arena::new(),
            cache,
            page_table,
            remap_queue,
            // One because header page
            next_page_id: 1,
        };

        pager.write_header()?;

        Ok(pager)
    }

    fn write_header(&self) -> Result<()> {
        let header = self.header.as_bytes();

        debug_assert!(header.len() < PAGE_SIZE, "header must be below PAGE_SIZE");

        self.file.write_at(header, 0)?;

        Ok(())
    }

    pub fn new_page_id(&mut self) -> LogicalPageId {
        // TODO: re-use freemap etc
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        LogicalPageId(page_id)
    }

    fn new_page_buffer(&mut self) -> Page {
        Page::init(&std::alloc::System, VERSION as u32, 0u64)
    }

    fn read_page(&mut self, page_id: PhysicalPageId) -> Result<Page> {
        // TODO: figure out how to hand out pages
        let logical_page_id = LogicalPageId(page_id.0);

        if let Some(entry) = self.cache.get(&logical_page_id) {
            Ok(entry.page.clone())
        } else {
            let mut page = match self.cache.evict() {
                Some((_, entry)) => entry.page,
                None => self.new_page_buffer(),
            };

            self.read_physical_page(page_id, &mut page)?;

            let entry = PageCacheEntry { page: page.clone() };

            self.cache.insert(logical_page_id, entry);

            Ok(page)
        }
    }

    fn read_physical_page(&self, page_id: PhysicalPageId, page: &mut Page) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.file.read_at(page.buf_mut(), offset as u64)?;

        Ok(())
    }

    fn write_page(&mut self, page_id: PhysicalPageId, page: &Page) -> Result<()> {
        let offset = page_id.0 * self.header.page_size.get() as usize;
        self.file.write_at(page.buf(), offset as u64)?;

        Ok(())
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, id: LogicalPageId, version: Version) -> Result<Page> {
        let page_id = self.get_physical_page_id(id, version);

        let page = self.read_page(page_id)?;

        Ok(page)
    }

    fn get_physical_page_id(&mut self, id: LogicalPageId, version: Version) -> PhysicalPageId {
        if let Some(remapped_pages) = self.page_table.get(&id) {
            if let Some((_, page)) = remapped_pages
                .range(..)
                .filter(|(v, _)| *v <= &version)
                .next_back()
            {
                return *page;
            }
        }

        PhysicalPageId(id.0)
    }

    pub fn update_page(&mut self, page_id: LogicalPageId, page: Page) -> Result<()> {
        if !self.cache.contains_key(&page_id) {
            self.cache
                .insert(page_id, PageCacheEntry { page: page.clone() });
        } else {
            let entry = self
                .cache
                .get_mut(&page_id)
                .expect("Update page failed on cache lookup");

            entry.page = page.clone();
        }

        self.write_page(PhysicalPageId(page_id.0), &page)?;

        Ok(())
    }

    /// Atomically update the page by creating a new page for the specified
    /// version.
    pub fn atomic_update(
        &mut self,
        page_id: LogicalPageId,
        version: Version,
        page: Page,
    ) -> Result<LogicalPageId> {
        // Copy page
        let new_page_id = self.new_page_id();

        self.update_page(new_page_id, page)?;

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
    pub fn free(&mut self, _page_id: LogicalPageId, _version: Version) {
        // First check if this page id matches any "originally" remapped pages
        // from the remapped_pages map. If it is an original page then add
        // it to the back of the `remap_queue`. If the version is older than
        // the last effective version we can add it to the freelist page,
        // otherwise add it to the delayed free list queue.
        todo!()
    }

    fn current_version(&self) -> Version {
        Version(self.header.commited_version.get() + 1)
    }
}

struct PageCacheEntry {
    page: Page,
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

#[cfg(test)]
mod tests {}
