#![allow(dead_code)]

#[cfg(test)]
mod test;

mod arena;
mod cache;
mod page;
mod queue;

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

use arena::Arena;
use bytes::BytesMut;
use page::{PageBuf, PageBufMut};
use zerocopy::{
    little_endian::{U16, U32, U64},
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
};

use crate::Result;

use self::{cache::Cache, queue::Queue};

/// First version of this!
const VERSION: u16 = 1;
/// 4kb page
const PAGE_SIZE: usize = 4 * 1024;

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

pub struct DWALPager {
    header: Header,
    page_table: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,
    page_cache: PageCache,
    remap_queue: Queue<RemappedPage>,
}

struct PageCache {
    file: Box<dyn File>,
    next_page_id: usize,
    cache: Cache<LogicalPageId, PageCacheEntry>,
    page_arena: Arena<std::alloc::System>,
}

impl DWALPager {
    /// Recover a `VersionedPager`, if the file is empty it will create a new
    /// pager.
    pub fn recover(file: impl File + 'static) -> Result<Self> {
        let file_size = file.len()?;

        let page_table = HashMap::new();

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

        let mut page_cache = PageCache::new(file);

        let remap_queue = Queue::create(&mut page_cache, 0)?;

        let pager = Self {
            header,
            page_table,
            page_cache,
            remap_queue,
        };

        pager.write_header()?;

        Ok(pager)
    }

    pub fn new_page_id(&mut self) -> LogicalPageId {
        // TODO: re-use freemap etc
        let page_id = self.page_cache.new_last_page_id();

        LogicalPageId(page_id.0)
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, id: LogicalPageId, version: Version) -> Result<PageBuf> {
        let page_id = self.get_physical_page_id(id, version);

        let page = self.page_cache.read_page(page_id)?;

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

    /// Atomically update the page by creating a new page for the specified
    /// version.
    pub fn atomic_update(
        &mut self,
        page_id: LogicalPageId,
        version: Version,
        page: PageBufMut,
    ) -> Result<LogicalPageId> {
        // Copy page
        let new_page_id = self.new_page_id();

        self.page_cache.update_page(new_page_id, page)?;

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
        self.page_cache.flush()?;

        Ok(())
    }

    pub fn new_page_buffer(&mut self) -> PageBufMut {
        self.page_cache.new_page_buffer()
    }

    pub fn update_page(&mut self, page_id: LogicalPageId, page: PageBufMut) -> Result<()> {
        self.page_cache.update_page(page_id, page)
    }

    fn write_page(&mut self, page_id: PhysicalPageId, page: &PageBuf) -> Result<()> {
        self.page_cache.write_page(page_id, page)
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

    fn write_header(&self) -> Result<()> {
        self.page_cache.write_header(&self.header)
    }
}

impl PageCache {
    fn new(file: Box<dyn File>) -> Self {
        let cache = Cache::new(1024);
        let page_arena = Arena::new(std::alloc::System, PAGE_SIZE, 1024);

        Self {
            file,
            cache,
            page_arena,
            // One because header page
            next_page_id: 1,
        }
    }

    pub fn new_last_page_id(&mut self) -> PhysicalPageId {
        // TODO: re-use freemap etc
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        PhysicalPageId(page_id)
    }

    fn new_page_buffer(&mut self) -> PageBufMut {
        match self.alloc_page_buffer() {
            Some(buf) => buf,
            None => {
                // TODO: handle allocation failed & evict failed
                let (_page_id, page_buf) = self.cache.evict().unwrap();

                // TODO: handle that this page_buf is currently has a ref outstanding
                let page_buf_mut = page_buf.page.try_take().unwrap();

                page_buf_mut
            }
        }
    }

    fn alloc_page_buffer(&mut self) -> Option<PageBufMut> {
        let ptr = self.page_arena.alloc().ok()?;
        // let ptr = NonNull::slice_from_raw_parts(ptr, PAGE_SIZE);

        Some(PageBufMut::new(ptr))
    }

    fn read_page(&mut self, page_id: PhysicalPageId) -> Result<PageBuf> {
        // TODO: figure out how to hand out pages
        let logical_page_id = LogicalPageId(page_id.0);

        if let Some(entry) = self.cache.get(&logical_page_id) {
            Ok(entry.page.clone())
        } else {
            let mut page = self.new_page_buffer();

            self.read_physical_page(page_id, &mut page)?;

            let page = page.freeze();

            let entry = PageCacheEntry { page: page.clone() };

            self.cache.insert(logical_page_id, entry);

            Ok(page)
        }
    }

    pub fn update_page(&mut self, page_id: LogicalPageId, page: PageBufMut) -> Result<()> {
        let page = page.freeze();

        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page = page.clone();
        } else {
            let entry = PageCacheEntry { page: page.clone() };

            self.cache.insert(page_id, entry);
        }

        self.write_page(PhysicalPageId(page_id.0), &page)?;

        Ok(())
    }

    fn read_physical_page(&self, page_id: PhysicalPageId, page: &mut PageBufMut) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.file.read_at(page.buf_mut(), offset as u64)?;

        Ok(())
    }

    fn write_page(&mut self, page_id: PhysicalPageId, page: &PageBuf) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.file.write_at(page.buf(), offset as u64)?;

        Ok(())
    }

    fn write_header(&self, header: &Header) -> Result<()> {
        let header = header.as_bytes();

        debug_assert!(header.len() < PAGE_SIZE, "header must be below PAGE_SIZE");

        self.file.write_at(header, 0)?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file.sync_data()
    }
}

struct PageCacheEntry {
    page: PageBuf,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct PhysicalPageId(usize);

impl PhysicalPageId {
    const INVALID_ID: Self = PhysicalPageId(usize::MAX);
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct LogicalPageId(usize);

#[derive(Debug)]
struct DelayedFreePage {
    version: Version,
    page_id: LogicalPageId,
}

#[derive(Debug)]
struct RemappedPage {
    version: Version,
    original_page_id: LogicalPageId,
    new_page_id: LogicalPageId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
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
