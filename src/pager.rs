use crate::{Error, Result};
use lru::LruCache;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;

/// A versioned pager that uses Cow semantics.
///
/// This pager will attempt to copy and remap pages in favor using locks. This
/// is achieved by collecting a linked lists of remapped pages and free pages.
#[derive(Debug)]
pub struct VersionedPager {
    pager: Pager,
    // Remapped pages go into this queue to allow us to
    // undo remapps at the next commit. So when a snapshot no longer
    // requires a page it will append it to this queue which we can start to
    // undo at commit time.
    remap_queue: VecDeque<LogicalPageId>,
    remapped_pages: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,
}

impl VersionedPager {
    /// Atomically update the page by creating a new page for the specified
    /// version.
    // TODO: add `update` which adds the page to the cache and writes it to the
    // block device.
    pub fn atomic_update(&mut self, page: Page, version: Version) -> Result<()> {
        todo!("VersionedPager::update_page")
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, page_id: LogicalPageId, version: Version) -> Result<Page> {
        todo!()
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
        todo!()
    }

    /// Get the effective last version which can be more than the last commited
    /// last version.
    pub fn effective_last_version(&self) -> Version {
        todo!()
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PhysicalPageId(usize);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct LogicalPageId(usize);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Version(u64);

#[derive(Debug)]
pub struct Page {
    id: LogicalPageId,
    dirty: bool,
    // TODO: use specialized buffer that can support uinitiatlized
    // bytes safely.
    buf: Vec<u8>,
}

struct RemappedPage {
    version: Version,
}

#[derive(Debug)]
pub struct Pager {
    file: File,
    lru: LruCache<LogicalPageId, Page>,
    next_page: usize,
    cache_size: usize,
    page_size: usize,
}

impl Pager {
    pub fn new(page_size: usize, file: File, cache_size: usize) -> Self {
        let lru = LruCache::new(cache_size);

        Pager {
            file,
            cache_size,
            page_size,
            next_page: 0,
            lru,
        }
    }

    pub fn new_page(&mut self) -> Result<&mut Page> {
        let page_id = LogicalPageId(self.next_page);
        self.next_page += 1;

        let page = Page {
            id: page_id,
            buf: Vec::new(),
            dirty: false,
        };

        self.pop_lru()?;
        self.lru.put(page_id, page);

        if let Some(page) = self.lru.get_mut(&page_id) {
            Ok(page)
        } else {
            unreachable!("page is statically set")
        }
    }

    pub fn get(&mut self, page_id: impl Into<LogicalPageId>) -> Result<&mut Page> {
        let page_id = page_id.into();

        if page_id.0 >= self.next_page {
            return Err(Error::IndexOutofBounds(page_id));
        }

        // Check if we need to page in
        if let None = self.lru.peek(&page_id) {
            let offset = self.page_size * usize::from(page_id);

            self.pop_lru()?;

            let mut buf = vec![0; self.page_size];

            self.file.read_exact_at(&mut buf[..], offset as u64)?;

            let page = Page {
                id: page_id,
                buf,
                dirty: false,
            };

            self.lru.put(page_id, page);
        }

        if let Some(page) = self.lru.get_mut(&page_id) {
            Ok(page)
        } else {
            unreachable!("page is statically set")
        }
    }

    fn pop_lru(&mut self) -> Result<()> {
        if self.lru.len() == self.lru.cap() {
            if let Some((_page_id, page)) = self.lru.pop_lru() {
                Pager::flush(self.page_size, &self.file, &page)?;
            }
        }

        Ok(())
    }

    fn flush(page_size: usize, file: &File, page: &Page) -> Result<()> {
        let offset = page_size * usize::from(page.id);

        file.write_all_at(&page.buf[..], offset as u64)?;

        Ok(())
    }

    pub fn evict(&mut self, page_id: impl Into<LogicalPageId>) -> Result<()> {
        let page_id = page_id.into();

        if let Some(page) = self.lru.pop(&page_id) {
            Pager::flush(self.page_size, &self.file, &page)?;
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        for (_, page) in &self.lru {
            if page.dirty {
                Pager::flush(self.page_size, &self.file, &page)?;
            }
        }

        Ok(())
    }
}

impl Page {
    pub fn id(&self) -> LogicalPageId {
        self.id
    }

    pub fn write(&mut self, src: &[u8]) {
        assert!(src.len() >= 4096, "buffer larger than 4096");
        self.buf.extend_from_slice(src);
    }

    pub fn read(&mut self) -> &[u8] {
        &self.buf[..]
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
