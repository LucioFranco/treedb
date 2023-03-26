#![allow(dead_code)]

mod page;
mod queue;
#[cfg(test)]
mod test;

use crate::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use page::{OwnedPage, SharedPage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;

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

/// A versioned pager that uses Cow semantics.
///
/// This pager will attempt to copy and remap pages in favor using locks. This
/// is achieved by collecting a linked lists of remapped pages and free pages.
#[derive(Debug)]
pub struct VersionedPager<F> {
    file: F,
    header: Header,

    // Remapped pages go into this queue to allow us to
    // undo remapps at the next commit. So when a snapshot no longer
    // requires a page it will append it to this queue which we can start to
    // undo at commit time.
    remap_queue: VecDeque<RemappedPage>,
    delayed_free_list: VecDeque<DelayedFreePage>,
    free_list: VecDeque<LogicalPageId>,

    page_table: HashMap<LogicalPageId, BTreeMap<Version, PhysicalPageId>>,

    next_page_id: usize,
}

impl<F> VersionedPager<F>
where
    F: File,
{
    /// Recover a `VersionedPager`, if the file is empty it will create a new
    /// pager.
    pub fn recover(file: F) -> Result<Self> {
        let file_size = file.len()?;

        if file_size > PAGE_SIZE {
            let mut header_buf = BytesMut::zeroed(PAGE_SIZE);
            // TODO: Probably need to make this read_exact?
            file.read_at(&mut header_buf[..], 0)?;
            let header = bincode::deserialize::<Header>(&header_buf[..])?;

            Ok(Self {
                file,
                header,
                page_table: HashMap::new(),
                // One because header page
                next_page_id: 1,
                delayed_free_list: VecDeque::default(),
                remap_queue: VecDeque::default(),
                free_list: VecDeque::default(),
            })
        } else {
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
                page_table: HashMap::new(),
                // One because header page
                next_page_id: 1,
                delayed_free_list: VecDeque::default(),
                remap_queue: VecDeque::default(),
                free_list: VecDeque::default(),
            };

            pager.write_header()?;

            Ok(pager)
        }
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

    pub fn new_page_buffer(&mut self) -> Result<OwnedPage> {
        let page = OwnedPage::new(
            self.new_page_id()?,
            self.current_version(),
            self.header.page_size as usize,
        );

        Ok(page)
    }

    pub fn write_page(&mut self, page: OwnedPage) -> Result<SharedPage> {
        let offset = page.id().0 * self.header.page_size as usize;
        self.file.write_at(page.chunk(), offset as u64)?;

        Ok(page.freeze())
    }

    /// Atomically update the page by creating a new page for the specified
    /// version.
    // TODO: add `update` which adds the page to the cache and writes it to the
    // block device.
    pub fn atomic_update(
        &mut self,
        page_id: LogicalPageId,
        version: Version,
        data: &[u8],
    ) -> Result<LogicalPageId> {
        // Copy page
        let new_page_id = self.new_page_id()?;

        let versions = self.page_table.entry(page_id).or_insert(BTreeMap::new());

        // Pushed into the queue to be un-mapped later
        self.remap_queue.push_back(RemappedPage {
            version,
            original_page_id: page_id,
            new_page_id,
        });

        versions.insert(version, PhysicalPageId(new_page_id.0));

        let mut page = OwnedPage::new(page_id, version, data.len());

        page.put(&data[..]);

        self.write_page(page)?;

        Ok(new_page_id)
    }

    /// Read a page at a specific version.
    // TODO: add `read` that can support optionally bypassing the cache.
    pub fn read_at(&mut self, id: LogicalPageId, version: Version) -> Result<SharedPage> {
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

        let buf = self.read_page(page_id)?;

        Ok(SharedPage::new(id, version, buf))
    }

    fn read_page(&self, page_id: PhysicalPageId) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(PAGE_SIZE);
        let offset = page_id.0 * PAGE_SIZE;
        self.file.read_at(&mut buf[..], offset as u64)?;

        Ok(buf.freeze())
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

    fn remap_cleanup(&mut self) -> Result<()> {
        todo!()
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

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPageId(usize);

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

// #[derive(Debug, Clone)]
// pub struct Page {
//     id: LogicalPageId,
//     version: Version,
//     buf: Buf,
// }

// #[derive(Debug, Clone)]
// pub enum Buf {
//     Shared(Arc<Vec<u8>>),
//     Owned(Vec<u8>),
// }

// impl Buf {
//     /// If this page is shared, this will make a copy.
//     pub fn to_mut(&mut self) -> &mut [u8] {
//         match *self {
//             Buf::Shared(ref buf) => {
//                 *self = Buf::Owned(Vec::clone(&buf));

//                 match *self {
//                     Buf::Shared(_) => unreachable!(),
//                     Buf::Owned(ref mut b) => b,
//                 }
//             }

//             Buf::Owned(ref mut buf) => &mut buf[..],
//         }
//     }
// }

// impl std::ops::Deref for Buf {
//     type Target = [u8];

//     fn deref(&self) -> &[u8] {
//         match &self {
//             Buf::Owned(b) => &b[..],
//             Buf::Shared(b) => &b[..],
//         }
//     }
// }

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
