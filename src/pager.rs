use crate::lru::Lru;
use lru::LruCache;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("index `{0}` out of bounds")]
    IndexOutofBounds(PageId),
}

#[derive(Debug)]
pub struct Pager {
    file: File,
    // map: HashMap<PageId, usize>,
    // slots: Box<[Page]>,
    lru: LruCache<PageId, Page>,
    cache_size: usize,
    page_size: usize,
    // lru: Lru<PageId>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PageId(usize);

#[derive(Debug)]
pub enum Page {
    Free,
    Uncached,
    Cached(Box<CachedPage>),
}

#[derive(Debug)]
pub struct CachedPage {
    id: PageId,
    data: Box<[u8]>,
    dirty: bool,
}

impl Pager {
    pub fn new(page_size: usize, file: File, cache_size: usize) -> Self {
        let mut slots = Vec::with_capacity(cache_size);
        slots.extend(std::iter::repeat_with(Page::default).take(cache_size));

        let lru = Lru::new(cache_size);

        Pager {
            file,
            // slots: slots.into_boxed_slice(),
            // map: HashMap::new(),
            cache_size,
            page_size,
            lru,
        }
    }

    pub fn get(&mut self, page_id: impl Into<PageId>) -> Result<&mut CachedPage> {
        let page_id = page_id.into();
        if let Some(page) = self.lru.get_mut(&page_id) {
            match page {
                Page::Free => {
                    *page = Page::Cached(Box::new(CachedPage {
                        id: page_id,
                        data: vec![0; self.page_size].into_boxed_slice(),
                        dirty: true,
                    }));
                }
                Page::Uncached => {
                    let offset = self.page_size * usize::from(page_id);

                    // let unused_id = self.lru.insert(page_id, offset);

                    let mut buf = vec![0; self.page_size];

                    self.file.read_exact_at(&mut buf[..], offset as u64)?;

                    let page = Page::Cached(Box::new(CachedPage {
                        id: page_id,
                        data: buf.into_boxed_slice(),
                        dirty: false,
                    }));

                    // if let Some((page_id, page)) = self.lru.put(page_id, page) {

                    // }
                }
                Page::Cached(_) => self.lru.bump(&page_id),
            };

            match page {
                Page::Cached(cached) => Ok(cached),
                _ => unreachable!(),
            }
        } else {
            Err(Error::IndexOutofBounds(page_id))
        }
    }

    pub fn flush(&mut self, page_id: impl Into<PageId>) -> Result<()> {
        let page_id = page_id.into();
        if let Some(page) = self.slots.get_mut(usize::from(page_id)) {
            if let Page::Cached(page) = page {
                let offset = self.page_size * usize::from(page_id);

                self.file.write_all_at(&page.data[..], offset as u64)?;

                page.dirty = false;
            }

            Ok(())
        } else {
            Err(Error::IndexOutofBounds(page_id))
        }
    }

    pub fn evict(&mut self, page_id: impl Into<PageId>) -> Result<()> {
        let page_id = page_id.into();
        self.flush(page_id)?;
        self.slots[usize::from(page_id)] = Page::Uncached;
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        todo!()
    }
}

impl Default for Page {
    fn default() -> Self {
        Page::Free
    }
}

impl CachedPage {
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.data[..]
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<PageId> for usize {
    fn from(t: PageId) -> Self {
        t.0 as usize
    }
}

impl From<usize> for PageId {
    fn from(t: usize) -> Self {
        PageId(t)
    }
}
