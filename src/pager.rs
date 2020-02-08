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
    slots: Box<[Page]>,
    cache_size: usize,
    page_size: usize,
}

#[derive(Debug, Clone, Copy)]
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

        Pager {
            file,
            slots: slots.into_boxed_slice(),
            cache_size,
            page_size,
        }
    }

    pub fn get(&mut self, page_id: impl Into<PageId>) -> Result<&mut CachedPage> {
        let page_id = page_id.into();
        if let Some(page) = self.slots.get_mut(usize::from(page_id)) {
            page.read(page_id, self.page_size, &self.file)
        } else {
            Err(Error::IndexOutofBounds(page_id))
        }
    }

    pub fn flush(&mut self, page_id: impl Into<PageId>) -> Result<()> {
        let page_id = page_id.into();
        if let Some(page) = self.slots.get_mut(usize::from(page_id)) {
            page.write(page_id, self.page_size, &self.file)
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

impl Page {
    fn read(&mut self, index: PageId, page_size: usize, file: &File) -> Result<&mut CachedPage> {
        let page = match self {
            Page::Free => {
                let page = Box::new(CachedPage {
                    id: index,
                    data: vec![0; page_size].into_boxed_slice(),
                    dirty: true,
                });

                *self = Page::Cached(page);

                match self {
                    Page::Cached(cached) => cached,
                    _ => unreachable!(),
                }
            }
            Page::Uncached => {
                let offset = page_size * usize::from(index);

                let mut buf = vec![0; page_size];

                file.read_exact_at(&mut buf[..], offset as u64)?;

                let page = Box::new(CachedPage {
                    id: index,
                    data: buf.into_boxed_slice(),
                    dirty: false,
                });

                *self = Page::Cached(page);

                match self {
                    Page::Cached(cached) => cached,
                    _ => unreachable!(),
                }
            }
            Page::Cached(cached) => cached,
        };

        Ok(page)
    }

    fn write(&mut self, index: PageId, page_size: usize, file: &File) -> Result<()> {
        if let Page::Cached(page) = self {
            let offset = page_size * usize::from(index);

            file.write_all_at(&page.data[..], offset as u64)?;

            page.dirty = false;
        }

        Ok(())
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
