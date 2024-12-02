use std::marker::PhantomData;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    pager::{
        page::{PageBuf, PageBufMut},
        PageCache, PhysicalPageId, PAGE_SIZE,
    },
    Result,
};

// #[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug, Unaligned)]
// #[repr(C)]
struct QueuePage<'a> {
    header: &'a QueuePageHeader,
    data: &'a [u8],
}

struct QueuePageMut<'a> {
    header: &'a mut QueuePageHeader,
    data: &'a mut [u8],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Debug)]
struct QueuePageHeader {
    next_page_id: PhysicalPageId,
    next_offset: u16,
    end_offset: u16,
    item_space: u16,
    _pad1: u16,
    _pad2: [u64; 2],
}

pub(crate) struct ReadCursor<T> {
    page_id: PhysicalPageId,
    page: Option<PageBuf>,

    next_page_id: PhysicalPageId,
    end_page_id: PhysicalPageId,

    offset: usize,

    _pd: PhantomData<fn(T)>,
}

impl<T: FromBytes + KnownLayout + Immutable> ReadCursor<T> {
    pub(crate) fn init(
        pager: &mut PageCache,
        init_page_id: PhysicalPageId,
        end_page_id: PhysicalPageId,
    ) -> Result<Self> {
        let (next_page_id, page) = if init_page_id != end_page_id {
            let buf = pager.read_page(init_page_id)?;
            (init_page_id, Some(buf))
        } else {
            (PhysicalPageId::INVALID_ID, None)
        };

        Ok(Self {
            page_id: init_page_id,
            page,
            next_page_id,
            end_page_id,

            offset: size_of::<QueuePageHeader>(),

            _pd: PhantomData,
        })
    }

    pub(crate) fn pop(&mut self, pager: &mut PageCache) -> Result<Option<T>> {
        if self.page_id == PhysicalPageId::INVALID_ID || self.page_id == self.end_page_id {
            return Ok(None);
        }

        if self.page.is_none() {
            // If our current page != next_page_id then we must load it so
            // that we can read the next_page's header to get its linked next page.
            if self.page_id != self.next_page_id {
                self.load_next_page(pager, self.page_id)?;
            }

            let page = self.page().unwrap();

            if page.header.next_page_id != self.end_page_id {
                self.load_next_page(pager, page.header.next_page_id)?;
            } else {
                self.next_page_id = PhysicalPageId::INVALID_ID;
            }
        }

        let page = self.page().unwrap();

        let (item, _) = T::read_from_prefix(&page.data[self.offset..]).unwrap();

        let item_size = size_of::<T>();

        // if we have reached the end of the page, start reading the next page.
        if self.offset + item_size == page.header.end_offset as usize {
            todo!("start reading the next page");
        }

        // Ensure we updated the offset, this was moved down here since
        // I got a borrow issue in the if statement to read the next page.
        self.offset += item_size;

        Ok(Some(item))
    }

    fn page(&self) -> Option<QueuePage<'_>> {
        self.page.as_ref().map(|p| {
            let (header, data) = QueuePageHeader::ref_from_prefix(p.buf()).unwrap();
            QueuePage { header, data }
        })
    }

    fn load_next_page(&mut self, pager: &mut PageCache, page_id: PhysicalPageId) -> Result<()> {
        self.next_page_id = page_id;

        let page = pager.read_page(page_id)?;
        self.page = Some(page);

        Ok(())
    }
}

pub(crate) struct WriteCursor<T> {
    page_id: PhysicalPageId,
    page: Option<PageBufMut>,

    next_page_id: PhysicalPageId,
    end_page_id: PhysicalPageId,

    offset: usize,

    _pd: PhantomData<fn(T)>,
}

impl<T: IntoBytes + Immutable> WriteCursor<T> {
    pub(crate) fn init(
        pager: &mut PageCache,
        init_page_id: PhysicalPageId,
        end_page_id: PhysicalPageId,
    ) -> Result<Self> {
        let mut me = Self {
            page_id: PhysicalPageId::INVALID_ID,
            page: None,
            next_page_id: PhysicalPageId::INVALID_ID,
            end_page_id,

            offset: size_of::<QueuePageHeader>(),

            _pd: PhantomData,
        };

        if init_page_id != PhysicalPageId::INVALID_ID {
            me.add_new_page(pager, init_page_id, 0, true)?;
        }

        Ok(me)
    }

    pub fn write(&mut self, pager: &mut PageCache, item: T) -> Result<()> {
        let bytes_needed = size_of::<T>();

        // Check if we need a new page
        if self.page_id == PhysicalPageId::INVALID_ID
            || self.offset + bytes_needed > self.page().unwrap().header.item_space as usize
        {
            // TODO: this should probably pull a free page from the original pager, but how?
            let new_page_id = pager.new_last_page_id();
            self.add_new_page(pager, new_page_id, 0, true)?;
        }

        let offset = self.offset;
        let page = self.page().unwrap();

        item.write_to(&mut page.data[offset as usize..bytes_needed])
            .unwrap();

        let new_offset = offset + bytes_needed;

        page.header.end_offset = new_offset as u16;
        self.offset = new_offset;

        // TODO: add queue numEntries++

        Ok(())
    }

    fn add_new_page(
        &mut self,
        pager: &mut PageCache,
        new_page_id: PhysicalPageId,
        new_offset: usize,
        init_page: bool,
    ) -> Result<()> {
        if let Some(mut page) = self.page.take() {
            let (mut queue_page_header, _) =
                QueuePageHeader::read_from_prefix(page.buf_mut()).unwrap();

            queue_page_header.next_page_id = new_page_id;
            queue_page_header.next_offset = new_offset as u16;

            pager.write_page(self.page_id, &page.freeze())?;
        }

        self.page_id = new_page_id;
        self.offset = new_offset;

        if init_page {
            let page = self.page.insert(pager.new_page_buffer());
            page.init();

            let (header, _data) = QueuePageHeader::mut_from_prefix(page.buf_mut()).unwrap();

            header.end_offset = 0;
            header.item_space = (PAGE_SIZE - size_of::<QueuePageHeader>()) as u16;
        }

        // TODO: clear the page if here we didn't init the page

        Ok(())
    }

    fn set_next(&mut self, page_id: PhysicalPageId, offset: usize) {
        let page = self.page().unwrap();
        page.header.next_page_id = page_id;
        page.header.next_offset = offset as u16;
    }

    fn page(&mut self) -> Option<QueuePageMut<'_>> {
        self.page.as_mut().map(|p| {
            let (header, data) = QueuePageHeader::mut_from_prefix(p.buf_mut()).unwrap();
            QueuePageMut { header, data }
        })
    }
}

impl<'a> QueuePageMut<'a> {
    fn set_next(&mut self, page_id: PhysicalPageId, offset: usize) {
        self.header.next_page_id = page_id;
        self.header.next_offset = offset as u16;
    }
}
