use std::marker::PhantomData;

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::{
    pager::{page::PageBuf, PageCache, PhysicalPageId},
    Result,
};

pub(crate) struct ReadCursor<T> {
    page_id: PhysicalPageId,
    page: Option<PageBuf>,

    next_page_id: PhysicalPageId,
    end_page_id: PhysicalPageId,

    offset: usize,

    _pd: PhantomData<fn(T)>,
}

#[derive(FromBytes, Immutable, KnownLayout, Debug)]
#[repr(C)]
struct QueuePage {
    header: QueuePageHeader,
    data: [u8],
}

#[derive(FromBytes, KnownLayout, Immutable, Debug)]
struct QueuePageHeader {
    next_page_id: PhysicalPageId,
    next_offset: u16,
    end_offset: u16,
    _pad: [u64; 2],
    item_space: u16,
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

            // TODO: get an accurate size_of QueuePageHeader,
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

    fn page(&self) -> Option<&QueuePage> {
        self.page
            .as_ref()
            .map(|p| QueuePage::ref_from_bytes(p.buf()).unwrap())
    }

    fn load_next_page(&mut self, pager: &mut PageCache, page_id: PhysicalPageId) -> Result<()> {
        self.next_page_id = page_id;

        let page = pager.read_page(page_id)?;
        self.page = Some(page);

        Ok(())
    }
}
