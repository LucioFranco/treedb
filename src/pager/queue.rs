use zerocopy::{FromBytes, Immutable, IntoBytes};

use super::page::PageBufMut;
use super::{DWALPager, LogicalPageId, PageCache, PhysicalPageId};
use crate::Result;
use std::marker::PhantomData;

pub struct Queue<T> {
    head_reader: Cursor<T>,
    head_writer: Cursor<T>,
    tail_writer: Cursor<T>,
}

#[derive(IntoBytes, FromBytes, Immutable, Debug, Default)]
pub struct QueuePageHeader {
    next_page_id: u64,
    end_offset: u16,
    _pad: [u16; 3],
}

impl<T> Queue<T> {
    pub fn create(pager: &mut PageCache, queue_id: u8) -> Result<Self> {
        let init_page_id = pager.new_last_page_id();

        let head_reader = Cursor::init(pager, init_page_id)?;
        let tail_writer = Cursor::init(pager, init_page_id)?;
        let head_writer = Cursor::init(pager, PhysicalPageId(0))?;

        Ok(Self {
            head_reader,
            head_writer,
            tail_writer,
        })
    }

    pub fn recover(_page_id: LogicalPageId) -> Result<Self> {
        todo!()
    }

    pub fn push_front(&mut self, pager: &mut PageCache, value: T) -> Result<()> {
        self.head_writer.write(pager, value)?;
        Ok(())
    }

    pub fn push_back(&mut self, pager: &mut PageCache, value: T) -> Result<()> {
        self.tail_writer.write(pager, value)?;
        Ok(())
    }

    /// Only return the records that have been flushed, and pops from the front of the queue.
    pub fn pop(&mut self, pager: &mut DWALPager) -> Result<Option<T>> {
        // self.head_reader.read_next(pager)
        todo!()
    }

    pub fn state(&self) -> &QueueState {
        // &self.state
        todo!()
    }
}

struct Cursor<T> {
    page_id: PhysicalPageId,
    page: PageBufMut,
    _pd: PhantomData<fn(T)>,
}

impl<T> Cursor<T> {
    pub(crate) fn init(pager: &mut PageCache, init_page_id: PhysicalPageId) -> Result<Self> {
        let mut page = pager.new_page_buffer();

        let queue_header = QueuePageHeader {
            next_page_id: 0,
            end_offset: 0,
            ..Default::default()
        };

        queue_header.write_to_prefix(page.buf_mut()).unwrap();

        Ok(Cursor {
            page_id: init_page_id,
            page,
            _pd: PhantomData,
        })
    }

    pub(crate) fn write(&mut self, pager: &mut PageCache, item: T) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, FromBytes, IntoBytes, Immutable)]
pub struct QueueState {
    // Only really need u8 but saving space for other things
    queue_id: u64,
    head_page: PhysicalPageId,
    tail_page: PhysicalPageId,
}
