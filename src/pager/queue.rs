mod cursor;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{PageCache, PhysicalPageId};
use crate::Result;

use cursor::{ReadCursor, WriteCursor};

pub struct FIFOQueue<T> {
    head_reader: ReadCursor<T>,
    head_writer: WriteCursor<T>,
    tail_writer: WriteCursor<T>,
}

impl<T: IntoBytes + FromBytes + KnownLayout + Immutable> FIFOQueue<T> {
    pub fn create(pager: &mut PageCache, _queue_id: u8) -> Result<Self> {
        let init_page_id = pager.new_last_page_id();

        let head_reader = ReadCursor::init(pager, init_page_id, init_page_id)?;
        let tail_writer = WriteCursor::init(pager, init_page_id, PhysicalPageId::INVALID_ID)?;
        let head_writer = WriteCursor::init(
            pager,
            PhysicalPageId::INVALID_ID,
            PhysicalPageId::INVALID_ID,
        )?;

        Ok(Self {
            head_reader,
            head_writer,
            tail_writer,
        })
    }

    pub fn recover(_page_id: PhysicalPageId) -> Result<Self> {
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
    pub fn pop(&mut self, pager: &mut PageCache) -> Result<Option<T>> {
        self.head_reader.pop(pager)
    }

    pub fn state(&self) -> &QueueState {
        // &self.state
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
