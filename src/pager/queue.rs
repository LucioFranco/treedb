use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{LogicalPageId, Page, Pager, PhysicalPageId};
use crate::Result;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Queue<T> {
    head_reader: HeadReader<T>,
    // head_writer: Cursor,
    // tail_writer: Cursor,
}

impl<T: DeserializeOwned + Serialize> Queue<T> {
    pub fn create(initial_page: PhysicalPageId, queue_id: u8) -> Result<Self> {
        Ok(Self {
            head_reader: HeadReader::init(initial_page),
        })
    }

    pub fn recover(_page_id: LogicalPageId) -> Result<Self> {
        todo!()
    }

    pub fn push_back(&mut self, value: T) {
        todo!()
    }

    pub fn pop_front(&mut self, pager: &mut Pager) -> Result<Option<T>> {
        self.head_reader.read_next(pager)
    }

    pub fn state(&self) -> &QueueState {
        // &self.state
        todo!()
    }
}

struct HeadReader<T> {
    page_id: PhysicalPageId,
    end_page: PhysicalPageId,
    header: Option<QueuePageHeader>,
    page: Option<Arc<Page>>,

    offset: u64,

    _pd: PhantomData<T>,
}

impl<T: DeserializeOwned> HeadReader<T> {
    fn init(page_id: PhysicalPageId) -> Self {
        Self {
            page_id,
            end_page: page_id,
            header: None,
            page: None,
            offset: 0,
            _pd: PhantomData,
        }
    }

    fn read_next(&mut self, pager: &mut Pager) -> Result<Option<T>> {
        // If we have not loaded a page do that now
        if self.page.is_none() {
            // self.head_reader
            //     .replace(read_queue_page(pager, self.page_id)?);
            todo!()
        }

        // We know we just loaded a page if it was empty
        // so this should never panic;
        let header = self.header.as_mut().expect("Page already loaded");

        // Check if we are at the end of the current page
        if self.offset == header.end_offset as u64 {
            // If the current page is the last page
            if self.page_id == self.end_page {
                return Ok(None);
            }

            // TODO: if we are in pop mode then we should also
            // free the page.

            // *page = read_queue_page(pager, page.next_page_id)?;
        }

        let page = self.page.as_mut().unwrap();
        let buf = &page.buf()[self.offset as usize..];
        let next_item = bincode::deserialize(&buf[..])?;

        Ok(Some(next_item))
    }
}

#[derive(Debug)]
struct TailWriter<T> {
    end_page_id: PhysicalPageId,
    page: Option<QueuePageHeader>,

    offset: u64,

    _pd: PhantomData<T>,
}

impl<T: Serialize> TailWriter<T> {
    fn new(end_page_id: PhysicalPageId) -> Self {
        Self {
            end_page_id,
            page: None,
            offset: 0,
            _pd: PhantomData,
        }
    }

    fn write(&mut self, pager: &mut Pager, item: T) -> Result<Option<T>> {
        let bytes_needed = bincode::serialized_size(&item);

        if self.page.is_none() {
            let page = pager.new_page_buffer();
            let header = QueuePageHeader {
                next_page_id: None,
                end_offset: 0,
            };
        }

        // TODO: handle case where we are at the end of the page and need to allocate a new one

        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueState {
    queue_id: u8,
    head_page: PhysicalPageId,
    tail_page: PhysicalPageId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueuePageHeader {
    next_page_id: Option<PhysicalPageId>,
    end_offset: u16,
}
