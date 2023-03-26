use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::{LogicalPageId, PhysicalPageId};
use crate::Result;
use std::cmp::Ordering;
use std::marker::PhantomData;

pub trait QueuePager {
    fn read_page(&self, page_id: PhysicalPageId) -> Result<Bytes>;
}

/// A FIFO Queue built ontop of the pager.
#[derive(Debug)]
pub struct Queue<T> {
    head_reader: HeadReader<T>,
    // head_writer: Cursor,
    // tail_writer: Cursor,
}

impl<T: Value> Queue<T> {
    pub fn create(initial_page: PhysicalPageId, queue_id: u8) -> Result<Self> {
        // Self {
        //     head_reader: Cursor::init_pop(initial_page),
        //     head_writer: Cursor::init_write(initial_page
        // }
        todo!()
    }

    pub fn recover(_page_id: LogicalPageId) -> Result<Self> {
        todo!()
    }

    pub fn push(&mut self, value: T) {
        todo!()
    }

    pub fn pop(&mut self, cutoff: T::Cutoff) -> Option<T> {
        // let front = self.queue.front()?;

        // if front.eq(cutoff) != Ordering::Greater {
        //     self.queue.pop_front()
        // } else {
        //     None
        // }
        todo!()
    }

    pub fn state(&self) -> &QueueState {
        // &self.state
        todo!()
    }
}

#[derive(Debug)]
struct HeadReader<T> {
    page_id: PhysicalPageId,
    end_page: PhysicalPageId,
    page: Option<QueuePage>,

    offset: u64,

    _pd: PhantomData<T>,
}

impl<T: DeserializeOwned> HeadReader<T> {
    fn init(page_id: PhysicalPageId) -> Self {
        Self {
            page_id,
            end_page: page_id,
            page: None,
            offset: 0,
            _pd: PhantomData,
        }
    }

    fn read_next(&mut self, pager: &mut impl QueuePager) -> Result<Option<T>> {
        // If we have not loaded a page do that now
        if self.page.is_none() {
            self.page.replace(read_queue_page(pager, self.page_id)?);
        }

        // We know we just loaded a page if it was empty
        // so this should never panic;
        let page = self.page.as_mut().expect("Page already loaded");

        // Check if we are at the end of the current page
        if self.offset == page.end_offset as u64 {
            // If the current page is the last page
            if self.page_id == self.end_page {
                return Ok(None);
            }

            // TODO: if we are in pop mode then we should also
            // free the page.

            *page = read_queue_page(pager, page.next_page_id)?;
        }

        let buf = &page.buf[self.offset as usize..];
        let next_item = bincode::deserialize(&buf[..])?;

        Ok(Some(next_item))
    }
}

fn read_queue_page<T: DeserializeOwned>(
    pager: &mut impl QueuePager,
    page_id: PhysicalPageId,
) -> Result<T> {
    let buf = pager.read_page(page_id)?;

    let page = bincode::deserialize(&buf[..])?;

    Ok(page)
}

pub trait Value {
    type Cutoff: PartialEq;

    fn eq(&self, other: Self::Cutoff) -> Ordering;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueState {
    queue_id: u8,
    head_page: PhysicalPageId,
    tail_page: PhysicalPageId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueuePage {
    next_page_id: PhysicalPageId,
    end_offset: u16,
    buf: Bytes,
}
