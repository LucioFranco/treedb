use super::LogicalPageId;
use crate::Result;
use std::cmp::Ordering;
use std::collections::VecDeque;

/// A FIFO Queue built ontop of the pager.
#[derive(Debug)]
pub struct Queue<T> {
    queue: VecDeque<T>,
}

impl<T: Value> Queue<T> {
    pub fn recover(page_id: LogicalPageId) -> Result<Self> {
        todo!()
    }

    pub fn push(&mut self, value: T) {
        self.queue.push_back(value)
    }

    pub fn pop(&mut self, cutoff: T::Cutoff) -> Option<T> {
        let front = self.queue.front()?;

        if front.eq(cutoff) != Ordering::Greater {
            self.queue.pop_front()
        } else {
            None
        }
    }
}

trait Value {
    type Cutoff: PartialEq;

    fn eq(&self, other: Self::Cutoff) -> Ordering;
}
