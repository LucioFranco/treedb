use std::collections::VecDeque;

/// A FIFO Queue built ontop of the pager.
#[derive(Debug)]
pub struct Queue<T> {
    queue: VecDeque<RawPage<T>>,
}

#[derive(Debug)]
struct RawPage<T> {
    inner: T,
}
