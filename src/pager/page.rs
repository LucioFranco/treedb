use super::{LogicalPageId, Version, PAGE_SIZE};

pub struct Page {
    buf: [u8; PAGE_SIZE],
}

#[derive(Debug, Clone, Copy)]
struct PageHeader {
    id: LogicalPageId,
    version: Version,
}

impl Page {
    pub fn new() -> Self {
        Self {
            buf: [0; PAGE_SIZE],
        }
    }

    pub fn buf(&self) -> &[u8] {
        &self.buf[..]
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..]
    }
}
