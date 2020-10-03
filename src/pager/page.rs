use super::{LogicalPageId, Version};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::MaybeUninit;

#[derive(Debug)]
pub struct OwnedPage {
    header: PageHeader,
    buf: BytesMut,
}

#[derive(Debug, Clone)]
pub struct SharedPage {
    header: PageHeader,
    buf: Bytes,
}

#[derive(Debug, Clone, Copy)]
struct PageHeader {
    id: LogicalPageId,
    version: Version,
}

impl OwnedPage {
    pub(crate) fn new(id: LogicalPageId, version: Version, page_size: usize) -> Self {
        Self {
            header: PageHeader { id, version },
            buf: BytesMut::with_capacity(page_size),
        }
    }

    pub fn id(&self) -> LogicalPageId {
        self.header.id
    }

    pub fn version(&self) -> Version {
        self.header.version
    }

    pub fn freeze(self) -> SharedPage {
        SharedPage {
            header: self.header,
            buf: self.buf.freeze(),
        }
    }
}

impl BufMut for OwnedPage {
    fn remaining_mut(&self) -> usize {
        // Return only the size we reserved upfront, we use the `remaining_mut`
        // impl on `BytesMut` we get a very large capacity since it will grow
        // as you add more data. We don't want this we want to hard cap this at
        // the page size.
        self.buf.capacity()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.buf.advance_mut(cnt)
    }

    fn bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.buf.bytes_mut()
    }
}

impl Buf for OwnedPage {
    fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    fn bytes(&self) -> &[u8] {
        self.buf.bytes()
    }

    fn advance(&mut self, cnt: usize) {
        self.buf.advance(cnt)
    }
}

impl SharedPage {
    pub(crate) fn new(id: LogicalPageId, version: Version, buf: Vec<u8>) -> Self {
        Self {
            header: PageHeader { id, version },
            buf: Bytes::from(buf),
        }
    }

    pub fn id(&self) -> LogicalPageId {
        self.header.id
    }

    pub fn version(&self) -> Version {
        self.header.version
    }
}

impl Buf for SharedPage {
    fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    fn bytes(&self) -> &[u8] {
        self.buf.bytes()
    }

    fn advance(&mut self, cnt: usize) {
        self.buf.advance(cnt)
    }
}
