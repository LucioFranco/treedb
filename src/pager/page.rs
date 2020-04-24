use super::{LogicalPageId, Version};
use bytes::{Bytes, BytesMut};
use std::ops::{Deref, DerefMut};

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
    pub fn new(id: LogicalPageId, version: Version, page_size: usize) -> Self {
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

impl Deref for OwnedPage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[..]
    }
}

impl DerefMut for OwnedPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf[..]
    }
}

impl SharedPage {
    pub fn id(&self) -> LogicalPageId {
        self.header.id
    }

    pub fn version(&self) -> Version {
        self.header.version
    }
}

impl Deref for SharedPage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[..]
    }
}
