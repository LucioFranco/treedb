use std::marker::PhantomData;

use zerocopy::{AsBytes, FromBytes, FromZeroes, LayoutVerified, Unaligned};

use super::{LogicalPageId, Version, PAGE_SIZE};

pub struct Page {
    buf: [u8; PAGE_SIZE],
}

pub struct PageView<'a, T> {
    header: LayoutVerified<&'a [u8], PageHeader>,
    sub_header: LayoutVerified<&'a [u8], T>,
    payload: &'a [u8],
}

pub struct PageViewMut<'a, T> {
    header: LayoutVerified<&'a mut [u8], PageHeader>,
    sub_header: LayoutVerified<&'a mut [u8], T>,
    payload: &'a mut [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PageType {
    Queue = 0,
    Btree = 1,
}

#[derive(FromBytes, AsBytes, FromZeroes, Debug, Clone)]
#[repr(C)]
pub struct QueuePageHeader {
    next_page_id: u64,
    end_offset: u64,
}

#[derive(FromBytes, AsBytes, FromZeroes, Debug, Clone)]
#[repr(C)]
struct PageHeader {
    version: u32,
    page_type: u32,
    checksum: u64,
}

impl Page {
    pub fn new() -> Self {
        Page {
            buf: [0; PAGE_SIZE],
        }
    }

    pub fn init<T: AsBytes>(version: u32, sub_header: T) -> Page {
        let page_header = PageHeader {
            version,
            page_type: 0,
            checksum: 0,
        };

        let mut page = Self::new();

        page_header.write_to_prefix(&mut page.buf[..]);
        let offset = std::mem::size_of::<PageHeader>();
        sub_header.write_to_prefix(&mut page.buf[offset..]);

        page
    }

    pub fn view<T: FromBytes>(&self) -> Option<PageView<'_, T>> {
        let (header, payload) = LayoutVerified::new_from_prefix(&self.buf[..])?;
        let (sub_header, payload) = LayoutVerified::<_, T>::new_from_prefix(&payload[..])?;

        Some(PageView {
            header,
            sub_header,
            payload,
        })
    }

    pub fn view_mut<T: FromBytes>(&mut self) -> Option<PageViewMut<'_, T>> {
        let (header, payload) = LayoutVerified::new_from_prefix(&mut self.buf[..])?;
        let (sub_header, payload) = LayoutVerified::<_, T>::new_from_prefix(&mut payload[..])?;

        Some(PageViewMut {
            header,
            sub_header,
            payload,
        })
    }

    pub fn buf(&self) -> &[u8] {
        &self.buf[..]
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..]
    }
}

#[cfg(test)]
mod tests {
    use super::{Page, QueuePageHeader};

    #[test]
    fn smoke() {
        let mut page = Page::init(
            42,
            QueuePageHeader {
                next_page_id: 42,
                end_offset: 42,
            },
        );

        let sub = page.view::<QueuePageHeader>().unwrap().sub_header;
        assert_eq!(sub.next_page_id, 42);

        let mut sub_mut = page.view_mut::<QueuePageHeader>().unwrap().sub_header;

        sub_mut.next_page_id = 65;

        let sub = page.view::<QueuePageHeader>().unwrap().sub_header;
        assert_eq!(sub.next_page_id, 65);
    }
}
