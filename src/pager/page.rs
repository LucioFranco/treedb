use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::PAGE_SIZE;

pub struct Page {
    buf: [u8; PAGE_SIZE],
}

#[derive(FromBytes, Immutable, Debug, Clone)]
#[repr(C)]
pub struct PageView<'a, T> {
    header: PageHeader,
    sub_header: T,
    payload: &'a [u8],
}

#[derive(FromBytes, Immutable, Debug)]
#[repr(C)]
pub struct PageViewMut<'a, T> {
    header: &'a mut PageHeader,
    sub_header: &'a mut T,
    payload: &'a mut [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PageType {
    Queue = 0,
    Btree = 1,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, Clone)]
#[repr(C)]
pub struct QueuePageHeader {
    next_page_id: u64,
    end_offset: u64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, Clone)]
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

    pub fn init<T: IntoBytes + Immutable>(version: u32, sub_header: T) -> Page {
        let page_header = PageHeader {
            version,
            page_type: 0,
            checksum: 0,
        };

        let mut page = Self::new();

        page_header.write_to_prefix(&mut page.buf[..]).unwrap();
        let offset = std::mem::size_of::<PageHeader>();
        sub_header.write_to_prefix(&mut page.buf[offset..]).unwrap();

        page
    }

    pub fn view<T: FromBytes + Immutable>(&self) -> Option<PageView<'_, T>> {
        let (header, rest) = PageHeader::read_from_prefix(&self.buf[..]).ok()?;
        let (sub_header, payload) = T::read_from_prefix(rest).ok()?;

        Some(PageView {
            header,
            sub_header,
            payload,
        })
    }

    pub fn view_mut<T: FromBytes + KnownLayout + IntoBytes>(
        &mut self,
    ) -> Option<PageViewMut<'_, T>> {
        let (header, rest) = PageHeader::mut_from_prefix(&mut self.buf[..]).ok()?;
        let (sub_header, payload) = T::mut_from_prefix(rest).ok()?;

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

        let sub_mut = page.view_mut::<QueuePageHeader>().unwrap().sub_header;

        sub_mut.next_page_id = 65;

        let sub = page.view::<QueuePageHeader>().unwrap().sub_header;
        assert_eq!(sub.next_page_id, 65);
    }
}
