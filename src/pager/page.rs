use std::{ptr::NonNull, rc::Rc};

use zerocopy::{FromBytes, IntoBytes, KnownLayout};

use crate::pager::VERSION;

use super::PAGE_SIZE;

const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();

#[derive(FromBytes, IntoBytes, KnownLayout, Debug, Clone)]
#[repr(C)]
pub struct PageHeader {
    checksum: u32,
    version: u8,
    page_type: u8,
    _pad: u16,
}

#[derive(IntoBytes, Debug, Clone, PartialEq, Eq)]
#[repr(C)]
enum PageType {
    Queue = 0,
    Btree = 1,
}

#[derive(Debug)]
pub struct PageBufMut {
    ptr: NonNull<u8>,
}

#[derive(Debug, Clone)]
pub struct PageBuf {
    ptr: Rc<NonNull<u8>>,
}

impl PageBufMut {
    pub(super) fn new(ptr: NonNull<u8>) -> Self {
        PageBufMut { ptr }
    }

    pub fn init(&mut self) {
        let buf = self.buf_mut();

        let (header, _) = PageHeader::mut_from_prefix(&mut buf[..]).unwrap();

        header.version = VERSION as u8;
        header.page_type = 0;
        header.checksum = 0;
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        let header_len = size_of::<PageHeader>();

        let buf = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), header_len) };

        PageHeader::mut_from_bytes(buf).unwrap()
    }

    pub fn get_usable_size(&self) -> usize {
        PAGE_SIZE - size_of::<PageHeader>()
    }

    pub fn buf(&self) -> &[u8] {
        let offset = size_of::<PageHeader>();
        let buf_size = PAGE_SIZE - offset;

        unsafe {
            let ptr = self.ptr.add(offset).as_ptr() as *const _;

            std::slice::from_raw_parts(ptr, buf_size)
        }
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        let offset = size_of::<PageHeader>();
        let buf_size = PAGE_SIZE - offset;

        unsafe {
            let ptr = self.ptr.add(offset).as_ptr();

            std::slice::from_raw_parts_mut(ptr, buf_size)
        }
    }

    pub(super) fn freeze(self) -> PageBuf {
        PageBuf {
            ptr: Rc::new(self.ptr),
        }
    }
}

impl PageBuf {
    pub fn buf(&self) -> &[u8] {
        let offset = size_of::<PageHeader>();
        let buf_size = PAGE_SIZE - offset;

        unsafe {
            let ptr = self.ptr.add(offset).as_ptr() as *const _;

            std::slice::from_raw_parts(ptr, buf_size)
        }
    }

    pub fn try_take(self) -> Result<PageBufMut, PageBuf> {
        match Rc::try_unwrap(self.ptr) {
            Ok(ptr) => Ok(PageBufMut { ptr }),
            Err(ptr) => Err(PageBuf { ptr }),
        }
    }
}

// #[derive(FromBytes, Debug)]
// #[repr(C)]
// pub struct PageView<'a, T> {
//     header: PageHeader,
//     sub_header: T,
//     payload: &'a [u8],
// }

// #[derive(FromBytes, Debug)]
// #[repr(C)]
// pub struct PageViewMut<'a, T> {
//     header: &'a mut PageHeader,
//     sub_header: &'a mut T,
//     payload: &'a mut [u8],
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// enum PageType {
//     Queue = 0,
//     Btree = 1,
// }

// #[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, Clone, Default)]
// #[repr(C)]
// pub struct QueuePageHeader {
//     next_page_id: u64,
//     end_offset: u64,
// }
