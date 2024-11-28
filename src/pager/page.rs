use std::{alloc::Layout, ptr::NonNull, rc::Rc};

use allocator_api2::alloc::Allocator;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::PAGE_SIZE;

#[derive(Clone)]
pub struct Page {
    buf: NonNull<[u8]>,
}

#[derive(Debug)]
pub struct PageBufMut {
    ptr: NonNull<[u8]>,
}

#[derive(Debug, Clone)]
pub struct PageBuf {
    ptr: Rc<NonNull<[u8]>>,
}

impl PageBufMut {
    pub(super) fn new(ptr: NonNull<[u8]>) -> Self {
        PageBufMut { ptr }
    }

    pub fn buf(&self) -> &[u8] {
        unsafe { self.ptr.as_ref() }
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        unsafe { self.ptr.as_mut() }
    }

    pub(super) fn freeze(self) -> PageBuf {
        PageBuf {
            ptr: Rc::new(self.ptr),
        }
    }
}

impl PageBuf {
    pub fn buf(&self) -> &[u8] {
        unsafe { self.ptr.as_ref().as_ref() }
    }

    pub fn try_take(self) -> Result<PageBufMut, PageBuf> {
        match Rc::try_unwrap(self.ptr) {
            Ok(ptr) => Ok(PageBufMut { ptr }),
            Err(ptr) => Err(PageBuf { ptr }),
        }
    }
}

#[derive(FromBytes, Immutable, Debug)]
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

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, Clone, Default)]
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
    pub fn init<T: IntoBytes + Immutable, A: Allocator>(
        alloc: &A,
        version: u32,
        sub_header: T,
    ) -> Page {
        let page_header = PageHeader {
            version,
            page_type: 0,
            checksum: 0,
        };

        let ptr = alloc
            .allocate(Layout::from_size_align(PAGE_SIZE, 8).unwrap())
            .unwrap();

        let mut page = Page { buf: ptr };

        let mut buf = unsafe { &mut page.buf.as_mut() };

        page_header.write_to_prefix(&mut buf).unwrap();
        let offset = std::mem::size_of::<PageHeader>();
        sub_header.write_to_prefix(&mut buf[offset..]).unwrap();

        page
    }

    pub fn view<T: FromBytes + Immutable>(&self) -> Option<PageView<'_, T>> {
        let buf = unsafe { self.buf.as_ref() };

        let (header, rest) = PageHeader::read_from_prefix(&buf).ok()?;
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
        let buf = unsafe { self.buf.as_mut() };

        let (header, rest) = PageHeader::mut_from_prefix(buf).ok()?;
        let (sub_header, payload) = T::mut_from_prefix(rest).ok()?;

        Some(PageViewMut {
            header,
            sub_header,
            payload,
        })
    }

    pub fn buf(&self) -> &[u8] {
        unsafe { self.buf.as_ref() }
    }

    pub fn buf_mut(&mut self) -> &mut [u8] {
        unsafe { self.buf.as_mut() }
    }
}
