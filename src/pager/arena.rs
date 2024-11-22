use std::{alloc::Layout, cell::Cell, ops::Add, ptr::NonNull};

use allocator_api2::alloc::{AllocError, Allocator};

pub struct Arena<A: Allocator> {
    ptr: NonNull<u8>,
    len: Cell<usize>,
    page_size: usize,
    num_pages: usize,
    alloc: A,
}

impl<A: Allocator> Arena<A> {
    pub fn new(alloc: A, page_size: usize, num_pages: usize) -> Self {
        assert!(page_size.is_power_of_two());
        assert!(num_pages.is_power_of_two());

        let size = page_size * num_pages;

        let layout = Layout::from_size_align(size, 8).unwrap();
        let ptr = alloc.allocate(layout).unwrap();

        let ptr = unsafe { NonNull::new_unchecked(ptr.as_ptr().cast::<u8>()) };

        Self {
            ptr,
            len: Cell::new(0),
            alloc,
            page_size,
            num_pages,
        }
    }

    pub fn alloc(&self) -> Result<NonNull<u8>, AllocError> {
        let len = self.len.get();

        if len >= self.num_pages {
            return Err(AllocError);
        }

        let offset = len * self.page_size;

        self.len.set(len.add(1));

        Ok(unsafe { self.ptr.add(offset) })
    }
}

unsafe impl<A: Allocator> Allocator for Arena<A> {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<NonNull<[u8]>, allocator_api2::alloc::AllocError> {
        if layout.size() != self.page_size || layout.align() != 8 {
            return Err(AllocError);
        }

        let ptr = self.alloc()?;

        Ok(NonNull::slice_from_raw_parts(ptr, self.page_size))
    }

    unsafe fn deallocate(&self, _: NonNull<u8>, _: std::alloc::Layout) {}
}

impl<A: Allocator> Drop for Arena<A> {
    fn drop(&mut self) {
        let size = self.page_size * self.num_pages;
        let layout = Layout::from_size_align(size, 8).unwrap();
        unsafe { self.alloc.deallocate(self.ptr, layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::System;

    #[test]
    fn test_arena_creation() {
        let arena = Arena::new(System, 4096, 4);
        assert_eq!(arena.page_size, 4096);
        assert_eq!(arena.num_pages, 4);
        assert_eq!(arena.len.get(), 0);
    }

    #[test]
    #[should_panic]
    fn test_arena_invalid_page_size() {
        // Not a power of two
        Arena::new(System, 4095, 4);
    }

    #[test]
    #[should_panic]
    fn test_arena_invalid_num_pages() {
        // Not a power of two
        Arena::new(System, 4096, 3);
    }

    #[test]
    fn test_arena_allocation() {
        let arena = Arena::new(System, 4096, 4);

        // First allocation should succeed
        let ptr1 = arena.alloc().unwrap();
        assert_eq!(arena.len.get(), 1);

        // Second allocation should return a different pointer
        let ptr2 = arena.alloc().unwrap();
        assert_ne!(ptr1, ptr2);
        assert_eq!(arena.len.get(), 2);
    }

    #[test]
    fn test_arena_allocation_exhaustion() {
        let arena = Arena::new(System, 4096, 2);

        // First two allocations should succeed
        arena.alloc().unwrap();
        arena.alloc().unwrap();

        // Third allocation should fail
        assert!(arena.alloc().is_err());
    }

    #[test]
    fn test_arena_as_allocator() {
        let arena = Arena::new(System, 8, 4); // Small pages for testing

        // Valid allocation
        let layout = Layout::from_size_align(8, 8).unwrap();
        let allocation = arena.allocate(layout);
        assert!(allocation.is_ok());

        // Wrong size
        let layout = Layout::from_size_align(16, 8).unwrap();
        let allocation = arena.allocate(layout);
        assert!(allocation.is_err());

        // Wrong alignment
        let layout = Layout::from_size_align(8, 16).unwrap();
        let allocation = arena.allocate(layout);
        assert!(allocation.is_err());
    }

    #[test]
    fn test_load() {
        let arena = Arena::new(System, 64, 1024); // 64 bytes * 1024 pages
        let mut allocations = Vec::new();

        // Allocate half the pages
        for _ in 0..512 {
            let ptr = arena.alloc().unwrap();
            allocations.push(ptr);
        }

        assert_eq!(arena.len.get(), 512);

        // Verify all pointers are different
        for i in 0..allocations.len() {
            for j in i + 1..allocations.len() {
                assert_ne!(allocations[i], allocations[j]);
            }
        }

        // Verify offset between consecutive allocations is equal to page_size
        for i in 0..allocations.len() - 1 {
            let addr1 = allocations[i].as_ptr() as usize;
            let addr2 = allocations[i + 1].as_ptr() as usize;
            assert_eq!(addr2 - addr1, arena.page_size);
        }
    }
}
