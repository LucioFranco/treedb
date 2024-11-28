use mock::MemoryFile;

use super::*;

#[test]
fn update() {
    let file = MemoryFile::default();
    let file2 = file.clone();

    let mut pager = Pager::recover(file).unwrap();

    let page1_id = pager.new_page_id();
    let mut page1 = pager.new_page_buffer();

    assert_eq!(page1_id, LogicalPageId(1));

    let page1_buf = page1.buf_mut();
    page1_buf.fill(42);

    pager.update_page(page1_id, page1).unwrap();

    let version = pager.current_version();

    let page1_read = pager.read_at(page1_id, version).unwrap();

    assert!(page1_read.buf().iter().all(|x| *x == 42));

    pager.commit().unwrap();

    drop(pager);

    let mut pager = Pager::recover(file2).unwrap();

    let page1_read2 = pager.read_at(page1_id, version).unwrap();

    assert!(page1_read2.buf().iter().all(|x| *x == 42));
}

#[test]
// TODO: re-enable this page once we have the API figured out. This currently fails under miri as
// expected.
#[ignore]
fn read_dropped_page() {
    let file = MemoryFile::default();

    let mut pager = Pager::recover(file).unwrap();

    let page1_id = pager.new_page_id();
    let mut page1 = pager.new_page_buffer();

    assert_eq!(page1_id, LogicalPageId(1));

    let page1_buf = page1.buf_mut();
    page1_buf.fill(42);

    pager.update_page(page1_id, page1).unwrap();

    let version = pager.current_version();

    let page1_read = pager.read_at(page1_id, version).unwrap();

    assert!(page1_read.buf().iter().all(|x| *x == 42));

    pager.commit().unwrap();

    drop(pager);

    assert!(page1_read.buf().iter().all(|x| *x == 42));
}

#[test]
fn multiple_pages() {
    let file = MemoryFile::default();
    let mut pager = Pager::recover(file).unwrap();

    // Create and write multiple pages
    let page_ids: Vec<_> = (0..3)
        .map(|i| {
            let page_id = pager.new_page_id();
            let mut page = pager.new_page_buffer();
            page.buf_mut().fill(i as u8);
            let page = page.freeze();
            pager.write_page(PhysicalPageId(page_id.0), &page).unwrap();
            page_id
        })
        .collect();

    let version = pager.current_version();

    // Verify all pages can be read correctly
    for (i, &page_id) in page_ids.iter().enumerate() {
        let page = pager.read_at(page_id, version).unwrap();
        assert!(page.buf().iter().all(|&b| b == i as u8));
    }
}

#[test]
fn page_updates() {
    let file = MemoryFile::default();
    let mut pager = Pager::recover(file).unwrap();

    // Create initial page
    let page_id = pager.new_page_id();
    let mut page = pager.new_page_buffer();
    page.buf_mut().fill(1);
    let version1 = pager.current_version();
    pager.update_page(page_id, page).unwrap();

    pager.commit().unwrap();

    // Update the same page
    let mut page = pager.new_page_buffer();
    page.buf_mut().fill(2);
    let version2 = pager.current_version();
    pager.atomic_update(page_id, version2, page).unwrap();

    // Verify we can read both versions
    let page_v1 = pager.read_at(page_id, version1).unwrap();
    assert!(page_v1.buf().iter().all(|&b| b == 1));

    let page_v2 = pager.read_at(page_id, version2).unwrap();
    assert!(page_v2.buf().iter().all(|&b| b == 2));
}

#[test]
fn recovery_after_crash() {
    let file = MemoryFile::default();
    let file2 = file.clone();

    // Session 1: Create and commit some pages
    let mut pager = Pager::recover(file).unwrap();
    let page_ids: Vec<_> = (0..3)
        .map(|i| {
            let page_id = pager.new_page_id();
            let mut page = pager.new_page_buffer();
            page.buf_mut().fill(i as u8);
            let page = page.freeze();
            pager.write_page(PhysicalPageId(page_id.0), &page).unwrap();
            page_id
        })
        .collect();

    pager.commit().unwrap();
    let version = pager.current_version();
    drop(pager);

    // Session 2: Recover and verify
    let mut pager = Pager::recover(file2).unwrap();
    for (i, &page_id) in page_ids.iter().enumerate() {
        let page = pager.read_at(page_id, version).unwrap();
        assert!(page.buf().iter().all(|&b| b == i as u8));
    }
}

#[test]
#[ignore]
fn read_nonexistent_page() {
    let file = MemoryFile::default();
    let mut pager = Pager::recover(file).unwrap();
    let version = pager.current_version();

    // Try reading a page ID that was never created
    let nonexistent_id = LogicalPageId(999);
    let result = pager.read_at(nonexistent_id, version);
    assert!(result.is_err());

    // Create a page, then try reading a different one
    let page_id = pager.new_page_id();
    let page = pager.new_page_buffer();
    let page = page.freeze();
    pager.write_page(PhysicalPageId(page_id.0), &page).unwrap();

    let another_nonexistent_id = LogicalPageId(page_id.0 + 1);
    let result = pager.read_at(another_nonexistent_id, version);
    assert!(result.is_err());
}

#[test]
// TODO: verify behavior in these test cases
#[ignore]
fn read_invalid_version() {
    let file = MemoryFile::default();
    let mut pager = Pager::recover(file).unwrap();

    // Create a page
    let page_id = pager.new_page_id();
    let page = pager.new_page_buffer();
    let page = page.freeze();
    pager.write_page(PhysicalPageId(page_id.0), &page).unwrap();
    let current_version = pager.current_version();

    // Try reading with a future version
    let future_version = current_version.0 + 1;
    let result = pager.read_at(page_id, Version(future_version));
    assert!(result.is_err());

    // Try reading with an invalid past version (0 or very old)
    let past_version = 0;
    let result = pager.read_at(page_id, Version(past_version));
    assert!(result.is_err());
}

// Mock in-memory file implementation for testing
mod mock {
    use std::cell::RefCell;
    use std::cmp;
    use std::rc::Rc;

    use super::{File, Result};

    #[derive(Clone, Default)]
    pub struct MemoryFile {
        data: Rc<RefCell<Vec<u8>>>,
    }

    impl MemoryFile {
        pub fn new() -> Self {
            MemoryFile {
                data: Rc::new(RefCell::new(Vec::new())),
            }
        }

        pub fn with_capacity(capacity: usize) -> Self {
            MemoryFile {
                data: Rc::new(RefCell::new(Vec::with_capacity(capacity))),
            }
        }
    }

    impl File for MemoryFile {
        fn len(&self) -> Result<usize> {
            Ok(self.data.borrow().len())
        }

        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let data = self.data.borrow();
            let offset = offset as usize;

            // If offset is beyond data length, return 0 bytes read
            if offset >= data.len() {
                return Ok(0);
            }

            // Calculate how many bytes we can actually read
            let available = data.len() - offset;
            let to_read = cmp::min(available, buf.len());

            // Copy the data
            buf[..to_read].copy_from_slice(&data[offset..offset + to_read]);

            Ok(to_read)
        }

        fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
            let mut data = self.data.borrow_mut();
            let offset = offset as usize;

            // Ensure the internal buffer is large enough
            if offset + buf.len() > data.len() {
                data.resize(offset + buf.len(), 0);
            }

            // Write the data
            data[offset..offset + buf.len()].copy_from_slice(buf);

            Ok(buf.len())
        }

        fn sync_data(&self) -> Result<()> {
            // No-op for in-memory implementation
            Ok(())
        }
    }
}
