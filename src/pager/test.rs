use std::{cell::RefCell, rc::Rc};

use super::*;
use bytes::BufMut;

#[derive(Debug, Default, Clone)]
struct MemoryFile(pub(crate) Rc<RefCell<Vec<u8>>>);

impl File for MemoryFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;
        let file = self.0.borrow();

        if offset > file.len() {
            todo!("return file too small error")
        }

        if offset + buf.len() > file.len() {
            todo!("return file too small error")
        }

        buf.copy_from_slice(&file[offset..offset + buf.len()]);

        Ok(buf.len())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let mut file = self.0.borrow_mut();

        let offs = offset as usize;
        if buf.len() == 0 {
            return Ok(0);
        }
        if offs > file.len() {
            /* fill extra space with zeros */
            file.resize(offs, 0);
            file.extend_from_slice(buf);
        } else {
            /* 2 pieces:
             *  - copy_from_slice() what fits
             *  - extend_from_slice() what doesn't
             */
            let l = {
                let r = &mut file[offs..];
                let l = std::cmp::min(buf.len(), r.len());
                let r = &mut r[..l];
                let buf = &buf[..l];
                r.copy_from_slice(buf);
                l
            };

            if l < buf.len() {
                file.extend_from_slice(&buf[l..]);
            }
        }
        Ok(buf.len())
    }

    fn sync_data(&self) -> Result<()> {
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        let len = self.0.borrow().len();
        Ok(len)
    }
}

#[test]
fn smoke() {
    let file = MemoryFile::default();
    let file2 = file.clone();

    let mut pager = VersionedPager::recover(file).unwrap();

    let mut page1 = pager.new_page_buffer().unwrap();

    assert_eq!(page1.id(), LogicalPageId(1));

    for _ in 0..(page1.remaining_mut() / 8) {
        page1.put_u64(1337);
    }

    let page1 = pager.write_page(page1).unwrap();

    let version = pager.current_version();
    let page1_id = page1.id();

    let page1_read = pager.read_at(page1_id, version).unwrap();

    assert_eq!(page1.id(), page1_read.id());
    assert_eq!(page1.version(), page1_read.version());

    pager.commit().unwrap();

    drop(pager);

    let mut pager = VersionedPager::recover(file2).unwrap();

    let mut page = pager.read_at(page1_id, version).unwrap();

    for _ in 0..(page.remaining() / 8) {
        assert_eq!(page.get_u64(), 1337);
    }
}
