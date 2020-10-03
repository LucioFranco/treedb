use super::*;
use bytes::BufMut;

#[test]
fn smoke() {
    let file = tempfile::tempfile().unwrap();
    let file2 = file.try_clone().unwrap();

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
