use super::*;

#[test]
fn smoke() {
    let file = tempfile::tempfile().unwrap();
    let mut pager = VersionedPager::from_file(file).unwrap();

    let page1 = pager.new_page_buffer().unwrap();

    assert_eq!(page1.id, LogicalPageId(1));

    let page1 = pager.write_page2(page1).unwrap();
}
