use std::fs::OpenOptions;
use treedb::pager::Pager;

#[test]
fn smoke() {
    let mut tmp = std::env::temp_dir();
    tmp.push("pager.smoke");

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(tmp)
        .unwrap();

    let mut pager = Pager::new(4096, file, 1024);

    let page = pager.get(0).unwrap();
    let data = page.data_mut();

    for b in data {
        *b = 1;
    }

    pager.flush(0).unwrap();

    pager.evict(0).unwrap();

    let page = pager.get(0).unwrap();

    for b in page.data_mut() {
        assert_eq!(b, &mut 1u8);
    }
}
