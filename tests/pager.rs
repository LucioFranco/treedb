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

    let page = pager.new_page().unwrap();
    // page.buf_mut().copy_from_slice(&vec![1u8; 4096][..]);
    page.write(&vec![1u8; 4096][..]);
    let id = page.id();

    let page = pager.get(id).unwrap();
    let data = page.read();

    for b in data {
        *b = 1;
    }

    pager.evict(0).unwrap();

    let page = pager.get(0).unwrap();

    for b in page.read() {
        assert_eq!(b, &mut 1u8);
    }
}

#[test]
fn full() {
    let mut tmp = std::env::temp_dir();
    tmp.push("pager.full");

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(tmp)
        .unwrap();

    let mut pager = Pager::new(4096, file, 10);

    let mut ids = Vec::new();

    for i in 0..=255 {
        let page = ;
        let page = pager.new_page().unwrap();
        let id = page.id();
        page.write(&vec![0u8; 4096][..]);

        ids.push((i, id));

        let page = pager.get(id).unwrap();

        let data = page.read();

        for b in data {
            *b = i;
        }
    }

    for (i, page_id) in &ids {
        let page = pager.get(page_id).unwrap();

        for b in page.read() {
            assert_eq!(b, i);
        }
    }
}
