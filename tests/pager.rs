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
    page.buf_mut().copy_from_slice(&vec![1u8; 4096][..]);
    let id = page.id();

    let page = pager.get(id).unwrap();
    let data = page.buf_mut();

    for b in data {
        *b = 1;
    }

    pager.evict(0).unwrap();

    let _page = pager.get(0).unwrap();

    // for b in page.data_mut() {
    //     assert_eq!(b, &mut 1u8);
    // }
}

// #[test]
// fn full() {
//     let mut tmp = std::env::temp_dir();
//     tmp.push("pager.full");

//     let file = OpenOptions::new()
//         .write(true)
//         .read(true)
//         .create(true)
//         .open(tmp)
//         .unwrap();

//     let mut pager = Pager::new(4096, file, 10);

//     let mut ids = Vec::new();

//     for i in 0..=255 {
//         let page = Page::new_leaf(4096);
//         let page = pager.alloc_page(page).unwrap();
//         let id = page.id();

//         ids.push((i, id));

//         let _page = pager.get(id).unwrap();

//         // let data = page.data_mut();

//         // for b in data {
//         //     *b = i;
//         // }
//     }

//     for (_i, page_id) in &ids {
//         let _page = pager.get(page_id).unwrap();

//         // for b in page.data() {
//         //     assert_eq!(b, i);
//         // }
//     }
// }
