// use treedb::tree::Tree;

// #[test]
// fn smoke() {
//     let mut tmp = std::env::temp_dir();
//     tmp.push("pager.smoke");

//     let mut tree = Tree::new(tmp).unwrap();

//     let key = "hello".to_string().into_bytes();
//     let value = "world".to_string().into_bytes();

//     tree.put(key.clone(), value.clone()).unwrap();

//     let res = tree.get(&key).unwrap().unwrap();

//     assert_eq!(res.0, key);
//     assert_eq!(res.1, value);

//     let key1 = "key".to_string().into_bytes();
//     let value1 = "value".to_string().into_bytes();

//     tree.put(key1.clone(), value1.clone()).unwrap();

//     let res = tree.get(&key1).unwrap().unwrap();

//     assert_eq!(res.0, key1);
//     assert_eq!(res.1, value1);

//     let res = tree.get(&key).unwrap().unwrap();

//     assert_eq!(res.0, key);
//     assert_eq!(res.1, value);

//     tree.flush_all().unwrap();
// }
