//! `treedb` is an on disk b-tree

pub mod node;
pub mod pager;
// pub mod tree;

use pager::PageId;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("index `{0}` out of bounds")]
    IndexOutofBounds(PageId),
    #[error("Unable to serialize page")]
    Bincode(#[from] bincode::Error),
}
