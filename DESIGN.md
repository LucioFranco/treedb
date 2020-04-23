# (To be named) Storage Engine Design

## Overview 

`TreeDB` is a CoW B+Tree designed to be embedded in async rust applications. The
main inspiration for this project is foundationdb's Redwood storage engine. This
B+Tree implementation is based on Copy-On-Write principles. This means each
mutation applied to the tree is done so on a copy of the original item. This
enables MVCC out of the box due to the nature of Copy-On-Write.

At a high level, the interface a user should interact with should be designed
around two major components. The first, is the actual BTree or `Db` type which
contains references to all versions and allows users to write to the current
version of the database and commit. The second component is the read cursor that
a user can get for any "active" version and will allow search features. These 
search features include get and range commands.

Internally, the database will use the concept delayed write ahead logging and
indirection tables for logical pages referenced from the B+Tree itself. This
then allows internal remapping of pages without the need to update pages back up
the tree. This also means we do not require any latches beyond the indirection
table used for remapped pages.

### Proposed features:

- B+Tree with copy on write semantics
- MVCC transations
- Built around async rust and uring/libaio

## Internals


### Components

#### Db

tbw

Remap table looks like this:

```rust
HashMap<LogicalPageId, BTreeMap<Version, LogicalPageId>>
```

##### Write Path

The write path should include getting a write transaction object that contains
the current version.

The write path should get the current commit version, traverse the tree to find
the insertion point then return the insertion point page. To insert a new key the
page is copied then modified. To copy the new page we must allocate a new page
id, then add it to the PageTable to remap. We can then append it to the logical
page id for the original page with the write version.

`PageTable` is the name of the indirection table that maps LogicalPageId to 
PhysicalPageId. This allows us to swap out the PhysicalPageId with a new one.
Following this we can store a map that goes from LogicalPageId to tuple of 
Version and PhysicalPageId.

#### Cursor

A `Cursor` is a type that is able to iterate through the tree. It is a combination
of 

#### Pager

##### Free implemenation

The pager within foundationdb uses three queues to track "freepages", there are
three types of freepages. 

- Regular free list for pages that have been freed while it was outside the
    effective version.
- Delayed free list are for pages that do fall into the effective version range.
- Remap queue which contains a list of pages that will get  "undone" at commit
    time.

Each queue is written into the pager on its own pages. This allows the queues
to be flushed to disk as well. Since, the queue is FIFO pages never need to be 
rewritten.

"undoing a remapped page" means copying the latest version for said page id into
the original page. This allows us to retroactively perform space reclaimation on
disk and in memory.

"Effecitve version" relates to versions that still contain some read reference
to them.