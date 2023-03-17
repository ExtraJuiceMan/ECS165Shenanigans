#![feature(new_uninit)]
#![feature(map_try_insert)]
#![feature(get_mut_unchecked)]
#![feature(return_position_impl_trait_in_trait)]

use std::mem::size_of;

const PAGE_SIZE: usize = 4096;
const PAGE_SLOTS: usize = PAGE_SIZE / size_of::<i64>();
const PAGE_RANGE_COUNT: usize = 16;
const PAGE_RANGE_SIZE: usize = PAGE_SIZE * PAGE_RANGE_COUNT;
const RANGE_PAGE_COUNT: usize = PAGE_RANGE_SIZE / PAGE_SIZE;

const NUM_METADATA_COLUMNS: usize = 5;

const METADATA_INDIRECTION: usize = 0;
const METADATA_RID: usize = 1;
const METADATA_BASE_RID: usize = 2;

const NUM_STATIC_COLUMNS: usize = 3;

const METADATA_PAGE_HEADER: usize = 3;
//const METADATA_TIMESTAMP: usize = 4;
const METADATA_SCHEMA_ENCODING: usize = 4;
// 0xFF...FF
const RID_INVALID: u64 = !0;

// usually 16, but 32 to
// allow for shared bufferpool with merge thread
const BUFFERPOOL_SIZE: usize = 256;

pub mod bufferpool;
pub mod crabstore;
pub mod disk_manager;
pub mod index;
pub mod lock_manager;
mod merge;
pub mod page;
mod page_directory;
mod range_directory;
pub mod record;
pub mod rid;
pub mod table;
pub mod transaction;
pub mod transaction_worker;

#[cfg(test)]
mod tests {
    use crate::crabstore::CrabStore;
    use tempfile::tempdir;

    #[test]
    fn open_close_db() {
        let dir = tempdir().expect("Failed to get temp directory");
        let mut db = CrabStore::new(dir.path().into());
        db.open();
        db.close();
    }

    #[test]
    fn create_table() {
        let dir = tempdir().expect("Failed to get temp directory");
        let mut db = CrabStore::new(dir.path().into());
        db.open();
        db.create_table("test_table", 2, 0);
        db.close();
    }

    #[test]
    fn get_table() {
        let dir = tempdir().expect("Failed to get temp directory");

        let mut db = CrabStore::new(dir.path().into());
        db.open();

        db.create_table("test_table", 2, 0);
        db.get_table("test_table");

        db.close();

        db.open();

        db.get_table("test_table");
        assert_eq!(db.get_table("test_table").columns(), 2);

        db.close();
    }

    #[test]
    fn check_aliasing() {
        let dir = tempdir().expect("Failed to get temp directory");

        let mut db = CrabStore::new(dir.path().into());
        db.open();
        let table1 = db.create_table("test_table", 2, 0);
        let table2 = db.get_table("test_table");
        table1.insert_query(&[1, 2], None);
        table2.insert_query(&[3, 4], None);
        assert_eq!(
            table1.select_query(1, 0, &[1, 1], None),
            table2.select_query(1, 0, &[1, 1], None)
        );
        assert_eq!(
            table1.select_query(2, 0, &[1, 1], None),
            table2.select_query(2, 0, &[1, 1], None)
        );
        db.close();
    }
}
