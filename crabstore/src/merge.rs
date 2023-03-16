use std::{
    hash::BuildHasherDefault,
    sync::{
        atomic::Ordering,
        mpsc::{channel, Sender},
        Arc,
    },
    thread::{self, JoinHandle},
};

use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::{
    bufferpool::BufferPool, disk_manager::DiskManager, page::Page, page_directory::PageDirectory,
    range_directory::RangeDirectory, rid::RID, table::Table, METADATA_BASE_RID,
    METADATA_INDIRECTION, METADATA_RID, NUM_METADATA_COLUMNS, NUM_STATIC_COLUMNS, PAGE_RANGE_COUNT,
    PAGE_SLOTS, RID_INVALID,
};

impl Table {
    pub fn spawn_merge_thread(
        page_directory: &Arc<RwLock<PageDirectory>>,
        range_directory: &Arc<Mutex<RangeDirectory>>,
        disk_manager: &Arc<DiskManager>,
        main_bufferpool: &Arc<Mutex<BufferPool>>,
        num_columns: usize,
    ) -> (JoinHandle<()>, Sender<usize>) {
        let page_dir_clone = Arc::clone(page_directory);
        let disk_manager_clone = Arc::clone(disk_manager);
        let range_dir_clone = Arc::clone(range_directory);
        let main_bp_clone = Arc::clone(main_bufferpool);
        let (send, recv) = channel();
        let handle = thread::spawn(move || {
            let num_columns = num_columns;
            let main_bufferpool = main_bp_clone;
            let page_dir = page_dir_clone;
            let range_dir = range_dir_clone;
            let disk = disk_manager_clone;
            let recv = recv;
            let mut seen: FxHashSet<u64> = FxHashSet::with_capacity_and_hasher(
                PAGE_SLOTS * PAGE_RANGE_COUNT,
                BuildHasherDefault::<FxHasher>::default(),
            );
            let mut merged: FxHashMap<usize, Arc<[usize]>> = FxHashMap::with_capacity_and_hasher(
                PAGE_SLOTS * PAGE_RANGE_COUNT,
                BuildHasherDefault::<FxHasher>::default(),
            );
            let mut rangecounts: FxHashMap<usize, usize> = FxHashMap::with_capacity_and_hasher(
                PAGE_SLOTS * PAGE_RANGE_COUNT,
                BuildHasherDefault::<FxHasher>::default(),
            );

            loop {
                let merge_range = loop {
                    let range_update = recv.recv();

                    if range_update.is_err() {
                        return;
                    }

                    let range_update: usize = range_update.unwrap();

                    *rangecounts.entry(range_update).or_default() += 1;

                    if *rangecounts.get(&range_update).unwrap() >= 4 {
                        *rangecounts.get_mut(&range_update).unwrap() = 0;
                        break range_update;
                    }
                };

                println!("Merge request received for range {merge_range}");

                let range_dir = range_dir.lock();
                let range = range_dir.get(merge_range);
                let merge_from = range.current_tail_page.load(Ordering::SeqCst);

                let last_page = Page::new(
                    page_dir
                        .read()
                        .get_page(merge_from)
                        .expect("Bad page ID for Page Range encountered in merge"),
                )
                .read_last_tail(&mut main_bufferpool.lock())
                    as usize;

                let merge_stop_at = range.merged_until.load(Ordering::SeqCst);

                range.merged_until.store(last_page, Ordering::SeqCst);

                drop(range_dir);

                let mut tail_page_id = last_page;

                while tail_page_id > merge_stop_at && tail_page_id != RID_INVALID as usize {
                    let tail_page = Page::new(
                        page_dir
                            .read()
                            .get_page(tail_page_id)
                            .expect("Bad page ID for Page Range encountered in merge"),
                    );

                    for tail_slot in (0..PAGE_SLOTS).rev() {
                        let base_rid = tail_page
                            .get_column(&mut main_bufferpool.lock(), METADATA_BASE_RID)
                            .slot(tail_slot);

                        assert!(base_rid != RID_INVALID);

                        if seen.contains(&base_rid) {
                            continue;
                        }

                        seen.insert(base_rid);

                        let base_page_id = RID(base_rid).page();

                        let merged_page = Page::new(Arc::clone(
                            merged.entry(base_page_id).or_insert_with(|| {
                                let mut new_page_dir_entry =
                                    Arc::new_uninit_slice(NUM_METADATA_COLUMNS + num_columns);

                                let page_dir = page_dir.read();

                                let base_cols = page_dir
                                    .get_page(base_page_id)
                                    .expect("Merge thread tried to access a non-existent page id");

                                drop(page_dir);

                                let new_page = Arc::get_mut(&mut new_page_dir_entry).unwrap();
                                new_page[METADATA_INDIRECTION]
                                    .write(base_cols[METADATA_INDIRECTION]);
                                new_page[METADATA_BASE_RID].write(base_cols[METADATA_BASE_RID]);
                                new_page[METADATA_RID].write(base_cols[METADATA_RID]);

                                let mut new_column_ids = disk.reserve_range(
                                    NUM_METADATA_COLUMNS - NUM_STATIC_COLUMNS + num_columns,
                                );

                                for i in NUM_STATIC_COLUMNS..(NUM_METADATA_COLUMNS + num_columns) {
                                    new_page[i].write(new_column_ids);
                                    new_column_ids += 1;
                                }

                                let new_page_dir_entry =
                                    unsafe { new_page_dir_entry.assume_init() };

                                let bp = &mut main_bufferpool.lock();
                                for i in NUM_STATIC_COLUMNS..(NUM_METADATA_COLUMNS + num_columns) {
                                    let page = bp.get_page(base_cols[i]);
                                    let page_copy = bp.get_page(new_page_dir_entry[i]);

                                    let page = page
                                        .raw()
                                        .read()
                                        .expect("Failed to acquire merge page lock");
                                    let mut page_copy = page_copy
                                        .raw()
                                        .write()
                                        .expect("Failed to acquire merge page lock");

                                    // println!("{:?}", page.page);

                                    page_copy.page.clone_from_slice(&page.page);
                                }

                                new_page_dir_entry
                            }),
                        ));

                        let bp = &mut main_bufferpool.lock();
                        let tid = tail_page.get_column(bp, METADATA_RID).slot(tail_slot);

                        if merged_page.read_page_tps(bp) > tid && tid != 0 {
                            merged_page.write_page_tps(bp, tid);
                        }

                        for i in (NUM_STATIC_COLUMNS + 1)..(NUM_METADATA_COLUMNS + num_columns) {
                            let updated_value = tail_page.get_column(bp, i).slot(tail_slot);
                            merged_page
                                .get_column(bp, i)
                                .write_slot(RID(base_rid).slot(), updated_value);
                        }
                    }

                    tail_page_id = tail_page.read_last_tail(&mut main_bufferpool.lock()) as usize;
                }

                //main_bufferpool.lock().flush_all();

                let mut page_dir = page_dir.write();

                for pair in &merged {
                    page_dir.replace_page(*pair.0, pair.1);
                }

                drop(page_dir);

                merged.clear();
                seen.clear();
            }
        });

        (handle, send)
    }
}
