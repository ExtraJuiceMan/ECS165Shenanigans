use crate::{
    bufferpool::{BufferPool, BufferPoolFrame},
    disk_manager::DiskManager,
    page::PhysicalPage,
    range_directory::RangeDirectory,
    rid::RID,
    BUFFERPOOL_SIZE, METADATA_BASE_RID, METADATA_PAGE_HEADER, NUM_STATIC_COLUMNS, PAGE_RANGE_COUNT,
    PAGE_SIZE, PAGE_SLOTS,
};
use crate::{index::Index, RID_INVALID};
use crate::{
    page::{Page, PageRange},
    page_directory::PageDirectory,
};
use crate::{
    Record, RecordRust, METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING,
    NUM_METADATA_COLUMNS,
};
use parking_lot::{lock_api::RawMutex, Mutex, RwLock};
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{prelude::*, types::PyCFunction};
use rkyv::{
    ser::{serializers::BufferSerializer, Serializer},
    Archive, Deserialize, Serialize,
};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use std::ops::{RangeBounds, RangeInclusive};
use std::{
    borrow::BorrowMut,
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use std::{
    hash::BuildHasherDefault,
    sync::mpsc::{channel, Sender},
    thread::{self, JoinHandle},
};

#[derive(Archive, Deserialize, Serialize, Clone, Debug)]
pub struct TableHeaderPage {
    num_columns: usize,
    primary_key_index: usize,
    indexed_columns: usize,
    next_free_page: usize,
    next_rid: u64,
    next_tid: u64,
}
#[derive(Debug)]
#[pyclass]
pub struct TablePy {
    table: Table,
}
#[derive(Debug)]
pub struct Table {
    name: String,
    num_columns: usize,
    primary_key_index: usize,
    index: Index,
    next_rid: AtomicU64,
    next_tid: AtomicU64,
    page_dir: Arc<RwLock<PageDirectory>>,
    range_dir: Arc<RwLock<RangeDirectory>>,
    bufferpool: Arc<Mutex<BufferPool>>,
    disk: Arc<DiskManager>,
    merge_thread_handle: Option<(JoinHandle<()>, Sender<usize>)>,
}

impl Table {
    fn spawn_merge_thread(
        page_directory: &Arc<RwLock<PageDirectory>>,
        range_directory: &Arc<RwLock<RangeDirectory>>,
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
            let copy_mask = (0b111);
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

            loop {
                let merge_range = recv.recv();

                if merge_range.is_err() {
                    return;
                }

                main_bufferpool.lock().flush_all();

                let merge_range = merge_range.unwrap();

                let range_dir = range_dir.write();
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

                        let base_page_id = RID::from(base_rid).page();

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
                                .write_slot(RID::from(base_rid).slot(), updated_value);
                        }
                    }

                    tail_page_id = tail_page.read_last_tail(&mut main_bufferpool.lock()) as usize;
                }

                main_bufferpool.lock().flush_all();

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
    pub fn new(
        name: String,
        num_columns: usize,
        key_index: usize,
        db_file: String,
        pd_file: String,
        id_file: String,
        rd_file: String,
    ) -> Table {
        let page_dir = Arc::new(RwLock::new(PageDirectory::new(Path::new(&pd_file))));
        let range_dir = Arc::new(RwLock::new(RangeDirectory::new(Path::new(&rd_file))));

        let disk = Arc::new(DiskManager::new(Path::new(&db_file)).unwrap());
        let bufferpool = Arc::new(Mutex::new(BufferPool::new(
            Arc::clone(&disk),
            BUFFERPOOL_SIZE,
        )));
        let merge_thread_handle =
            Table::spawn_merge_thread(&page_dir, &range_dir, &disk, &bufferpool, num_columns);

        Table {
            name,
            num_columns,
            primary_key_index: key_index,
            index: Index::new(key_index, num_columns, Path::new(&id_file)),
            next_rid: 0.into(),
            next_tid: (!0 - 1).into(),
            page_dir,
            range_dir,
            disk,
            bufferpool,
            merge_thread_handle: Some(merge_thread_handle),
        }
    }
    pub fn load(
        name: &str,
        db_file: &Path,
        pd_file: &Path,
        id_file: &Path,
        rd_file: &Path,
    ) -> Self {
        let disk = Arc::new(DiskManager::new(db_file).expect("Failed to open table file"));

        let mut page = PhysicalPage::default();

        disk.read_page(0, &mut page.page);

        let header = unsafe {
            rkyv::from_bytes_unchecked::<TableHeaderPage>(
                &page.page[0..size_of::<<TableHeaderPage as Archive>::Archived>()],
            )
            .expect("Failed to deserialize table header")
        };

        disk.set_free_page_pointer(header.next_free_page);

        let index = Index::load(id_file);
        let page_dir = Arc::new(RwLock::new(PageDirectory::load(pd_file)));
        let range_dir = Arc::new(RwLock::new(RangeDirectory::load(rd_file)));
        let bufferpool = Arc::new(Mutex::new(BufferPool::new(
            Arc::clone(&disk),
            BUFFERPOOL_SIZE,
        )));

        let merge_thread_handle = Table::spawn_merge_thread(
            &page_dir,
            &range_dir,
            &disk,
            &bufferpool,
            header.num_columns,
        );

        Table {
            name: name.into(),
            num_columns: header.num_columns,
            primary_key_index: header.primary_key_index,
            index,
            page_dir,
            range_dir,
            disk,
            bufferpool,
            next_rid: header.next_rid.into(),
            next_tid: header.next_tid.into(),
            merge_thread_handle: Some(merge_thread_handle),
        }
    }

    pub fn persist(&mut self) {
        let merge_thread_handle = std::mem::replace(&mut self.merge_thread_handle, None).unwrap();
        drop(merge_thread_handle.1);
        merge_thread_handle
            .0
            .join()
            .expect("Failed to join merge thread");

        let header = TableHeaderPage {
            num_columns: self.num_columns,
            primary_key_index: self.primary_key_index,
            next_rid: self.next_rid.load(Ordering::Relaxed),
            next_tid: self.next_tid.load(Ordering::Relaxed),
            next_free_page: self.disk.free_page_pointer(),
            indexed_columns: self.index.index_meta_to_bit_vector(),
        };

        let mut page = [0; PAGE_SIZE];
        let mut serializer = BufferSerializer::new(&mut page);

        serializer
            .serialize_value(&header)
            .expect("Unable to serialize table header");

        self.disk.write_page(0, &page);
        self.disk.flush();

        self.bufferpool.lock().flush_all();

        let page_dir = self.page_dir.write();
        page_dir.persist();

        let range_dir = self.range_dir.write();
        range_dir.persist();

        self.index.persist();
    }

    pub fn next_tid(&self, range_id: usize) -> RID {
        let mut range_dir = self.range_dir.write();

        if range_id >= range_dir.next_range_id() {
            assert!(range_id == range_dir.next_range_id());

            let new_page = self.allocate_tail_page();

            self.get_page_by_id(new_page.current_tail_page.load(Ordering::Relaxed))
                .write_last_tail(&mut self.bufferpool.lock(), RID_INVALID);

            range_dir.allocate_range(new_page);
        }

        let range = range_dir.get(range_id);
        if range.tail_is_full() {
            let last_tail_page = range.current_tail_page.load(Ordering::Relaxed);
            let new_tail = self.allocate_tail_page();

            self.get_page_by_id(new_tail.current_tail_page.load(Ordering::Relaxed))
                .write_last_tail(&mut self.bufferpool.lock(), last_tail_page as u64);

            range_dir.new_range_tail(range_id, new_tail);

            self.merge_thread_handle
                .as_ref()
                .expect("No merge handle")
                .1
                .send(range_id)
                .expect("Unable to send range id to merge channel");
        }

        range_dir.get(range_id).next_tid()
    }

    pub fn allocate_tail_page(&self) -> PageRange {
        let next_tid: RID = self
            .next_tid
            .fetch_sub(PAGE_SLOTS as u64, Ordering::Relaxed)
            .into();

        let tail_reserve_start = self.disk.reserve_range(self.total_columns());
        let mut column_pages = Arc::<[usize]>::new_uninit_slice(self.total_columns());

        for (i, x) in (tail_reserve_start..(tail_reserve_start + self.total_columns())).enumerate()
        {
            Arc::get_mut(&mut column_pages).unwrap()[i].write(x);
        }

        let column_pages = unsafe { column_pages.assume_init() };

        let mut page_dir = self.page_dir.write();

        page_dir.new_page(next_tid.page(), column_pages);

        drop(page_dir);

        PageRange::new(next_tid.raw(), next_tid.page())
    }

    #[inline(always)]
    fn get_page(&self, rid: RID) -> Page {
        Page::new(self.page_dir.read().get(rid).expect("Page get fail"))
    }

    #[inline(always)]
    fn get_page_by_id(&self, id: usize) -> Page {
        Page::new(self.page_dir.read().get_page(id).expect("Page get fail"))
    }

    fn find_row(&self, column_index: usize, value: u64) -> Option<RID> {
        match self.index.get_from_index(column_index, value) {
            Some(vals) => vals
                .iter()
                .find(|x| {
                    self.get_page(**x)
                        .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                        .slot(x.slot())
                        != RID_INVALID
                })
                .copied(),
            None => {
                let mut rid: RID = 0.into();

                let next_rid = self.next_rid.load(Ordering::Relaxed);

                while rid.raw() < next_rid {
                    let page = self.get_page(rid);

                    if page
                        .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                        .slot(rid.slot())
                        == RID_INVALID
                    {
                        rid = rid.next();
                        continue;
                    }

                    drop(page);

                    let latest_rid = self.get_latest(rid);
                    let latest_page = self.get_page(latest_rid);

                    if latest_page
                        .get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + column_index,
                        )
                        .slot(latest_rid.slot())
                        == value
                    {
                        return Some(rid);
                    }
                }

                None
            }
        }
    }

    fn find_rows(&self, column_index: usize, value: u64) -> Vec<RID> {
        match self.index.get_from_index(column_index, value) {
            Some(vals) => vals
                .into_iter()
                .filter(|x| {
                    self.get_page(*x)
                        .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                        .slot(x.page())
                        != RID_INVALID
                })
                .collect(),
            None => {
                let mut rid: RID = 0.into();
                let mut rids = Vec::new();
                let next_rid = self.next_rid.load(Ordering::Relaxed);

                while rid.raw() < next_rid {
                    let page = self.get_page(rid);

                    if page
                        .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                        .slot(rid.slot())
                        == RID_INVALID
                    {
                        rid = rid.next();
                        continue;
                    }

                    let latest_rid = self.get_latest(rid);

                    if self
                        .get_page(latest_rid)
                        .get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + column_index,
                        )
                        .slot(latest_rid.slot())
                        == value
                    {
                        rids.push(rid);
                    }

                    rid = rid.next();
                }

                rids
            }
        }
    }

    fn find_rows_range(
        &self,
        column_index: usize,
        range: impl RangeBounds<u64> + Clone,
    ) -> Vec<RID> {
        match self.index.range_from_index(column_index, range.clone()) {
            Some(vals) => vals,
            None => {
                let mut rids: Vec<RID> = Vec::new();
                let mut rid: RID = 0.into();
                let next_rid = self.next_rid.load(Ordering::Relaxed);

                while rid.raw() < next_rid {
                    let key = self
                        .get_page(rid)
                        .get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + self.primary_key_index,
                        )
                        .slot(rid.slot());

                    if range.contains(&key) {
                        rids.push(rid);
                    }

                    rid = rid.next();
                }

                rids
            }
        }
    }

    pub fn is_latest(&self, rid: RID) -> bool {
        let mut bp = self.bufferpool.lock();
        self.get_page(rid).read_page_tps(bp.borrow_mut())
            <= self
                .get_page(rid)
                .get_column(bp.borrow_mut(), METADATA_INDIRECTION)
                .slot(rid.slot())
    }

    pub fn get_latest(&self, rid: RID) -> RID {
        let page = self.get_page(rid);

        let mut bp = self.bufferpool.lock();

        let indir = page
            .get_column(bp.borrow_mut(), METADATA_INDIRECTION)
            .slot(rid.slot());

        if indir == RID_INVALID || page.read_page_tps(bp.borrow_mut()) <= indir {
            rid
        } else {
            indir.into()
        }
    }

    pub fn get_latest_with_bp(&self, bp: &mut BufferPool, rid: RID) -> RID {
        let page = self.get_page(rid);

        let indir = page
            .get_column(bp.borrow_mut(), METADATA_INDIRECTION)
            .slot(rid.slot());

        if indir == RID_INVALID || page.read_page_tps(bp.borrow_mut()) <= indir {
            rid
        } else {
            indir.into()
        }
    }

    pub fn merge_values(&self, base_rid: RID, columns: &[Option<u64>]) -> Vec<u64> {
        let rid = self.get_latest(base_rid);
        let page = self.get_page(rid);

        columns
            .iter()
            .zip(
                (NUM_METADATA_COLUMNS..(self.num_columns + NUM_METADATA_COLUMNS)).map(|column| {
                    page.get_column(self.bufferpool.lock().borrow_mut(), column)
                        .slot(rid.slot())
                }),
            )
            .map(|(x, y)| match x {
                None => y,
                Some(x) => *x,
            })
            .collect()
    }

    pub fn total_columns(&self) -> usize {
        NUM_METADATA_COLUMNS + self.num_columns
    }
    pub fn select_query(
        &self,
        search_value: u64,
        column_index: usize,
        included_columns: &Vec<usize>,
    ) -> Vec<RecordRust> {
        let vals: Vec<RID> = self.find_rows(column_index, search_value);

        vals.into_iter()
            .map(|rid| {
                let rid = self.get_latest(rid);
                let page = self.get_page(rid);

                let result_cols = included_columns
                    .iter()
                    .map(|i| {
                        page.get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + i,
                        )
                        .slot(rid.slot())
                    })
                    .collect::<Vec<u64>>();

                let original_rid = page
                    .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                    .slot(rid.slot());

                let indirection = page
                    .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
                    .slot(rid.slot());

                let schema = page
                    .get_column(
                        self.bufferpool.lock().borrow_mut(),
                        METADATA_SCHEMA_ENCODING,
                    )
                    .slot(rid.slot());

                let record = RecordRust {
                    rid: original_rid,
                    indirection,
                    schema_encoding: schema,
                    columns: result_cols,
                };

                record
            })
            .collect()
    }
    pub fn insert_query(&mut self, values: Vec<u64>) {
        let rid: RID = self.next_rid.fetch_add(1, Ordering::Relaxed).into();

        let page_dir = self.page_dir.read();

        let page: Arc<[usize]> = match page_dir.get(rid) {
            None => {
                drop(page_dir);
                let mut page_dir = self.page_dir.write();
                // Check again since unlocking read and acquiring write are not atomic
                if page_dir.get(rid).is_none() {
                    let reserve_count = self.total_columns() * PAGE_RANGE_COUNT;
                    let reserved = self.disk.reserve_range(reserve_count);

                    for i in 0..PAGE_RANGE_COUNT {
                        let page_id = (rid.page_range() * PAGE_RANGE_COUNT) + i;
                        let mut column_pages =
                            Arc::<[usize]>::new_uninit_slice(self.total_columns());

                        let start_offset = reserved + (i * self.total_columns());

                        for (i, x) in
                            (start_offset..(start_offset + self.total_columns())).enumerate()
                        {
                            Arc::get_mut(&mut column_pages).unwrap()[i].write(x);
                        }

                        let column_pages = unsafe { column_pages.assume_init() };

                        self.bufferpool
                            .lock()
                            .get_page(column_pages[METADATA_PAGE_HEADER])
                            .write_slot(0, RID_INVALID);

                        page_dir.new_page(page_id, column_pages);
                    }
                }

                page_dir
                    .get(rid)
                    .expect("Allocated new pages but no mapping in directory")
            }
            Some(cols) => cols,
        };

        let page = Page::new(page);

        page.get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(rid.slot(), RID_INVALID);

        page.get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
            .write_slot(rid.slot(), rid.raw());

        page.get_column(
            self.bufferpool.lock().borrow_mut(),
            METADATA_SCHEMA_ENCODING,
        )
        .write_slot(rid.slot(), 0);

        for (i, val) in values.iter().enumerate() {
            page.get_column(
                self.bufferpool.lock().borrow_mut(),
                NUM_METADATA_COLUMNS + i,
            )
            .write_slot(rid.slot(), *val);
        }

        for i in 0..self.num_columns {
            self.index.update_index(i, *values.get(i).unwrap(), rid);
        }
    }

    pub fn sum_query(&self, start_range: u64, end_range: u64, column_index: usize) -> u64 {
        let mut bp = self.bufferpool.lock();
        let mut sum: u64 = 0;
        for rid in self
            .find_rows_range(column_index, RangeInclusive::new(start_range, end_range))
            .iter()
        {
            let latest = self.get_latest_with_bp(&mut bp, *rid);
            sum += self
                .get_page(latest)
                .get_column(&mut bp, NUM_METADATA_COLUMNS + column_index)
                .slot(latest.slot());
        }

        sum
    }

    pub fn update_query(&mut self, key: u64, values: &Vec<Option<u64>>) -> bool {
        let row = self.find_row(self.primary_key_index, key);

        if let Some(pk) = values[self.primary_key_index] {
            if row.is_some() {
                return false;
            }
        }

        if row.is_none() {
            return false;
        }

        let base_rid = row.unwrap();
        let base_page = self.get_page(base_rid);
        let updated_values = self.merge_values(base_rid, &values);

        let old_latest_rid: RID = self
            .get_page(base_rid)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .slot(base_rid.slot())
            .into();

        let base_latest = self.get_latest(base_rid);
        let old_schema_encoding = self
            .get_page(base_latest)
            .get_column(
                self.bufferpool.lock().borrow_mut(),
                METADATA_SCHEMA_ENCODING,
            )
            .slot(base_latest.slot());

        let tail_rid = self.next_tid(base_rid.page_range());
        let tail_page = self.get_page(tail_rid);

        tail_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_BASE_RID)
            .write_slot(tail_rid.slot(), base_rid.raw());

        tail_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(
                tail_rid.slot(),
                if old_latest_rid.is_invalid() {
                    base_rid.raw()
                } else {
                    old_latest_rid.raw()
                },
            );

        tail_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
            .write_slot(tail_rid.slot(), tail_rid.raw());

        //print!("Update vals: {:?}\n", columns);

        for (i, val) in updated_values.iter().enumerate() {
            tail_page
                .get_column(
                    self.bufferpool.lock().borrow_mut(),
                    NUM_METADATA_COLUMNS + i,
                )
                .write_slot(tail_rid.slot(), *val);

            //print!("Base Page: {:?}\n",&base_page.get_column(crate::NUM_METADATA_COLUMNS + i).page[0..50],);
            //print!("Tail Page: {:?}\n",&page.get_column(crate::NUM_METADATA_COLUMNS + i).page[0..50]);
        }

        let mut schema_encoding: u64 = 0;

        for (i, v) in values.iter().enumerate() {
            if !v.is_none() {
                schema_encoding |= 1 << i;
                self.index.update_index(i, v.unwrap(), base_rid);
                if (old_schema_encoding & (1 << i)) == 1 || old_latest_rid.is_invalid() {
                    self.index.remove_index(
                        i,
                        base_page
                            .get_column(
                                self.bufferpool.lock().borrow_mut(),
                                NUM_METADATA_COLUMNS + i,
                            )
                            .slot(base_rid.slot()),
                        base_rid,
                    );
                } else if !old_latest_rid.is_invalid() && (old_schema_encoding & (1 << i)) == 1 {
                    self.index.remove_index(
                        i,
                        self.get_page(old_latest_rid)
                            .get_column(
                                self.bufferpool.lock().borrow_mut(),
                                NUM_METADATA_COLUMNS + i,
                            )
                            .slot(old_latest_rid.slot()),
                        base_rid,
                    );
                }
            }
        }

        tail_page
            .get_column(
                self.bufferpool.lock().borrow_mut(),
                METADATA_SCHEMA_ENCODING,
            )
            .write_slot(base_rid.slot(), schema_encoding);

        //print!("Update called\n");

        base_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(base_rid.slot(), tail_rid.raw());

        true
    }
    pub fn delete_query(&mut self, key: u64) -> bool {
        let row = self.find_row(self.primary_key_index, key);

        if row.is_none() {
            return false;
        }

        let row = row.unwrap();

        let mut next_tail: RID = self
            .get_page(row)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .slot(row.slot())
            .into();

        while next_tail.raw() != RID_INVALID && next_tail.raw() != row.raw() {
            let next = self
                .get_page(next_tail)
                .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
                .slot(next_tail.slot());

            self.get_page(next_tail)
                .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                .write_slot(next_tail.slot(), RID_INVALID);

            next_tail = next.into();
        }

        self.get_page(row)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
            .write_slot(row.slot(), RID_INVALID);

        true
    }
    pub fn build_index(&mut self, column_num: usize) {
        self.index.create_index(column_num);
        let mut rid: RID = 0.into();
        let max_rid = self.next_rid.load(Ordering::Relaxed);
        while rid.raw() < max_rid {
            if self
                .get_page(rid)
                .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                .slot(rid.slot())
                == RID_INVALID
            {
                continue;
            }

            let latest = self.get_latest(rid);
            self.index.update_index(
                column_num,
                self.get_page(latest)
                    .get_column(
                        self.bufferpool.lock().borrow_mut(),
                        NUM_METADATA_COLUMNS + column_num,
                    )
                    .slot(latest.slot()),
                rid,
            );
            rid = rid.next();
        }
    }
    pub fn drop_index(&mut self, column_num: usize) {
        self.index.drop_index(column_num);
    }
    pub fn print(&self) {
        println!("{}", self.name);
        println!("{}", self.num_columns);
        println!("{}", self.primary_key_index);
        println!("{}", self.index);
    }


}
impl TablePy {
    pub fn load(
        name: &str,
        db_file: &Path,
        pd_file: &Path,
        id_file: &Path,
        rd_file: &Path,
    ) -> Self {
        Self {
            table: Table::load(name, db_file, pd_file, id_file, rd_file),
        }
    }
    pub fn persist(&mut self) {
        self.table.persist();
    }
}
#[pymethods]
impl TablePy {
    #[getter]
    fn num_columns(&self) -> usize {
        self.table.num_columns
    }

    #[new]
    pub fn new(
        name: String,
        num_columns: usize,
        key_index: usize,
        db_file: String,
        pd_file: String,
        id_file: String,
        rd_file: String,
    ) -> TablePy {
        let table = Table::new(
            name,
            num_columns,
            key_index,
            db_file,
            pd_file,
            id_file,
            rd_file,
        );
        return TablePy { table };
    }

    pub fn sum(&self, start_range: u64, end_range: u64, column_index: usize) -> u64 {
        self.table.sum_query(start_range, end_range, column_index)
    }

    pub fn select(&self, search_value: u64, column_index: usize, columns: &PyList) -> Py<PyList> {
        if column_index >= self.table.num_columns {
            return Python::with_gil(|py| -> Py<PyList> { PyList::empty(py).into() });
        }

        let included_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_i, x)| x.extract::<u64>().unwrap() != 0)
            .map(|(i, _x)| i)
            .collect();

        let results = self
            .table
            .select_query(search_value, column_index, &included_columns);
        Python::with_gil(|py| -> Py<PyList> {
            let selected_records: Py<PyList> = PyList::empty(py).into();
            for result in results {
                selected_records
                    .as_ref(py)
                    .append(Record::from(&result, py).into_py(py))
                    .expect("Failed to append to python list");
            }
            selected_records
        })
    }

    pub fn update(&mut self, key: u64, values: &PyTuple) -> bool {
        let vals: Vec<Option<u64>> = values
            .iter()
            .map(|val| val.extract::<Option<u64>>().unwrap())
            .collect::<Vec<Option<u64>>>();
        self.table.update_query(key, &vals)
    }

    pub fn delete(&mut self, key: u64) -> bool {
        self.table.delete_query(key)
    }

    #[args(values = "*")]
    pub fn insert(&mut self, values: &PyTuple) {
        if self
            .table
            .find_row(
                self.table.primary_key_index,
                values
                    .get_item(self.table.primary_key_index)
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
            )
            .is_some()
        {
            return;
        }
        let vals = values
            .iter()
            .map(|v| v.extract::<u64>().unwrap())
            .collect::<Vec<u64>>();
        self.table.insert_query(vals);
    }

    pub fn build_index(&mut self, column_num: usize) {
        self.table.build_index(column_num);
    }

    pub fn drop_index(&mut self, column_num: usize) {
        self.table.drop_index(column_num);
    }

    pub fn print(&self) {
        self.table.print();
    }
}
