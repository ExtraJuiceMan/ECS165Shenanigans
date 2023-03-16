use crate::{
    bufferpool::{BufferPool, BufferPoolFrame},
    disk_manager::DiskManager,
    lock_manager::{LockManager, LockType},
    page::PhysicalPage,
    range_directory::RangeDirectory,
    record::Record,
    rid::RID,
    transaction::Transaction,
    BUFFERPOOL_SIZE, METADATA_BASE_RID, METADATA_PAGE_HEADER, NUM_STATIC_COLUMNS, PAGE_RANGE_COUNT,
    PAGE_SIZE, PAGE_SLOTS,
};
use crate::{index::Index, RID_INVALID};
use crate::{
    page::{Page, PageRange},
    page_directory::PageDirectory,
};
use crate::{
    record, METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS,
};
use parking_lot::{lock_api::RawMutex, Mutex, RwLock};
use rkyv::{
    ser::{serializers::BufferSerializer, Serializer},
    with::Lock,
    Archive, Deserialize, Serialize,
};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
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
    fmt,
    ops::{RangeBounds, RangeInclusive},
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
    next_free_page: usize,
    next_rid: u64,
    next_tid: u64,
}

pub struct Table {
    name: String,
    num_columns: usize,
    primary_key_index: usize,
    index: RwLock<Index>,
    next_rid: AtomicU64,
    next_tid: AtomicU64,
    page_dir: Arc<RwLock<PageDirectory>>,
    range_dir: Arc<Mutex<RangeDirectory>>,
    bufferpool: Arc<Mutex<BufferPool>>,
    lock_manager: Arc<LockManager>,
    disk: Arc<DiskManager>,
    merge_thread_handle: Mutex<Option<(JoinHandle<()>, Sender<usize>)>>,
}

impl Table {
    pub fn new(
        name: String,
        num_columns: usize,
        key_index: usize,
        db_file: &Path,
        pd_file: &Path,
        id_file: &Path,
        rd_file: &Path,
    ) -> Table {
        let page_dir = Arc::new(RwLock::new(PageDirectory::new(pd_file)));
        let range_dir = Arc::new(Mutex::new(RangeDirectory::new(rd_file)));

        let disk = Arc::new(DiskManager::new(db_file).unwrap());
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
            index: RwLock::new(Index::new(key_index, num_columns, id_file)),
            next_rid: 0.into(),
            next_tid: (!0 - 1).into(),
            page_dir,
            range_dir,
            disk,
            bufferpool,
            merge_thread_handle: Mutex::new(Some(merge_thread_handle)),
            lock_manager: Arc::new(LockManager::new()),
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

        let index = RwLock::new(Index::load(id_file));
        let page_dir = Arc::new(RwLock::new(PageDirectory::load(pd_file)));
        let range_dir = Arc::new(Mutex::new(RangeDirectory::load(rd_file)));
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
            merge_thread_handle: Mutex::new(Some(merge_thread_handle)),
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    pub fn persist(&self) {
        let merge_thread_handle =
            std::mem::replace(&mut *self.merge_thread_handle.lock(), None).unwrap();

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

        let range_dir = self.range_dir.lock();
        range_dir.persist();

        let index = self.index.write();
        index.persist();
    }

    pub fn next_tid(&self, range_id: usize) -> RID {
        let mut range_dir = self.range_dir.lock();

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

            let merge_thread_handle = self.merge_thread_handle.lock();
            merge_thread_handle
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
    pub fn get_page(&self, rid: RID) -> Page {
        Page::new(self.page_dir.read().get(rid).expect("Page get fail"))
    }

    #[inline(always)]
    fn get_page_by_id(&self, id: usize) -> Page {
        Page::new(self.page_dir.read().get_page(id).expect("Page get fail"))
    }

    pub fn get_bufferpool(&self) -> Arc<Mutex<BufferPool>> {
        Arc::clone(&self.bufferpool)
    }

    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        Arc::clone(&self.lock_manager)
    }

    fn find_row(&self, column_index: usize, value: u64) -> Option<RID> {
        match self.index.read().get_from_index(column_index, value) {
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
        match self.index.read().get_from_index(column_index, value) {
            Some(vals) => vals
                .into_iter()
                .filter(|x| {
                    self.get_page(*x)
                        .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                        .slot(x.slot())
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
        match self
            .index
            .read()
            .range_from_index(column_index, range.clone())
        {
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

        let mut bp = self.bufferpool.lock();
        columns
            .iter()
            .enumerate()
            .map(|(i, x)| match x {
                None => page
                    .get_column(&mut bp, NUM_METADATA_COLUMNS + i)
                    .slot(rid.slot()),
                Some(val) => *val,
            })
            .collect()
    }

    pub fn total_columns(&self) -> usize {
        NUM_METADATA_COLUMNS + self.num_columns
    }

    pub fn columns(&self) -> usize {
        self.num_columns
    }

    pub fn primary_key(&self) -> usize {
        self.primary_key_index
    }

    pub fn select_query(
        &self,
        search_value: u64,
        column_index: usize,
        included_columns: &[usize],
    ) -> Vec<Record> {
        let vals: Vec<RID> = self.find_rows(column_index, search_value);

        vals.into_iter()
            .map(|rid| {
                let rid = self.get_latest(rid);
                let page = self.get_page(rid);

                let result_cols = included_columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, x)| {
                        if *x != 0 {
                            Some(
                                page.get_column(
                                    self.bufferpool.lock().borrow_mut(),
                                    NUM_METADATA_COLUMNS + i,
                                )
                                .slot(rid.slot()),
                            )
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<u64>>();

                Record {
                    rid: rid.raw(),
                    columns: result_cols,
                }
            })
            .collect()
    }

    pub fn insert_query(&self, values: &[u64]) {
        if self
            .find_row(self.primary_key_index, values[self.primary_key_index])
            .is_some()
        {
            return;
        }

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

        let mut index = self.index.write();
        for i in 0..self.num_columns {
            index.update_index(i, values[i], rid);
        }
    }

    pub fn sum_query(&self, start_range: u64, end_range: u64, column_index: usize) -> u64 {
        let mut sum: u64 = 0;
        for rid in self
            .find_rows_range(column_index, RangeInclusive::new(start_range, end_range))
            .iter()
        {
            let latest = self.get_latest_with_bp(&mut self.bufferpool.lock(), *rid);
            sum += self
                .get_page(latest)
                .get_column(
                    &mut self.bufferpool.lock(),
                    NUM_METADATA_COLUMNS + column_index,
                )
                .slot(latest.slot());
        }

        sum
    }

    pub fn update_query(
        &self,
        key: u64,
        values: &[Option<u64>],
        mut transaction: Option<&mut Transaction>,
    ) -> bool {
        let row = self.find_row(self.primary_key_index, key);

        if let Some(pk) = values[self.primary_key_index] {
            if self.find_row(self.primary_key_index, pk).is_some() {
                if let Some(t) = transaction.borrow_mut() {
                    t.set_aborted(false);
                }
                return false;
            }
        }

        if row.is_none() {
            return false;
        }

        let base_rid = row.unwrap();

        if let Some(t) = transaction.borrow_mut() {
            if !t.try_lock_with_abort(&self.lock_manager, base_rid, LockType::Exclusive) {
                return false;
            }
        }

        let base_page = self.get_page(base_rid);
        let updated_values = self.merge_values(base_rid, values);

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
                let mut index = self.index.write();

                index.update_index(i, v.unwrap(), base_rid);
                if (old_schema_encoding & (1 << i)) == 1 || old_latest_rid.is_invalid() {
                    index.remove_index(
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
                    let mut index = self.index.write();

                    index.remove_index(
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

        if let Some(t) = transaction.borrow_mut() {
            t.log_write(METADATA_INDIRECTION, base_rid, old_latest_rid.raw());
            t.log_write(METADATA_RID, tail_rid, RID_INVALID);
        }

        base_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(base_rid.slot(), tail_rid.raw());

        true
    }

    pub fn delete_query(&self, key: u64, mut transaction: Option<&mut Transaction>) -> bool {
        let row = self.find_row(self.primary_key_index, key);

        if row.is_none() {
            return false;
        }

        let row = row.unwrap();

        if let Some(t) = transaction.borrow_mut() {
            if !t.try_lock_with_abort(&self.lock_manager, row, LockType::Exclusive) {
                return false;
            }
        }

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

        if let Some(t) = transaction.borrow_mut() {
            t.log_write(METADATA_RID, row, row.raw());
        }

        self.get_page(row)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
            .write_slot(row.slot(), RID_INVALID);

        true
    }

    pub fn build_index(&self, column_num: usize) {
        let mut index = self.index.write();
        index.create_index(column_num);
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
            index.update_index(
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

    pub fn drop_index(&self, column_num: usize) {
        self.index.write().drop_index(column_num);
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[Table \"{}\"]", self.name)?;
        writeln!(f, "{} Columns: ", self.num_columns)?;
        writeln!(f, "PK: {}", self.primary_key_index)?;
        writeln!(f, "Current RID: {}", self.next_rid.load(Ordering::Relaxed))?;
        writeln!(f, "Current TID: {}", self.next_rid.load(Ordering::Relaxed))?;
        writeln!(f)
    }
}
