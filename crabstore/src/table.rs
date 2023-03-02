use std::{
    borrow::BorrowMut,
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    bufferpool::BufferPool, disk_manager::DiskManager, page::PhysicalPage, rid::RID,
    PAGE_RANGE_SIZE, PAGE_SIZE, PAGE_SLOTS,
};
use crate::{index::Index, RID_INVALID};
use crate::{
    page::{Page, PageRange},
    page_directory::PageDirectory,
};
use crate::{
    Record, METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS,
};
use parking_lot::{lock_api::RawMutex, Mutex, RwLock};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use rkyv::{
    ser::{serializers::BufferSerializer, Serializer},
    Archive, Deserialize, Serialize,
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

#[pyclass(subclass)]
pub struct Table {
    name: String,
    num_columns: usize,
    primary_key_index: usize,
    index: Index,
    next_rid: AtomicU64,
    next_tid: AtomicU64,
    page_dir: RwLock<PageDirectory>,
    bufferpool: Mutex<BufferPool>,
    range_dir: RwLock<Vec<PageRange>>,
    disk: Arc<DiskManager>,
}

impl Table {
    pub fn load(name: &str, db_file: &Path, pd_file: &Path) -> Self {
        let disk = Arc::new(DiskManager::new(db_file).expect("Failed to open table file"));

        let mut page = PhysicalPage::default();

        disk.read_page(0, &mut page.page);

        let header = unsafe {
            rkyv::from_bytes_unchecked::<TableHeaderPage>(
                &page.page[0..size_of::<<TableHeaderPage as Archive>::Archived>()],
            )
            .expect("Failed to deserialize table header")
        };

        let mut index = Index::new(header.primary_key_index, header.num_columns);

        index.create_indexes_from_bit_vector(header.indexed_columns);

        let page_dir = RwLock::new(PageDirectory::load(pd_file));
        let range_dir: RwLock<Vec<PageRange>> = RwLock::new(Vec::new());

        let bufferpool = Mutex::new(BufferPool::new(Arc::clone(&disk), 128));

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
        }
    }

    pub fn persist(&mut self) {
        let header = TableHeaderPage {
            num_columns: self.num_columns,
            primary_key_index: self.primary_key_index,
            next_rid: self.next_rid.load(Ordering::SeqCst),
            next_tid: self.next_tid.load(Ordering::SeqCst),
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

        let mut page_dir = self.page_dir.write();
        page_dir.persist();
    }

    /*
    pub fn get_range(&self, range_id: usize) -> PageRange {
        let mut range_dir = self.range_dir.read();
        if range_id >= range_dir.len() {
            assert!(range_id == range_dir.len());
            drop(range_dir);
            let mut range_dirw = self.range_dir.write();
            if range_id >= range_dirw.len() {
                range_dirw.push(self.allocate_tail_page());
            }
            range_dir = self.range_dir.read();
        }

        let range = &range_dir[range_id];

        if range.is_full() {
            drop(range_dir);
            drop(range);
            let range_dirw = self.range_dir.write();
            if range_dirw[range_id].is_full() {
                range_dirw[range_id] = self.allocate_tail_page();
                return range_dirw[range_id];
            }
        }

        range
    }
    */

    pub fn next_tid(&self, range_id: usize) -> RID {
        let mut range_dir = self.range_dir.write();
        if range_id >= range_dir.len() {
            assert!(range_id == range_dir.len());
            if range_id >= range_dir.len() {
                range_dir.push(self.allocate_tail_page());
            }
        }

        if range_dir[range_id].is_full() {
            range_dir[range_id] = self.allocate_tail_page();
        }

        range_dir[range_id].next_tid()
    }

    pub fn allocate_tail_page(&self) -> PageRange {
        let next_tid: RID = self
            .next_tid
            .fetch_sub(PAGE_SLOTS as u64, Ordering::SeqCst)
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

    fn get_page(&self, rid: RID) -> Page {
        Page::new(self.page_dir.read().get(rid).expect("Page get fail"))
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

                let next_rid = self.next_rid.load(Ordering::SeqCst);

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
                let next_rid = self.next_rid.load(Ordering::SeqCst);

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
                        .slot(rid.slot())
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

    fn find_rows_range(&self, column_index: usize, begin: u64, end: u64) -> Vec<RID> {
        match self.index.range_from_index(column_index, begin, end) {
            Some(vals) => vals,
            None => {
                let mut rids: Vec<RID> = Vec::new();
                let mut rid: RID = 0.into();
                let next_rid = self.next_rid.load(Ordering::SeqCst);

                while rid.raw() < next_rid {
                    let key = self
                        .get_page(rid)
                        .get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + self.primary_key_index,
                        )
                        .slot(rid.slot());

                    if key >= begin && key <= end {
                        rids.push(rid);
                    }

                    rid = rid.next();
                }

                rids
            }
        }
    }

    pub fn is_latest(&self, rid: RID) -> bool {
        self.get_page(rid)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .slot(rid.slot())
            == RID_INVALID
    }

    pub fn get_latest(&self, rid: RID) -> RID {
        let indir = self
            .get_page(rid)
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .slot(rid.slot());

        if indir == RID_INVALID {
            rid
        } else {
            indir.into()
        }
    }

    pub fn merge_values(&self, base_rid: RID, columns: &Vec<Option<u64>>) -> Vec<u64> {
        let rid = self.get_latest(base_rid);
        let page = self.get_page(rid);

        columns
            .iter()
            .zip(
                (NUM_METADATA_COLUMNS..self.num_columns + NUM_METADATA_COLUMNS).map(|column| {
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
}

#[pymethods]
impl Table {
    #[getter]
    fn num_columns(&self) -> usize {
        self.num_columns
    }

    #[new]
    pub fn new(
        name: String,
        num_columns: usize,
        key_index: usize,
        db_file: String,
        pd_file: String,
    ) -> Table {
        let page_dir = RwLock::new(PageDirectory::new(Path::new(&pd_file)));
        let range_dir: RwLock<Vec<PageRange>> = RwLock::new(Vec::new());

        let disk = Arc::new(DiskManager::new(Path::new(&db_file)).unwrap());
        let bufferpool = Mutex::new(BufferPool::new(Arc::clone(&disk), 128));

        Table {
            name,
            num_columns,
            primary_key_index: key_index,
            index: Index::new(key_index, num_columns),
            next_rid: 0.into(),
            next_tid: (!0 - 1).into(),
            page_dir,
            range_dir,
            disk,
            bufferpool,
        }
    }

    pub fn sum(&self, start_range: u64, end_range: u64, column_index: usize) -> u64 {
        self.find_rows_range(column_index, start_range, end_range)
            .iter()
            .map(|rid| {
                let latest = self.get_latest(*rid);
                self.get_page(latest)
                    .get_column(
                        self.bufferpool.lock().borrow_mut(),
                        NUM_METADATA_COLUMNS + column_index,
                    )
                    .slot(latest.slot())
            })
            .sum()
    }

    pub fn select(&self, search_value: u64, column_index: usize, columns: &PyList) -> Py<PyList> {
        let included_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_i, x)| x.extract::<u64>().unwrap() != 0)
            .map(|(i, _x)| i)
            .collect();

        let vals: Vec<RID> = self.find_rows(column_index, search_value);

        Python::with_gil(|py| -> Py<PyList> {
            let selected_records: Py<PyList> = PyList::empty(py).into();

            for rid in vals {
                let result_cols = PyList::empty(py);
                let rid = self.get_latest(rid);
                let page = self.get_page(rid);

                for i in included_columns.iter() {
                    result_cols.append(
                        page.get_column(
                            self.bufferpool.lock().borrow_mut(),
                            NUM_METADATA_COLUMNS + i,
                        )
                        .slot(rid.slot()),
                    );
                }

                let record = PyCell::new(
                    py,
                    Record::new(
                        page.get_column(self.bufferpool.lock().borrow_mut(), METADATA_RID)
                            .slot(rid.slot()) as u64,
                        page.get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
                            .slot(rid.slot()) as u64,
                        page.get_column(
                            self.bufferpool.lock().borrow_mut(),
                            METADATA_SCHEMA_ENCODING,
                        )
                        .slot(rid.slot()) as u64,
                        result_cols.into(),
                    ),
                )
                .unwrap();

                selected_records.as_ref(py).append(record);
            }
            selected_records
        })
    }

    pub fn update(&mut self, key: u64, values: &PyTuple) -> bool {
        let vals: Vec<Option<u64>> = values
            .iter()
            .map(|val| val.extract::<Option<u64>>().unwrap())
            .collect::<Vec<Option<u64>>>();

        let row = self.find_row(self.primary_key_index, key);

        if row.is_none() {
            return false;
        }

        let base_rid = row.unwrap();
        let base_page = self.get_page(base_rid);
        let updated_values = self.merge_values(base_rid, &vals);

        let indirection_column_rid = match base_page
            .get_column(
                self.bufferpool.lock().borrow_mut(),
                METADATA_SCHEMA_ENCODING,
            )
            .slot(base_rid.slot())
        {
            0 => base_rid.raw(),
            _ => base_page
                .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
                .slot(base_rid.slot()),
        };

        let tail_rid = self.next_tid(base_rid.page_range());
        let tail_page = self.get_page(tail_rid);

        tail_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(tail_rid.slot(), indirection_column_rid);

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

        let old_latest_rid = self.get_latest(base_rid);
        let old_schema_encoding = base_page
            .get_column(
                self.bufferpool.lock().borrow_mut(),
                METADATA_SCHEMA_ENCODING,
            )
            .slot(base_rid.slot());

        let mut schema_encoding: u64 = 0;

        for (i, v) in vals.iter().enumerate() {
            if !v.is_none() {
                schema_encoding |= 1 << i;
                self.index.update_index(i, v.unwrap(), base_rid);
                if old_schema_encoding == 0 {
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
                } else {
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

        //print!("Update called\n");

        base_page
            .get_column(self.bufferpool.lock().borrow_mut(), METADATA_INDIRECTION)
            .write_slot(base_rid.slot(), tail_rid.raw());

        base_page
            .get_column(
                self.bufferpool.lock().borrow_mut(),
                METADATA_SCHEMA_ENCODING,
            )
            .write_slot(base_rid.slot(), schema_encoding);

        true
    }

    pub fn delete(&mut self, key: u64) -> bool {
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

    #[args(values = "*")]
    pub fn insert(&mut self, values: &PyTuple) {
        let rid: RID = self.next_rid.fetch_add(1, Ordering::SeqCst).into();

        let page_dir = self.page_dir.read();

        let page: Arc<[usize]> = match page_dir.get(rid) {
            None => {
                drop(page_dir);
                let mut page_dir = self.page_dir.write();
                // Check again since unlocking read and acquiring write are not atomic
                if page_dir.get(rid).is_none() {
                    let reserve_count = self.total_columns() * PAGE_RANGE_SIZE;
                    let reserved = self.disk.reserve_range(reserve_count);

                    for i in 0..PAGE_RANGE_SIZE {
                        let page_id = (rid.page_range() * PAGE_RANGE_SIZE) + i;
                        let mut column_pages =
                            Arc::<[usize]>::new_uninit_slice(self.total_columns());

                        let start_offset = reserved + (i * self.total_columns());

                        for (i, x) in
                            (start_offset..(start_offset + self.total_columns())).enumerate()
                        {
                            Arc::get_mut(&mut column_pages).unwrap()[i].write(x);
                        }

                        let column_pages = unsafe { column_pages.assume_init() };

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
            .write_slot(rid.slot(), val.extract().unwrap())
        }

        self.index.update_index(
            self.primary_key_index,
            values
                .get_item(self.primary_key_index)
                .unwrap()
                .extract()
                .unwrap(),
            rid,
        );
    }

    pub fn print(&self) {
        println!("{}", self.name);
        println!("{}", self.num_columns);
        println!("{}", self.primary_key_index);
        println!("{}", self.index);
    }
}
