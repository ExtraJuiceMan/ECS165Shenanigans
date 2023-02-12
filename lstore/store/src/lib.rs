use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use std::cell::RefCell;
use std::{borrow::Borrow, mem::size_of};
use std::{cell::Cell, collections::HashMap};

const PAGE_SIZE: usize = 4096;
const PAGE_SLOTS: usize = PAGE_SIZE / size_of::<i64>();
const PAGE_RANGE_SIZE: usize = PAGE_SIZE * 16;
const RANGE_PAGE_COUNT: usize = PAGE_RANGE_SIZE / PAGE_SIZE;
const NUM_METADATA_COLUMNS: usize = 4;

#[derive(Clone, Copy, Debug, Default)]
struct RID {
    rid: u64,
}

impl RID {
    fn new(rid: u64) -> Self {
        RID { rid }
    }

    fn next(&self) -> RID {
        RID { rid: self.rid + 1 }
    }

    fn slot(&self) -> usize {
        (self.rid & 0b111111111) as usize
    }

    fn page(&self) -> usize {
        ((self.rid >> 9) & 0b1111) as usize
    }

    fn page_range(&self) -> usize {
        (self.rid >> 13) as usize
    }
}

impl From<u64> for RID {
    fn from(value: u64) -> Self {
        RID { rid: value }
    }
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Index {
    indices: HashMap<i64, HashMap<i64, Vec<i64>>>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            indices: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, column_number: i64) {}

    pub fn drop_index(&mut self, column_number: i64) {}
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Record {
    rid: u64,
    indirection: u64,
    schema_encoding: u64,
    row: Vec<i64>,
}

#[pymethods]
impl Record {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Record {
            rid: 0,
            indirection: 0,
            schema_encoding: 0,
            row: Vec::new(),
        })
    }
}

#[derive(Debug)]
struct PhysicalPage {
    page: RefCell<[i64; PAGE_SLOTS]>,
}

impl Default for PhysicalPage {
    fn default() -> Self {
        PhysicalPage {
            page: RefCell::new([0; PAGE_SLOTS]),
        }
    }
}

impl PhysicalPage {
    fn slot(&self, index: usize) -> i64 {
        self.page.borrow()[index]
    }

    fn write_slot(&self, index: usize, value: i64) {
        self.page.borrow_mut()[index] = value;
    }
}

#[derive(Debug)]
struct Page {
    columns: Box<[PhysicalPage]>,
}

impl Page {
    pub fn new(num_columns: usize) -> Self {
        let mut columns: Vec<PhysicalPage> = Vec::with_capacity(NUM_METADATA_COLUMNS + num_columns);
        columns.resize_with(NUM_METADATA_COLUMNS + num_columns, Default::default);
        let columns = columns.into_boxed_slice();

        Page { columns }
    }

    pub fn get_column(&self, index: usize) -> &PhysicalPage {
        self.columns.as_ref()[index].borrow()
    }
}

#[derive(Debug)]
struct PageRange {
    write_offset: Cell<usize>,
    tail_offset: Cell<usize>,
    base_pages: Vec<Page>,
    tail_pages: Vec<Page>,
}

impl PageRange {
    pub fn new(num_columns: usize) -> Self {
        let tail_pages: Vec<Page> = vec![Page::new(num_columns)];
        let mut base_pages: Vec<Page> = Vec::new();

        for _ in 0..RANGE_PAGE_COUNT {
            base_pages.push(Page::new(num_columns));
        }

        PageRange {
            write_offset: Cell::new(0),
            tail_offset: Cell::new(0),
            base_pages,
            tail_pages,
        }
    }

    pub fn get_page(&self, page_num: usize) -> &Page {
        &self.base_pages[page_num]
    }
}

#[derive(Debug, Default)]
#[pyclass(subclass)]
struct Table {
    name: String,
    num_columns: usize,
    key_index: usize,
    index: u64,
    next_rid: Cell<RID>,
    ranges: Vec<PageRange>,
}

impl Table {
    fn get_page_range(&self, range_number: usize) -> &PageRange {
        &self.ranges[range_number]
    }
}

#[pymethods]
impl Table {
    #[new]
    pub fn new(name: String, num_columns: usize, key_index: usize) -> Table {
        Table {
            name,
            num_columns,
            key_index,
            index: 0,
            next_rid: Cell::new(RID::new(0)),
            ranges: Vec::new(),
        }
    }

    #[args(values = "*")]
    pub fn insert(&mut self, values: &PyTuple) {
        let rid: RID = self.next_rid.get();
        let page_range = rid.page_range();
        let page = rid.page();
        let slot = rid.slot();

        if self.ranges.len() <= page_range {
            self.ranges.push(PageRange::new(self.num_columns));
        }

        let page_range = self.get_page_range(page_range);
        let page = page_range.get_page(page);

        for (i, val) in values.iter().enumerate() {
            page.get_column(NUM_METADATA_COLUMNS + i)
                .write_slot(slot, val.extract().unwrap())
        }

        self.next_rid.set(rid.next());
    }

    pub fn print(&self) {
        println!("{}", self.name);
        println!("{}", self.num_columns);
        println!("{}", self.key_index);
        println!("{}", self.index);
    }
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Rstore {
    tables: HashMap<String, Py<Table>>,
}

#[pymethods]
impl Rstore {
    #[new]
    pub fn new() -> Self {
        Rstore {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Py<Table> {
        Python::with_gil(|py| -> Py<Table> {
            let table: Py<Table> =
                Py::new(py, Table::new(name.clone(), num_columns, key_index)).unwrap();
            self.tables.insert(name.clone(), Py::clone_ref(&table, py));
            table
        })
    }

    pub fn drop_table(&mut self, name: String) -> PyResult<()> {
        self.tables.remove(&name);
        Ok(())
    }

    pub fn get_table(&self, name: String) -> Py<Table> {
        Py::clone(self.tables.get(&name).unwrap())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn store(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Record>()?;
    m.add_class::<Table>()?;
    m.add_class::<Rstore>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
