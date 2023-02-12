use std::collections::HashMap;
use std::{borrow::Borrow, mem::size_of};
use std::{borrow::BorrowMut, cell::RefCell};
// use std::io::{Write, BufReader, BufRead, ErrorKind};
use pyo3::prelude::*;
use std::fmt::{self, write, Display};
const PAGE_SIZE: usize = 4096;
const PAGE_SLOTS: usize = PAGE_SIZE / size_of::<i64>();
const PAGE_RANGE_SIZE: usize = PAGE_SIZE * 16;
const RANGE_PAGE_COUNT: usize = PAGE_RANGE_SIZE / PAGE_SIZE;
const NUM_METADATA_COLUMNS: usize = 4;

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Index {
    indices: HashMap<i64, HashMap<i64, Vec<i64>>>,
}
impl fmt::Display for Index{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        for (key1, value) in self.indices.iter() {
            write!(f, "key: {}, value: \n", key1)?;
            for (key2, value2) in value.iter(){
                write!(f,"key: {}, value: {:?}\n", key2, value2)?;
            }
        }
        write!(f, "{}", 0)
    }
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
        let columns: Box<[PhysicalPage]> =
            Vec::with_capacity(NUM_METADATA_COLUMNS + num_columns).into_boxed_slice();

        Page { columns }
    }

    pub fn get_column(&self, index: usize) -> &PhysicalPage {
        self.columns.as_ref()[index].borrow()
    }
}

#[derive(Debug)]
struct PageRange {
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
    num_columns: u64,
    key_index: usize,
    index: Index,
    page_directory: HashMap<usize, PageRange>,
}

impl Table {
    fn get_page_range(&self, range_number: usize) -> &PageRange {
        self.page_directory.get(&range_number).unwrap()
    }
}

#[pymethods]
impl Table {
    #[new]
    pub fn new(name: String, num_columns: u64, key_index: usize) -> Table {
        Table {
            name,
            num_columns,
            key_index,
            index: Index::new(),
            page_directory: HashMap::new(),
        }
    }

    pub fn print(&self) {
        println!("{}", self.name);
        println!("{}", self.num_columns);
        println!("{}", self.key_index);
        println!("{}", self.index);
        println!("{}", self.page_directory.is_empty());
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

    pub fn create_table(&mut self, name: String, num_columns: u64, key_index: usize) -> Py<Table> {
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
