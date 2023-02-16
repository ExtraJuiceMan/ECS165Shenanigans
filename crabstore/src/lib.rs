use core::fmt;
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use std::{borrow::Borrow, cell::RefCell, mem::size_of};
use std::{collections::BTreeMap, collections::HashMap};
const PAGE_SIZE: usize = 4096;
const PAGE_SLOTS: usize = PAGE_SIZE / size_of::<i64>();
const PAGE_RANGE_SIZE: usize = PAGE_SIZE * 16;
const RANGE_PAGE_COUNT: usize = PAGE_RANGE_SIZE / PAGE_SIZE;
const NUM_METADATA_COLUMNS: usize = 4;

const METADATA_INDIRECTION: usize = 0;
const METADATA_RID: usize = 1;
const METADATA_TIMESTAMP: usize = 2;
const METADATA_SCHEMA_ENCODING: usize = 3;
// 0xFF...FF
const RID_INVALID: i64 = !0;

pub mod index;
pub mod page;
pub mod rid;
pub mod table;
use crate::index::Index;
use crate::page::{Page, PageRange, PhysicalPage};
use crate::rid::RID;
use crate::table::Table;
#[derive(Clone, Debug)]
#[pyclass(subclass, get_all)]
struct Record {
    rid: u64,
    indirection: u64,
    schema_encoding: u64,
    columns: Py<PyList>,
}

#[pymethods]
impl Record {
    #[new]
    pub fn new(rid: u64, indirection: u64, schema_encoding: u64, columns: Py<PyList>) -> Self {
        Record {
            rid: 0,
            indirection: 0,
            schema_encoding: 0,
            columns,
        }
    }

    pub fn __str__(&self) -> String {
        let mut p = "[".to_owned();

        Python::with_gil(|py| {
            for c in self.columns.as_ref(py).iter() {
                p.push_str(&c.extract::<i64>().unwrap().to_string());
                p.push(',');
            }
        });

        p.push(']');

        p
    }
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct CrabStore {
    tables: HashMap<String, Py<Table>>,
}

#[pymethods]
impl CrabStore {
    #[new]
    pub fn new() -> Self {
        CrabStore {
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
fn crabstore(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Record>()?;
    m.add_class::<Table>()?;
    m.add_class::<CrabStore>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
