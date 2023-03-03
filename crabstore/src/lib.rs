#![feature(new_uninit)]
#![feature(map_try_insert)]
#![feature(get_mut_unchecked)]

use core::fmt;
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use rkyv::ser::{
    serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
    Serializer,
};
use std::{
    borrow::Borrow,
    cell::RefCell,
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
};
use std::{collections::BTreeMap, collections::HashMap};
const PAGE_SIZE: usize = 4096;
const PAGE_SLOTS: usize = PAGE_SIZE / size_of::<i64>();
const PAGE_RANGE_COUNT: usize = 16;
const PAGE_RANGE_SIZE: usize = PAGE_SIZE * PAGE_RANGE_COUNT;
const RANGE_PAGE_COUNT: usize = PAGE_RANGE_SIZE / PAGE_SIZE;

const NUM_METADATA_COLUMNS: usize = 6;

const METADATA_INDIRECTION: usize = 0;
const METADATA_RID: usize = 1;
const METADATA_BASE_RID: usize = 2;

const NUM_STATIC_COLUMNS: usize = 3;

const METADATA_PAGE_HEADER: usize = 3;
const METADATA_TIMESTAMP: usize = 4;
const METADATA_SCHEMA_ENCODING: usize = 5;
// 0xFF...FF
const RID_INVALID: u64 = !0;

const BUFFERPOOL_SIZE: usize = 16;

pub mod bufferpool;
pub mod disk_manager;
pub mod index;
pub mod page;
mod page_directory;
mod range_directory;
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
            rid,
            indirection,
            schema_encoding,
            columns,
        }
    }

    pub fn __str__(&self) -> String {
        let mut p = "[".to_owned();

        Python::with_gil(|py| {
            for c in self.columns.as_ref(py).iter() {
                p.push_str(&c.extract::<u64>().unwrap().to_string());
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
    directory: Option<PathBuf>,
    tables: HashMap<String, Py<Table>>,
}

impl CrabStore {
    pub fn database_filename(&self) -> PathBuf {
        self.directory
            .as_ref()
            .unwrap()
            .join(Path::new("crab_dt.CRAB"))
    }

    pub fn table_filename(&self, table: &str) -> PathBuf {
        let mut table_file = table.to_string();
        table_file.push_str("_db.CRAB");

        self.directory
            .as_ref()
            .unwrap()
            .join(Path::new(&table_file))
    }

    pub fn page_dir_filename(&self, table: &str) -> PathBuf {
        let mut pd_file = table.to_string();
        pd_file.push_str("_pd.CRAB");

        self.directory.as_ref().unwrap().join(Path::new(&pd_file))
    }

    pub fn index_filename(&self, table: &str) -> PathBuf {
        let mut id_file = table.to_string();
        id_file.push_str("_id.CRAB");

        self.directory.as_ref().unwrap().join(Path::new(&id_file))
    }

    pub fn range_filename(&self, table: &str) -> PathBuf {
        let mut rd_file = table.to_string();
        rd_file.push_str("_rd.CRAB");

        self.directory.as_ref().unwrap().join(Path::new(&rd_file))
    }
}

#[pymethods]
impl CrabStore {
    #[new]
    pub fn new() -> Self {
        CrabStore {
            directory: None,
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
            let table: Py<Table> = Py::new(
                py,
                Table::new(
                    name.clone(),
                    num_columns,
                    key_index,
                    self.table_filename(name.as_str())
                        .to_str()
                        .unwrap()
                        .to_string(),
                    self.page_dir_filename(name.as_str())
                        .to_str()
                        .unwrap()
                        .to_string(),
                    self.index_filename(name.as_str())
                        .to_str()
                        .unwrap()
                        .to_string(),
                    self.range_filename(name.as_str())
                        .to_str()
                        .unwrap()
                        .to_string(),
                ),
            )
            .unwrap();
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

    pub fn open(&mut self, path: String) {
        fs::create_dir_all(&path);
        self.directory = Some(Path::new(&path).into());
        let crab_file = File::options().read(true).open(self.database_filename());

        if crab_file.is_err() && crab_file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
            File::options()
                .write(true)
                .create(true)
                .open(self.database_filename())
                .expect("Failed to open database file");

            return;
        }

        let mut crab_file = crab_file.unwrap();

        let mut crab_bytes = Vec::new();
        crab_file.read_to_end(&mut crab_bytes);

        let table_names = unsafe {
            rkyv::from_bytes_unchecked::<Vec<String>>(&crab_bytes)
                .expect("Failed to deserialize database file")
        };

        for name in table_names.iter() {
            Python::with_gil(|py| {
                self.tables.insert(
                    name.to_string(),
                    Py::new(
                        py,
                        Table::load(
                            name,
                            &self.table_filename(name.as_ref()),
                            &self.page_dir_filename(name.as_ref()),
                            &self.index_filename(name.as_ref()),
                            &self.range_filename(name.as_ref()),
                        ),
                    )
                    .unwrap(),
                )
            });
        }
    }

    pub fn close(&mut self) {
        let crab_file = File::options()
            .write(true)
            .truncate(true)
            .open(self.database_filename())
            .expect("Failed to open database file");

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(crab_file)),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        let table_names = self.tables.keys().cloned().collect::<Vec<String>>();

        serializer
            .serialize_value(&table_names)
            .expect("Unable to serialize table names");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner().flush();

        for table in self.tables.values() {
            Python::with_gil(|py| table.borrow_mut(py).persist())
        }
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
