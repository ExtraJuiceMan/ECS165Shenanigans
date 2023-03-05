#![feature(new_uninit)]
#![feature(map_try_insert)]
#![feature(get_mut_unchecked)]

use dashmap::DashMap;
use pyo3::{prelude::*, types::PyList};
use rkyv::ser::{
    serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
    Serializer,
};
use std::{
    borrow::BorrowMut,
    collections::HashMap,
    sync::{Arc, RwLock},
};
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
};
use table::TablePy;
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
const BUFFERPOOL_SIZE: usize = 64;

pub mod bufferpool;
pub mod disk_manager;
pub mod index;
pub mod page;
mod page_directory;
mod range_directory;
pub mod rid;
pub mod table;

use crate::table::Table;
#[derive(Clone, Debug)]
#[pyclass(subclass, get_all)]
pub struct Record {
    rid: u64,
    indirection: u64,
    schema_encoding: u64,
    columns: Py<PyList>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordRust {
    rid: u64,
    indirection: u64,
    schema_encoding: u64,
    columns: Vec<u64>,
}
impl RecordRust {
    pub fn new(rid: u64, indirection: u64, schema_encoding: u64, columns: Vec<u64>) -> Self {
        RecordRust {
            rid,
            indirection,
            schema_encoding,
            columns,
        }
    }
    pub fn from(record: Record) -> Self {
        let mut p = Vec::new();
        Python::with_gil(|py| {
            for c in record.columns.as_ref(py).iter() {
                p.push(c.extract::<u64>().unwrap());
            }
        });
        RecordRust {
            rid: record.rid,
            indirection: record.indirection,
            schema_encoding: record.schema_encoding,
            columns: p,
        }
    }
}
impl Record {
    pub fn from(record: &RecordRust, py: Python) -> Self {
        let result_cols = PyList::empty(py);
        for c in record.columns.iter() {
            result_cols.append(c).unwrap();
        }
        Record::new(
            record.rid,
            record.indirection,
            record.schema_encoding,
            result_cols.into(),
        )
    }
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
struct CrabStore {
    directory: Option<PathBuf>,
    tables: HashMap<String, Arc<RwLock<Table>>>,
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
impl CrabStore {
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
    ) -> Arc<RwLock<Table>> {
        let mut table = Arc::new(RwLock::new(Table::new(
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
        )));
        self.tables.insert(name.clone(), table.clone());
        table
    }

    pub fn drop_table(&mut self, name: String) -> bool {
        self.tables.remove(&name);
        true
    }

    pub fn get_table(&self, name: String) -> &Arc<RwLock<Table>> {
        self.tables.get(&name).unwrap()
    }

    pub fn open(&mut self, path: String) {
        fs::create_dir_all(&path).unwrap();
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
        crab_file.read_to_end(&mut crab_bytes).unwrap();

        let table_names = unsafe {
            rkyv::from_bytes_unchecked::<Vec<String>>(&crab_bytes)
                .expect("Failed to deserialize database file")
        };

        for name in table_names.iter() {
            self.tables.insert(
                name.to_string(),
                Arc::new(RwLock::new(Table::load(
                    name,
                    &self.table_filename(name.as_ref()),
                    &self.page_dir_filename(name.as_ref()),
                    &self.index_filename(name.as_ref()),
                    &self.range_filename(name.as_ref()),
                ))),
            );
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

        buf.into_inner().flush().unwrap();

        for table in self.tables.values() {
            table.write().unwrap().persist();
        }
    }
    fn delete(path: String) {
        fs::remove_dir_all(path).unwrap();
    }
}
#[derive(Clone, Debug)]
#[pyclass]
struct CrabStorePy {
    directory: Option<PathBuf>,
    tables: HashMap<String, Py<TablePy>>,
}
impl CrabStorePy {
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
impl CrabStorePy {
    #[new]
    pub fn new() -> Self {
        Python::with_gil(|py| Self {
            directory: None,
            tables: HashMap::new(),
        })
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Py<TablePy> {
        Python::with_gil(|py| {
            let mut table = Py::new(
                py,
                TablePy::new(
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
        self.drop_table(name);
        Ok(())
    }

    pub fn get_table(&self, name: String) -> Py<TablePy> {
        Py::clone(self.tables.get(&name).unwrap())
    }

    pub fn open(&mut self, path: String) {
        fs::create_dir_all(&path).unwrap();
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
        crab_file.read_to_end(&mut crab_bytes).unwrap();

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
                        TablePy::load(
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

        buf.into_inner().flush().unwrap();

        for table in self.tables.values() {
            Python::with_gil(|py| {
                table.borrow_mut(py).persist();
            })
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn crabstore(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Record>()?;
    m.add_class::<TablePy>()?;
    m.add_class::<CrabStorePy>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use crate::CrabStore;

    #[test]
    fn open_close_db() {
        let mut db = CrabStore::new();
        db.open("test_db".to_string());
        db.close();
        CrabStore::delete("test_db".to_string());
    }
    #[test]
    fn create_table() {
        let mut db = CrabStore::new();
        db.open("test_db".to_string());
        db.create_table("test_table".to_string(), 2, 0);
        db.close();
        CrabStore::delete("test_db".to_string());
    }
    #[test]
    fn get_table() {
        let mut db = CrabStore::new();
        db.open("test_db".to_string());
        let table1 = db.create_table("test_table".to_string(), 2, 0);
        let table = db.get_table("test_table".to_string());
        db.close();
        CrabStore::delete("test_db".to_string());
    }
    #[test]
    fn check_aliasing() {
        let mut db = CrabStore::new();
        db.open("test_db".to_string());
        let table1 = db.create_table("test_table".to_string(), 2, 0);
        let table2 = db.get_table("test_table".to_string());
        table1.write().unwrap().insert_query(vec![1, 2]);
        table2.write().unwrap().insert_query(vec![3, 4]);
        assert_eq!(
            table1.read().unwrap().select_query(1, 0, &vec![1, 1]),
            table2.read().unwrap().select_query(1, 0, &vec![1, 1])
        );
        assert_eq!(
            table1.read().unwrap().select_query(2, 0, &vec![1, 1]),
            table2.read().unwrap().select_query(2, 0, &vec![1, 1])
        );
        db.close();
        CrabStore::delete("test_db".to_string());
    }
}
