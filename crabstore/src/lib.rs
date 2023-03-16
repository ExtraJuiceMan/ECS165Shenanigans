#![feature(new_uninit)]
#![feature(map_try_insert)]
#![feature(get_mut_unchecked)]
#![feature(return_position_impl_trait_in_trait)]
use dashmap::DashMap;
use pyo3::{prelude::*, types::PyList};
use rkyv::ser::{
    serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
    Serializer,
};
use std::{
    borrow::BorrowMut,
    collections::HashMap,
    io::{Seek, SeekFrom},
    str::FromStr,
    sync::{Arc, RwLock},
};
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
};
use tablepy::TablePy;

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
const BUFFERPOOL_SIZE: usize = 2048;

pub mod bufferpool;
pub mod disk_manager;
pub mod index;
pub mod lock_manager;
mod merge;
pub mod page;
mod page_directory;
pub mod query;
mod range_directory;
pub mod rid;
pub mod table;
pub mod tablepy;
use crate::table::Table;

#[derive(Clone, Debug)]
#[pyclass(subclass, get_all)]
pub struct RecordPy {
    rid: u64,
    columns: Py<PyList>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordRust {
    rid: u64,
    pub columns: Vec<u64>,
}

impl RecordRust {
    pub fn new(rid: u64, columns: Vec<u64>) -> Self {
        RecordRust {
            rid,
            columns,
        }
    }

    pub fn from(record: RecordPy) -> Self {
        let mut p = Vec::new();

        Python::with_gil(|py| {
            for c in record.columns.as_ref(py).iter() {
                p.push(c.extract::<u64>().unwrap());
            }
        });

        RecordRust {
            rid: record.rid,
            columns: p,
        }
    }
}

impl RecordPy {
    pub fn from(record: &RecordRust, py: Python) -> Py<Self> {
        let result_cols = PyList::empty(py);
        for c in record.columns.iter() {
            result_cols.append(c).unwrap();
        }
        Py::new(
            py,
            RecordPy::new(
                record.rid,
                result_cols.into(),
            ),
        )
        .unwrap()
    }
}

#[pymethods]
impl RecordPy {
    #[new]
    pub fn new(rid: u64, columns: Py<PyList>) -> Self {
        RecordPy {
            rid,
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
pub struct CrabStore {
    directory: PathBuf,
    tables: HashMap<String, Arc<Table>>,
}

impl CrabStore {
    pub fn load_table_index(file: &Path) -> Vec<String> {
        let crab_file = File::options().read(true).open(file);

        if crab_file.is_err() && crab_file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
            File::create(file).expect("Failed to find database file");

            return Vec::new();
        }

        let mut crab_file = crab_file.unwrap();

        crab_file.rewind().unwrap();

        let mut crab_bytes = Vec::new();
        crab_file.read_to_end(&mut crab_bytes).unwrap();

        unsafe {
            rkyv::from_bytes_unchecked::<Vec<String>>(&crab_bytes)
                .expect("Failed to deserialize database file")
        }
    }

    pub fn persist_table_index(file: &Path, table_names: Vec<String>) {
        let mut crab_file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(file)
            .expect("Failed to open database file");

        crab_file.rewind().unwrap();

        let mut bufwriter = BufWriter::new(crab_file);

        bufwriter.rewind().unwrap();

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(bufwriter),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        serializer
            .serialize_value(&table_names)
            .expect("Unable to serialize table names");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner().flush().unwrap();
    }

    pub fn database_filename(directory: &Path) -> PathBuf {
        directory.join(Path::new("crab_dt.CRAB"))
    }

    pub fn table_filename(directory: &Path, table: &str) -> PathBuf {
        let mut table_file = table.to_string();
        table_file.push_str("_db.CRAB");

        directory.join(Path::new(&table_file))
    }

    pub fn page_dir_filename(directory: &Path, table: &str) -> PathBuf {
        let mut pd_file = table.to_string();
        pd_file.push_str("_pd.CRAB");

        directory.join(Path::new(&pd_file))
    }

    pub fn index_filename(directory: &Path, table: &str) -> PathBuf {
        let mut id_file = table.to_string();
        id_file.push_str("_id.CRAB");

        directory.join(Path::new(&id_file))
    }

    pub fn range_filename(directory: &Path, table: &str) -> PathBuf {
        let mut rd_file = table.to_string();
        rd_file.push_str("_rd.CRAB");

        directory.join(Path::new(&rd_file))
    }
}

impl CrabStore {
    pub fn new(directory: PathBuf) -> Self {
        CrabStore {
            directory,
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, num_columns: usize, key_index: usize) -> Arc<Table> {
        let table = Arc::new(Table::new(
            name.to_string(),
            num_columns,
            key_index,
            &CrabStore::table_filename(&self.directory, name),
            &CrabStore::page_dir_filename(&self.directory, name),
            &CrabStore::index_filename(&self.directory, name),
            &CrabStore::range_filename(&self.directory, name),
        ));
        self.tables.insert(name.to_string(), Arc::clone(&table));
        table
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name);
        true
    }

    pub fn get_table(&self, name: &str) -> &Arc<Table> {
        self.tables.get(name).expect("Table not found")
    }

    pub fn open(&mut self) {
        fs::create_dir_all(&self.directory).expect("Failed to create database directories.");

        let table_names =
            CrabStore::load_table_index(&CrabStore::database_filename(&self.directory));

        for name in table_names.iter() {
            self.tables.insert(
                name.to_string(),
                Arc::new(Table::load(
                    name,
                    &CrabStore::table_filename(&self.directory, name),
                    &CrabStore::page_dir_filename(&self.directory, name),
                    &CrabStore::index_filename(&self.directory, name),
                    &CrabStore::range_filename(&self.directory, name),
                )),
            );
        }
    }

    pub fn close(&mut self) {
        let table_names = self.tables.keys().cloned().collect::<Vec<String>>();

        CrabStore::persist_table_index(&CrabStore::database_filename(&self.directory), table_names);

        for table in self.tables.values() {
            table.persist();
        }

        self.tables.clear();
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

#[pymethods]
impl CrabStorePy {
    #[new]
    pub fn new() -> Self {
        Self {
            directory: None,
            tables: HashMap::new(),
        }
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Py<TablePy> {
        Python::with_gil(|py| {
            let table = Py::new(
                py,
                TablePy::new(
                    name.clone(),
                    num_columns,
                    key_index,
                    &CrabStore::table_filename(self.directory.as_ref().unwrap(), &name),
                    &CrabStore::page_dir_filename(self.directory.as_ref().unwrap(), &name),
                    &CrabStore::index_filename(self.directory.as_ref().unwrap(), &name),
                    &CrabStore::range_filename(self.directory.as_ref().unwrap(), &name),
                ),
            )
            .unwrap();
            self.tables.insert(name.clone(), Py::clone_ref(&table, py));
            table
        })
    }

    pub fn drop_table(&mut self, name: String) {
        self.tables.remove(&name);
    }

    pub fn get_table(&self, name: String) -> Py<TablePy> {
        Py::clone(self.tables.get(&name).unwrap())
    }

    pub fn open(&mut self, path: String) {
        fs::create_dir_all(&path).unwrap();
        self.directory = Some(PathBuf::from_str(&path).unwrap());

        let table_names = CrabStore::load_table_index(&CrabStore::database_filename(
            self.directory.as_ref().unwrap(),
        ));

        for name in table_names.iter() {
            Python::with_gil(|py| {
                self.tables.insert(
                    name.to_string(),
                    Py::new(
                        py,
                        TablePy::load(
                            name,
                            &CrabStore::table_filename(self.directory.as_ref().unwrap(), name),
                            &CrabStore::page_dir_filename(self.directory.as_ref().unwrap(), name),
                            &CrabStore::index_filename(self.directory.as_ref().unwrap(), name),
                            &CrabStore::range_filename(self.directory.as_ref().unwrap(), name),
                        ),
                    )
                    .unwrap(),
                )
            });
        }
    }

    pub fn close(&mut self) {
        let table_names = self.tables.keys().cloned().collect::<Vec<String>>();

        CrabStore::persist_table_index(
            &CrabStore::database_filename(self.directory.as_ref().unwrap()),
            table_names,
        );

        for table in self.tables.values() {
            Python::with_gil(|py| {
                table.borrow(py).persist();
            })
        }

        self.tables.clear();
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn crabstore(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecordPy>()?;
    m.add_class::<TablePy>()?;
    m.add_class::<CrabStorePy>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::CrabStore;

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
        table1.insert_query(&vec![1, 2]);
        table2.insert_query(&vec![3, 4]);
        assert_eq!(
            table1.select_query(1, 0, &[1, 1]),
            table2.select_query(1, 0, &[1, 1])
        );
        assert_eq!(
            table1.select_query(2, 0, &[1, 1]),
            table2.select_query(2, 0, &[1, 1])
        );
        db.close();
    }
}
