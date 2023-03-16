use std::{path::PathBuf, str::FromStr, sync::Arc};

use crabcore::crabstore::CrabStore;
use parking_lot::Mutex;
use pyo3::prelude::*;

use super::tablepy::TablePy;

#[derive(Clone)]
#[pyclass]
pub struct CrabStorePy(Arc<Mutex<CrabStore>>);

#[pymethods]
impl CrabStorePy {
    #[new]
    pub fn new() -> Self {
        CrabStorePy(Arc::new(Mutex::new(CrabStore::new(PathBuf::default()))))
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Py<TablePy> {
        let table = self.0.lock().create_table(&name, num_columns, key_index);
        Python::with_gil(|py| Py::new(py, TablePy(table))).unwrap()
    }

    pub fn drop_table(&mut self, name: String) {
        self.0.lock().drop_table(&name);
    }

    pub fn get_table(&self, name: String) -> Py<TablePy> {
        let table = self.0.lock().get_table(&name);
        Python::with_gil(|py| Py::new(py, TablePy(table))).unwrap()
    }

    pub fn open(&mut self, path: String) {
        let mut crabstore = self.0.lock();
        crabstore.directory = PathBuf::from_str(&path).unwrap();
        crabstore.open();
    }

    pub fn close(&mut self) {
        self.0.lock().close();
    }
}
