use std::{path::Path, sync::Arc};

use crabcore::table::Table;
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use super::recordpy::RecordPy;

#[pyclass]
pub struct TablePy(pub Arc<Table>);

impl TablePy {
    pub fn new(
        name: String,
        num_columns: usize,
        key_index: usize,
        db_file: &Path,
        pd_file: &Path,
        id_file: &Path,
        rd_file: &Path,
    ) -> Self {
        Self(Arc::new(Table::new(
            name,
            num_columns,
            key_index,
            db_file,
            pd_file,
            id_file,
            rd_file,
        )))
    }

    pub fn load(
        name: &str,
        db_file: &Path,
        pd_file: &Path,
        id_file: &Path,
        rd_file: &Path,
    ) -> Self {
        Self(Arc::new(Table::load(
            name, db_file, pd_file, id_file, rd_file,
        )))
    }
}

#[pymethods]
impl TablePy {
    #[getter]
    fn num_columns(&self) -> usize {
        self.0.columns()
    }

    pub fn sum(
        &self,
        py: Python<'_>,
        start_range: u64,
        end_range: u64,
        column_index: usize,
    ) -> u64 {
        py.allow_threads(move || self.0.sum_query(start_range, end_range, column_index))
    }

    pub fn select(
        &self,
        py: Python<'_>,
        search_value: u64,
        column_index: usize,
        columns: &PyList,
    ) -> Py<PyList> {
        if column_index >= self.0.columns() {
            return Python::with_gil(|py| -> Py<PyList> { PyList::empty(py).into() });
        }

        let included_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_i, x)| x.extract::<u64>().unwrap() != 0)
            .map(|(i, _x)| i)
            .collect();

        let mut results = vec![];
        py.allow_threads(|| {
            results = self
                .0
                .select_query(search_value, column_index, &included_columns);
        });

        Python::with_gil(|py| -> Py<PyList> {
            let selected_records: Py<PyList> = PyList::empty(py).into();
            for result in results {
                selected_records
                    .as_ref(py)
                    .append(RecordPy::from(&result, py))
                    .expect("Failed to append to python list");
            }
            selected_records
        })
    }

    pub fn update(&self, py: Python<'_>, key: u64, values: &PyTuple) -> bool {
        let vals: Vec<Option<u64>> = values
            .iter()
            .map(|val| val.extract::<Option<u64>>().unwrap())
            .collect::<Vec<Option<u64>>>();

        py.allow_threads(move || self.0.update_query(key, &vals, None))
    }

    pub fn delete(&self, py: Python<'_>, key: u64) -> bool {
        py.allow_threads(move || self.0.delete_query(key, None))
    }

    #[pyo3(signature = (*values))]
    pub fn insert(&self, py: Python<'_>, values: &PyTuple) {
        let vals = values
            .iter()
            .map(|v| v.extract::<u64>().unwrap())
            .collect::<Vec<u64>>();

        py.allow_threads(move || self.0.insert_query(&vals));
    }

    pub fn build_index(&self, column_num: usize) {
        self.0.build_index(column_num);
    }

    pub fn drop_index(&self, column_num: usize) {
        self.0.drop_index(column_num);
    }

    pub fn persist(&self) {
        self.0.persist();
    }
}
