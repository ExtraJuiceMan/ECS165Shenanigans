use crabstore::record::Record;
use pyo3::{prelude::*, types::PyList};

#[derive(Clone, Debug)]
#[pyclass(subclass, get_all)]
pub struct RecordPy {
    pub rid: u64,
    pub columns: Py<PyList>,
}

impl RecordPy {
    pub fn from(record: &Record, py: Python) -> Py<Self> {
        let result_cols = PyList::empty(py);
        for c in record.columns.iter() {
            result_cols.append(c).unwrap();
        }
        Py::new(py, RecordPy::new(record.rid, result_cols.into())).unwrap()
    }
}

#[pymethods]
impl RecordPy {
    #[new]
    pub fn new(rid: u64, columns: Py<PyList>) -> Self {
        RecordPy { rid, columns }
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
