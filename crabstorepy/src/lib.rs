use crabstorepy::CrabStorePy;
use pyo3::prelude::*;
use recordpy::RecordPy;
use tablepy::TablePy;

pub mod crabstorepy;
pub mod recordpy;
pub mod tablepy;

#[pymodule]
fn crabstore(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecordPy>()?;
    m.add_class::<TablePy>()?;
    m.add_class::<CrabStorePy>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
