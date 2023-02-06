use std::collections::{HashMap};
use std::string::*;
use pyo3::prelude::*;
use crate::index::*;
use core::any::Any;
#[pyclass]
#[derive(Default)]
pub struct Record{
    rid: i64,
    key: String,
    columns: u64,
}
#[pymethods]
impl Record {
    #[new]
    fn new(rid : i64, key: String, columns: u64) -> Self {
        Self {rid, key, columns}
    }
    
}



#[pyclass]
pub struct Table{
    name: String,
    key: i64,
    page_directory: HashMap<i64,Box<dyn Send>>,
    num_columns: u64,
    index: Index,
}
#[pymethods]
impl Table{
    #[new]
    pub(crate) fn new( name: String, key: i64, num_columns: u64) -> Self { 
        Self { name: name, key: key, page_directory: HashMap::new(), num_columns: num_columns, index: Index::new() } 
    }

}