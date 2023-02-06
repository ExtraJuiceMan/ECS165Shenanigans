use pyo3::prelude::*;
use std::any::Any;
#[pyclass]
#[derive(Clone)]
pub struct Index{
    indices: Vec<i64>
}
impl Index {
    pub fn new() -> Index {
        Index{indices: Vec::new()} 
    }
    fn locate(column: &dyn Any, value: &dyn Any ) -> bool{
        unimplemented!()
    }
    fn locate_range(self, begin: &dyn Any, end: &dyn Any)  -> bool{
        unimplemented!()
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }

}