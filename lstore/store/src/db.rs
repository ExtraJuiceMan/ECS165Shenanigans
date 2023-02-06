use pyo3::prelude::*;
use crate::table::Table;
#[pyclass]
struct Database {
    tables: Vec<Table>
}
#[pymethods]
impl Database {
    #[new]
    fn new() -> Self{
        Database { tables: Vec::new() }
    }
    fn createTable(&self, name: String, num_columns: u64, key_index: i64) -> &Table{
        let mut table = Arc::new(RwLock::new(Table::new(name, key_index, num_columns)));
        self.tables.push(mut table);
        table
    }
}