// use pyo3::prelude::*;
// use crate::table::Table;


// #[pyclass]
// pub struct Database {
//     tables: Vec<Table>
// }
// #[pymethods]
// impl Database {
//     #[new]
//     pub fn createDatabase() -> Database{
//         Database {
//             tables: Vec::new(),
//         }
        
//     }
// }
// pub fn insertTable(&self, name: String, num_columns: u64, key_index: i64) -> Void{
//     let mut table = Arc::new(RwLock::new(Table::new(name, key_index, num_columns)));
//     self.tables.push(table);
//     table
// }