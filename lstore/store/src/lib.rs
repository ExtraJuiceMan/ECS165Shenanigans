use std::collections::HashMap;
// use std::io::{Write, BufReader, BufRead, ErrorKind};
use pyo3::prelude::*;

// #[pyclass(subclass)]
// struct Index{
//     indices: Vec<i64>,
// }

// #[pymethods]
// impl Index{
//     #[new]
//     #[allow(clippy::too_many_arguments)]
//     pub fn new(
//     ) -> PyResult<Self>{

//         Ok(Index {
//             indices: Vec::new(),
//         })
//     }

// }

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Table{
    name: String,
    num_columns: u64,
    key: i64,
    index: u64,
    page_directory: HashMap<i64,i64>,
}

#[pymethods]
impl Table{
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        num_columns: u64,
        key: i64,
    ) -> Table{
        Table {
            name,
            num_columns,
            key,
            index: 0,
            page_directory: HashMap::new(),
        }
    }

    pub fn print(&self){
        println!("{}",self.name);
        println!("{}",self.num_columns);
        println!("{}",self.key);
        println!("{}",self.index);
        println!("{}",self.page_directory.is_empty());
    }
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Rstore{
    tables: HashMap<String, Table>,
}

#[pymethods]
impl Rstore{
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new() -> PyResult<Self>{

        Ok(Rstore {
            tables: HashMap::new(),
        })
    }

    pub fn create_table(&mut self, name: String, num_columns: u64, index: i64){
        let table: Table = Table::new(name.clone(), num_columns, index);
        self.tables.insert(name, table);
    }

    pub fn drop_table(&mut self, name: String){
        self.tables.remove(&name);
    }

    pub fn get_table(&self, name: String) -> PyResult<Table>{
        let table: &Table= self.tables.get(&name).unwrap();
        Ok(table.clone())
    }
}
/// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }

/// A Python module implemented in Rust.
#[pymodule]
fn store(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rstore>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}