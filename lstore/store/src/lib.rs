use std::collections::HashMap;
// use std::io::{Write, BufReader, BufRead, ErrorKind};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Api {
    table: Table,
}

#[pymethods]
impl Api {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(table: Table,) -> PyResult<Self> {
        Ok(Api {table,})
    }
    // pub fn delete(&self, primary_key){

    // }
    #[pyo3(signature = (*column))]
    pub fn insert(&mut self, column: &PyTuple){
        
    }

    // pub fn select(&self, search_key, search_key_index, projected_columns_index){
        
    // }

    // pub fn select_version(&self, search_key, search_key_index, projected_columns_index, relative_version){
        
    // }

    // pub fn update(&self, primary_key, *columns){
        
    // }

    // pub fn sum(&self, start_range, end_range, aggregate_column_index){
        
    // }

    // pub fn sum_version(&self, start_range, end_range, aggregate_column_index, relative_version){
        
    // }

    // pub fn increment(&self, key, column){
        
    // }
}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Index{
    indices: HashMap<i64,HashMap<i64, Vec<i64>>>,
}

#[pymethods]
impl Index{
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
    ) -> PyResult<Self>{

        Ok(Index {
            indices: HashMap::new(),
        })
    }

    pub fn create_index(&mut self, column_number: i64){

    }

    pub fn drop_index(&mut self, column_number: i64){

    }

}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Page{
    num_columns: u64,
    data: Vec<[i64;1000]>,
}

#[pymethods]
impl Page{
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
    ) -> PyResult<Self>{

        Ok(Page {
            num_columns: 0,
            data: Vec::new(),
        })
    }

}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Record{
    rid: i64,
    indirection:i64,
    schema_encoding: Vec<i64>,
    row: Vec<&i64>,
}

#[pymethods]
impl Record{
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
    ) -> PyResult<Self>{

        Ok(Record {
            rid: 0,
            indirection: 0,
            schema_encoding: Vec::new(),
            row: Vec::new(),
        })
    }

}

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
struct Table{
    name: String,
    num_columns: u64,
    key: i64,
    index: u64,
    page_directory: HashMap<i64,Record>,
}

#[pymethods]
impl Table {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        num_columns: u64,
        key: i64,
    ) -> Table {
        Table {
            name,
            num_columns,
            key,
            index: 0,
            page_directory: HashMap::new(),
        }
    }

    pub fn print(&self) {
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
    m.add_class::<Api>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}