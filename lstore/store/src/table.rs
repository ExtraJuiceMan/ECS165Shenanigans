use std::collections::{HashMap};
use std::string::*;
use pyo3::prelude::*;
use crate::index::*;
use core::any::Any;
use bytes::{BytesMut, BufMut};

// pub struct page{
//     num_records: i64,
//     data: 
// }


// #[pyclass]
// #[derive(Default)]
// pub struct Record{
//     rid: i64,
//     key: String,
//     columns: u64,
// }

// #[pymethods]
// impl Record {
//     #[new]
//     fn new(rid : i64, key: String, columns: u64) -> Self {
//         Self {rid, key, columns}

//         let mut buf = BytesMut::with_capacity(4096);
//     }
    
// }

pub mod table {
    pub struct Table{
        pub name: String,
        pub key: i64,
        pub page_directory: HashMap<i64,Box<dyn Send>>,
        pub num_columns: u64,
        pub index: Index,
    }
    
    impl Table{
        pub fn create( name: String, key: i64, num_columns: u64) -> Table { 
            Table{
                name: String::from(name), 
                key, 
                page_directory: HashMap::new(), 
                num_columns, 
                index: Index::new(),
            } 
        }
    
    }

    pub mod init_table{
        pub fn create_table(){
            
        }
    }
}
