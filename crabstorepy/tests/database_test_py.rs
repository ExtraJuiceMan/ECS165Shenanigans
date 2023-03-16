#![feature(test)]
extern crate test;
use ::crabstore as crabstorewrap;
use crabstore::crabstore;
use crabstorewrap::{crabstorepy::CrabStorePy, recordpy::RecordPy, tablepy::TablePy};

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use pyo3::prelude::*;
fn build_environment() {
    let module_base = "lstore.";
    pyo3::append_to_inittab!(crabstore);
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let modules = [
            ("lstore/db.py", include_str!("../../lstore/db.py")),
            ("lstore/index.py", include_str!("../../lstore/index.py")),
            ("lstore/query.py", include_str!("../../lstore/query.py")),
        ];

        for module in modules {
            let mut module_name = module_base.to_string();
            module_name.push_str(
                &module
                    .0
                    .chars()
                    .skip(7)
                    .take_while(|x| *x != '.')
                    .collect::<String>(),
            );

            PyModule::from_code(py, module.1, module.0, &module_name).unwrap();
        }
    });
}

#[test]
fn database_test_py() {
    build_environment();
    Python::with_gil(|py| {
        PyModule::from_code(py, include_str!("../../m2_1.py"), "", "").unwrap();
    });
}
