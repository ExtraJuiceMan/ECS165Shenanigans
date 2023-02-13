use crate::index::Index;
use crate::page::PageRange;
use crate::rid::RID;
use crate::{
    Record, METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS,
};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

#[derive(Debug, Default)]
#[pyclass(subclass)]
pub struct Table {
    name: String,
    num_columns: usize,
    primary_key_index: usize,
    index: Index,
    next_rid: RID,
    ranges: Vec<PageRange>,
}

impl Table {
    fn get_page_range(&self, range_number: usize) -> &PageRange {
        &self.ranges[range_number]
    }
    fn find_rows(&self, column_index: usize, value: i64) -> Vec<RID> {
        match self.index.get_from_index(column_index, value) {
            Some(vals) => vals,
            None => {
                let rid: RID = 0.into();
                let mut rids = Vec::new();
                while rid.raw() < self.next_rid.raw() {
                    let page = self.get_page_range(rid.page_range()).get_page(rid.page());

                    if page
                        .get_column(NUM_METADATA_COLUMNS + column_index)
                        .slot(rid.slot())
                        == value
                    {
                        rids.push(rid.clone());
                    }
                }
                rids
            }
        }
    }
    fn find_rows_range(&self, column_index: usize, begin: i64, end: i64) -> Vec<RID> {
        match self.index.range_from_index(column_index, begin, end) {
            Some(vals) => vals,
            None => {
                let mut rids: Vec<RID> = Vec::new();
                let mut rid: RID = 0.into();
                while rid.raw() < self.next_rid.raw() {
                    let page = self.get_page_range(rid.page_range()).get_page(rid.page());

                    let key = page
                        .get_column(NUM_METADATA_COLUMNS + self.primary_key_index)
                        .slot(rid.slot());

                    if key >= begin && key < end {
                        rids.push(rid.clone());
                    }

                    rid = rid.next();
                }
                rids
            }
        }
    }
}

#[pymethods]
impl Table {
    #[new]
    pub fn new(name: String, num_columns: usize, key_index: usize) -> Table {
        Table {
            name,
            num_columns,
            primary_key_index: key_index,
            index: Index::new(key_index, num_columns),
            next_rid: 0.into(),
            ranges: Vec::new(),
        }
    }

    pub fn sum(&self, start_range: i64, end_range: i64, column_index: usize) -> i64 {
        let mut rid: RID = 0.into();
        let mut sum: i64 = 0;

        while rid.raw() < self.next_rid.raw() {
            let page = self.get_page_range(rid.page_range()).get_page(rid.page());

            let key = page
                .get_column(NUM_METADATA_COLUMNS + self.primary_key_index)
                .slot(rid.slot());

            if key >= start_range && key < end_range {
                sum += page
                    .get_column(NUM_METADATA_COLUMNS + column_index)
                    .slot(rid.slot());
            }

            rid = rid.next();
        }

        sum
    }

    pub fn select(&self, search_value: i64, column_index: usize, columns: &PyList) -> Py<PyList> {
        let selected_records = Python::with_gil(|py| -> Py<PyList> { PyList::empty(py).into() });

        let included_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_i, x)| x.extract::<i64>().unwrap() != 0)
            .map(|(i, _x)| i)
            .collect();

        let vals: Vec<RID> = self.find_rows(column_index, search_value);

        Python::with_gil(|py| {
            let result_cols = PyList::empty(py);
            for rid in vals {
                let page = self.get_page_range(rid.page_range()).get_page(rid.page());
                for i in included_columns.iter() {
                    result_cols.append(page.get_column(NUM_METADATA_COLUMNS + i).slot(rid.slot()));
                }
                let record = PyCell::new(
                    py,
                    Record::new(
                        page.get_column(METADATA_RID).slot(rid.slot()) as u64,
                        page.get_column(METADATA_INDIRECTION).slot(rid.slot()) as u64,
                        page.get_column(METADATA_SCHEMA_ENCODING).slot(rid.slot()) as u64,
                        result_cols.into(),
                    ),
                )
                .unwrap();

                selected_records.as_ref(py).append(record);
            }
        });

        selected_records
    }

    #[args(values = "*")]
    pub fn insert(&mut self, values: &PyTuple) {
        let rid: RID = self.next_rid;
        let page_range = rid.page_range();
        let page = rid.page();
        let slot = rid.slot();

        if self.ranges.len() <= page_range {
            self.ranges.push(PageRange::new(self.num_columns));
        }

        let page_range = self.get_page_range(page_range);
        let page = page_range.get_page(page);

        page.get_column(METADATA_RID)
            .write_slot(slot, rid.raw() as i64);

        for (i, val) in values.iter().enumerate() {
            page.get_column(NUM_METADATA_COLUMNS + i)
                .write_slot(slot, val.extract().unwrap())
        }

        self.index.update_index(
            self.primary_key_index,
            values
                .get_item(self.primary_key_index)
                .unwrap()
                .extract()
                .unwrap(),
            rid,
        );

        self.next_rid = rid.next();
    }

    pub fn print(&self) {
        println!("{}", self.name);
        println!("{}", self.num_columns);
        println!("{}", self.primary_key_index);
        println!("{}", self.index);
    }
}
