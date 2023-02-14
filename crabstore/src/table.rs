use crate::index::Index;
use crate::page::{Page, PageRange};
use crate::rid::{BaseRID, TailRID, RID};
use crate::{
    Record, METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS,
};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use rayon::prelude::*;

#[derive(Debug, Default)]
#[pyclass(subclass)]
pub struct Table {
    name: String,
    num_columns: usize,
    primary_key_index: usize,
    index: Index,
    next_rid: BaseRID,
    ranges: Vec<PageRange>,
}

impl Table {
    fn get_page_range(&self, range_number: usize) -> &PageRange {
        &self.ranges[range_number]
    }

    fn get_page_range_mut(&mut self, range_number: usize) -> &mut PageRange {
        &mut self.ranges[range_number]
    }
    fn get_base_page(&self, rid: &BaseRID) -> Option<&Page> {
        self.ranges[rid.page_range()].get_base_page(rid)
    }
    fn get_base_page_mut(&mut self, rid: &BaseRID) -> Option<&mut Page> {
        self.ranges[rid.page_range()].get_base_page_mut(rid)
    }
    fn get_tail_page(&self, rid: &TailRID) -> Option<&Page> {
        self.ranges[rid.page_range()].get_tail_page(rid)
    }
    fn get_tail_page_mut(&mut self, rid: &TailRID) -> Option<&mut Page> {
        self.ranges[rid.page_range()].get_tail_page_mut(rid)
    }
    fn find_row(&self, column_index: usize, value: i64) -> Option<BaseRID> {
        match self.index.get_from_index(column_index, value) {
            Some(vals) => Some(vals[0]),
            None => {
                let rid: BaseRID = 0.into();

                while rid.raw() < self.next_rid.raw() {
                    let page = self.get_page_range(rid.page_range()).get_base_page(&rid);

                    if page
                        .unwrap()
                        .get_column(NUM_METADATA_COLUMNS + column_index)
                        .slot(rid.slot())
                        == value
                    {
                        return Some(rid);
                    }
                }
                None
            }
        }
    }
    fn find_rows(&self, column_index: usize, value: i64) -> Vec<BaseRID> {
        match self.index.get_from_index(column_index, value) {
            Some(vals) => vals,
            None => {
                let rid: BaseRID = 0.into();
                let mut rids = Vec::new();
                while rid.raw() < self.next_rid.raw() {
                    let page = self.get_page_range(rid.page_range()).get_base_page(&rid);

                    if page
                        .unwrap()
                        .get_column(NUM_METADATA_COLUMNS + column_index)
                        .slot(rid.slot())
                        == value
                    {
                        rids.push(rid);
                    }
                }
                rids
            }
        }
    }
    fn find_rows_range(&self, column_index: usize, begin: i64, end: i64) -> Vec<BaseRID> {
        match self.index.range_from_index(column_index, begin, end) {
            Some(vals) => vals,
            None => {
                let mut rids: Vec<BaseRID> = Vec::new();
                let mut rid: BaseRID = 0.into();
                while rid.raw() < self.next_rid.raw() {
                    let page = self.get_page_range(rid.page_range()).get_base_page(&rid);

                    let key = page
                        .unwrap()
                        .get_column(NUM_METADATA_COLUMNS + self.primary_key_index)
                        .slot(rid.slot());

                    if key >= begin && key < end {
                        rids.push(rid);
                    }

                    rid = rid.next();
                }
                rids
            }
        }
    }
    pub fn is_latest(&self, rid: &BaseRID) -> bool {
        self.get_base_page(rid)
            .unwrap()
            .get_column(METADATA_SCHEMA_ENCODING)
            .slot(rid.slot())
            == 0
    }
    pub fn find_latest(&self, rid: &BaseRID) -> TailRID {
        let page = self.get_base_page(rid);

        TailRID::from(
            page.unwrap()
                .get_column(METADATA_INDIRECTION)
                .slot(rid.slot()),
        )
    }
}

#[pymethods]
impl Table {
    #[getter]
    fn num_columns(&self) -> usize {
        self.num_columns
    }

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
        self.find_rows_range(self.primary_key_index, start_range, end_range)
            .iter()
            .map(|rid| {
                self.get_page_range(rid.page_range())
                    .get_base_page(rid)
                    .unwrap()
                    .get_column(NUM_METADATA_COLUMNS + column_index)
                    .slot(rid.slot())
            })
            .sum()
    }

    pub fn select(&self, search_value: i64, column_index: usize, columns: &PyList) -> Py<PyList> {
        let included_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_i, x)| x.extract::<i64>().unwrap() != 0)
            .map(|(i, _x)| i)
            .collect();

        let vals: Vec<BaseRID> = self.find_rows(column_index, search_value);

        Python::with_gil(|py| {
            let selected_records: Py<PyList> = PyList::empty(py).into();
            let result_cols = PyList::empty(py);
            for rid in vals {
                if self.is_latest(&rid) {
                    for i in included_columns.iter() {
                        result_cols.append(
                            self.get_base_page(&rid)
                                .unwrap()
                                .get_column(NUM_METADATA_COLUMNS + i)
                                .slot(rid.slot()),
                        );
                    }
                } else {
                }
                let rid: TailRID = self.find_latest(&rid);

                let page = self.get_page_range(rid.page_range()).get_tail_page(&rid);

                let slot = rid.slot();
                for i in included_columns.iter() {
                    result_cols.append(
                        page.unwrap()
                            .get_column(NUM_METADATA_COLUMNS + i)
                            .slot(rid.slot()),
                    );
                }
                let record = PyCell::new(
                    py,
                    Record::new(
                        page.unwrap().get_column(METADATA_RID).slot(slot) as u64,
                        page.unwrap().get_column(METADATA_INDIRECTION).slot(slot) as u64,
                        page.unwrap()
                            .get_column(METADATA_SCHEMA_ENCODING)
                            .slot(slot) as u64,
                        result_cols.into(),
                    ),
                )
                .unwrap();

                selected_records.as_ref(py).append(record);
            }
            selected_records
        })
    }

    pub fn update(&mut self, search_value: i64, values: &PyTuple) -> bool {
        let vals: Vec<Option<i64>> = values
            .iter()
            .map(|val| val.extract::<Option<i64>>().unwrap())
            .collect::<Vec<Option<i64>>>();

        let row = self.find_row(self.primary_key_index, search_value);

        if row.is_none() {
            return false;
        }

        let row = row.unwrap();
        print!("Update called");
        let tail_rid = self
            .get_page_range_mut(row.page_range())
            .append_update_record(&row, &vals);

        self.get_base_page_mut(&row)
            .unwrap()
            .get_column_mut(METADATA_INDIRECTION)
            .write_slot(row.slot(), tail_rid.raw());

        let mut schema_encoding: i64 = 0;

        for (i, v) in vals.iter().enumerate() {
            if !v.is_none() {
                schema_encoding |= 1 << i;
            }
        }

        self.get_base_page_mut(&row)
            .unwrap()
            .get_column_mut(METADATA_SCHEMA_ENCODING)
            .write_slot(row.slot(), schema_encoding);

        true
    }

    #[args(values = "*")]
    pub fn insert(&mut self, values: &PyTuple) {
        let rid: BaseRID = self.next_rid;
        let page_range = rid.page_range();
        let slot = rid.slot();

        if self.ranges.len() <= page_range {
            self.ranges.push(PageRange::new(self.num_columns));
        }

        let page = self.get_base_page_mut(&rid).unwrap();

        page.get_column_mut(METADATA_RID)
            .write_slot(slot, rid.raw());

        page.get_column_mut(METADATA_SCHEMA_ENCODING)
            .write_slot(slot, 0);

        for (i, val) in values.iter().enumerate() {
            page.get_column_mut(NUM_METADATA_COLUMNS + i)
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
