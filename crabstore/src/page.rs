use crate::{
    rid::{self, BaseRID, TailRID, RID},
    METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS, PAGE_SLOTS,
};
use std::{
    any::TypeId,
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::btree_map::Values,
    fmt::Display,
};
#[derive(Debug)]
pub struct PhysicalPage {
    page: [i64; crate::PAGE_SLOTS],
}

impl Default for PhysicalPage {
    fn default() -> Self {
        PhysicalPage {
            page: [0; crate::PAGE_SLOTS],
        }
    }
}

impl PhysicalPage {
    pub fn slot(&self, index: usize) -> i64 {
        self.page[index]
    }

    pub fn write_slot(&mut self, index: usize, value: i64) {
        self.page[index] = value;
    }
}

#[derive(Debug)]
pub struct Page {
    columns: Box<[PhysicalPage]>,
}

impl Page {
    pub fn new(num_columns: usize) -> Self {
        let mut columns: Vec<PhysicalPage> = Vec::with_capacity(NUM_METADATA_COLUMNS + num_columns);
        columns.resize_with(NUM_METADATA_COLUMNS + num_columns, Default::default);
        let columns = columns.into_boxed_slice();

        Page { columns }
    }

    pub fn get_column(&self, index: usize) -> &PhysicalPage {
        self.columns.as_ref()[index].borrow()
    }
    pub fn get_column_mut(&mut self, index: usize) -> &mut PhysicalPage {
        &mut self.columns[index]
    }
    pub fn get_slot(&self, column: usize, rid: &impl RID) -> i64 {
        self.columns.as_ref()[column].borrow().slot(rid.slot())
    }
    pub fn write_slot(&mut self, column: usize, rid: &impl RID, value: i64) {
        (&mut self.columns[column]).page[rid.slot()] = value;
    }
}

#[derive(Debug)]
pub struct PageRange {
    num_columns: usize,
    tail_id: usize,
    base_pages: Vec<Page>,
    tail_pages: Vec<Page>,
}

impl PageRange {
    pub fn new(num_columns: usize) -> Self {
        let mut tail_pages: Vec<Page> = Vec::with_capacity(crate::RANGE_PAGE_COUNT);
        let mut base_pages: Vec<Page> = Vec::with_capacity(crate::RANGE_PAGE_COUNT);

        for _ in 0..crate::RANGE_PAGE_COUNT {
            base_pages.push(Page::new(num_columns));
            tail_pages.push(Page::new(num_columns));
        }

        PageRange {
            num_columns,
            tail_id: 0,
            base_pages,
            tail_pages,
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
        TailRID::from(
            self.base_pages[rid.page()]
                .get_column(METADATA_INDIRECTION)
                .slot(rid.slot()),
        )
    }
    pub fn merge_values(&self, base_rid: &BaseRID, columns: &Vec<Option<i64>>) -> Vec<i64> {
        match self.is_latest(base_rid) {
            true => self.merge_values_given_page(
                base_rid,
                self.get_base_page(base_rid).unwrap(),
                columns,
            ),
            false => {
                let new_rid = self.find_latest(base_rid);
                let page = self.get_tail_page(&new_rid);
                self.merge_values_given_page(&new_rid, page.unwrap(), columns)
            }
        }
    }
    pub fn merge_values_given_page(
        &self,
        rid: &impl RID,
        page: &Page,
        columns: &[Option<i64>],
    ) -> Vec<i64> {
        columns
            .iter()
            .zip(
                (NUM_METADATA_COLUMNS..self.num_columns + NUM_METADATA_COLUMNS)
                    .map(|column| page.get_column(column).slot(rid.slot())),
            )
            .map(|(x, y)| match x {
                None => y,
                Some(x) => *x,
            })
            .collect()
    }
    pub fn append_update_record(
        &mut self,
        base_rid: &BaseRID,
        columns: &Vec<Option<i64>>,
    ) -> TailRID {
        if self.tail_id / PAGE_SLOTS == self.tail_pages.len() {
            self.allocate_new_page_set()
        }
        let values = self.merge_values(base_rid, columns);
        let page = &mut self.tail_pages[self.tail_id / PAGE_SLOTS];
        let base_page = &mut self.base_pages[base_rid.page()];
        let indirection_column_rid = match base_page.get_slot(METADATA_SCHEMA_ENCODING, base_rid) {
            0 => base_rid.raw(),
            _ => base_page.get_slot(METADATA_INDIRECTION, base_rid),
        };
        let slot = self.tail_id % PAGE_SLOTS;

        page.get_column_mut(METADATA_INDIRECTION)
            .write_slot(slot, indirection_column_rid);

        let newrid = TailRID::new_tail(base_rid.page_range(), self.tail_id);

        page.get_column_mut(crate::METADATA_RID)
            .write_slot(slot, newrid.raw());
        //print!("Update vals: {:?}\n", columns);
        for (i, val) in values.iter().enumerate() {
            page.get_column_mut(crate::NUM_METADATA_COLUMNS + i)
                .write_slot(slot, *val);

            //print!("Base Page: {:?}\n",&base_page.get_column(crate::NUM_METADATA_COLUMNS + i).page[0..50],);
            //print!("Tail Page: {:?}\n",&page.get_column(crate::NUM_METADATA_COLUMNS + i).page[0..50]);
        }

        self.tail_id += 1;
        newrid
    }

    pub fn page_exists(&self, rid: &TailRID) -> bool {
        return rid.id() / PAGE_SLOTS < self.tail_pages.len();
    }

    pub fn allocate_new_page_set(&mut self) {
        for _ in 0..crate::RANGE_PAGE_COUNT {
            self.tail_pages.push(Page::new(self.num_columns));
        }
    }
    pub fn get_base_page(&self, rid: &BaseRID) -> Option<&Page> {
        Some(&self.base_pages[rid.page()])
    }
    pub fn get_tail_page(&self, rid: &TailRID) -> Option<&Page> {
        if !self.page_exists(rid) {
            None
        } else {
            Some(&self.tail_pages[rid.page()])
        }
    }
    pub fn get_base_page_mut(&mut self, rid: &BaseRID) -> Option<&mut Page> {
        Some(&mut self.base_pages[rid.page()])
    }
    pub fn get_tail_page_mut(&mut self, rid: &TailRID) -> Option<&mut Page> {
        if !self.page_exists(rid) {
            None
        } else {
            Some(&mut self.tail_pages[rid.page()])
        }
    }
}
