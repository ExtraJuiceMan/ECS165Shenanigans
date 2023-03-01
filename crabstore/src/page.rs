use parking_lot::lock_api::RwLock;
use rclite::Arc;

use crate::{
    bufferpool::{BufferPool, BufferPoolFrame},
    rid::{self, RID},
    METADATA_INDIRECTION, METADATA_RID, METADATA_SCHEMA_ENCODING, NUM_METADATA_COLUMNS, PAGE_SLOTS,
};
use std::{
    any::TypeId,
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::btree_map::Values,
    fmt::Display,
    mem::size_of,
    rc::Rc,
};

pub struct PhysicalPage {
    pub page: [u8; crate::PAGE_SIZE],
}

impl Default for PhysicalPage {
    fn default() -> Self {
        PhysicalPage {
            page: [0; crate::PAGE_SIZE],
        }
    }
}

impl PhysicalPage {
    pub fn slot(&self, index: usize) -> u64 {
        u64::from_ne_bytes(
            self.page[size_of::<u64>() * index..size_of::<u64>() * (index + 1)]
                .try_into()
                .unwrap(),
        )
    }

    pub fn write_slot(&mut self, index: usize, value: u64) {
        self.page[size_of::<u64>() * index..size_of::<u64>() * (index + 1)]
            .copy_from_slice(u64::to_ne_bytes(value).as_slice())
    }
}

pub struct Page {
    column_pages: Arc<[usize]>,
}

impl Page {
    pub fn new(column_pages: Arc<[usize]>) -> Self {
        Page { column_pages }
    }

    pub fn get_column(&self, bp: &mut BufferPool, index: usize) -> Arc<BufferPoolFrame> {
        bp.get_page(self.column_pages[index])
    }

    pub fn slot(&self, bp: &mut BufferPool, column: usize, rid: RID) -> u64 {
        self.get_column(bp, self.column_pages[column])
            .slot(rid.slot())
    }

    pub fn write_slot(&mut self, bp: &mut BufferPool, column: usize, rid: RID, value: u64) {
        self.get_column(bp, self.column_pages[column])
            .write_slot(rid.slot(), value);
    }
}

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
        rid: RID,
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
