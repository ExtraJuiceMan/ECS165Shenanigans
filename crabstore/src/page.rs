use crate::rid::RID;
use std::{borrow::Borrow, cell::RefCell};
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
        let mut columns: Vec<PhysicalPage> =
            Vec::with_capacity(crate::NUM_METADATA_COLUMNS + num_columns);
        columns.resize_with(crate::NUM_METADATA_COLUMNS + num_columns, Default::default);
        let columns = columns.into_boxed_slice();

        Page { columns }
    }

    pub fn get_column(&self, index: usize) -> &PhysicalPage {
        self.columns.as_ref()[index].borrow()
    }
    pub fn get_column_mut(&mut self, index: usize) -> &mut PhysicalPage {
        &mut self.columns[index]
    }
}

#[derive(Debug)]
pub struct PageRange {
    base_pages: Vec<Page>,
    tail_pages: Vec<Page>,
}

impl PageRange {
    pub fn new(num_columns: usize) -> Self {
        let tail_pages: Vec<Page> = vec![Page::new(num_columns)];
        let mut base_pages: Vec<Page> = Vec::with_capacity(crate::RANGE_PAGE_COUNT);

        for _ in 0..crate::RANGE_PAGE_COUNT {
            base_pages.push(Page::new(num_columns));
        }

        PageRange {
            base_pages,
            tail_pages,
        }
    }

    pub fn get_page(&self, rid: &RID) -> &Page {
        match rid.is_base_page() {
            true => &self.base_pages[rid.page()],
            false => &self.tail_pages[rid.page()],
        }
    }
    pub fn get_page_mut(&mut self, rid: &RID) -> &mut Page {
        match rid.is_base_page() {
            true => &mut self.base_pages[rid.page()],
            false => &mut self.tail_pages[rid.page()],
        }
    }
}
