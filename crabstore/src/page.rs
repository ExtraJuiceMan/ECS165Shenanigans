use crate::{rid::RID, PAGE_SLOTS};
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
    num_columns: usize,
    tail_id: usize,
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
            num_columns,
            tail_id: 0,
            base_pages,
            tail_pages,
        }
    }

    pub fn append_update_record(&mut self, base_rid: &RID, columns: &Vec<Option<i64>>) -> RID {
        if self.tail_id / PAGE_SLOTS < self.tail_pages.len() {
            self.allocate_new_page()
        }

        let page = &mut self.tail_pages[self.tail_id / PAGE_SLOTS];
        let base_page = &mut self.base_pages[base_rid.page()];

        let indirection_column_rid;

        if base_page
            .get_column(crate::METADATA_SCHEMA_ENCODING)
            .slot(base_rid.slot())
            == 0
        {
            indirection_column_rid = base_page
                .get_column(crate::METADATA_RID)
                .slot(base_rid.slot());
        } else {
            indirection_column_rid = base_page
                .get_column(crate::METADATA_INDIRECTION)
                .slot(base_rid.slot());
        }

        let slot = self.tail_id % PAGE_SLOTS;

        page.get_column_mut(crate::METADATA_INDIRECTION)
            .write_slot(slot, indirection_column_rid);

        let newrid = RID::new_tail(base_rid.page_range(), self.tail_id);

        page.get_column_mut(crate::METADATA_RID)
            .write_slot(slot, newrid.raw());

        for (i, val) in columns.iter().enumerate() {
            match val {
                None => {
                    let oldval = base_page
                        .get_column(crate::NUM_METADATA_COLUMNS + i)
                        .slot(base_rid.slot());
                    page.get_column_mut(crate::NUM_METADATA_COLUMNS + i)
                        .write_slot(slot, oldval)
                }
                Some(v) => {
                    page.get_column_mut(crate::NUM_METADATA_COLUMNS + i)
                        .write_slot(slot, *v);
                }
            }
        }

        self.tail_id += 1;

        newrid
    }

    pub fn page_exists(&self, rid: &RID) -> bool {
        return rid.tail_page_id() / PAGE_SLOTS < self.tail_pages.len();
    }

    pub fn allocate_new_page(&mut self) {
        self.tail_pages.push(Page::new(self.num_columns))
    }

    pub fn get_page(&self, rid: &RID) -> Option<&Page> {
        match rid.is_base_page() {
            true => Some(&self.base_pages[rid.page()]),
            false => {
                if !self.page_exists(rid) {
                    None
                } else {
                    Some(&self.tail_pages[rid.tail_page_id() / PAGE_SLOTS])
                }
            }
        }
    }
    pub fn get_page_mut(&mut self, rid: &RID) -> Option<&mut Page> {
        match rid.is_base_page() {
            true => Some(&mut self.base_pages[rid.page()]),
            false => {
                if !self.page_exists(rid) {
                    None
                } else {
                    Some(&mut self.tail_pages[rid.tail_page_id() / PAGE_SLOTS])
                }
            }
        }
    }
}
