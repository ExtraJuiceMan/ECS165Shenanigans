use std::{borrow::Borrow, cell::RefCell};

#[derive(Debug)]
pub struct PhysicalPage {
    page: RefCell<[i64; crate::PAGE_SLOTS]>,
}

impl Default for PhysicalPage {
    fn default() -> Self {
        PhysicalPage {
            page: RefCell::new([0; crate::PAGE_SLOTS]),
        }
    }
}

impl PhysicalPage {
    pub fn slot(&self, index: usize) -> i64 {
        self.page.borrow()[index]
    }

    pub fn write_slot(&self, index: usize, value: i64) {
        self.page.borrow_mut()[index] = value;
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
}

#[derive(Debug)]
pub struct PageRange {
    base_pages: Vec<Page>,
    tail_pages: Vec<Page>,
}

impl PageRange {
    pub fn new(num_columns: usize) -> Self {
        let tail_pages: Vec<Page> = vec![Page::new(num_columns)];
        let mut base_pages: Vec<Page> = Vec::new();

        for _ in 0..crate::RANGE_PAGE_COUNT {
            base_pages.push(Page::new(num_columns));
        }

        PageRange {
            base_pages,
            tail_pages,
        }
    }

    pub fn get_page(&self, page_num: usize) -> &Page {
        &self.base_pages[page_num]
    }
}