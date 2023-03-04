use parking_lot::{lock_api::RwLock, Mutex};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    bufferpool::{BufferPool, BufferPoolFrame},
    rid::{self, RID},
    METADATA_INDIRECTION, METADATA_PAGE_HEADER, METADATA_RID, METADATA_SCHEMA_ENCODING,
    NUM_METADATA_COLUMNS, PAGE_SLOTS,
};
use std::{
    any::TypeId,
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::btree_map::Values,
    fmt::Display,
    mem::size_of,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug)]
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
    #[inline(always)]
    pub fn new(column_pages: Arc<[usize]>) -> Self {
        Page { column_pages }
    }

    pub fn print_cols(&self) {
        for x in self.column_pages.iter() {
            println!("Base/Tail Page Column = Page {x}");
        }
    }

    pub fn read_col(&self, index: usize) -> usize {
        self.column_pages[index]
    }

    pub fn read_metadata(&self, bp: &mut BufferPool) -> u64 {
        bp.get_page(self.column_pages[METADATA_PAGE_HEADER]).slot(0)
    }

    pub fn write_metadata(&self, bp: &mut BufferPool, val: u64) {
        bp.get_page(self.column_pages[METADATA_PAGE_HEADER])
            .write_slot(0, val);
    }

    pub fn write_page_tps(&self, bp: &mut BufferPool, val: u64) {
        self.write_metadata(bp, val);
    }

    pub fn write_last_tail(&self, bp: &mut BufferPool, val: u64) {
        self.write_metadata(bp, val);
    }

    pub fn read_page_tps(&self, bp: &mut BufferPool) -> u64 {
        self.read_metadata(bp)
    }

    pub fn read_last_tail(&self, bp: &mut BufferPool) -> u64 {
        self.read_metadata(bp)
    }

    #[inline(always)]
    pub fn get_column(&self, bp: &mut BufferPool, index: usize) -> Arc<BufferPoolFrame> {
        bp.get_page(self.column_pages[index])
    }

    #[inline(always)]
    pub fn slot(&self, bp: &mut BufferPool, column: usize, rid: RID) -> u64 {
        self.get_column(bp, column).slot(rid.slot())
    }

    #[inline(always)]
    pub fn write_slot(&mut self, bp: &mut BufferPool, column: usize, rid: RID, value: u64) {
        self.get_column(bp, column).write_slot(rid.slot(), value);
    }
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct PageRange {
    pub next_tid: AtomicU64,
    pub current_tail_page: AtomicUsize,
    pub merged_until: AtomicUsize,
}

impl PageRange {
    pub fn new(next_tid: u64, current_tail_page: usize) -> Self {
        PageRange {
            next_tid: next_tid.into(),
            current_tail_page: current_tail_page.into(),
            merged_until: 0.into(),
        }
    }

    pub fn tail_is_full(&self) -> bool {
        RID::from(self.next_tid.load(Ordering::Relaxed)).page()
            != self.current_tail_page.load(Ordering::Relaxed)
    }

    pub fn next_tid(&self) -> RID {
        self.next_tid.fetch_sub(1, Ordering::Relaxed).into()
    }
}
