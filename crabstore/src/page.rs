use parking_lot::lock_api::RwLock;
use rkyv::{Archive, Deserialize, Serialize};

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
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
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
    #[inline(always)]
    pub fn new(column_pages: Arc<[usize]>) -> Self {
        Page { column_pages }
    }

    pub fn print_cols(&self) {
        for x in self.column_pages.iter() {
            println!("Base/Tail Page Column = Page {x}");
        }
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
    next_tid: AtomicU64,
    current_tail_page: AtomicUsize,
}

impl PageRange {
    pub fn new(next_tid: u64, current_tail_page: usize) -> Self {
        PageRange {
            next_tid: next_tid.into(),
            current_tail_page: current_tail_page.into(),
        }
    }
    pub fn is_full(&self) -> bool {
        RID::from(self.next_tid.load(Ordering::SeqCst)).page()
            < self.current_tail_page.load(Ordering::SeqCst)
    }

    pub fn next_tid(&self) -> RID {
        self.next_tid.fetch_sub(1, Ordering::SeqCst).into()
    }
}
