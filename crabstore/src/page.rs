use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    bufferpool::{BufferPool, BufferPoolFrame},
    rid::RID,
    METADATA_PAGE_HEADER, PAGE_SLOTS,
};
use std::{
    fmt::Display,
    mem::size_of,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug)]

pub struct PhysicalPage {
    pub page: [u8; crate::PAGE_SIZE],
}

impl Display for PhysicalPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Page")?;
        for i in 0..PAGE_SLOTS {
            write!(f, "{} ", self.slot(i))?;
        }
        writeln!(f)
    }
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

#[derive(Debug)]

pub struct Page(Arc<[usize]>);

impl Display for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Logical Page")?;
        for x in self.0.iter() {
            writeln!(f, "Column Block ID: {x}")?;
        }
        writeln!(f)
    }
}

impl Page {
    #[inline(always)]
    pub fn new(column_pages: Arc<[usize]>) -> Self {
        Page(Arc::clone(&column_pages))
    }

    pub fn read_col(&self, index: usize) -> usize {
        self.0[index]
    }

    pub fn read_metadata(&self, bp: &mut BufferPool) -> u64 {
        bp.get_page(self.0[METADATA_PAGE_HEADER]).slot(0)
    }

    pub fn write_metadata(&self, bp: &mut BufferPool, val: u64) {
        bp.get_page(self.0[METADATA_PAGE_HEADER]).write_slot(0, val);
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
        bp.get_page(self.0[index])
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
