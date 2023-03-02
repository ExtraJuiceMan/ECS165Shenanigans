use std::{
    backtrace::Backtrace,
    borrow::Borrow,
    collections::{HashMap, VecDeque},
    hash::BuildHasherDefault,
    io::Read,
    sync::{
        atomic::{self, Ordering},
        Arc, Mutex, RwLock,
    },
};

use rustc_hash::{FxHashMap, FxHasher};

use crate::{disk_manager::DiskManager, page::PhysicalPage};

pub struct BufferPoolFrame {
    page_id: atomic::AtomicUsize,
    dirty: atomic::AtomicBool,
    page: RwLock<PhysicalPage>,
}

impl BufferPoolFrame {
    pub fn new() -> Self {
        BufferPoolFrame {
            page_id: (!0).into(),
            dirty: false.into(),
            page: RwLock::new(PhysicalPage::default()),
        }
    }

    pub fn flush(&self, disk: &DiskManager) {
        let page = self
            .page
            .write()
            .expect("Failed to acquire lock, lock poisoning?");

        disk.write_page(self.page_id.load(Ordering::SeqCst), &page.page);

        disk.flush();
        self.dirty.store(false, Ordering::SeqCst);
        self.page_id.store(!0, Ordering::SeqCst);
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }

    pub fn get_page_id(&self) -> usize {
        self.page_id.load(Ordering::SeqCst)
    }

    pub fn slot(&self, slot: usize) -> u64 {
        let page = self
            .page
            .read()
            .expect("Couldn't lock physical page, poisoned?");
        page.slot(slot)
    }

    pub fn write_slot(&self, slot: usize, value: u64) {
        self.mark_dirty();
        let mut page = self
            .page
            .write()
            .expect("Couldn't lock physical page, poisoned?");
        page.write_slot(slot, value);
    }
}

pub struct BufferPool {
    disk: Arc<DiskManager>,
    size: usize,
    page_frame_map: FxHashMap<usize, usize>,
    frames: Vec<Arc<BufferPoolFrame>>,
    clock_refs: Vec<bool>,
    clock_hand: usize,
}

impl BufferPool {
    pub fn new(disk: Arc<DiskManager>, size: usize) -> Self {
        let mut frames = Vec::with_capacity(size);
        let page_frame_map =
            FxHashMap::with_capacity_and_hasher(size, BuildHasherDefault::<FxHasher>::default());
        let mut clock_refs = Vec::with_capacity(size);

        for _ in 0..size {
            frames.push(Arc::new(BufferPoolFrame::new()));
            clock_refs.push(false);
        }

        BufferPool {
            disk,
            size,
            page_frame_map,
            frames,
            clock_refs,
            clock_hand: 0,
        }
    }

    fn find_evict_victim(&mut self) -> usize {
        let victim = loop {
            if self.clock_refs[self.clock_hand]
                || Arc::strong_count(&self.frames[self.clock_hand]) > 1
            {
                self.clock_refs[self.clock_hand] = false;
                self.clock_hand = (self.clock_hand + 1) % self.size;
                continue;
            }

            break self.clock_hand;
        };

        self.clock_hand = (self.clock_hand + 1) % self.size;

        victim
    }

    pub fn flush_all(&mut self) {
        for i in 0..self.size {
            if self.frames[i].dirty.load(Ordering::SeqCst) {
                self.frames[i].flush(self.disk.borrow());
            }
        }
        self.disk.flush();
    }

    fn evict(&mut self, victim: usize) {
        let frame = &self.frames[victim];

        self.page_frame_map
            .remove(&frame.page_id.load(Ordering::SeqCst));

        if frame.dirty.load(Ordering::SeqCst) {
            frame.flush(self.disk.borrow());
            println!("Flushing toilet for {}", frame.get_page_id());
        }

        frame.page_id.store(!0, Ordering::SeqCst);
    }

    pub fn new_page(&mut self) -> Arc<BufferPoolFrame> {
        let new_page_id = self.disk.reserve_page();

        let victim = self.find_evict_victim();

        self.evict(victim);

        let frame = Arc::clone(&self.frames[victim]);

        frame.page_id.store(new_page_id, Ordering::SeqCst);
        self.page_frame_map.insert(new_page_id, victim);

        frame
    }

    pub fn get_page(&mut self, page_id: usize) -> Arc<BufferPoolFrame> {
        if page_id == !0 {
            panic!("Tried to load invalid page");
        }
        if let Some(frame_id) = self.page_frame_map.get(&page_id) {
            self.clock_refs[*frame_id] = true;
            let frame = &self.frames[*frame_id];
            return Arc::clone(frame);
        }

        let victim = self.find_evict_victim();
        self.evict(victim);

        let frame = Arc::clone(&self.frames[victim]);

        frame.page_id.store(page_id, Ordering::SeqCst);

        let mut page = frame
            .page
            .write()
            .expect("Failed to acquire RwLock, poisoned?");

        self.disk.read_page(page_id, &mut page.page);

        self.clock_refs[victim] = true;

        drop(page);

        self.page_frame_map
            .try_insert(page_id, victim)
            .expect("Tried to re-map existing page in bufferpool");

        frame
    }
}
