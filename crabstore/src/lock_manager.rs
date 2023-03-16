use crate::{
    page::Page,
    query::{Query, QueryEnum},
    table::TableData,
};
use core::time;
use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, channel, Receiver, Sender},
        Arc, RwLock,
    },
    thread,
};
#[derive(Debug)]
pub struct LockManager {
    locks: Mutex<HashMap<RID, RwLock<()>>>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_lock_shared(&mut self, rid: RID) -> bool {
        unsafe {
            self.locks
                .lock()
                .entry(rid)
                .or_insert(RwLock::new(()))
                .raw()
                .try_lock_shared()
        }
    }

    pub fn try_lock_exclusive(&mut self, rid: RID) -> bool {
        unsafe {
            self.locks
                .lock()
                .entry(rid)
                .or_insert(RwLock::new(()))
                .raw()
                .try_lock_exclusive()
        }
    }

    pub fn unlock(&mut self, rid: RID) {}
}
