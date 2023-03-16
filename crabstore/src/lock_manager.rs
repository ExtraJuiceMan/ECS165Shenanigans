use std::collections::HashMap;

use parking_lot::{lock_api::RawRwLock, Mutex, RwLock};

use crate::rid::RID;

pub struct LockManager {
    locks: Mutex<HashMap<RID, RwLock<()>>>,
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
