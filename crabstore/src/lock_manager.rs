use std::collections::HashMap;

use parking_lot::{lock_api::RawRwLock, Mutex, RwLock};

use crate::rid::RID;

pub enum LockType {
    Shared,
    Exclusive,
}

pub struct LockHandle {
    rid: RID,
    lock_type: LockType,
}

impl LockHandle {
    fn new(rid: RID, lock_type: LockType) -> Self {
        LockHandle { rid, lock_type }
    }
}

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

    pub fn try_lock(&self, rid: RID, lock_type: LockType) -> Option<LockHandle> {
        let mut guard = self.locks.lock();
        let lock = guard.entry(rid).or_insert(RwLock::new(()));

        unsafe {
            match lock_type {
                LockType::Shared => {
                    if !lock.raw().try_lock_shared() {
                        None
                    } else {
                        Some(LockHandle::new(rid, lock_type))
                    }
                }
                LockType::Exclusive => {
                    if !lock.raw().try_lock_exclusive() {
                        None
                    } else {
                        Some(LockHandle::new(rid, lock_type))
                    }
                }
            }
        }
    }

    pub fn unlock(&self, lock_handle: LockHandle) {
        let guard = self.locks.lock();
        let lock = guard
            .get(&lock_handle.rid)
            .expect("Invalid unlock requested from Lock Manager");

        unsafe {
            match lock_handle.lock_type {
                LockType::Shared => lock.raw().unlock_shared(),
                LockType::Exclusive => lock.raw().unlock_exclusive(),
            }
        }
    }
}
