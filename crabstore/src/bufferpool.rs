use std::{
    collections::HashMap,
    sync::{atomic, Arc, Mutex, RwLock},
};

use crate::{disk_manager::DiskManager, page::PhysicalPage};

struct BufferPoolReference {
    frame_id: usize,
}

/*
struct BufferPoolFrame {
    frame_id: Arc<atomic::AtomicUsize>,
    dirty: Arc<atomic::AtomicBool>,
    pins: Arc<atomic::AtomicUsize>,
    page: Arc<RwLock<PhysicalPage>,
}

impl BufferPoolFrame {
    fn new(id: usize) -> Self {
        BufferPoolFrame { frame_id: (), dirty: (), pins: (), page: () }
    }
}

struct BufferPool {
    disk: DiskManager,
    page_frame_map: RwLock<HashMap<usize, usize>>,
    frames: Arc<Vec<RwLock<BufferPoolFrame>>>,
}
impl BufferPool {}

*/
