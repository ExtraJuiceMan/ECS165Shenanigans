use crate::query::{Query, QueryEnum};
use std::collections::HashMap;
#[derive(Debug)]
pub struct LockManager {
    page_demanders: HashMap<usize, QueryEnum>,
}
impl LockManager {
    pub fn new() -> Self {
        Self {
            page_demanders: HashMap::new(),
        }
    }
    pub fn lock(&mut self, page_id: usize, query: QueryEnum) {
        self.page_demanders.insert(page_id, query);
    }
    pub fn unlock(&mut self, page_id: usize) {
        self.page_demanders.remove(&page_id);
    }
    pub fn get_demanders(&self, page_id: usize) -> Vec<QueryEnum> {
        let mut demanders = Vec::new();
        for (key, value) in &self.page_demanders {
            if *key == page_id {
                demanders.push(value.clone());
            }
        }
        demanders
    }
}
