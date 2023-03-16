use crate::query::{Query, QueryEnum};
use std::collections::HashMap;
struct LockManager {
    page_demanders: HashMap<usize, QueryEnum>,
}
