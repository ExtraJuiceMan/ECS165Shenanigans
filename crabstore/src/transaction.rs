use crate::query::{Query, QueryEnum};
#[derive(Debug)]
pub struct Transaction {
    queries: Vec<QueryEnum>,
}
