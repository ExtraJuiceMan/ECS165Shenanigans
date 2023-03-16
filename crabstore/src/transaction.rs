use std::sync::Arc;

use crate::{rid::RID, table::Table};

#[derive(Clone, Debug)]
struct RecordMutation {
    modified_entry: RID,
    original_value: u64,
    modified_column: usize,
}

enum Query {
    Select(Box<[u64]>),
    Sum(u64, u64, usize),
    Insert(Box<[u64]>),
    Update(Box<[Option<u64>]>),
    Delete(u64),
}

enum ExecutedQuery {
    Select { num_locks: usize },
    Sum { num_locks: usize },
    Insert { num_locks: usize, num_muts: usize },
    Update { num_locks: usize, num_muts: usize },
    Delete { num_locks: usize, num_muts: usize },
}

struct TransactionLogEntry {}

pub struct Transaction {
    query_log: Vec<ExecutedQuery>,
    queries: Vec<(Query, Arc<Table>)>,
}

/*
impl Transaction {
    fn run()
}
*/
