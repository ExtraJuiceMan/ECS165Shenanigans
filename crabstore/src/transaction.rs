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

pub struct Transaction {
    query_log: Vec<ExecutedQuery>,
    queries: Vec<(Query, Arc<Table>)>,
    write_log: Vec<RecordMutation>,
    current_writes: usize,
    current_locks: usize,
}

impl Transaction {
    fn new() -> Self {
        Transaction {
            query_log: Vec::new(),
            queries: Vec::new(),
            write_log: Vec::new(),
            current_writes: 0,
            current_locks: 0,
        }
    }

    fn run(&mut self) -> bool {
        true
    }

    fn log_write(&mut self, modified_column: usize, modified_entry: RID, original_value: u64) {
        self.current_writes += 1;
        self.write_log.push(RecordMutation {
            modified_entry,
            modified_column,
            original_value,
        });
    }

    fn log_lock(&mut self) {}
}
