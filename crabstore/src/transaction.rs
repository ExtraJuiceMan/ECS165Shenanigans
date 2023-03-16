use std::{borrow::Borrow, cell::RefCell, sync::Arc};

use crate::{
    lock_manager::{LockHandle, LockManager, LockType},
    rid::RID,
    table::Table,
};

#[derive(Clone, Debug)]
struct RecordMutation {
    modified_entry: RID,
    original_value: u64,
    modified_column: usize,
}

enum QueryStatus {
    Idle,
    Executing,
    AbortedRetryable,
    AbortedNotRetryable,
}

#[derive(Clone)]
enum Query {
    Select(u64, usize, Box<[usize]>),
    Sum(u64, u64, usize),
    Insert(Box<[u64]>),
    Update(u64, Box<[Option<u64>]>),
    Delete(u64),
}

#[derive(Clone)]
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
    locks_acquired: Vec<LockHandle>,
    current_writes: usize,
    current_locks: usize,
    current_status: QueryStatus,
}

impl Transaction {
    fn new() -> Self {
        Transaction {
            query_log: Vec::new(),
            queries: Vec::new(),
            write_log: Vec::new(),
            locks_acquired: Vec::new(),
            current_writes: 0,
            current_locks: 0,
            current_status: QueryStatus::Idle,
        }
    }

    fn add_query(&mut self, query: Query, table: &Arc<Table>) {
        self.queries.push((query, table.clone()));
    }

    fn run(&mut self) {
        self.current_status = QueryStatus::Executing;

        for query in self.queries.clone().iter() {
            self.current_locks = 0;
            self.current_writes = 0;

            match &query.0 {
                Query::Select(search_val, col_idx, selected) => {
                    query.1.select_query(*search_val, *col_idx, selected);

                    self.query_log.push(ExecutedQuery::Select {
                        num_locks: self.current_locks,
                    });
                }
                Query::Sum(start, end, val) => {
                    query.1.sum_query(*start, *end, *val);

                    self.query_log.push(ExecutedQuery::Delete {
                        num_locks: self.current_locks,
                        num_muts: self.current_writes,
                    });
                }
                Query::Insert(vals) => {
                    query.1.insert_query(vals);

                    self.query_log.push(ExecutedQuery::Insert {
                        num_locks: self.current_locks,
                        num_muts: self.current_writes,
                    });
                }
                Query::Update(key, vals) => {
                    query.1.update_query(*key, vals);

                    self.query_log.push(ExecutedQuery::Update {
                        num_locks: self.current_locks,
                        num_muts: self.current_writes,
                    });
                }
                Query::Delete(key) => {
                    query.1.delete_query(*key, Some(self));

                    self.query_log.push(ExecutedQuery::Delete {
                        num_locks: self.current_locks,
                        num_muts: self.current_writes,
                    });
                }
            }
        }
    }

    fn set_aborted(&mut self, retry: bool) {
        if retry {
            self.current_status = QueryStatus::AbortedRetryable;
        } else {
            self.current_status = QueryStatus::AbortedNotRetryable;
        }
    }

    fn commit(&mut self) {}

    fn rollback(&mut self) {}

    pub fn log_write(&mut self, modified_column: usize, modified_entry: RID, original_value: u64) {
        self.current_writes += 1;
        self.write_log.push(RecordMutation {
            modified_entry,
            modified_column,
            original_value,
        });
    }

    pub fn try_lock(&mut self, locks: &LockManager, rid: RID, lock_type: LockType) -> bool {
        if let Some(handle) = locks.try_lock(rid, lock_type) {
            self.current_locks += 1;
            self.locks_acquired.push(handle);
            true
        } else {
            false
        }
    }

    pub fn try_lock_with_abort(
        &mut self,
        locks: &LockManager,
        rid: RID,
        lock_type: LockType,
    ) -> bool {
        if !self.try_lock(locks, rid, lock_type) {
            self.set_aborted(true);
            return false;
        }
        true
    }
}
