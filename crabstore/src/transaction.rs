use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    sync::Arc,
};

use crate::{
    lock_manager::{LockHandle, LockManager, LockType},
    rid::RID,
    table::Table,
};

#[derive(Clone, Debug)]
struct RecordMutation {
    pub modified_entry: RID,
    pub original_value: u64,
    pub modified_column: usize,
}

#[derive(Debug)]

pub enum IndexMutation {
    Add {
        rid: RID,
        value: u64,
        column: usize,
    },
    Remove {
        rid: RID,
        old_value: u64,
        column: usize,
    },
}

enum Mutation {
    Index(IndexMutation),
    Record(RecordMutation),
}

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum QueryStatus {
    Idle,
    Executing,
    AbortedRetryable,
    AbortedNotRetryable,
}

#[derive(Clone)]
pub enum Query {
    Select(u64, usize, Box<[usize]>),
    Sum(u64, u64, usize),
    Insert(Box<[u64]>),
    Update(u64, Box<[Option<u64>]>),
    Delete(u64),
}

#[derive(Clone)]
struct ExecutedQuery {
    pub num_locks: usize,
    pub num_muts: usize,
}

impl ExecutedQuery {
    fn new(num_locks: usize, num_muts: usize) -> Self {
        ExecutedQuery {
            num_locks,
            num_muts,
        }
    }
}

pub struct Transaction {
    query_log: Vec<ExecutedQuery>,
    queries: Vec<(Query, Arc<Table>)>,
    write_log: Vec<Mutation>,
    locks_acquired: Vec<LockHandle>,
    current_writes: usize,
    current_locks: usize,
    current_status: QueryStatus,
}

impl Transaction {
    pub fn new() -> Self {
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

    pub fn add_query(&mut self, query: Query, table: &Arc<Table>) {
        self.queries.push((query, table.clone()));
    }

    pub fn run(&mut self) -> bool {
        self.write_log.reserve(self.queries.len());
        self.current_status = QueryStatus::Executing;

        for query in self.queries.clone().iter() {
            self.current_locks = 0;
            self.current_writes = 0;

            match &query.0 {
                Query::Select(search_val, col_idx, selected) => {
                    query
                        .1
                        .select_query(*search_val, *col_idx, selected, Some(self));

                    self.query_log
                        .push(ExecutedQuery::new(self.current_locks, self.current_writes));
                }
                Query::Sum(start, end, val) => {
                    query.1.sum_query(*start, *end, *val, Some(self));

                    self.query_log
                        .push(ExecutedQuery::new(self.current_locks, self.current_writes));
                }
                Query::Insert(vals) => {
                    query.1.insert_query(vals, Some(self));

                    self.query_log
                        .push(ExecutedQuery::new(self.current_locks, self.current_writes));
                }
                Query::Update(key, vals) => {
                    query.1.update_query(*key, vals, Some(self));

                    self.query_log
                        .push(ExecutedQuery::new(self.current_locks, self.current_writes));
                }
                Query::Delete(key) => {
                    query.1.delete_query(*key, Some(self));

                    self.query_log
                        .push(ExecutedQuery::new(self.current_locks, self.current_writes));
                }
            }

            println!(
                "Current writes: {} Current locks: {}, Thread Id: {:?}",
                self.current_writes,
                self.current_locks,
                std::thread::current().id()
            );

            match self.current_status {
                QueryStatus::AbortedNotRetryable | QueryStatus::AbortedRetryable => {
                    self.rollback();
                    return false;
                }
                _ => {}
            }
        }

        self.commit();
        true
    }

    fn commit(&mut self) {
        self.write_log.clear();

        for idx in (0..self.query_log.len()).rev() {
            let table = Arc::clone(&self.queries[idx].1);
            let entry = self.query_log.remove(idx);

            for _ in 0..entry.num_locks {
                let lock = self.locks_acquired.remove(self.locks_acquired.len() - 1);
                table.get_lock_manager().unlock(lock);
            }
        }

        self.current_status = QueryStatus::Idle;
    }

    fn rollback(&mut self) {
        for idx in (0..(self.query_log.len())).rev() {
            let table = Arc::clone(&self.queries[idx].1);
            let entry = self.query_log.remove(idx);

            let bp = table.get_bufferpool();
            let mut bpl = bp.lock();

            for _ in 0..entry.num_muts {
                let write_entry = self.write_log.remove(self.write_log.len() - 1);

                match write_entry {
                    Mutation::Index(index_entry) => match index_entry {
                        IndexMutation::Add { rid, value, column } => {
                            table.index.write().remove_index(column, value, rid)
                        }
                        IndexMutation::Remove {
                            rid,
                            old_value,
                            column,
                        } => table.index.write().update_index(column, old_value, rid),
                    },
                    Mutation::Record(write_entry) => {
                        table
                            .get_page(write_entry.modified_entry)
                            .get_column(bpl.borrow_mut(), write_entry.modified_column)
                            .write_slot(
                                write_entry.modified_entry.slot(),
                                write_entry.original_value,
                            );
                    }
                }
            }

            for _ in 0..entry.num_locks {
                let lock = self.locks_acquired.remove(self.locks_acquired.len() - 1);
                table.get_lock_manager().unlock(lock);
            }
        }
    }

    pub fn set_aborted(&mut self, retry: bool) {
        if retry {
            self.current_status = QueryStatus::AbortedRetryable;
        } else {
            self.current_status = QueryStatus::AbortedNotRetryable;
        }
    }

    pub fn get_status(&self) -> QueryStatus {
        self.current_status
    }

    pub fn log_index_write(&mut self, mutation: IndexMutation) {
        self.current_writes += 1;
        self.write_log.push(Mutation::Index(mutation));
    }

    pub fn log_write(&mut self, modified_column: usize, modified_entry: RID, original_value: u64) {
        self.current_writes += 1;
        self.write_log.push(Mutation::Record(RecordMutation {
            modified_entry,
            modified_column,
            original_value,
        }));
    }

    fn try_lock(&mut self, locks: &LockManager, rid: RID, lock_type: LockType) -> bool {
        if self.has_lock(rid) {
            return true;
        }

        if let Some(handle) = locks.try_lock(rid, lock_type) {
            self.current_locks += 1;
            self.locks_acquired.push(handle);
            true
        } else {
            false
        }
    }

    fn has_lock(&mut self, rid: RID) -> bool {
        println!("{} ", self.locks_acquired.len());
        self.locks_acquired.iter().any(|x| x.rid == rid)
    }

    pub fn try_lock_with_abort(
        &mut self,
        locks: &LockManager,
        rid: RID,
        lock_type: LockType,
    ) -> bool {
        if !self.try_lock(locks, rid, lock_type) {
            println!(
                "Thread {:?} failed to lock on RID {:?}",
                std::thread::current().id(),
                rid
            );
            self.set_aborted(true);
            return false;
        }
        true
    }
}
