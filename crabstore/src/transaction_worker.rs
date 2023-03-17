use std::{
    collections::VecDeque,
    process::id,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};

use parking_lot::RwLock;

use crate::transaction::{QueryStatus, Transaction};

pub struct TransactionWorker {
    transactions: Arc<RwLock<VecDeque<Transaction>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    stats: Arc<RwLock<Vec<bool>>>,
    result: usize,
}

impl TransactionWorker {
    fn spawn_worker_thread(
        queue: &Arc<RwLock<VecDeque<Transaction>>>,
        stats: &Arc<RwLock<Vec<bool>>>,
    ) -> std::thread::JoinHandle<()> {
        let queue = Arc::clone(queue);
        let stats = Arc::clone(stats);

        std::thread::spawn(move || {
            let mut queue = queue.write();
            let mut stats = stats.write();

            while !queue.is_empty() {
                let mut transaction = queue.pop_front().unwrap();
                let result = transaction.run();
                stats.push(result);

                if !result && transaction.get_status() == QueryStatus::AbortedRetryable {
                    queue.push_back(transaction);
                }
            }
        })
    }

    pub fn new() -> Self {
        let transactions = Arc::new(RwLock::new(VecDeque::new()));

        Self {
            transactions,
            thread: None,
            stats: Arc::new(RwLock::new(Vec::new())),
            result: 0,
        }
    }

    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.write().push_back(transaction);
    }

    pub fn add_transactions(&mut self, transaction: Vec<Transaction>) {
        self.transactions.write().extend(transaction);
    }

    pub fn run(&mut self) {
        if self.thread.is_none() {
            self.thread = Some(TransactionWorker::spawn_worker_thread(
                &self.transactions,
                &self.stats,
            ));
        }
    }

    pub fn join(&mut self) {
        if self.thread.is_none() {
            return;
        }

        let handle = std::mem::replace(&mut self.thread, None).unwrap();

        handle.join().unwrap();
    }
}
