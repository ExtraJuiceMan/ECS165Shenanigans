#![feature(test)]
extern crate test;
use core::num;
use crabcore::{
    crabstore::CrabStore,
    transaction::{Query, Transaction},
    transaction_worker::TransactionWorker,
};
use rand::prelude::*;
use std::{collections::HashMap, path::Path};
use tempfile::tempdir;
use test::Bencher;

#[test]
fn transaction_test() {
    let dir = tempdir().unwrap();
    let mut rand = StdRng::from_entropy();

    let mut crabstore = CrabStore::new(dir.path().into());

    let grades = crabstore.create_table("Grades", 5, 0);

    let mut records: HashMap<u64, Vec<u64>> = HashMap::new();

    let number_of_records = 1000;
    let number_of_transactions = 100;
    let num_threads = 8;

    grades.build_index(2);
    grades.build_index(3);
    grades.build_index(4);

    let mut keys: Vec<u64> = Vec::new();
    let mut insert_transactions = Vec::new();

    for _ in 0..number_of_transactions {
        insert_transactions.push(Transaction::new());
    }

    for i in 0..number_of_records {
        let key = 92106429 + i;
        keys.push(key);
        let cols = vec![
            key,
            rand.gen_range((i * 20)..((i + 1) * 20)),
            rand.gen_range((i * 20)..((i + 1) * 20)),
            rand.gen_range((i * 20)..((i + 1) * 20)),
            rand.gen_range((i * 20)..((i + 1) * 20)),
        ];
        records.insert(key, cols.clone());

        insert_transactions[(i % number_of_transactions) as usize]
            .add_query(Query::Insert(cols.into()), &grades);
    }

    let mut workers: Vec<TransactionWorker> = Vec::new();

    for _ in 0..num_threads {
        workers.push(TransactionWorker::new());
    }

    for i in (0..number_of_transactions).rev() {
        workers[(i % num_threads) as usize].add_transaction(insert_transactions.remove(i as usize));
    }

    for worker in workers.iter_mut() {
        worker.run();
    }

    for worker in workers.iter_mut() {
        worker.join();
    }

    for key in keys {
        let record = &grades.select_query(key, 0, &[1, 1, 1, 1, 1], None)[0].columns;

        for (i, col) in record.iter().enumerate() {
            assert_eq!(*col, records.get(&key).unwrap()[i]);
        }
    }
}
