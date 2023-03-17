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
    transaction_test1(dir.path());
    transaction_test2(dir.path());
}

const NUMBER_OF_RECORDS: u64 = 10000;
const NUMBER_OF_TRANSACTIONS: u64 = 100;
const NUMBER_OF_OPERATIONS_PER_RECORD: u64 = 10;
const NUM_THREADS: u64 = 4;

fn transaction_test2(dir: &Path) {
    let mut rand = StdRng::seed_from_u64(3562901);
    let mut crabstore = CrabStore::new(dir.into());
    crabstore.open();

    let grades = crabstore.get_table("Grades");
    let mut records: HashMap<u64, Vec<u64>> = HashMap::new();

    let mut keys: Vec<u64> = Vec::new();
    let mut transactions = Vec::new();

    for _ in 0..NUMBER_OF_TRANSACTIONS {
        transactions.push(Transaction::new());
    }

    for i in 0..NUMBER_OF_RECORDS {
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
    }

    for key in keys.iter() {
        let record = &grades.select_query(*key, 0, &[1, 1, 1, 1, 1], None)[0].columns;

        for (i, col) in record.iter().enumerate() {
            assert_eq!(*col, records.get(key).unwrap()[i]);
        }
    }

    let mut workers: Vec<TransactionWorker> = Vec::new();

    for _ in 0..NUM_THREADS {
        workers.push(TransactionWorker::new());
    }

    for i in (0..NUMBER_OF_OPERATIONS_PER_RECORD).rev() {
        for key in keys.iter() {
            let mut updated_cols = [None, None, None, None, None];
            for i in 2..grades.columns() {
                let value = rand.gen_range(0..20);
                updated_cols[i] = Some(value);

                records.get_mut(key).unwrap()[i] = value;
                transactions[(*key % NUMBER_OF_TRANSACTIONS) as usize]
                    .add_query(Query::Select(*key, 0, Box::new([1, 1, 1, 1, 1])), &grades);

                transactions[(*key % NUMBER_OF_TRANSACTIONS) as usize]
                    .add_query(Query::Update(*key, Box::new(updated_cols)), &grades);
            }
        }
    }

    for i in 0..NUMBER_OF_TRANSACTIONS {
        workers
            .get_mut((i % NUM_THREADS) as usize)
            .unwrap()
            .add_transaction(transactions.remove(0));
    }

    for worker in workers.iter_mut() {
        worker.run();
    }

    for worker in workers.iter_mut() {
        worker.join();
    }

    let mut score = keys.len();

    for key in keys.iter() {
        let record = &grades.select_query(*key, 0, &[1, 1, 1, 1, 1], None)[0].columns;

        for (i, col) in record.iter().enumerate() {
            if *col != records.get(key).unwrap()[i] {
                score -= 1;
                println!(
                    "Select Error: Key {} | Result: {:?} | Correct: {:?}",
                    *key,
                    record,
                    records.get(key).unwrap()
                );
                break;
            }
        }
    }

    println!("Score: {score}/{}", keys.len());

    crabstore.close();
}

fn transaction_test1(dir: &Path) {
    let mut rand = StdRng::seed_from_u64(3562901);

    let mut crabstore = CrabStore::new(dir.into());

    let grades = crabstore.create_table("Grades", 5, 0);

    let mut records: HashMap<u64, Vec<u64>> = HashMap::new();

    grades.build_index(2);
    grades.build_index(3);
    grades.build_index(4);

    let mut keys: Vec<u64> = Vec::new();
    let mut insert_transactions = Vec::new();

    for _ in 0..NUMBER_OF_TRANSACTIONS {
        insert_transactions.push(Transaction::new());
    }

    for i in 0..NUMBER_OF_RECORDS {
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

        insert_transactions[(i % NUMBER_OF_TRANSACTIONS) as usize]
            .add_query(Query::Insert(cols.into()), &grades);
    }

    let mut workers: Vec<TransactionWorker> = Vec::new();

    for _ in 0..NUM_THREADS {
        workers.push(TransactionWorker::new());
    }

    for i in (0..NUMBER_OF_TRANSACTIONS).rev() {
        workers[(i % NUM_THREADS) as usize].add_transaction(insert_transactions.remove(i as usize));
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

    crabstore.close();
}
