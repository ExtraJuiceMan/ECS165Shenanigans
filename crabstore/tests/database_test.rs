#![feature(test)]
extern crate test;
use crabcore::{crabstore::CrabStore, record::Record};
use rand::prelude::*;
use std::{collections::HashMap, path::Path};
use tempfile::tempdir;
use test::Bencher;

#[test]
fn verify() {
    let num_records = 20000;

    let dir = tempdir().unwrap();

    let mut crabstore = CrabStore::new(dir.path().into());
    crabstore.open();
    let grades = crabstore.create_table("Grades", 4, 0);

    for i in 0..num_records {
        grades.insert_query(&[i, 2, 3, 4]);
    }

    let sum = grades.sum_query(0, num_records, 1);
    assert_eq!(sum, 2 * num_records);
    let sum = grades.sum_query(0, num_records, 2);
    assert_eq!(sum, 3 * num_records);

    let selected = grades.select_query(19999, 0, &[1, 1, 1, 1]);
    assert_eq!(selected[0].columns, &[19999, 2, 3, 4]);

    for i in 0..num_records {
        let old_values = &grades.select_query(i, 0, &[1, 1, 1, 1])[0].columns;
        let mut new_values = old_values
            .iter()
            .map(|x| Some(x + i))
            .collect::<Vec<Option<u64>>>();

        new_values[0] = None;

        grades.update_query(i, &new_values);
    }

    let selected = grades.select_query(19965, 0, &[1, 1, 1, 1]);
    assert_eq!(selected[0].columns, [19965, 19967, 19968, 19969]);
    drop(grades);

    crabstore.close();
}

fn regorganize_result(result: Vec<Record>) -> Vec<Vec<u64>> {
    let mut val = Vec::with_capacity(result.len());
    for r in result.iter() {
        val.push(r.columns.clone());
    }
    val.sort();
    val
}

#[test]
fn correctness_tester1() {
    let records = [
        [0, 1, 1, 2, 1],
        [1, 1, 1, 1, 2],
        [2, 0, 3, 5, 1],
        [3, 1, 5, 1, 3],
        [4, 2, 7, 1, 1],
        [5, 1, 1, 1, 1],
        [6, 0, 9, 1, 0],
        [7, 1, 1, 1, 1],
    ];

    let dir = tempdir().unwrap();

    let mut crabstore = CrabStore::new(dir.path().into());
    crabstore.open();

    let table = crabstore.create_table("test", 5, 0);

    for record in records {
        table.insert_query(&record);
    }

    table.build_index(2);
    let result = regorganize_result(table.select_query(1, 2, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 4);
    assert!(result.iter().any(|x| x.eq(&records[0])));
    assert!(result.iter().any(|x| x.eq(&records[1])));
    assert!(result.iter().any(|x| x.eq(&records[5])));
    assert!(result.iter().any(|x| x.eq(&records[7])));

    table.drop_index(2);
    let result = regorganize_result(table.select_query(3, 2, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 1);
    assert!(result.iter().any(|x| x.eq(&records[2])));

    let result = regorganize_result(table.select_query(1, 2, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 4);
    assert!(result.iter().any(|x| x.eq(&records[0])));
    assert!(result.iter().any(|x| x.eq(&records[1])));
    assert!(result.iter().any(|x| x.eq(&records[5])));
    assert!(result.iter().any(|x| x.eq(&records[7])));

    let result = regorganize_result(table.select_query(10, 2, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 0);

    table.update_query(8, &[None, Some(2), Some(2), Some(2), Some(2)]);
    let result = regorganize_result(table.select_query(8, 2, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 0);

    table.update_query(7, &[Some(8), Some(2), Some(2), Some(2), Some(2)]);
    let result = regorganize_result(table.select_query(7, 0, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 0);

    table.delete_query(5, None);
    let result = regorganize_result(table.select_query(5, 0, &[1, 1, 1, 1, 1]));
    assert_eq!(result.len(), 0);

    let table2 = crabstore.create_table("test2", 5, 0);
    let records2 = [
        [1, 1, 1, 2, 1],
        [2, 1, 1, 1, 2],
        [3, 0, 3, 5, 1],
        [4, 1, 5, 1, 3],
        [5, 2, 7, 1, 1],
        [6, 1, 1, 1, 1],
        [7, 0, 9, 1, 0],
        [8, 1, 1, 1, 1],
    ];

    for record in records2.iter() {
        table2.insert_query(record);
    }

    let result = regorganize_result(table2.select_query(1, 0, &[1, 1, 1, 1, 1]));

    assert_eq!(result.len(), 1);
    assert!(result.iter().any(|x| x.eq(&records2[0])));
}

#[test]
fn correctness_tester2() {
    let records = [
        [1, 1, 0, 2, 1],
        [2, 1, 1, 1, 2],
        [3, 0, 2, 5, 1],
        [4, 1, 3, 1, 3],
        [5, 2, 4, 1, 1],
        [6, 1, 5, 1, 1],
        [7, 0, 6, 1, 0],
        [8, 1, 7, 1, 1],
    ];
    let dir = tempdir().unwrap();

    let mut crabstore = CrabStore::new(dir.path().into());
    crabstore.open();

    let table = crabstore.create_table("test3", 5, 2);

    for record in records.iter() {
        table.insert_query(record);
    }

    let result = table.sum_query(3, 5, 4);
    assert_eq!(result, 5);
}

const NUMBER_OF_RECORDS: u64 = 1000;
const NUMBER_OF_AGGREGATES: u64 = 100;
const NUMBER_OF_UPDATES: u64 = 1;

fn durability_tester1(directory: &Path, records: &mut HashMap<u64, Vec<u64>>, keys: &Vec<u64>) {
    let mut crabstore = CrabStore::new(directory.to_path_buf());
    crabstore.open();

    let table = crabstore.create_table("Grades", 5, 0);

    let mut rand = StdRng::seed_from_u64(3562901);

    for i in 0..NUMBER_OF_RECORDS {
        let key = 92106429 + i;
        let record = vec![
            key,
            rand.gen_range(0..20),
            rand.gen_range(0..20),
            rand.gen_range(0..20),
            rand.gen_range(0..20),
        ];
        table.insert_query(&record);
        records.insert(key, record);
    }

    for key in keys.iter() {
        let record = &table.select_query(*key, 0, &[1, 1, 1, 1, 1])[0].columns;
        for (i, column) in record.iter().enumerate() {
            assert_eq!(*column, records.get(key).unwrap()[i]);
        }
    }

    for _ in 0..NUMBER_OF_UPDATES {
        for key in keys.iter() {
            let mut updated_columns = [None, None, None, None, None];
            let original = records.get(key).unwrap().clone();
            for i in 1..table.columns() {
                let val = rand.gen_range(0..20);
                updated_columns[i] = Some(val);
                records.get_mut(key).unwrap()[i] = val;
            }
            table.update_query(*key, &updated_columns);
            let record = &table.select_query(*key, 0, &[1, 1, 1, 1, 1])[0];
            for (i, val) in record.columns.iter().enumerate() {
                assert_eq!(*val, records.get(key).unwrap()[i]);
            }
        }
    }

    /*
    for i in 0..NUMBER_OF_AGGREGATES {
        let mut range = (0..2)
            .map(|_| rand.gen_range(0..keys.len()))
            .collect::<Vec<usize>>();
        range.sort();
        let low = range[0];
        let high = range[1];

        let column_sum =
    }
    */
    crabstore.close();
}

fn durability_tester2(directory: &Path, records: &mut HashMap<u64, Vec<u64>>, keys: &Vec<u64>) {
    let mut crabstore = CrabStore::new(directory.to_path_buf());
    crabstore.open();

    let table = crabstore.get_table("Grades");

    for key in keys.iter() {
        let record = &table.select_query(*key, 0, &[1, 1, 1, 1, 1])[0].columns;
        for (i, column) in record.iter().enumerate() {
            assert_eq!(*column, records.get(key).unwrap()[i]);
        }
    }

    crabstore.close();
}

#[test]
fn durability_tester() {
    let dir = tempdir().unwrap();
    let mut records: HashMap<u64, Vec<u64>> = HashMap::new();

    let mut rand = StdRng::seed_from_u64(3562901);

    for i in 0..NUMBER_OF_RECORDS {
        let key = 92106429 + i;
        records.insert(
            key,
            vec![
                key,
                rand.gen_range(0..20),
                rand.gen_range(0..20),
                rand.gen_range(0..20),
                rand.gen_range(0..20),
            ],
        );
    }

    let mut keys = records.keys().copied().collect::<Vec<u64>>();
    keys.sort();

    for _ in 0..NUMBER_OF_UPDATES {
        for key in keys.iter() {
            let mut updated_columns = [None, None, None, None, None];
            for i in 1..5 {
                let value = rand.gen_range(0..20);
                updated_columns[i] = Some(value);
                records.get_mut(key).unwrap()[i] = value;
            }
        }
    }

    durability_tester1(dir.path(), &mut records, &keys);
    durability_tester2(dir.path(), &mut records, &keys);
}
