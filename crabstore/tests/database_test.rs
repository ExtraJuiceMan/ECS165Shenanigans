use crabstore::*;
use tempfile::tempdir;

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

fn regorganize_result(result: Vec<RecordRust>) -> Vec<Vec<u64>> {
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

    table.delete_query(5);
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

