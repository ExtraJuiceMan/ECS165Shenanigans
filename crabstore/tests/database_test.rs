use crabstore::*;
use tempfile::tempdir;

#[test]
fn verify() {
    let num_records = 100;

    let dir = tempdir().unwrap();

    let mut crabstore = CrabStore::new(dir.path().into());
    crabstore.open();
    let grades = crabstore.create_table("Grades", 4, 0);

    for i in 0..num_records {
        grades.insert_query(&[i, 2, 3, 4]);
    }

    let sum = grades.sum_query(0, 99, 1);
    assert_eq!(sum, 2 * num_records);
    let sum = grades.sum_query(0, 99, 2);
    assert_eq!(sum, 3 * num_records);

    let selected = grades.select_query(69, 0, &[1, 1, 1, 1]);
    assert_eq!(selected[0].columns, &[69, 2, 3, 4]);

    drop(grades);

    crabstore.close();
}
