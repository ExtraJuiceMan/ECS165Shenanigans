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
