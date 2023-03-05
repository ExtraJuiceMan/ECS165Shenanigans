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
        grades.insert_query(vec![i, 2, 3, 4]);
    }

    let records = grades.select_query(15000, 0, &vec![1, 1, 1, 1]);
    let record = &records[0];

    

    drop(grades);

    crabstore.close();
}
