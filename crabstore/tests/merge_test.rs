#![feature(test)]
extern crate test;
use crabstore::crabstore::CrabStore;
use rand::prelude::*;
use tempfile::tempdir;
use test::Bencher;

#[test]
fn merge_test() {
    let dir = tempdir().unwrap();
    let mut rand = StdRng::from_entropy();

    let mut crabstore = CrabStore::new(dir.path().into());
    crabstore.open();

    let table = crabstore.create_table("merge", 5, 0);
    let update_nums = [2, 4, 8, 16];
    let records_num = 10000;
    let sample_count = 200;
    let select_repeat = 200;

    for i in 0..records_num {
        table.insert_query(&vec![
            i,
            (i + 100) % records_num,
            (i + 200) % records_num,
            (i + 300) % records_num,
            (i + 400) % records_num,
        ]);
    }

    for index in 0..update_nums.len() {
        let update_num = update_nums[index];
        for count in 0..update_num {
            for i in 0..records_num {
                let mut update_record = [
                    None,
                    Some((i + 101 + count) % records_num),
                    Some((i + 102 + count) % records_num),
                    Some((i + 103 + count) % records_num),
                    Some((i + 104 + count) % records_num),
                ];

                for idx in 0..index {
                    update_record[4 - idx] = None;
                }

                table.update_query(i, &update_record);
            }
        }
        let keys = (0..records_num).choose_multiple(&mut rand, sample_count);
        let mut time = 0;
        while time < select_repeat {
            time += 1;
            for key in keys.iter() {
                table.select_query(*key, 0, &[1, 1, 1, 1, 1]);
            }
        }
    }
}

/*
#[bench]
fn merge_bench(b: &mut Bencher) {
    b.iter(|| {
        merge_test();
    });
}
*/
