#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    pub rid: u64,
    pub columns: Vec<u64>,
}

impl Record {
    pub fn new(rid: u64, columns: Vec<u64>) -> Self {
        Record { rid, columns }
    }
}
