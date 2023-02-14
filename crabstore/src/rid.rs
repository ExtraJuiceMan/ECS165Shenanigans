use crate::PAGE_SLOTS;

#[derive(Clone, Copy, Debug, Default)]
pub struct BaseRID {
    rid: i64,
}
pub struct TailRID {
    rid: i64,
}

pub trait RID {
    fn slot(&self) -> usize;
    fn page_range(&self) -> usize;
    fn raw(&self) -> i64;
    fn page(&self) -> usize;
}
impl RID for BaseRID {
    fn slot(&self) -> usize {
        (self.rid & 0b111111111) as usize
    }
    fn page_range(&self) -> usize {
        (self.rid >> 13) as usize
    }
    fn raw(&self) -> i64 {
        self.rid
    }
    fn page(&self) -> usize {
        ((self.rid >> 9) & 0b1111) as usize
    }
}
impl BaseRID {
    pub fn new(rid: i64) -> Self {
        BaseRID { rid }
    }
    pub fn next(&self) -> Self {
        BaseRID { rid: self.rid + 1 }
    }
}
impl From<i64> for BaseRID {
    fn from(value: i64) -> Self {
        BaseRID { rid: value }
    }
}
impl RID for TailRID {
    fn page_range(&self) -> usize {
        (self.rid & 0b11111111111111111111111111111111) as usize
    }

    fn slot(&self) -> usize {
        self.id() % PAGE_SLOTS
    }

    fn raw(&self) -> i64 {
        self.rid
    }
    fn page(&self) -> usize {
        self.id() / PAGE_SLOTS
    }
}
impl TailRID {
    pub fn new(rid: i64) -> Self {
        TailRID { rid }
    }
    pub fn new_tail(page_range: usize, id: usize) -> Self {
        TailRID {
            rid: ((page_range | (id << 32)) | 1 << 63) as i64,
        }
    }
    pub fn id(&self) -> usize {
        ((self.rid >> 32) & 0b1111111111111111111111111111111) as usize
    }
}
impl From<i64> for TailRID {
    fn from(value: i64) -> Self {
        TailRID { rid: value }
    }
}
