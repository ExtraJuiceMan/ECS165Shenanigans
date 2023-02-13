#[derive(Clone, Copy, Debug, Default)]
pub struct RID {
    rid: u64,
}

impl RID {
    pub fn new(rid: u64) -> Self {
        RID { rid }
    }

    pub fn next(&self) -> RID {
        RID { rid: self.rid + 1 }
    }

    pub fn slot(&self) -> usize {
        (self.rid & 0b111111111) as usize
    }

    pub fn page(&self) -> usize {
        ((self.rid >> 9) & 0b1111) as usize
    }

    pub fn page_range(&self) -> usize {
        (self.rid >> 13) as usize
    }

    pub fn raw(&self) -> u64 {
        self.rid
    }
}

impl From<u64> for RID {
    fn from(value: u64) -> Self {
        RID { rid: value }
    }
}
