use crate::PAGE_SLOTS;

#[derive(Clone, Copy, Debug, Default)]
pub struct RID {
    rid: i64,
}

impl RID {
    pub fn new_tail(page_range: usize, id: usize) -> Self {
        RID {
            rid: (page_range | (id << 32)) as i64,
        }
    }

    pub fn new(rid: i64) -> Self {
        RID { rid }
    }

    pub fn next(&self) -> Self {
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

    pub fn is_base_page(&self) -> bool {
        (self.rid >> 63) == 0
    }

    pub fn tail_page_range(&self) -> usize {
        (self.rid & 0b11111111111111111111111111111111) as usize
    }

    pub fn tail_page_id(&self) -> usize {
        ((self.rid >> 32) & 0b1111111111111111111111111111111) as usize
    }

    pub fn tail_page_slot(&self) -> usize {
        self.tail_page_id() % PAGE_SLOTS
    }

    pub fn raw(&self) -> i64 {
        self.rid
    }
}

impl From<i64> for RID {
    fn from(value: i64) -> Self {
        RID { rid: value }
    }
}
