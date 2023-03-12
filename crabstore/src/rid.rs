use rkyv::{Archive, Deserialize, Serialize};

use crate::PAGE_RANGE_COUNT;

#[derive(
    Archive, Serialize, Deserialize, Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd,
)]
pub struct RID(pub u64);

impl RID {
    /*
        MSB set, then tail since tail grows downwards from 64 bit max
    */
    pub fn is_tail(&self) -> bool {
        self.0 >> (u64::BITS - 1) & 1 != 0
    }

    pub fn is_invalid(&self) -> bool {
        self.0 == !0
    }

    /*
       "Untail" the page if it is a tail by inverting the bits to create readable numbers
    */
    pub fn untail(&self) -> usize {
        if self.is_tail() {
            (!(self.0 + 1)) as usize
        } else {
            self.0 as usize
        }
    }

    /*
        We untail because we want the offset from the start of the page
    */
    pub fn slot(&self) -> usize {
        self.untail() & 0b111111111
    }

    pub fn page(&self) -> usize {
        if self.is_tail() {
            ((self.0 + 1) >> 9) as usize
        } else {
            (self.0 >> 9) as usize
        }
    }

    pub fn page_range(&self) -> usize {
        self.page() / PAGE_RANGE_COUNT
    }

    pub fn raw(&self) -> u64 {
        self.0
    }

    /*
        Tail RIDs grow downwards.
    */
    pub fn next(&self) -> RID {
        RID(if self.is_tail() {
            self.0 - 1
        } else {
            self.0 + 1
        })
    }
}

impl From<u64> for RID {
    fn from(value: u64) -> Self {
        RID(value)
    }
}
