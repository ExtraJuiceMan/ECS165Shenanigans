use std::mem::size_of;

use rkyv::{Archive, Deserialize, Serialize};

use crate::{PAGE_RANGE_COUNT, PAGE_RANGE_SIZE};

#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, Default)]
pub struct RID {
    rid: u64,
}

impl RID {
    /*
        MSB set, then tail since tail grows downwards from 64 bit max
    */
    pub fn is_tail(&self) -> bool {
        self.rid >> (usize::BITS - 1) & 1 != 0
    }

    /*
       "Untail" the page if it is a tail by inverting the bits to create readable numbers
    */
    pub fn untail(&self) -> usize {
        if self.is_tail() {
            (!(self.rid + 1)) as usize
        } else {
            self.rid as usize
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
            ((self.rid + 1) >> 9) as usize
        } else {
            (self.rid >> 9) as usize
        }
    }

    pub fn page_range(&self) -> usize {
        self.page() / PAGE_RANGE_COUNT
    }

    pub fn raw(&self) -> u64 {
        self.rid
    }

    /*
        Tail RIDs grow downwards.
    */
    pub fn next(&self) -> RID {
        RID {
            rid: if self.is_tail() {
                self.rid - 1
            } else {
                self.rid + 1
            },
        }
    }
}

impl From<u64> for RID {
    fn from(value: u64) -> Self {
        RID { rid: value }
    }
}
