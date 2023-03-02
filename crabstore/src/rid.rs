use std::mem::size_of;

use crate::PAGE_RANGE_SIZE;

#[derive(Clone, Copy, Debug, Default)]
pub struct RID {
    rid: u64,
}

impl RID {
    /*
        MSB set, then tail since tail grows downwards from 64 bit max
    */
    fn is_tail(&self) -> bool {
        self.rid & (1 << (size_of::<usize>() * 8)) != 0
    }

    /*
       "Untail" the page if it is a tail by inverting the bits to create readable numbers
    */
    fn untail(&self) -> usize {
        if self.is_tail() {
            !self.rid as usize
        } else {
            self.rid as usize
        }
    }

    /*
        We untail because we want the offset from the start of the page
    */
    pub fn slot(&self) -> usize {
        (self.untail() & 0b111111111) as usize
    }

    /*
       No untail here since we want unique page sequences and the
       pages are virtually mapped by our directory anyway
    */
    pub fn page(&self) -> usize {
        ((self.rid >> 9) & 0b1111) as usize
    }

    pub fn page_range(&self) -> usize {
        self.page() / PAGE_RANGE_SIZE
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
