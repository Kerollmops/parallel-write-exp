use std::iter::{Chain, FromIterator};
use std::ops::RangeInclusive;

use roaring::bitmap::{IntoIter, RoaringBitmap};

pub struct SequentialDocids {
    iter: Chain<IntoIter, RangeInclusive<u32>>,
}

impl SequentialDocids {
    pub fn new(used: &RoaringBitmap) -> SequentialDocids {
        match used.max() {
            Some(last_id) => {
                let mut available = RoaringBitmap::from_iter(0..last_id);
                available -= used;

                let iter = match last_id.checked_add(1) {
                    Some(id) => id..=u32::MAX,
                    #[allow(clippy::reversed_empty_ranges)]
                    None => 1..=0, // empty range iterator
                };

                SequentialDocids { iter: available.into_iter().chain(iter) }
            }
            None => {
                let empty = RoaringBitmap::new().into_iter();
                SequentialDocids { iter: empty.chain(0..=u32::MAX) }
            }
        }
    }
}

impl Iterator for SequentialDocids {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
