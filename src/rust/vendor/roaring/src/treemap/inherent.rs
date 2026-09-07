use alloc::collections::btree_map::{BTreeMap, Entry};
use core::iter;
use core::ops::RangeBounds;

use crate::IntegerTooSmall;
use crate::RoaringBitmap;
use crate::RoaringTreemap;

use super::util;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

impl RoaringTreemap {
    /// Creates an empty `RoaringTreemap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    /// let rb = RoaringTreemap::new();
    /// ```
    pub fn new() -> RoaringTreemap {
        RoaringTreemap { map: BTreeMap::new() }
    }

    /// Creates a full `RoaringTreemap`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use roaring::RoaringTreemap;
    /// let rb = RoaringTreemap::full();
    /// ```
    pub fn full() -> RoaringTreemap {
        RoaringTreemap { map: (0..=u32::MAX).zip(iter::repeat(RoaringBitmap::full())).collect() }
    }

    /// Adds a value to the set. Returns `true` if the value was not already present in the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.insert(3), true);
    /// assert_eq!(rb.insert(3), false);
    /// assert_eq!(rb.contains(3), true);
    /// ```
    pub fn insert(&mut self, value: u64) -> bool {
        let (hi, lo) = util::split(value);
        self.map.entry(hi).or_default().insert(lo)
    }

    /// Inserts a range of values.
    ///
    /// Returns the number of inserted values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert_range(2..4);
    /// assert!(rb.contains(2));
    /// assert!(rb.contains(3));
    /// assert!(!rb.contains(4));
    /// ```
    pub fn insert_range<R: RangeBounds<u64>>(&mut self, range: R) -> u64 {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Some(range) => (*range.start(), *range.end()),
            None => return 0,
        };

        let (start_hi, start_lo) = util::split(start);
        let (end_hi, end_lo) = util::split(end);

        let mut counter = 0u64;

        // Split the input range by the leading 32 bits
        for hi in start_hi..=end_hi {
            let entry = self.map.entry(hi);

            // Calculate the sub-range from the lower 32 bits
            counter += if hi == end_hi && hi == start_hi {
                entry.or_default().insert_range(start_lo..=end_lo)
            } else if hi == start_hi {
                entry.or_default().insert_range(start_lo..=u32::MAX)
            } else if hi == end_hi {
                entry.or_default().insert_range(0..=end_lo)
            } else {
                // We insert a full bitmap if it doesn't already exist and return the size of it.
                // But if the bitmap already exists at this spot we replace it with a full bitmap
                // and specify that we didn't inserted the integers from the previous bitmap.
                let full_bitmap = RoaringBitmap::full();
                match entry {
                    Entry::Vacant(entry) => entry.insert(full_bitmap).len(),
                    Entry::Occupied(mut entry) => {
                        full_bitmap.len() - entry.insert(full_bitmap).len()
                    }
                }
            };
        }

        counter
    }

    /// Pushes `value` in the treemap only if it is greater than the current maximum value.
    ///
    /// Returns whether the value was inserted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert!(rb.push(1));
    /// assert!(rb.push(3));
    /// assert_eq!(rb.push(3), false);
    /// assert!(rb.push(5));
    ///
    /// assert_eq!(rb.iter().collect::<Vec<u64>>(), vec![1, 3, 5]);
    /// ```
    #[deprecated(since = "0.11.0", note = "use `try_push` instead")]
    pub fn push(&mut self, value: u64) -> bool {
        let (hi, lo) = util::split(value);
        self.map.entry(hi).or_default().try_push(lo).is_ok()
    }

    /// Pushes `value` in the treemap only if it is greater than the current maximum value.
    ///
    /// Returns an error if the value is not greater than the current maximum value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::{RoaringTreemap, IntegerTooSmall};
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert!(rb.try_push(1).is_ok());
    /// assert!(rb.try_push(3).is_ok());
    /// assert_eq!(rb.try_push(3), Err(IntegerTooSmall));
    /// assert!(rb.try_push(5).is_ok());
    ///
    /// assert_eq!(rb.iter().collect::<Vec<u64>>(), vec![1, 3, 5]);
    /// ```
    pub fn try_push(&mut self, value: u64) -> Result<(), IntegerTooSmall> {
        let (hi, lo) = util::split(value);
        self.map.entry(hi).or_default().try_push(lo)
    }

    /// Pushes `value` in the treemap only if it is greater than the current maximum value.
    /// It is up to the caller to have validated index > self.max()
    ///
    /// # Panics
    ///
    /// If debug_assertions enabled and index is > self.max()
    pub(crate) fn push_unchecked(&mut self, value: u64) {
        let (hi, lo) = util::split(value);
        // BTreeMap last_mut not stabilized see https://github.com/rust-lang/rust/issues/62924
        match self.map.iter_mut().next_back() {
            Some((&key, bitmap)) if key == hi => bitmap.push_unchecked(lo),
            Some((&key, _)) if cfg!(debug_assertions) && key > hi => {
                panic!("last bitmap key > key of value")
            }
            _otherwise => {
                // The tree is empty
                let mut rb = RoaringBitmap::new();
                rb.push_unchecked(lo);
                self.map.insert(hi, rb);
            }
        }
    }

    /// Removes a value from the set. Returns `true` if the value was present in the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert(3);
    /// assert_eq!(rb.remove(3), true);
    /// assert_eq!(rb.remove(3), false);
    /// assert_eq!(rb.contains(3), false);
    /// ```
    pub fn remove(&mut self, value: u64) -> bool {
        let (hi, lo) = util::split(value);
        match self.map.entry(hi) {
            Entry::Vacant(_) => false,
            Entry::Occupied(mut ent) => {
                if ent.get_mut().remove(lo) {
                    if ent.get().is_empty() {
                        ent.remove();
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Removes a range of values.
    /// Returns the number of removed values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert(2);
    /// rb.insert(3);
    /// assert_eq!(rb.remove_range(2..4), 2);
    /// ```
    pub fn remove_range<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Some(range) => (*range.start(), *range.end()),
            None => return 0,
        };

        let (start_container_key, start_index) = util::split(start);
        let (end_container_key, end_index) = util::split(end);

        let mut keys_to_remove = Vec::new();
        let mut removed = 0;

        for (&key, rb) in &mut self.map {
            if key >= start_container_key && key <= end_container_key {
                let a = if key == start_container_key { start_index } else { 0 };
                let b = if key == end_container_key { end_index } else { u32::MAX };
                removed += rb.remove_range(a..=b);
                if rb.is_empty() {
                    keys_to_remove.push(key);
                }
            }
        }

        for key in keys_to_remove {
            self.map.remove(&key);
        }

        removed
    }

    /// Returns `true` if this set contains the specified integer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert(1);
    /// assert_eq!(rb.contains(0), false);
    /// assert_eq!(rb.contains(1), true);
    /// assert_eq!(rb.contains(100), false);
    /// ```
    pub fn contains(&self, value: u64) -> bool {
        let (hi, lo) = util::split(value);
        match self.map.get(&hi) {
            None => false,
            Some(r) => r.contains(lo),
        }
    }

    /// Returns `true` if all values in the range are present in this set.
    ///
    /// An empty range is always contained.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// // An empty range is always contained
    /// assert!(rb.contains_range(7..7));
    ///
    /// rb.insert_range(1..0x1_0000_0000);
    /// assert!(rb.contains_range(1..0x1_0000_0000));
    /// assert!(rb.contains_range(2..0x1_0000_0000));
    /// // 0 is not contained
    /// assert!(!rb.contains_range(0..2));
    /// // 0x1_0000_0000 is not contained
    /// assert!(!rb.contains_range(1..=0x1_0000_0000));
    /// ```
    pub fn contains_range<R>(&self, range: R) -> bool
    where
        R: RangeBounds<u64>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Some(range) => (*range.start(), *range.end()),
            None => return true,
        };
        let (start_hi, start_lo) = util::split(start);
        let (end_hi, end_lo) = util::split(end);

        let mut expected_key = start_hi;
        for (&key, bitmap) in self.map.range(start_hi..=end_hi) {
            if key != expected_key {
                return false;
            }
            let lo_start = if key == start_hi { start_lo } else { 0 };
            let lo_end = if key == end_hi { end_lo } else { u32::MAX };
            if !bitmap.contains_range(lo_start..=lo_end) {
                return false;
            }
            match key.checked_add(1) {
                Some(k) => expected_key = k,
                None => return true,
            }
        }
        expected_key > end_hi
    }

    /// Returns the number of elements in this set which are in the passed range.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert_range(0x1_0000_0000..0x4_0000_0000);
    /// rb.insert(0x5_0000_0001);
    /// rb.insert(0x5_0000_0005);
    /// rb.insert(u64::MAX);
    ///
    /// assert_eq!(rb.range_cardinality(0..0x1_0000_0000), 0);
    /// assert_eq!(rb.range_cardinality(0x1_0000_0000..0x4_0000_0000), 0x3_0000_0000);
    /// assert_eq!(rb.range_cardinality(0x5_0000_0000..0x6_0000_0000), 2);
    /// assert_eq!(rb.range_cardinality(0x1_0000_0000..0x1_0000_0000), 0);
    /// assert_eq!(rb.range_cardinality(0x5_0000_0000..=u64::MAX), 3);
    /// ```
    pub fn range_cardinality<R>(&self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Some(range) => (*range.start(), *range.end()),
            None => return 0,
        };
        let (start_hi, start_lo) = util::split(start);
        let (end_hi, end_lo) = util::split(end);

        self.map
            .range(start_hi..=end_hi)
            .map(|(&key, bitmap)| {
                let lo_start = if key == start_hi { start_lo } else { 0 };
                let lo_end = if key == end_hi { end_lo } else { u32::MAX };
                bitmap.range_cardinality(lo_start..=lo_end)
            })
            .sum::<u64>()
    }

    /// Clears all integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.insert(1);
    /// assert_eq!(rb.contains(1), true);
    /// rb.clear();
    /// assert_eq!(rb.contains(1), false);
    /// ```
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Returns `true` if there are no integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.is_empty(), true);
    ///
    /// rb.insert(3);
    /// assert_eq!(rb.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.values().all(RoaringBitmap::is_empty)
    }

    /// Returns `true` if there are every possible integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::full();
    /// assert!(!rb.is_empty());
    /// assert!(rb.is_full());
    /// ```
    pub fn is_full(&self) -> bool {
        self.map.len() == (u32::MAX as u64 + 1) as usize
            && self.map.values().all(RoaringBitmap::is_full)
    }

    /// Returns the number of distinct integers added to the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.len(), 0);
    ///
    /// rb.insert(3);
    /// assert_eq!(rb.len(), 1);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.len(), 2);
    /// ```
    pub fn len(&self) -> u64 {
        self.map.values().map(RoaringBitmap::len).sum()
    }

    /// Optimizes the underlying bitmap storage and removes empty partitions.
    ///
    /// Returns true if the treemap storage was modified, false if not.
    ///
    /// # Examples
    ///
    /// ```
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::from_iter(1000..100000);
    /// rb.optimize();
    /// ```
    pub fn optimize(&mut self) -> bool {
        let mut changed = false;

        self.map.retain(|_, bitmap| {
            changed |= bitmap.optimize();

            if bitmap.is_empty() {
                changed = true;
                false
            } else {
                true
            }
        });

        changed
    }

    /// Returns the minimum value in the set (if the set is non-empty).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.min(), None);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.min(), Some(3));
    /// ```
    pub fn min(&self) -> Option<u64> {
        self.map
            .iter()
            .find(|&(_, rb)| rb.min().is_some())
            .map(|(k, rb)| util::join(*k, rb.min().unwrap()))
    }

    /// Returns the maximum value in the set (if the set is non-empty).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.max(), None);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.max(), Some(4));
    /// ```
    pub fn max(&self) -> Option<u64> {
        self.map
            .iter()
            .rev()
            .find(|&(_, rb)| rb.max().is_some())
            .map(|(k, rb)| util::join(*k, rb.max().unwrap()))
    }

    /// Returns the number of integers that are <= value. rank(u64::MAX) == len()
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.rank(0), 0);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.rank(3), 1);
    /// assert_eq!(rb.rank(10), 2)
    /// ```
    pub fn rank(&self, value: u64) -> u64 {
        // if len becomes cached for RoaringTreemap: return len if len > value

        let (hi, lo) = util::split(value);
        let mut iter = self.map.range(..=hi).rev();

        iter.next()
            .map(|(&k, bitmap)| if k == hi { bitmap.rank(lo) } else { bitmap.len() })
            .unwrap_or(0)
            + iter.map(|(_, bitmap)| bitmap.len()).sum::<u64>()
    }

    /// Returns the `n`th integer in the set or `None` if `n >= len()`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// assert_eq!(rb.select(0), None);
    ///
    /// rb.append(vec![0, 10, 100]);
    ///
    /// assert_eq!(rb.select(0), Some(0));
    /// assert_eq!(rb.select(1), Some(10));
    /// assert_eq!(rb.select(2), Some(100));
    /// assert_eq!(rb.select(3), None);
    /// ```
    pub fn select(&self, mut n: u64) -> Option<u64> {
        for (&key, bitmap) in &self.map {
            let len = bitmap.len();
            if len > n {
                return Some(((key as u64) << 32) | bitmap.select(n as u32).unwrap() as u64);
            }
            n -= len;
        }

        None
    }
}

impl Default for RoaringTreemap {
    fn default() -> RoaringTreemap {
        RoaringTreemap::new()
    }
}

impl Clone for RoaringTreemap {
    fn clone(&self) -> Self {
        RoaringTreemap { map: self.map.clone() }
    }

    fn clone_from(&mut self, other: &Self) {
        self.map.clone_from(&other.map);
    }
}

#[cfg(test)]
mod test {
    use crate::{RoaringBitmap, RoaringTreemap};
    use alloc::vec::Vec;
    use proptest::collection::btree_map;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn optimize_preserves_values(mut treemap in RoaringTreemap::arbitrary()) {
            let before = treemap.iter().collect::<Vec<_>>();

            treemap.optimize();

            let after = treemap.iter().collect::<Vec<_>>();
            prop_assert_eq!(before, after);
        }

        #[test]
        fn optimize_prunes_empty_partitions_from_bitmaps(
            bitmaps in btree_map(0u32..=16, RoaringBitmap::arbitrary(), 0usize..=16)
        ) {
            let mut treemap = RoaringTreemap::from_bitmaps(bitmaps);
            let before = treemap.iter().collect::<Vec<_>>();
            let had_empty_partition = treemap.bitmaps().any(|(_, bitmap)| bitmap.is_empty());

            let changed = treemap.optimize();

            prop_assert_eq!(treemap.iter().collect::<Vec<_>>(), before);
            prop_assert!(treemap.bitmaps().all(|(_, bitmap)| !bitmap.is_empty()));

            if had_empty_partition {
                prop_assert!(changed);
            }
        }
    }
}
