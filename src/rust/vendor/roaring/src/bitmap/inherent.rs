use core::cmp::Ordering;
use core::mem::size_of;
use core::ops::{RangeBounds, RangeInclusive};

use crate::bitmap::store::BITMAP_LENGTH;
use crate::{IntegerTooSmall, RoaringBitmap};

use super::container::Container;
use super::util;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

impl RoaringBitmap {
    /// Creates an empty `RoaringBitmap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// let rb = RoaringBitmap::new();
    /// ```
    pub fn new() -> RoaringBitmap {
        RoaringBitmap { containers: Vec::new() }
    }

    /// Creates a full `RoaringBitmap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// let rb = RoaringBitmap::full();
    /// ```
    pub fn full() -> RoaringBitmap {
        RoaringBitmap { containers: (0..=u16::MAX).map(Container::full).collect() }
    }

    /// Creates a `RoaringBitmap` from a byte slice, interpreting the bytes as a bitmap with a specified offset.
    ///
    /// # Arguments
    ///
    /// - `offset: u32` - The starting position in the bitmap where the byte slice will be applied, specified in bits.
    ///                   This means that if `offset` is `n`, the first byte in the slice will correspond to the `n`th bit(0-indexed) in the bitmap.
    /// - `bytes: &[u8]` - The byte slice containing the bitmap data. The bytes are interpreted in "Least-Significant-First" bit order.
    ///
    /// # Interpretation of `bytes`
    ///
    /// The `bytes` slice is interpreted in "Least-Significant-First" bit order. Each byte is read from least significant bit (LSB) to most significant bit (MSB).
    /// For example, the byte `0b00000101` represents the bits `1, 0, 1, 0, 0, 0, 0, 0` in that order (see Examples section).
    ///
    ///
    /// # Panics
    ///
    /// This function will panic if `bytes.len() + offset` is greater than 2^32.
    ///
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let bytes = [0b00000101, 0b00000010, 0b00000000, 0b10000000];
    /// //             ^^^^^^^^    ^^^^^^^^    ^^^^^^^^    ^^^^^^^^
    /// //             76543210          98
    /// let rb = RoaringBitmap::from_lsb0_bytes(0, &bytes);
    /// assert!(rb.contains(0));
    /// assert!(!rb.contains(1));
    /// assert!(rb.contains(2));
    /// assert!(rb.contains(9));
    /// assert!(rb.contains(31));
    ///
    /// let rb = RoaringBitmap::from_lsb0_bytes(8, &bytes);
    /// assert!(rb.contains(8));
    /// assert!(!rb.contains(9));
    /// assert!(rb.contains(10));
    /// assert!(rb.contains(17));
    /// assert!(rb.contains(39));
    ///
    /// let rb = RoaringBitmap::from_lsb0_bytes(3, &bytes);
    /// assert!(rb.contains(3));
    /// assert!(!rb.contains(4));
    /// assert!(rb.contains(5));
    /// assert!(rb.contains(12));
    /// assert!(rb.contains(34));
    /// ```
    pub fn from_lsb0_bytes(offset: u32, mut bytes: &[u8]) -> RoaringBitmap {
        fn shift_bytes(bytes: &[u8], amount: usize) -> Vec<u8> {
            let mut result = Vec::with_capacity(bytes.len() + 1);
            let mut carry = 0u8;

            for &byte in bytes {
                let shifted = (byte << amount) | carry;
                carry = byte >> (8 - amount);
                result.push(shifted);
            }

            if carry != 0 {
                result.push(carry);
            }

            result
        }
        if !offset.is_multiple_of(8) {
            let shift = offset as usize % 8;
            let shifted_bytes = shift_bytes(bytes, shift);
            return RoaringBitmap::from_lsb0_bytes(offset - shift as u32, &shifted_bytes);
        }

        if bytes.is_empty() {
            return RoaringBitmap::new();
        }

        // Using inclusive range avoids overflow: the max exclusive value is 2^32 (u32::MAX + 1).
        let end_bit_inc = u32::try_from(bytes.len())
            .ok()
            .and_then(|len_bytes| len_bytes.checked_mul(8))
            // `bytes` is non-empty, so len_bits is > 0
            .and_then(|len_bits| offset.checked_add(len_bits - 1))
            .expect("offset + bytes.len() must be <= 2^32");

        // offsets are in bytes
        let (mut start_container, start_offset) =
            (offset as usize >> 16, (offset as usize % 0x1_0000) / 8);
        let (end_container_inc, end_offset) =
            (end_bit_inc as usize >> 16, (end_bit_inc as usize % 0x1_0000 + 1) / 8);

        let n_containers_needed = end_container_inc + 1 - start_container;
        let mut containers = Vec::with_capacity(n_containers_needed);

        // Handle a partial first container
        if start_offset != 0 {
            let end_byte = if end_container_inc == start_container {
                end_offset
            } else {
                BITMAP_LENGTH * size_of::<u64>()
            };

            let (src, rest) = bytes.split_at(end_byte - start_offset);
            bytes = rest;

            if let Some(container) =
                Container::from_lsb0_bytes(start_container as u16, src, start_offset)
            {
                containers.push(container);
            }

            start_container += 1;
        }

        // Handle all full containers
        for full_container_key in start_container..end_container_inc {
            let (src, rest) = bytes.split_at(BITMAP_LENGTH * size_of::<u64>());
            bytes = rest;

            if let Some(container) = Container::from_lsb0_bytes(full_container_key as u16, src, 0) {
                containers.push(container);
            }
        }

        // Handle a last container
        if !bytes.is_empty() {
            if let Some(container) = Container::from_lsb0_bytes(end_container_inc as u16, bytes, 0)
            {
                containers.push(container);
            }
        }

        RoaringBitmap { containers }
    }

    /// Adds a value to the set.
    ///
    /// Returns whether the value was absent from the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.insert(3), true);
    /// assert_eq!(rb.insert(3), false);
    /// assert_eq!(rb.contains(3), true);
    /// ```
    #[inline]
    pub fn insert(&mut self, value: u32) -> bool {
        let (key, index) = util::split(value);
        let container = match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(loc) => &mut self.containers[loc],
            Err(loc) => {
                self.containers.insert(loc, Container::new(key));
                &mut self.containers[loc]
            }
        };
        container.insert(index)
    }

    /// Searches for the specific container by the given key.
    /// Creates a new container if it doesn't exist.
    ///
    /// Return the index of the target container.
    #[inline]
    pub(crate) fn find_container_by_key(&mut self, key: u16) -> usize {
        match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(loc) => loc,
            Err(loc) => {
                self.containers.insert(loc, Container::new(key));
                loc
            }
        }
    }

    /// Searches and then modifies a specific container with `M` by the given key.
    /// Creates a new container using `B` if it doesn't exist.
    ///
    /// Returns `R` based on `M` or `B`.
    #[inline]
    pub(crate) fn mod_or_build_container_by_key<
        R,
        M: FnMut(&mut Container) -> R,
        B: FnMut(u16) -> (Container, R),
    >(
        &mut self,
        key: u16,
        mut modifier: M,
        mut builder: B,
    ) -> R {
        match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(loc) => modifier(&mut self.containers[loc]),
            Err(loc) => {
                let build_value = builder(key);
                self.containers.insert(loc, build_value.0);
                build_value.1
            }
        }
    }

    /// Inserts a range of values.
    /// Returns the number of inserted values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert_range(2..4);
    /// assert!(rb.contains(2));
    /// assert!(rb.contains(3));
    /// assert!(!rb.contains(4));
    /// ```
    #[inline]
    pub fn insert_range<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u32>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Ok(range) => (*range.start(), *range.end()),
            Err(_) => return 0,
        };

        let (start_container_key, start_index) = util::split(start);
        let (end_container_key, end_index) = util::split(end);
        let modify_container_range =
            |bitmap: &mut Self, container_key: u16, range: RangeInclusive<u16>| {
                bitmap.mod_or_build_container_by_key(
                    container_key,
                    |container| container.insert_range(range.clone()),
                    |key| (Container::new_with_range(key, range.clone()), range.len() as u64),
                )
            };

        // If the end range value is in the same container, just call into
        // the one container.
        if start_container_key == end_container_key {
            return modify_container_range(self, start_container_key, start_index..=end_index);
        }

        // For the first container, insert start_index..=u16::MAX, with
        // subsequent containers inserting 0..MAX.
        //
        // The last container (end_container_key) is handled explicitly outside
        // the loop.
        let mut low = start_index;
        let mut inserted = 0;

        for i in start_container_key..end_container_key {
            inserted += modify_container_range(self, i, low..=u16::MAX);

            // After the first container, always fill the containers.
            low = 0;
        }

        // Handle the last container
        inserted += modify_container_range(self, end_container_key, 0..=end_index);

        inserted
    }

    /// Pushes `value` in the bitmap only if it is greater than the current maximum value.
    ///
    /// Returns whether the value was inserted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert!(rb.push(1));
    /// assert!(rb.push(3));
    /// assert_eq!(rb.push(3), false);
    /// assert!(rb.push(5));
    ///
    /// assert_eq!(rb.iter().collect::<Vec<u32>>(), vec![1, 3, 5]);
    /// ```
    #[inline]
    #[deprecated(since = "0.11.0", note = "use `try_push` instead")]
    pub fn push(&mut self, value: u32) -> bool {
        self.try_push(value).is_ok()
    }

    /// Pushes `value` in the bitmap only if it is greater than the current maximum value.
    ///
    /// Returns an error if the value is not greater than the current maximum value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::{RoaringBitmap, IntegerTooSmall};
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert!(rb.try_push(1).is_ok());
    /// assert!(rb.try_push(3).is_ok());
    /// assert_eq!(rb.try_push(3), Err(IntegerTooSmall));
    /// assert!(rb.try_push(5).is_ok());
    ///
    /// assert_eq!(rb.iter().collect::<Vec<u32>>(), vec![1, 3, 5]);
    /// ```
    #[inline]
    pub fn try_push(&mut self, value: u32) -> Result<(), IntegerTooSmall> {
        let (key, index) = util::split(value);

        match self.containers.last_mut() {
            Some(container) if container.key == key => {
                if container.push(index) {
                    Ok(())
                } else {
                    Err(IntegerTooSmall)
                }
            }
            Some(container) if container.key > key => Err(IntegerTooSmall),
            _otherwise => {
                let mut container = Container::new(key);
                container.push(index);
                self.containers.push(container);
                Ok(())
            }
        }
    }

    /// Pushes `value` at the end of the bitmap.
    /// It is up to the caller to have validated index > self.max()
    ///
    /// # Panics
    ///
    /// If debug_assertions enabled and index is > self.max()
    #[inline]
    pub(crate) fn push_unchecked(&mut self, value: u32) {
        let (key, index) = util::split(value);

        match self.containers.last_mut() {
            Some(container) if container.key == key => container.push_unchecked(index),
            Some(container) if cfg!(debug_assertions) && container.key > key => {
                panic!("last container key > key of value")
            }
            _otherwise => {
                let mut container = Container::new(key);
                container.push_unchecked(index);
                self.containers.push(container);
            }
        }
    }

    /// Removes a value from the set. Returns `true` if the value was present in the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert(3);
    /// assert_eq!(rb.remove(3), true);
    /// assert_eq!(rb.remove(3), false);
    /// assert_eq!(rb.contains(3), false);
    /// ```
    #[inline]
    pub fn remove(&mut self, value: u32) -> bool {
        let (key, index) = util::split(value);
        match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(loc) if self.containers[loc].remove(index) => {
                if self.containers[loc].is_empty() {
                    self.containers.remove(loc);
                }
                true
            }
            _ => false,
        }
    }

    /// Removes a range of values.
    /// Returns the number of removed values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert(2);
    /// rb.insert(3);
    /// assert_eq!(rb.remove_range(2..4), 2);
    /// ```
    #[inline]
    pub fn remove_range<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u32>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Ok(range) => (*range.start(), *range.end()),
            Err(_) => return 0,
        };

        let (start_container_key, start_index) = util::split(start);
        let (end_container_key, end_index) = util::split(end);

        let mut index = 0;
        let mut removed = 0;
        while index < self.containers.len() {
            let key = self.containers[index].key;
            if key >= start_container_key && key <= end_container_key {
                let a = if key == start_container_key { start_index } else { 0 };
                let b = if key == end_container_key { end_index } else { u16::MAX };
                removed += self.containers[index].remove_range(a..=b);
                if self.containers[index].is_empty() {
                    self.containers.remove(index);
                    continue;
                }
            }
            index += 1;
        }
        removed
    }

    /// Returns `true` if this set contains the specified integer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert(1);
    /// assert_eq!(rb.contains(0), false);
    /// assert_eq!(rb.contains(1), true);
    /// assert_eq!(rb.contains(100), false);
    /// ```
    #[inline]
    pub fn contains(&self, value: u32) -> bool {
        let (key, index) = util::split(value);
        match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(loc) => self.containers[loc].contains(index),
            Err(_) => false,
        }
    }

    /// Returns `true` if all values in the range are present in this set.
    ///
    /// # Examples
    ///
    /// ```
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// // An empty range is always contained
    /// assert!(rb.contains_range(7..7));
    ///
    /// rb.insert_range(1..0xFFF);
    /// assert!(rb.contains_range(1..0xFFF));
    /// assert!(rb.contains_range(2..0xFFF));
    /// // 0 is not contained
    /// assert!(!rb.contains_range(0..2));
    /// // 0xFFF is not contained
    /// assert!(!rb.contains_range(1..=0xFFF));
    /// ```
    #[inline]
    pub fn contains_range<R>(&self, range: R) -> bool
    where
        R: RangeBounds<u32>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Ok(range) => (*range.start(), *range.end()),
            // Empty/Invalid ranges are always contained
            Err(_) => return true,
        };
        let (start_high, start_low) = util::split(start);
        let (end_high, end_low) = util::split(end);
        debug_assert!(start_high <= end_high);

        let containers =
            match self.containers.binary_search_by_key(&start_high, |container| container.key) {
                Ok(i) => &self.containers[i..],
                Err(_) => return false,
            };

        if start_high == end_high {
            return containers[0].contains_range(start_low..=end_low);
        }

        let high_span = usize::from(end_high - start_high);
        // If this contains everything in the range, there should be a container for every item in the span
        // and the container that many items away should be the high key
        let containers = match containers.get(high_span) {
            Some(c) if c.key == end_high => &containers[..=high_span],
            _ => return false,
        };

        match containers {
            [first, rest @ .., last] => {
                first.contains_range(start_low..=u16::MAX)
                    && rest.iter().all(|container| container.is_full())
                    && last.contains_range(0..=end_low)
            }
            _ => unreachable!("already validated containers has at least 2 items"),
        }
    }

    /// Returns the number of elements in this set which are in the passed range.
    ///
    /// # Examples
    ///
    /// ```
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert_range(0x10000..0x40000);
    /// rb.insert(0x50001);
    /// rb.insert(0x50005);
    /// rb.insert(u32::MAX);
    ///
    /// assert_eq!(rb.range_cardinality(0..0x10000), 0);
    /// assert_eq!(rb.range_cardinality(0x10000..0x40000), 0x30000);
    /// assert_eq!(rb.range_cardinality(0x50000..0x60000), 2);
    /// assert_eq!(rb.range_cardinality(0x10000..0x10000), 0);
    /// assert_eq!(rb.range_cardinality(0x50000..=u32::MAX), 3);
    /// ```
    #[inline]
    pub fn range_cardinality<R>(&self, range: R) -> u64
    where
        R: RangeBounds<u32>,
    {
        let (start, end) = match util::convert_range_to_inclusive(range) {
            Ok(range) => (*range.start(), *range.end()),
            // Empty/invalid ranges have 0 bits set in them
            Err(_) => return 0,
        };

        let (start_key, start_low) = util::split(start);
        let (end_key, end_low) = util::split(end);

        let mut cardinality = 0;

        let i = match self.containers.binary_search_by_key(&start_key, |c| c.key) {
            Ok(i) => {
                let container = &self.containers[i];
                if start_key == end_key {
                    cardinality += container.rank(end_low)
                } else {
                    cardinality += container.len();
                }
                if start_low != 0 {
                    cardinality -= container.rank(start_low - 1);
                }
                i + 1
            }
            Err(i) => i,
        };
        for container in &self.containers[i..] {
            match container.key.cmp(&end_key) {
                Ordering::Less => cardinality += container.len(),
                Ordering::Equal => {
                    cardinality += container.rank(end_low);
                    break;
                }
                Ordering::Greater => {
                    break;
                }
            }
        }

        cardinality
    }

    /// Clears all integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.insert(1);
    /// assert_eq!(rb.contains(1), true);
    /// rb.clear();
    /// assert_eq!(rb.contains(1), false);
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.containers.clear();
    }

    /// Returns `true` if there are no integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.is_empty(), true);
    ///
    /// rb.insert(3);
    /// assert_eq!(rb.is_empty(), false);
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.containers.is_empty()
    }

    /// Returns `true` if there are every possible integers in this set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::full();
    /// assert!(!rb.is_empty());
    /// assert!(rb.is_full());
    /// ```
    #[inline]
    pub fn is_full(&self) -> bool {
        self.containers.len() == (u16::MAX as usize + 1)
            && self.containers.iter().all(Container::is_full)
    }

    /// Returns the number of distinct integers added to the set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.len(), 0);
    ///
    /// rb.insert(3);
    /// assert_eq!(rb.len(), 1);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.len(), 2);
    /// ```
    #[inline]
    pub fn len(&self) -> u64 {
        self.containers.iter().map(|container| container.len()).sum()
    }

    /// Returns the minimum value in the set (if the set is non-empty).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.min(), None);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.min(), Some(3));
    /// ```
    #[inline]
    pub fn min(&self) -> Option<u32> {
        self.containers.first().and_then(|tail| tail.min().map(|min| util::join(tail.key, min)))
    }

    /// Returns the maximum value in the set (if the set is non-empty).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.max(), None);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.max(), Some(4));
    /// ```
    #[inline]
    pub fn max(&self) -> Option<u32> {
        self.containers.last().and_then(|tail| tail.max().map(|max| util::join(tail.key, max)))
    }

    /// Returns the number of integers that are <= value. rank(u32::MAX) == len()
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.rank(0), 0);
    ///
    /// rb.insert(3);
    /// rb.insert(4);
    /// assert_eq!(rb.rank(3), 1);
    /// assert_eq!(rb.rank(10), 2)
    /// ```
    #[inline]
    pub fn rank(&self, value: u32) -> u64 {
        // if len becomes cached for RoaringBitmap: return len if len > value

        let (key, index) = util::split(value);

        match self.containers.binary_search_by_key(&key, |c| c.key) {
            Ok(i) => {
                // For optimal locality of reference:
                //  * container[i] should be a cache hit after binary search, rank it first
                //  * sum in reverse to avoid cache misses near i
                unsafe { self.containers.get_unchecked(i) }.rank(index)
                    + self.containers[..i].iter().rev().map(|c| c.len()).sum::<u64>()
            }
            Err(i) => self.containers[..i].iter().map(|c| c.len()).sum(),
        }
    }

    /// Returns the `n`th integer in the set or `None` if `n >= len()`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.select(0), None);
    ///
    /// rb.append(vec![0, 10, 100]);
    ///
    /// assert_eq!(rb.select(0), Some(0));
    /// assert_eq!(rb.select(1), Some(10));
    /// assert_eq!(rb.select(2), Some(100));
    /// assert_eq!(rb.select(3), None);
    /// ```
    #[inline]
    pub fn select(&self, n: u32) -> Option<u32> {
        let mut n = n as u64;

        for container in &self.containers {
            let len = container.len();
            if len > n {
                return container
                    .store
                    .select(n as u16)
                    .map(|index| util::join(container.key, index));
            }
            n -= len;
        }

        None
    }

    /// Removes the `n` smallests values from this bitmap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::from_iter([1, 5, 7, 9]);
    /// rb.remove_smallest(2);
    /// assert_eq!(rb, RoaringBitmap::from_iter([7, 9]));
    ///
    /// let mut rb = RoaringBitmap::from_iter([1, 3, 7, 9]);
    /// rb.remove_smallest(2);
    /// assert_eq!(rb, RoaringBitmap::from_iter([7, 9]));
    #[inline]
    pub fn remove_smallest(&mut self, mut n: u64) {
        // remove containers up to the front of the target
        let position = self.containers.iter().position(|container| {
            let container_len = container.len();
            if container_len <= n {
                n -= container_len;
                false
            } else {
                true
            }
        });
        let position = position.unwrap_or(self.containers.len());
        if position > 0 {
            self.containers.drain(..position);
        }
        // remove data in containers if there are still targets for deletion
        if n > 0 && !self.containers.is_empty() {
            // container immediately before should have been deleted, so the target is 0 index
            self.containers[0].remove_smallest(n);
        }
    }

    /// Removes the `n` biggests values from this bitmap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::from_iter([1, 5, 7, 9]);
    /// rb.remove_biggest(2);
    /// assert_eq!(rb, RoaringBitmap::from_iter([1, 5]));
    /// rb.remove_biggest(1);
    /// assert_eq!(rb, RoaringBitmap::from_iter([1]));
    #[inline]
    pub fn remove_biggest(&mut self, mut n: u64) {
        // remove containers up to the back of the target
        let position = self.containers.iter().rposition(|container| {
            let container_len = container.len();
            if container_len <= n {
                n -= container_len;
                false
            } else {
                true
            }
        });
        // It is checked at the beginning of the function, so it is usually never an Err
        if let Some(position) = position {
            self.containers.drain(position + 1..);
            if n > 0 && !self.containers.is_empty() {
                self.containers[position].remove_biggest(n);
            }
        } else {
            self.containers.clear();
        }
    }

    /// Optimizes the container storage for this bitmap.
    /// Returns true if the container storage was modified, false if not.
    ///
    /// # Examples
    ///
    /// ```
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::from_iter(1000..100000);
    /// rb.optimize();
    /// ```
    pub fn optimize(&mut self) -> bool {
        let mut changed = false;
        for container in &mut self.containers {
            changed |= container.optimize()
        }
        changed
    }

    /// Removes run-length encoding even when it is more space efficient.
    ///
    /// Returns true if the container storage was modified, false if not.
    ///
    /// # Examples
    ///
    /// ```
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::from_iter(0..=10000);
    /// rb.optimize();
    /// assert!(rb.remove_run_compression());
    /// ```
    pub fn remove_run_compression(&mut self) -> bool {
        let mut changed = false;
        for container in &mut self.containers {
            changed |= container.remove_run_compression()
        }
        changed
    }

    /// Ensure the bitmap is internally valid
    ///
    /// This is useful for development, but is not needed for normal use:
    /// bitmaps should _always_ be internally valid.
    ///
    /// # Errors
    ///
    /// Returns an error if the bitmap is not valid, with a description of the problem.
    #[doc(hidden)]
    pub fn internal_validate(&self) -> Result<(), &'static str> {
        for window in self.containers.windows(2) {
            let [first, second] = window else { unreachable!() };
            if second.key <= first.key {
                return Err("keys are not strictly increasing");
            }
        }
        for container in &self.containers {
            container.store.internal_validate()?;
        }
        Ok(())
    }
}

impl Default for RoaringBitmap {
    fn default() -> RoaringBitmap {
        RoaringBitmap::new()
    }
}

impl Clone for RoaringBitmap {
    fn clone(&self) -> Self {
        RoaringBitmap { containers: self.containers.clone() }
    }

    fn clone_from(&mut self, other: &Self) {
        self.containers.clone_from(&other.containers);
    }
}

#[cfg(test)]
mod tests {
    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn insert_range(
            lo in 0u32..=65535, hi in 65536u32..=131071,
            checks in vec(0u32..=262143, 1000)
        ){
            let r = lo..hi;
            let mut b = RoaringBitmap::new();
            let inserted = b.insert_range(r.clone());
            if r.end > r.start {
                assert_eq!(inserted, r.end as u64 - r.start as u64);
            } else {
                assert_eq!(inserted, 0);
            }

            // Assert all values in the range are present
            for i in r.clone() {
                assert!(b.contains(i), "does not contain {i}");
            }

            // Run the check values looking for any false positives
            for i in checks {
                let bitmap_has = b.contains(i);
                let range_has = r.contains(&i);
                assert_eq!(
                    bitmap_has, range_has,
                    "value {i} in bitmap={bitmap_has} and range={range_has}"
                );
            }
        }
    }

    #[test]
    fn test_insert_remove_range_same_container() {
        let mut b = RoaringBitmap::new();
        let inserted = b.insert_range(1..5);
        assert_eq!(inserted, 4);

        for i in 1..5 {
            assert!(b.contains(i));
        }

        let removed = b.remove_range(2..10);
        assert_eq!(removed, 3);
        assert!(b.contains(1));
        for i in 2..5 {
            assert!(!b.contains(i));
        }
    }

    #[test]
    fn test_insert_remove_range_pre_populated() {
        let mut b = RoaringBitmap::new();
        let inserted = b.insert_range(1..20_000);
        assert_eq!(inserted, 19_999);

        let removed = b.remove_range(10_000..21_000);
        assert_eq!(removed, 10_000);

        let inserted = b.insert_range(1..20_000);
        assert_eq!(inserted, 10_000);
    }

    #[test]
    fn test_insert_max_u32() {
        let mut b = RoaringBitmap::new();
        let inserted = b.insert(u32::MAX);
        // We are allowed to add u32::MAX
        assert!(inserted);
    }

    #[test]
    fn test_insert_remove_across_container() {
        let mut b = RoaringBitmap::new();
        let inserted = b.insert_range(u16::MAX as u32..=u16::MAX as u32 + 1);
        assert_eq!(inserted, 2);

        assert_eq!(b.containers.len(), 2);

        let removed = b.remove_range(u16::MAX as u32 + 1..=u16::MAX as u32 + 1);
        assert_eq!(removed, 1);

        assert_eq!(b.containers.len(), 1);
    }

    #[test]
    fn test_insert_remove_single_element() {
        let mut b = RoaringBitmap::new();
        let inserted = b.insert_range(u16::MAX as u32 + 1..=u16::MAX as u32 + 1);
        assert_eq!(inserted, 1);

        assert_eq!(b.containers[0].len(), 1);
        assert_eq!(b.containers.len(), 1);

        let removed = b.remove_range(u16::MAX as u32 + 1..=u16::MAX as u32 + 1);
        assert_eq!(removed, 1);

        assert_eq!(b.containers.len(), 0);
    }

    #[test]
    fn test_insert_remove_range_multi_container() {
        let mut bitmap = RoaringBitmap::new();
        assert_eq!(bitmap.insert_range(0..((1_u32 << 16) + 1)), (1_u64 << 16) + 1);
        assert_eq!(bitmap.containers.len(), 2);
        assert_eq!(bitmap.containers[0].key, 0);
        assert_eq!(bitmap.containers[1].key, 1);
        assert_eq!(bitmap.insert_range(0..((1_u32 << 16) + 1)), 0);

        assert!(bitmap.insert((1_u32 << 16) * 4));
        assert_eq!(bitmap.containers.len(), 3);
        assert_eq!(bitmap.containers[2].key, 4);

        assert_eq!(bitmap.remove_range(((1_u32 << 16) * 3)..=((1_u32 << 16) * 4)), 1);
        assert_eq!(bitmap.containers.len(), 2);
    }

    #[test]
    fn insert_range_single() {
        let mut bitmap = RoaringBitmap::new();
        assert_eq!(bitmap.insert_range((1_u32 << 16)..(2_u32 << 16)), 1_u64 << 16);
        assert_eq!(bitmap.containers.len(), 1);
        assert_eq!(bitmap.containers[0].key, 1);
    }

    #[test]
    fn remove_smallest_for_vec() {
        let mut bitmap = RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]);
        bitmap.remove_smallest(3);
        assert_eq!(bitmap.len(), 3);
        assert_eq!(bitmap, RoaringBitmap::from_iter([7, 9, 11]));

        bitmap = RoaringBitmap::from_iter([1, 2, 5, 7, 9, 11]);
        bitmap.remove_smallest(3);
        assert_eq!(bitmap.len(), 3);
        assert_eq!(bitmap, RoaringBitmap::from_iter([7, 9, 11]));

        bitmap = RoaringBitmap::from_iter([1, 3]);
        bitmap.remove_smallest(2);
        assert_eq!(bitmap.len(), 0);

        bitmap = RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]);
        bitmap.remove_smallest(0);
        assert_eq!(bitmap.len(), 6);
        assert_eq!(bitmap, RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]));

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..(1_u32 << 16) + 5);
        bitmap.remove_smallest(65537);
        assert_eq!(bitmap.len(), 4);
        assert_eq!(bitmap, RoaringBitmap::from_iter([65537, 65538, 65539, 65540]));

        bitmap = RoaringBitmap::from_iter([1, 2, 5, 7, 9, 11]);
        bitmap.remove_smallest(7);
        assert_eq!(bitmap, RoaringBitmap::default());
    }

    #[test]
    fn remove_smallest_for_bit() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..4098);
        bitmap.remove_smallest(4095);
        assert_eq!(bitmap.len(), 3);
        // removed bit to vec
        assert_eq!(bitmap, RoaringBitmap::from_iter([4095, 4096, 4097]));

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..6000);
        bitmap.remove_smallest(999);
        assert_eq!(bitmap.len(), 5001);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..8000);
        bitmap.remove_smallest(10);
        assert_eq!(bitmap.len(), 7990);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..200000);
        bitmap.remove_smallest(2000);
        assert_eq!(bitmap.len(), 198000);
        assert_eq!(bitmap, RoaringBitmap::from_iter(2000..200000));

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..2);
        bitmap.insert_range(4..7);
        bitmap.insert_range(1000..6000);
        bitmap.remove_smallest(30);
        assert_eq!(bitmap.len(), 4975);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..65535);
        bitmap.remove_smallest(0);
        assert_eq!(bitmap.len(), 65535);
    }

    #[test]
    fn remove_biggest_for_bit() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..5000);
        bitmap.remove_biggest(1000);
        assert_eq!(bitmap.len(), 4000);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..6000);
        bitmap.remove_biggest(1000);
        assert_eq!(bitmap.len(), 5000);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..200000);
        bitmap.remove_biggest(196000);
        assert_eq!(bitmap.len(), 4000);

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..200000);
        bitmap.remove_biggest(2000);
        assert_eq!(bitmap.len(), 198000);
        assert_eq!(bitmap, RoaringBitmap::from_iter(0..198000));

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..65535);
        bitmap.remove_biggest(0);
        assert_eq!(bitmap.len(), 65535);
    }

    #[test]
    fn remove_biggest_for_vec() {
        let mut bitmap = RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]);
        bitmap.remove_biggest(2);
        assert_eq!(bitmap, RoaringBitmap::from_iter([1, 2, 3, 7]));

        bitmap = RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]);
        bitmap.remove_biggest(6);
        assert_eq!(bitmap.len(), 0);

        bitmap = RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]);
        bitmap.remove_biggest(0);
        assert_eq!(bitmap.len(), 6);
        assert_eq!(bitmap, RoaringBitmap::from_iter([1, 2, 3, 7, 9, 11]));

        bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..(1_u32 << 16) + 5);
        bitmap.remove_biggest(65537);
        assert_eq!(bitmap.len(), 4);
        assert_eq!(bitmap, RoaringBitmap::from_iter([0, 1, 2, 3]));

        let mut bitmap = RoaringBitmap::from_iter([1, 2, 3]);
        bitmap.remove_biggest(4);
        assert_eq!(bitmap, RoaringBitmap::default());
    }
}
