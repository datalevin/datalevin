use core::borrow::Borrow;
use core::cmp::Ordering;
use core::fmt::{Display, Formatter};
use core::mem::size_of;
use core::ops::{BitAndAssign, BitOrAssign, BitXorAssign, RangeInclusive, SubAssign};

use super::{ArrayStore, Interval};

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::mem;

pub const BITMAP_LENGTH: usize = 1024;
pub const BITMAP_BYTES: usize = BITMAP_LENGTH * 8;

#[derive(Clone, Eq, PartialEq)]
pub struct BitmapStore {
    len: u64,
    bits: Box<[u64; BITMAP_LENGTH]>,
}

impl BitmapStore {
    pub fn new() -> BitmapStore {
        BitmapStore { len: 0, bits: Box::new([0; BITMAP_LENGTH]) }
    }

    pub fn capacity(&self) -> usize {
        BITMAP_LENGTH * u64::BITS as usize
    }

    pub fn try_from(len: u64, bits: Box<[u64; BITMAP_LENGTH]>) -> Result<BitmapStore, Error> {
        let actual_len = bits.iter().map(|v| v.count_ones() as u64).sum();
        if len != actual_len {
            Err(Error { kind: ErrorKind::Cardinality { expected: len, actual: actual_len } })
        } else {
            Ok(BitmapStore { len, bits })
        }
    }

    pub fn from_lsb0_bytes_unchecked(bytes: &[u8], byte_offset: usize, bits_set: u64) -> Self {
        const BITMAP_BYTES: usize = BITMAP_LENGTH * size_of::<u64>();
        assert!(byte_offset.checked_add(bytes.len()).is_some_and(|sum| sum <= BITMAP_BYTES));

        // If we know we're writing the full bitmap, we can avoid the initial memset to 0
        let mut bits = if bytes.len() == BITMAP_BYTES {
            debug_assert_eq!(byte_offset, 0); // Must be true from the above assert

            // Safety: We've checked that the length is correct, and we use an unaligned load in case
            //         the bytes are not 8 byte aligned.
            // The optimizer can see through this, and avoid the double copy to copy directly into
            // the allocated box from bytes with memcpy
            let bytes_as_words =
                unsafe { bytes.as_ptr().cast::<[u64; BITMAP_LENGTH]>().read_unaligned() };
            Box::new(bytes_as_words)
        } else {
            let mut bits = Box::new([0u64; BITMAP_LENGTH]);
            // Safety: It's safe to reinterpret u64s as u8s because u8 has less alignment requirements,
            // and has no padding/uninitialized data.
            let dst = unsafe {
                core::slice::from_raw_parts_mut(bits.as_mut_ptr().cast::<u8>(), BITMAP_BYTES)
            };
            let dst = &mut dst[byte_offset..][..bytes.len()];
            dst.copy_from_slice(bytes);
            bits
        };

        if !cfg!(target_endian = "little") {
            // Convert all words we touched (even partially) to little-endian
            let start_word = byte_offset / size_of::<u64>();
            let end_word = (byte_offset + bytes.len()).div_ceil(size_of::<u64>());

            // The 0th byte is the least significant byte, so we've written the bytes in little-endian
            for word in &mut bits[start_word..end_word] {
                *word = u64::from_le(*word);
            }
        }

        Self::from_unchecked(bits_set, bits)
    }

    ///
    /// Create a new BitmapStore from a given len and bits array
    /// It is up to the caller to ensure len == cardinality of bits
    /// Favor `try_from` for cases in which this invariants should be checked
    ///
    /// # Panics
    ///
    /// When debug_assertions are enabled and the above invariant is not met
    pub fn from_unchecked(len: u64, bits: Box<[u64; BITMAP_LENGTH]>) -> BitmapStore {
        if cfg!(debug_assertions) {
            BitmapStore::try_from(len, bits).unwrap()
        } else {
            BitmapStore { len, bits }
        }
    }

    #[inline]
    pub fn insert(&mut self, index: u16) -> bool {
        let (key, bit) = (key(index), bit(index));
        let old_w = self.bits[key];
        let new_w = old_w | (1 << bit);
        let inserted = (old_w ^ new_w) >> bit; // 1 or 0
        self.bits[key] = new_w;
        self.len += inserted;
        inserted != 0
    }

    pub fn insert_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        let start = *range.start();
        let end = *range.end();

        let (start_key, start_bit) = (key(start), bit(start));
        let (end_key, end_bit) = (key(end), bit(end));

        // MSB > start_bit > end_bit > LSB
        if start_key == end_key {
            // Set the end_bit -> LSB to 1
            let mut mask = if end_bit == 63 { u64::MAX } else { (1 << (end_bit + 1)) - 1 };
            // Set MSB -> start_bit to 1
            mask &= !((1 << start_bit) - 1);

            let existed = (self.bits[start_key] & mask).count_ones();
            self.bits[start_key] |= mask;

            let inserted = u64::from(end - start + 1) - u64::from(existed);
            self.len += inserted;
            return inserted;
        }

        // Mask off the left-most bits (MSB -> start_bit)
        let mask = !((1 << start_bit) - 1);

        // Keep track of the number of bits that were already set to
        // return how many new bits were set later
        let mut existed = (self.bits[start_key] & mask).count_ones();

        self.bits[start_key] |= mask;

        // Set the full blocks, tracking the number of set bits
        for i in (start_key + 1)..end_key {
            existed += self.bits[i].count_ones();
            self.bits[i] = u64::MAX;
        }

        // Set the end bits in the last chunk (MSB -> end_bit)
        let mask = if end_bit == 63 { u64::MAX } else { (1 << (end_bit + 1)) - 1 };
        existed += (self.bits[end_key] & mask).count_ones();
        self.bits[end_key] |= mask;

        let inserted = end as u64 - start as u64 + 1 - existed as u64;
        self.len += inserted;
        inserted
    }

    pub fn push(&mut self, index: u16) -> bool {
        if self.max().is_none_or(|max| max < index) {
            self.insert(index);
            true
        } else {
            false
        }
    }

    ///
    /// Pushes `index` at the end of the store.
    /// It is up to the caller to have validated index > self.max()
    ///
    /// # Panics
    ///
    /// If debug_assertions enabled and index is > self.max()
    pub(crate) fn push_unchecked(&mut self, index: u16) {
        if cfg!(debug_assertions) {
            if let Some(max) = self.max() {
                assert!(index > max, "store max >= index")
            }
        }
        self.insert(index);
    }

    pub fn remove(&mut self, index: u16) -> bool {
        let (key, bit) = (key(index), bit(index));
        let old_w = self.bits[key];
        let new_w = old_w & !(1 << bit);
        let removed = (old_w ^ new_w) >> bit; // 0 or 1
        self.bits[key] = new_w;
        self.len -= removed;
        removed != 0
    }

    pub fn remove_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        let start = *range.start();
        let end = *range.end();

        let (start_key, start_bit) = (key(start), bit(start));
        let (end_key, end_bit) = (key(end), bit(end));

        if start_key == end_key {
            let mask = (u64::MAX << start_bit) & (u64::MAX >> (63 - end_bit));
            let removed = (self.bits[start_key] & mask).count_ones();
            self.bits[start_key] &= !mask;
            let removed = u64::from(removed);
            self.len -= removed;
            return removed;
        }

        let mut removed = 0;
        // start key bits
        removed += (self.bits[start_key] & (u64::MAX << start_bit)).count_ones();
        self.bits[start_key] &= !(u64::MAX << start_bit);
        // counts bits in between
        for word in &self.bits[start_key + 1..end_key] {
            removed += word.count_ones();
            // When popcnt is available zeroing in this loop is faster,
            // but we opt to perform reasonably on most cpus by zeroing after.
            // By doing that the compiler uses simd to count ones.
        }
        // do zeroing outside the loop
        for word in &mut self.bits[start_key + 1..end_key] {
            *word = 0;
        }
        // end key bits
        removed += (self.bits[end_key] & (u64::MAX >> (63 - end_bit))).count_ones();
        self.bits[end_key] &= !(u64::MAX >> (63 - end_bit));
        let removed = u64::from(removed);
        self.len -= removed;
        removed
    }

    pub fn contains(&self, index: u16) -> bool {
        self.bits[key(index)] & (1 << bit(index)) != 0
    }

    pub fn contains_range(&self, range: RangeInclusive<u16>) -> bool {
        let start = *range.start();
        let end = *range.end();
        if self.len() < u64::from(end - start) + 1 {
            return false;
        }

        let (start_i, start_bit) = (key(start), bit(start));
        let (end_i, end_bit) = (key(end), bit(end));

        // Create a mask to exclude the first `start_bit` bits
        // e.g. if we start at bit index 1, this will create a mask which includes all but the bit
        // at index 0.
        let start_mask = !((1 << start_bit) - 1);
        // We want to create a mask which includes the end_bit, so we create a mask of
        // `end_bit + 1` bits. `end_bit` will be between [0, 63], so we create a mask including
        // between [1, 64] bits. For example, if the last bit is the 0th bit, we make a mask with
        // only the 0th bit set (one bit).
        let end_mask = (!0) >> (64 - (end_bit + 1));

        match &self.bits[start_i..=end_i] {
            [] => unreachable!(),
            &[word] => word & (start_mask & end_mask) == (start_mask & end_mask),
            &[first, ref rest @ .., last] => {
                (first & start_mask) == start_mask
                    && rest.iter().all(|&word| word == !0)
                    && (last & end_mask) == end_mask
            }
        }
    }

    pub fn is_disjoint(&self, other: &BitmapStore) -> bool {
        self.bits.iter().zip(other.bits.iter()).all(|(&i1, &i2)| (i1 & i2) == 0)
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.bits.iter().zip(other.bits.iter()).all(|(&i1, &i2)| (i1 & i2) == i1)
    }

    pub(crate) fn to_array_store(&self) -> ArrayStore {
        let mut vec = Vec::with_capacity(self.len as usize);
        for (index, mut bit) in self.bits.iter().cloned().enumerate() {
            while bit != 0 {
                vec.push((u64::trailing_zeros(bit) + (64 * index as u32)) as u16);
                bit &= bit - 1;
            }
        }
        ArrayStore::from_vec_unchecked(vec)
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn min(&self) -> Option<u16> {
        self.bits
            .iter()
            .enumerate()
            .find(|&(_, &bit)| bit != 0)
            .map(|(index, bit)| (index * 64 + (bit.trailing_zeros() as usize)) as u16)
    }

    #[inline]
    pub fn max(&self) -> Option<u16> {
        self.bits
            .iter()
            .enumerate()
            .rev()
            .find(|&(_, &bit)| bit != 0)
            .map(|(index, bit)| (index * 64 + (63 - bit.leading_zeros() as usize)) as u16)
    }

    pub fn rank(&self, index: u16) -> u64 {
        let (key, bit) = (key(index), bit(index));

        self.bits[..key].iter().map(|v| v.count_ones() as u64).sum::<u64>()
            + (self.bits[key] << (63 - bit)).count_ones() as u64
    }

    pub fn select(&self, n: u16) -> Option<u16> {
        let mut n = n as u64;

        for (key, value) in self.bits.iter().cloned().enumerate() {
            let len = value.count_ones() as u64;
            if n < len {
                let index = select(value, n);
                return Some((64 * key as u64 + index) as u16);
            }
            n -= len;
        }

        None
    }

    pub fn intersection_len_bitmap(&self, other: &BitmapStore) -> u64 {
        self.bits.iter().zip(other.bits.iter()).map(|(&a, &b)| (a & b).count_ones() as u64).sum()
    }

    pub(crate) fn intersection_len_interval(&self, interval: &Interval) -> u64 {
        if interval.is_full() {
            return self.len();
        }
        let (start_id, start_bit) = (key(interval.start()), bit(interval.start()));
        let (end_id, end_bit) = (key(interval.end()), bit(interval.end()));
        let mut amount: u64 = 0;
        for (i, mut cur_bit) in self.bits[start_id..=end_id].iter().copied().enumerate() {
            if i == 0 {
                cur_bit &= u64::MAX << start_bit;
            }
            if i == end_id - start_id {
                cur_bit &= u64::MAX >> (64 - end_bit - 1);
            }
            amount += u64::from(cur_bit.count_ones());
        }
        amount
    }

    pub(crate) fn intersection_len_array(&self, other: &ArrayStore) -> u64 {
        other
            .iter()
            .map(|&index| {
                let (key, bit) = (key(index), bit(index));
                let old_w = self.bits[key];
                let new_w = old_w & (1 << bit);
                new_w >> bit
            })
            .sum::<u64>()
    }

    pub fn iter(&self) -> BitmapIter<&[u64; BITMAP_LENGTH]> {
        BitmapIter::new(&self.bits)
    }

    pub fn into_iter(self) -> BitmapIter<Box<[u64; BITMAP_LENGTH]>> {
        BitmapIter::new(self.bits)
    }

    pub fn as_array(&self) -> &[u64; BITMAP_LENGTH] {
        &self.bits
    }

    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.len = 0;
    }

    /// Set N bits that are currently 1 bit from the lower bit to 0.
    pub fn remove_smallest(&mut self, mut clear_bits: u64) {
        if self.len() < clear_bits {
            self.clear();
            return;
        }
        self.len -= clear_bits;
        for word in self.bits.iter_mut() {
            let count = word.count_ones() as u64;
            if clear_bits < count {
                for _ in 0..clear_bits {
                    *word = *word & (*word - 1);
                }
                return;
            }
            *word = 0;
            clear_bits -= count;
            if clear_bits == 0 {
                return;
            }
        }
    }

    /// Set N bits that are currently 1 bit from the lower bit to 0.
    pub fn remove_biggest(&mut self, mut clear_bits: u64) {
        if self.len() < clear_bits {
            self.clear();
            return;
        }
        self.len -= clear_bits;
        for word in self.bits.iter_mut().rev() {
            let count = word.count_ones() as u64;
            if clear_bits < count {
                for _ in 0..clear_bits {
                    *word &= !(1 << (63 - word.leading_zeros()));
                }
                return;
            }
            *word = 0;
            clear_bits -= count;
            if clear_bits == 0 {
                return;
            }
        }
    }

    pub(crate) fn internal_validate(&self) -> Result<(), &'static str> {
        let expected_len: u64 = self.bits.iter().map(|bits| u64::from(bits.count_ones())).sum();
        if self.len != expected_len {
            return Err("bitmap cardinality is incorrect");
        }
        if self.len <= super::ARRAY_LIMIT {
            return Err("cardinality is too small for a bitmap container");
        }
        Ok(())
    }
}

// this can be done in 3 instructions on x86-64 with bmi2 with: tzcnt(pdep(1 << rank, value))
// if n > value.count_ones() this method returns 0
fn select(mut value: u64, n: u64) -> u64 {
    // reset n of the least significant bits
    for _ in 0..n {
        value &= value - 1;
    }
    value.trailing_zeros() as u64
}

impl Default for BitmapStore {
    fn default() -> Self {
        BitmapStore::new()
    }
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

#[derive(Debug)]
pub enum ErrorKind {
    Cardinality { expected: u64, actual: u64 },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self.kind {
            ErrorKind::Cardinality { expected, actual } => {
                write!(f, "Expected cardinality was {expected} but was {actual}")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

#[derive(Clone)]
pub struct BitmapIter<B: Borrow<[u64; BITMAP_LENGTH]>> {
    key: u16,
    value: u64,
    key_back: u16,
    // If key_back <= key, current back value is actually in `value`
    value_back: u64,
    bits: B,
}

impl<B: Borrow<[u64; BITMAP_LENGTH]>> BitmapIter<B> {
    fn new(bits: B) -> BitmapIter<B> {
        BitmapIter {
            key: 0,
            value: bits.borrow()[0],
            key_back: BITMAP_LENGTH as u16 - 1,
            value_back: bits.borrow()[BITMAP_LENGTH - 1],
            bits,
        }
    }

    /// Advance the iterator to the first value greater than or equal to `n`.
    pub(crate) fn advance_to(&mut self, index: u16) {
        let new_key = key(index) as u16;
        let value = match new_key.cmp(&self.key) {
            Ordering::Less => return,
            Ordering::Equal => self.value,
            Ordering::Greater => {
                let bits = self.bits.borrow();
                let cmp = new_key.cmp(&self.key_back);
                // Match arms can be reordered, this ordering is perf sensitive
                if cmp == Ordering::Less {
                    // new_key is > self.key, < self.key_back, so it must be in bounds
                    unsafe { *bits.get_unchecked(new_key as usize) }
                } else if cmp == Ordering::Equal {
                    self.value_back
                } else {
                    // New key is greater than original key and key_back, this iterator is now empty
                    self.key = self.key_back;
                    self.value = 0;
                    self.value_back = 0;
                    return;
                }
            }
        };
        let bit = bit(index);
        let low_bits = (1 << bit) - 1;

        self.key = new_key;
        self.value = value & !low_bits;
    }

    /// Advance the back of iterator to the first value less than or equal to `n`.
    pub(crate) fn advance_back_to(&mut self, index: u16) {
        let new_key = key(index) as u16;
        let (value, dst) = match new_key.cmp(&self.key_back) {
            Ordering::Greater => return,
            Ordering::Equal => {
                let dst =
                    if self.key_back <= self.key { &mut self.value } else { &mut self.value_back };
                (*dst, dst)
            }
            Ordering::Less => {
                let bits = self.bits.borrow();
                let cmp = new_key.cmp(&self.key);
                // Match arms can be reordered, this ordering is perf sensitive
                if cmp == Ordering::Greater {
                    // new_key is > self.key, < self.key_back, so it must be in bounds
                    let value = unsafe { *bits.get_unchecked(new_key as usize) };
                    (value, &mut self.value_back)
                } else if cmp == Ordering::Equal {
                    (self.value, &mut self.value)
                } else {
                    // New key is less than original key and key_back, this iterator is now empty
                    self.key_back = self.key;
                    self.value = 0;
                    self.value_back = 0;
                    return;
                }
            }
        };
        let bit = bit(index);
        let low_bits = u64::MAX >> (64 - bit - 1);

        self.key_back = new_key;
        *dst = value & low_bits;
    }

    pub(crate) fn next_range(&mut self) -> Option<RangeInclusive<u16>> {
        let value = *advance_to_next_nonzero_word(
            &mut self.key,
            &mut self.value,
            self.bits.borrow(),
            &mut self.key_back,
            &mut self.value_back,
        )?;
        let offset = value.trailing_zeros() as u16;
        let start = self.key * 64 + offset;
        let value = value >> offset;
        let num_set = value.trailing_ones() as u16;
        let mut end_inclusive = start + (num_set - 1);
        if num_set + offset != 64 {
            self.value &= !0 << (num_set + offset);
            return Some(start..=end_inclusive);
        }
        self.value = 0;
        if self.key == self.key_back {
            return Some(start..=end_inclusive);
        }
        loop {
            debug_assert!(self.key < self.key_back);
            self.key += 1;
            self.value = if self.key == self.key_back {
                mem::replace(&mut self.value_back, 0)
            } else {
                // Safety:
                //   - self.key and self.key_back are always kept in bounds
                unsafe { *self.bits.borrow().get_unchecked(self.key as usize) }
            };
            let set_bits = self.value.trailing_ones() as u16;
            end_inclusive += set_bits;
            if set_bits != 64 || self.key == self.key_back {
                if set_bits != 64 {
                    self.value &= !0 << set_bits;
                } else {
                    self.value = 0;
                }
                return Some(start..=end_inclusive);
            }
        }
    }

    pub(crate) fn next_range_back(&mut self) -> Option<RangeInclusive<u16>> {
        let value_dst = advance_back_to_next_nonzero_word(
            &mut self.key,
            &mut self.value,
            self.bits.borrow(),
            &mut self.key_back,
            &mut self.value_back,
        )?;

        let end_offset = value_dst.leading_zeros() as u16;
        let end_inclusive = self.key_back * 64 + (63 - end_offset);
        let value = *value_dst << end_offset;
        let num_set = value.leading_ones() as u16;
        let mut start = end_inclusive - (num_set - 1);
        if num_set + end_offset != 64 {
            *value_dst &= !0 >> (num_set + end_offset);
            return Some(start..=end_inclusive);
        }
        *value_dst = 0;
        if self.key == self.key_back {
            return Some(start..=end_inclusive);
        }
        loop {
            debug_assert!(self.key_back > self.key);
            self.key_back -= 1;
            let value_dst = if self.key_back == self.key {
                &mut self.value
            } else {
                // Safety:
                //   - self.key and self.key_back are always kept in bounds
                let value = unsafe { *self.bits.borrow().get_unchecked(self.key_back as usize) };
                self.value_back = value;
                &mut self.value_back
            };
            let set_bits = value_dst.leading_ones() as u16;
            start -= set_bits;
            if set_bits != 64 || self.key_back == self.key {
                if set_bits != 64 {
                    *value_dst &= !0 >> set_bits;
                } else {
                    *value_dst = 0;
                }
                return Some(start..=end_inclusive);
            }
        }
    }

    pub(crate) fn peek(&self) -> Option<u16> {
        let mut key = self.key;
        let mut value = self.value;
        let mut key_back = self.key_back;
        let mut value_back = self.value_back;
        let value = advance_to_next_nonzero_word(
            &mut key,
            &mut value,
            self.bits.borrow(),
            &mut key_back,
            &mut value_back,
        )?;

        let index = value.trailing_zeros() as u16;
        Some(64 * key + index)
    }

    pub(crate) fn peek_back(&self) -> Option<u16> {
        let mut key = self.key;
        let mut key_back = self.key_back;
        let mut value = self.value;
        let mut value_back = self.value_back;
        let value = advance_back_to_next_nonzero_word(
            &mut key,
            &mut value,
            self.bits.borrow(),
            &mut key_back,
            &mut value_back,
        )?;
        let index_from_left = value.leading_zeros() as u16;
        let index = 63 - index_from_left;
        Some(64 * key_back + index)
    }
}

fn advance_to_next_nonzero_word<'a>(
    key: &mut u16,
    value: &'a mut u64,
    bits: &[u64; BITMAP_LENGTH],
    key_back: &mut u16,
    value_back: &'a mut u64,
) -> Option<&'a mut u64> {
    if *value == 0 {
        if *key >= *key_back {
            return None;
        }
        loop {
            debug_assert!(*key < *key_back);
            *key += 1;
            if *key == *key_back {
                *value = mem::replace(value_back, 0);
                if *value == 0 {
                    return None;
                }
                break;
            }
            // Safety:
            //   - self.key and self.key_back are always kept in bounds
            *value = unsafe { *bits.get_unchecked(*key as usize) };
            if *value != 0 {
                break;
            }
        }
    }
    debug_assert!(*value != 0);
    Some(value)
}

fn advance_back_to_next_nonzero_word<'a>(
    key: &mut u16,
    value: &'a mut u64,
    bits: &[u64; BITMAP_LENGTH],
    key_back: &mut u16,
    value_back: &'a mut u64,
) -> Option<&'a mut u64> {
    if *key_back > *key {
        if *value_back != 0 {
            return Some(value_back);
        }
        loop {
            debug_assert!(key_back > key);
            *key_back -= 1;
            if *key_back == *key {
                break;
            }
            // Safety:
            //   - self.key and self.key_back are always kept in bounds
            *value_back = unsafe { *bits.get_unchecked(*key_back as usize) };
            if *value_back != 0 {
                return Some(value_back);
            }
        }
    }
    debug_assert!(*key_back == *key);
    if *value != 0 {
        Some(value)
    } else {
        None
    }
}

impl<B: Borrow<[u64; BITMAP_LENGTH]>> Iterator for BitmapIter<B> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        let value = advance_to_next_nonzero_word(
            &mut self.key,
            &mut self.value,
            self.bits.borrow(),
            &mut self.key_back,
            &mut self.value_back,
        )?;
        let index = value.trailing_zeros() as u16;
        *value &= *value - 1;
        Some(64 * self.key + index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut len: u32 = self.value.count_ones();
        if self.key < self.key_back {
            for v in &self.bits.borrow()[self.key as usize + 1..self.key_back as usize] {
                len += v.count_ones();
            }
            len += self.value_back.count_ones();
        }
        (len as usize, Some(len as usize))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.len()
    }
}

impl<B: Borrow<[u64; BITMAP_LENGTH]>> DoubleEndedIterator for BitmapIter<B> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let value_dst = advance_back_to_next_nonzero_word(
            &mut self.key,
            &mut self.value,
            self.bits.borrow(),
            &mut self.key_back,
            &mut self.value_back,
        )?;
        let index_from_left = value_dst.leading_zeros() as u16;
        let index = 63 - index_from_left;
        *value_dst &= !(1 << index);
        Some(64 * self.key_back + index)
    }
}

impl<B: Borrow<[u64; BITMAP_LENGTH]>> ExactSizeIterator for BitmapIter<B> {}

#[inline]
pub fn key(index: u16) -> usize {
    index as usize / 64
}

#[inline]
pub fn bit(index: u16) -> usize {
    index as usize % 64
}

#[inline]
fn op_bitmaps(bits1: &mut BitmapStore, bits2: &BitmapStore, op: impl Fn(&mut u64, u64)) {
    bits1.len = 0;
    for (index1, &index2) in bits1.bits.iter_mut().zip(bits2.bits.iter()) {
        op(index1, index2);
        bits1.len += index1.count_ones() as u64;
    }
}

impl BitOrAssign<&Self> for BitmapStore {
    fn bitor_assign(&mut self, rhs: &Self) {
        op_bitmaps(self, rhs, BitOrAssign::bitor_assign);
    }
}

impl BitOrAssign<&ArrayStore> for BitmapStore {
    fn bitor_assign(&mut self, rhs: &ArrayStore) {
        for &index in rhs.iter() {
            let (key, bit) = (key(index), bit(index));
            let old_w = self.bits[key];
            let new_w = old_w | (1 << bit);
            self.len += (old_w ^ new_w) >> bit;
            self.bits[key] = new_w;
        }
    }
}

impl BitAndAssign<&Self> for BitmapStore {
    fn bitand_assign(&mut self, rhs: &Self) {
        op_bitmaps(self, rhs, BitAndAssign::bitand_assign);
    }
}

impl SubAssign<&Self> for BitmapStore {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn sub_assign(&mut self, rhs: &Self) {
        op_bitmaps(self, rhs, |l, r| *l &= !r);
    }
}

impl SubAssign<&ArrayStore> for BitmapStore {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn sub_assign(&mut self, rhs: &ArrayStore) {
        for &index in rhs.iter() {
            let (key, bit) = (key(index), bit(index));
            let old_w = self.bits[key];
            let new_w = old_w & !(1 << bit);
            self.len -= (old_w ^ new_w) >> bit;
            self.bits[key] = new_w;
        }
    }
}

impl BitXorAssign<&Self> for BitmapStore {
    fn bitxor_assign(&mut self, rhs: &Self) {
        op_bitmaps(self, rhs, BitXorAssign::bitxor_assign);
    }
}

impl BitXorAssign<&ArrayStore> for BitmapStore {
    fn bitxor_assign(&mut self, rhs: &ArrayStore) {
        let mut len = self.len as i64;
        for &index in rhs.iter() {
            let (key, bit) = (key(index), bit(index));
            let old_w = self.bits[key];
            let new_w = old_w ^ (1 << bit);
            len += 1 - 2 * (((1 << bit) & old_w) >> bit) as i64; // +1 or -1
            self.bits[key] = new_w;
        }
        self.len = len as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_remove_smallest() {
        let mut store = BitmapStore::new();
        let range = RangeInclusive::new(1, 3);
        store.insert_range(range);
        let range_second = RangeInclusive::new(5, 65535);
        // store.bits[0] = 0b1111111111111111111111111111111111111111111111111111111111101110
        store.insert_range(range_second);
        store.remove_smallest(2);
        assert_eq!(
            store.bits[0],
            0b1111111111111111111111111111111111111111111111111111111111101000
        );
    }

    #[test]
    fn test_bitmap_remove_biggest() {
        let mut store = BitmapStore::new();
        let range = RangeInclusive::new(1, 3);
        store.insert_range(range);
        let range_second = RangeInclusive::new(5, 65535);
        // store.bits[1023] = 0b1111111111111111111111111111111111111111111111111111111111111111
        store.insert_range(range_second);
        store.remove_biggest(2);
        assert_eq!(
            store.bits[1023],
            0b11111111111111111111111111111111111111111111111111111111111111
        );
    }
}
