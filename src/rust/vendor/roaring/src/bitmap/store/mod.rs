mod array_store;
mod bitmap_store;
mod interval_store;

use alloc::vec;
use core::mem;
use core::ops::{
    BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, RangeInclusive, Sub, SubAssign,
};
use core::slice;

pub use self::bitmap_store::{BITMAP_BYTES, BITMAP_LENGTH};
use self::Store::{Array, Bitmap, Run};

pub(crate) use self::array_store::ArrayStore;
pub use self::bitmap_store::{BitmapIter, BitmapStore};
pub(crate) use self::interval_store::Interval;
pub(crate) use interval_store::{IntervalStore, RunIterBorrowed, RunIterOwned};
#[cfg(feature = "std")]
pub(crate) use interval_store::{RUN_ELEMENT_BYTES, RUN_NUM_BYTES};

use crate::bitmap::container::ARRAY_LIMIT;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

#[derive(Clone)]
pub(crate) enum Store {
    Array(ArrayStore),
    Bitmap(BitmapStore),
    Run(IntervalStore),
}

#[derive(Clone)]
pub(crate) enum Iter<'a> {
    Array(slice::Iter<'a, u16>),
    Vec(vec::IntoIter<u16>),
    BitmapBorrowed(BitmapIter<&'a [u64; BITMAP_LENGTH]>),
    BitmapOwned(BitmapIter<Box<[u64; BITMAP_LENGTH]>>),
    RunBorrowed(RunIterBorrowed<'a>),
    RunOwned(RunIterOwned),
}

impl Store {
    pub fn new() -> Store {
        Store::Array(ArrayStore::new())
    }

    #[cfg(feature = "std")]
    pub fn with_capacity(capacity: usize) -> Store {
        if capacity <= ARRAY_LIMIT as usize {
            Store::Array(ArrayStore::with_capacity(capacity))
        } else {
            Store::Bitmap(BitmapStore::new())
        }
    }

    pub fn full() -> Store {
        Store::Run(IntervalStore::full())
    }

    pub fn from_lsb0_bytes(bytes: &[u8], byte_offset: usize) -> Option<Self> {
        assert!(byte_offset + bytes.len() <= BITMAP_LENGTH * mem::size_of::<u64>());

        // It seems to be pretty considerably faster to count the bits
        // using u64s than for each byte
        let bits_set = {
            let mut bits_set = 0;
            let (chunks, remainder) = bytes.as_chunks::<{ mem::size_of::<u64>() }>();
            for chunk in chunks {
                let chunk = u64::from_ne_bytes(*chunk);
                bits_set += u64::from(chunk.count_ones());
            }
            for byte in remainder {
                bits_set += u64::from(byte.count_ones());
            }
            bits_set
        };
        if bits_set == 0 {
            return None;
        }

        Some(if bits_set < ARRAY_LIMIT {
            Array(ArrayStore::from_lsb0_bytes(bytes, byte_offset, bits_set))
        } else {
            Bitmap(BitmapStore::from_lsb0_bytes_unchecked(bytes, byte_offset, bits_set))
        })
    }

    #[inline]
    pub fn insert(&mut self, index: u16) -> bool {
        match self {
            Array(vec) => vec.insert(index),
            Bitmap(bits) => bits.insert(index),
            Run(runs) => runs.insert(index),
        }
    }

    pub fn insert_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        // A Range is defined as being of size 0 if start >= end.
        if range.is_empty() {
            return 0;
        }

        match self {
            Array(vec) => vec.insert_range(range),
            Bitmap(bits) => bits.insert_range(range),
            Run(runs) => runs.insert_range(range),
        }
    }

    /// Push `index` at the end of the store only if `index` is the new max.
    ///
    /// Returns whether `index` was effectively pushed.
    pub fn push(&mut self, index: u16) -> bool {
        match self {
            Array(vec) => vec.push(index),
            Bitmap(bits) => bits.push(index),
            Run(runs) => runs.push(index),
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
        match self {
            Array(vec) => vec.push_unchecked(index),
            Bitmap(bits) => bits.push_unchecked(index),
            Run(runs) => {
                // push unchecked for intervals doesn't make sense since we have to check anyways to
                // intervals and such when the index is consecutive
                debug_assert!(runs.max().map(|f| f < index).unwrap_or(true));
                runs.push(index);
            }
        }
    }

    pub fn remove(&mut self, index: u16) -> bool {
        match self {
            Array(vec) => vec.remove(index),
            Bitmap(bits) => bits.remove(index),
            Run(runs) => runs.remove(index),
        }
    }

    pub fn remove_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        if range.is_empty() {
            return 0;
        }

        match self {
            Array(vec) => vec.remove_range(range),
            Bitmap(bits) => bits.remove_range(range),
            Run(runs) => runs.remove_range(range),
        }
    }

    pub fn remove_smallest(&mut self, index: u64) {
        match self {
            Array(vec) => vec.remove_smallest(index),
            Bitmap(bits) => bits.remove_smallest(index),
            Run(runs) => runs.remove_smallest(index),
        }
    }

    pub fn remove_biggest(&mut self, index: u64) {
        match self {
            Array(vec) => vec.remove_biggest(index),
            Bitmap(bits) => bits.remove_biggest(index),
            Run(runs) => runs.remove_biggest(index),
        }
    }

    pub fn contains(&self, index: u16) -> bool {
        match self {
            Array(vec) => vec.contains(index),
            Bitmap(bits) => bits.contains(index),
            Run(intervals) => intervals.contains(index),
        }
    }

    pub fn contains_range(&self, range: RangeInclusive<u16>) -> bool {
        match self {
            Array(vec) => vec.contains_range(range),
            Bitmap(bits) => bits.contains_range(range),
            Run(runs) => runs.contains_range(range),
        }
    }

    pub fn is_full(&self) -> bool {
        self.len() == (1 << 16)
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        match (self, other) {
            (Array(vec1), Array(vec2)) => vec1.is_disjoint(vec2),
            (Bitmap(bits1), Bitmap(bits2)) => bits1.is_disjoint(bits2),
            (Array(vec), Bitmap(bits)) | (Bitmap(bits), Array(vec)) => {
                vec.iter().all(|&i| !bits.contains(i))
            }
            (Run(intervals1), Run(intervals2)) => intervals1.is_disjoint(intervals2),
            (Run(runs), Array(vec)) | (Array(vec), Run(runs)) => runs.is_disjoint_array(vec),
            (Run(intervals), Bitmap(bitmap)) | (Bitmap(bitmap), Run(intervals)) => {
                intervals.is_disjoint_bitmap(bitmap)
            }
        }
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        match (self, other) {
            (Array(vec1), Array(vec2)) => vec1.is_subset(vec2),
            (Bitmap(bits1), Bitmap(bits2)) => bits1.is_subset(bits2),
            (Array(vec), Bitmap(bits)) => vec.iter().all(|&i| bits.contains(i)),
            (Bitmap(..), &Array(..)) => false,
            (Array(vec), Run(runs)) => vec.iter().all(|&i| runs.contains(i)),
            (Bitmap(bitmap), Run(runs)) => bitmap.iter().all(|i| runs.contains(i)),
            (Run(intervals1), Run(intervals2)) => intervals1.is_subset(intervals2),
            (Run(intervals), Array(vec)) => intervals.is_subset_array(vec),
            (Run(intervals), Bitmap(bitmap)) => intervals.is_subset_bitmap(bitmap),
        }
    }

    pub fn intersection_len(&self, other: &Self) -> u64 {
        match (self, other) {
            (Array(vec1), Array(vec2)) => vec1.intersection_len(vec2),
            (Bitmap(bits1), Bitmap(bits2)) => bits1.intersection_len_bitmap(bits2),
            (Array(vec), Bitmap(bits)) => bits.intersection_len_array(vec),
            (Bitmap(bits), Array(vec)) => bits.intersection_len_array(vec),
            (Run(runs), Array(vec)) | (Array(vec), Run(runs)) => runs.intersection_len_array(vec),
            (Run(runs), Bitmap(bitmap)) | (Bitmap(bitmap), Run(runs)) => {
                runs.intersection_len_bitmap(bitmap)
            }
            (Run(runs1), Run(runs2)) => runs1.intersection_len(runs2),
        }
    }

    pub fn len(&self) -> u64 {
        match self {
            Array(vec) => vec.len(),
            Bitmap(bits) => bits.len(),
            Run(intervals) => intervals.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Array(vec) => vec.is_empty(),
            Bitmap(bits) => bits.is_empty(),
            Run(runs) => runs.is_empty(),
        }
    }

    pub fn min(&self) -> Option<u16> {
        match self {
            Array(vec) => vec.min(),
            Bitmap(bits) => bits.min(),
            Run(runs) => runs.min(),
        }
    }

    #[inline]
    pub fn max(&self) -> Option<u16> {
        match self {
            Array(vec) => vec.max(),
            Bitmap(bits) => bits.max(),
            Run(runs) => runs.max(),
        }
    }

    pub fn rank(&self, index: u16) -> u64 {
        match self {
            Array(vec) => vec.rank(index),
            Bitmap(bits) => bits.rank(index),
            Run(runs) => runs.rank(index),
        }
    }

    pub fn select(&self, n: u16) -> Option<u16> {
        match self {
            Array(vec) => vec.select(n),
            Bitmap(bits) => bits.select(n),
            Run(runs) => runs.select(n),
        }
    }

    pub fn count_runs(&self) -> u64 {
        match self {
            Array(vec) => {
                vec.iter()
                    .fold((-2, 0u64), |(prev, runs), &v| {
                        let new = v as i32;
                        if prev + 1 != new {
                            (new, runs + 1)
                        } else {
                            (new, runs)
                        }
                    })
                    .1
            }
            Bitmap(bits) => {
                let mut num_runs = 0u64;

                for i in 0..BITMAP_LENGTH - 1 {
                    let word = bits.as_array()[i];
                    let next_word = bits.as_array()[i + 1];
                    num_runs +=
                        ((word << 1) & !word).count_ones() as u64 + ((word >> 63) & !next_word);
                }

                let last = bits.as_array()[BITMAP_LENGTH - 1];
                num_runs += ((last << 1) & !last).count_ones() as u64 + (last >> 63);
                num_runs
            }
            Run(intervals) => intervals.run_amount(),
        }
    }

    pub(crate) fn to_bitmap(&self) -> Store {
        match self {
            Array(arr) => Bitmap(arr.to_bitmap_store()),
            Bitmap(_) => self.clone(),
            Run(intervals) => Bitmap(intervals.to_bitmap()),
        }
    }

    pub(crate) fn to_run(&self) -> Self {
        match self {
            Array(vec) => {
                let mut intervals = IntervalStore::new();
                if let Some(mut start) = vec.as_slice().first().copied() {
                    for (idx, &v) in vec.as_slice()[1..].iter().enumerate() {
                        // subtract current and previous values, then check if the gap is too large
                        // for a run
                        if v - vec.as_slice()[idx] > 1 {
                            intervals.push_interval_unchecked(Interval::new_unchecked(
                                start,
                                vec.as_slice()[idx],
                            ));
                            start = v
                        }
                    }
                    intervals.push_interval_unchecked(Interval::new_unchecked(
                        start,
                        *vec.as_slice().last().unwrap(),
                    ));
                }
                Run(intervals)
            }
            Bitmap(bits) => {
                let mut current = bits.as_array()[0];
                let mut i = 0u16;
                let mut start;
                let mut last;

                let mut intervals = IntervalStore::new();

                loop {
                    // Skip over empty words
                    while current == 0 && i < BITMAP_LENGTH as u16 - 1 {
                        i += 1;
                        current = bits.as_array()[i as usize];
                    }
                    // Reached end of the bitmap without finding anymore bits set
                    if current == 0 {
                        break;
                    }
                    let current_start = current.trailing_zeros() as u16;
                    start = 64 * i + current_start;

                    // Pad LSBs with 1s
                    current |= current - 1;

                    // Find next 0
                    while current == u64::MAX && i < BITMAP_LENGTH as u16 - 1 {
                        i += 1;
                        current = bits.as_array()[i as usize];
                    }

                    // Run continues until end of this container
                    if current == u64::MAX {
                        intervals.push_interval_unchecked(Interval::new_unchecked(start, u16::MAX));
                        break;
                    }

                    let current_last = (!current).trailing_zeros() as u16;
                    last = 64 * i + current_last;
                    intervals.push_interval_unchecked(Interval::new_unchecked(start, last - 1));

                    // pad LSBs with 0s
                    current &= current + 1;
                }
                Run(intervals)
            }
            Run(intervals) => Run(intervals.clone()),
        }
    }

    pub(crate) fn internal_validate(&self) -> Result<(), &'static str> {
        match self {
            Array(vec) => vec.internal_validate(),
            Bitmap(bits) => bits.internal_validate(),
            Run(runs) => runs.internal_validate(),
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

impl BitOr<&Store> for &Store {
    type Output = Store;

    fn bitor(self, rhs: &Store) -> Store {
        match (self, rhs) {
            (Array(vec1), Array(vec2)) => Array(BitOr::bitor(vec1, vec2)),
            (&Bitmap(..), &Array(..)) => {
                let mut lhs = self.clone();
                BitOrAssign::bitor_assign(&mut lhs, rhs);
                lhs
            }
            (&Bitmap(..), &Bitmap(..)) => {
                let mut lhs = self.clone();
                BitOrAssign::bitor_assign(&mut lhs, rhs);
                lhs
            }
            (&Array(..), &Bitmap(..)) => {
                let mut rhs = rhs.clone();
                BitOrAssign::bitor_assign(&mut rhs, self);
                rhs
            }
            (Run(left), Run(right)) => {
                let (smallest, biggest) = if left.run_amount() > right.run_amount() {
                    (right, left)
                } else {
                    (left, right)
                };
                let mut res = biggest.clone();
                BitOrAssign::bitor_assign(&mut res, smallest);
                Run(res)
            }
            (Run(runs), Array(array)) | (Array(array), Run(runs)) => {
                let mut ret = runs.clone();
                BitOrAssign::bitor_assign(&mut ret, array);
                Run(ret)
            }
            (Run(runs), Bitmap(bitmap)) | (Bitmap(bitmap), Run(runs)) => {
                let mut ret = runs.to_bitmap();
                BitOrAssign::bitor_assign(&mut ret, bitmap);
                Bitmap(ret)
            }
        }
    }
}

impl BitOrAssign<Store> for Store {
    fn bitor_assign(&mut self, rhs: Store) {
        match (self, rhs) {
            (&mut Array(ref mut vec1), Array(ref vec2)) => {
                *vec1 = BitOr::bitor(&*vec1, vec2);
            }
            (&mut Bitmap(ref mut bits1), Array(ref vec2)) => {
                BitOrAssign::bitor_assign(bits1, vec2);
            }
            (&mut Bitmap(ref mut bits1), Bitmap(ref bits2)) => {
                BitOrAssign::bitor_assign(bits1, bits2);
            }
            (this @ &mut Bitmap(..), rhs @ Run(..)) => {
                let other = rhs.to_bitmap();
                BitOrAssign::bitor_assign(this, other);
            }
            (Run(intervals1), Run(intervals2)) => BitOrAssign::bitor_assign(intervals1, intervals2),
            (Run(intervals1), Array(ref vec)) => BitOrAssign::bitor_assign(intervals1, vec),
            (this @ Array(..), Run(mut intervals)) => {
                let Array(vec) = &this else { unreachable!() };
                BitOrAssign::bitor_assign(&mut intervals, vec);
                *this = Run(intervals);
            }
            (this @ Run(..), rhs @ Bitmap(..)) => {
                *this = this.to_bitmap();
                BitOrAssign::bitor_assign(this, rhs);
            }
            (this @ &mut Array(..), mut rhs @ Bitmap(..)) => {
                mem::swap(this, &mut rhs);
                BitOrAssign::bitor_assign(this, rhs);
            }
        }
    }
}

impl BitOrAssign<&Store> for Store {
    fn bitor_assign(&mut self, rhs: &Store) {
        match (self, rhs) {
            (&mut Array(ref mut vec1), Array(vec2)) => {
                let this = mem::take(vec1);
                *vec1 = BitOr::bitor(&this, vec2);
            }
            (&mut Bitmap(ref mut bits1), Array(vec2)) => {
                BitOrAssign::bitor_assign(bits1, vec2);
            }
            (&mut Bitmap(ref mut bits1), Bitmap(bits2)) => {
                BitOrAssign::bitor_assign(bits1, bits2);
            }
            (this @ &mut Array(..), Bitmap(bits2)) => {
                let mut lhs: Store = Bitmap(bits2.clone());
                BitOrAssign::bitor_assign(&mut lhs, &*this);
                *this = lhs;
            }
            (Run(runs1), Run(runs2)) => {
                BitOrAssign::bitor_assign(runs1, runs2);
            }
            (Run(runs), Array(array)) => {
                BitOrAssign::bitor_assign(runs, array);
            }
            (this @ Array(..), Run(runs)) => {
                let mut runs = runs.clone();
                let Array(array) = &this else { unreachable!() };
                BitOrAssign::bitor_assign(&mut runs, array);
                *this = Run(runs);
            }
            (this @ Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new = runs.to_bitmap();
                BitOrAssign::bitor_assign(&mut new, bitmap);
                *this = Bitmap(new);
            }
            (Bitmap(bitmap), Run(runs)) => {
                BitOrAssign::bitor_assign(bitmap, &runs.to_bitmap());
            }
        }
    }
}

impl BitAnd<&Store> for &Store {
    type Output = Store;

    fn bitand(self, rhs: &Store) -> Store {
        match (self, rhs) {
            (Array(vec1), Array(vec2)) => Array(BitAnd::bitand(vec1, vec2)),
            (&Bitmap(..), &Array(..)) => {
                let mut rhs = rhs.clone();
                BitAndAssign::bitand_assign(&mut rhs, self);
                rhs
            }
            _ => {
                let mut lhs = self.clone();
                BitAndAssign::bitand_assign(&mut lhs, rhs);
                lhs
            }
        }
    }
}

impl BitAndAssign<Store> for Store {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn bitand_assign(&mut self, mut rhs: Store) {
        match (self, &mut rhs) {
            (&mut Array(ref mut vec1), &mut Array(ref mut vec2)) => {
                if vec2.len() < vec1.len() {
                    mem::swap(vec1, vec2);
                }
                BitAndAssign::bitand_assign(vec1, &*vec2);
            }
            (&mut Bitmap(ref mut bits1), &mut Bitmap(ref bits2)) => {
                BitAndAssign::bitand_assign(bits1, bits2);
            }
            (&mut Array(ref mut vec1), &mut Bitmap(ref bits2)) => {
                BitAndAssign::bitand_assign(vec1, bits2);
            }
            (Run(intervals1), Run(intervals2)) => {
                *intervals1 = BitAnd::bitand(&*intervals1, &*intervals2);
            }
            (this @ &mut Run(..), Array(array)) => {
                let Run(runs) = &this else { unreachable!() };
                BitAndAssign::bitand_assign(array, runs);
                *this = rhs;
            }
            (Array(array), Run(runs)) => {
                BitAndAssign::bitand_assign(array, &*runs);
            }
            (this @ &mut Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new_bitmap = runs.to_bitmap();
                BitAndAssign::bitand_assign(&mut new_bitmap, &*bitmap);
                *this = Bitmap(new_bitmap);
            }
            (Bitmap(bitmap), Run(runs)) => {
                BitAndAssign::bitand_assign(bitmap, &runs.to_bitmap());
            }
            (this @ &mut Bitmap(..), &mut Array(..)) => {
                mem::swap(this, &mut rhs);
                BitAndAssign::bitand_assign(this, rhs);
            }
        }
    }
}

impl BitAndAssign<&Store> for Store {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn bitand_assign(&mut self, rhs: &Store) {
        match (self, rhs) {
            (&mut Array(ref mut vec1), Array(vec2)) => {
                let (mut lhs, rhs) = if vec2.len() < vec1.len() {
                    (vec2.clone(), &*vec1)
                } else {
                    (mem::take(vec1), vec2)
                };

                BitAndAssign::bitand_assign(&mut lhs, rhs);
                *vec1 = lhs;
            }
            (&mut Bitmap(ref mut bits1), Bitmap(bits2)) => {
                BitAndAssign::bitand_assign(bits1, bits2);
            }
            (&mut Array(ref mut vec1), Bitmap(bits2)) => {
                BitAndAssign::bitand_assign(vec1, bits2);
            }
            (this @ &mut Bitmap(..), &Array(..)) => {
                let mut new = rhs.clone();
                BitAndAssign::bitand_assign(&mut new, &*this);
                *this = new;
            }
            (Run(runs1), Run(runs2)) => {
                *runs1 = BitAnd::bitand(&*runs1, runs2);
            }
            (this @ Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new_bitmap = runs.to_bitmap();
                BitAndAssign::bitand_assign(&mut new_bitmap, bitmap);
                *this = Bitmap(new_bitmap);
            }
            (Bitmap(bitmap), Run(runs)) => {
                BitAndAssign::bitand_assign(bitmap, &runs.to_bitmap());
            }
            (this @ Run(..), Array(array)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new_array = array.clone();
                new_array.retain(|f| runs.contains(f));
                *this = Array(new_array);
            }
            (Array(array), Run(runs)) => array.retain(|f| runs.contains(f)),
        }
    }
}

impl Sub<&Store> for &Store {
    type Output = Store;

    fn sub(self, rhs: &Store) -> Store {
        match (self, rhs) {
            (Array(vec1), Array(vec2)) => Array(Sub::sub(vec1, vec2)),
            _ => {
                let mut lhs = self.clone();
                SubAssign::sub_assign(&mut lhs, rhs);
                lhs
            }
        }
    }
}

impl SubAssign<&Store> for Store {
    fn sub_assign(&mut self, rhs: &Store) {
        match (self, rhs) {
            (&mut Array(ref mut vec1), Array(vec2)) => {
                SubAssign::sub_assign(vec1, vec2);
            }
            (&mut Bitmap(ref mut bits1), Array(vec2)) => {
                SubAssign::sub_assign(bits1, vec2);
            }
            (&mut Bitmap(ref mut bits1), Bitmap(bits2)) => {
                SubAssign::sub_assign(bits1, bits2);
            }
            (&mut Array(ref mut vec1), Bitmap(bits2)) => {
                SubAssign::sub_assign(vec1, bits2);
            }
            (Run(runs1), Run(runs2)) => {
                SubAssign::sub_assign(runs1, runs2);
            }
            (Run(runs), Array(array)) => {
                array.iter().for_each(|&f| {
                    runs.remove(f);
                });
            }
            (Array(array), Run(runs)) => {
                runs.iter_intervals().for_each(|iv| {
                    array.remove_range(iv.start()..=iv.end());
                });
            }
            (this @ Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new_bitmap = runs.to_bitmap();
                SubAssign::sub_assign(&mut new_bitmap, bitmap);
                *this = Bitmap(new_bitmap);
            }
            (Bitmap(bitmap), Run(runs)) => {
                let new_bitmap = runs.to_bitmap();
                SubAssign::sub_assign(bitmap, &new_bitmap);
            }
        }
    }
}

impl BitXor<&Store> for &Store {
    type Output = Store;

    fn bitxor(self, rhs: &Store) -> Store {
        match (self, rhs) {
            (Array(vec1), Array(vec2)) => Array(BitXor::bitxor(vec1, vec2)),
            (&Array(..), &Bitmap(..)) => {
                let mut lhs = rhs.clone();
                BitXorAssign::bitxor_assign(&mut lhs, self);
                lhs
            }
            _ => {
                let mut lhs = self.clone();
                BitXorAssign::bitxor_assign(&mut lhs, rhs);
                lhs
            }
        }
    }
}

impl BitXorAssign<Store> for Store {
    fn bitxor_assign(&mut self, mut rhs: Store) {
        match (self, &mut rhs) {
            (&mut Array(ref mut vec1), &mut Array(ref vec2)) => {
                *vec1 = BitXor::bitxor(&*vec1, vec2);
            }
            (&mut Bitmap(ref mut bits1), &mut Array(ref vec2)) => {
                BitXorAssign::bitxor_assign(bits1, vec2);
            }
            (&mut Bitmap(ref mut bits1), &mut Bitmap(ref bits2)) => {
                BitXorAssign::bitxor_assign(bits1, bits2);
            }
            (this @ &mut Array(..), &mut Bitmap(..)) => {
                mem::swap(this, &mut rhs);
                BitXorAssign::bitxor_assign(this, rhs);
            }
            (Run(runs1), Run(runs2)) => {
                *runs1 = BitXor::bitxor(&*runs1, &*runs2);
            }
            (Run(runs1), Array(array)) => BitXorAssign::bitxor_assign(runs1, array),
            (this @ Array(..), Run(runs1)) => {
                let Array(array) = &this else { unreachable!() };
                BitXorAssign::bitxor_assign(runs1, array);
                *this = rhs;
            }
            (Bitmap(bitmap), Run(runs)) => {
                BitXorAssign::bitxor_assign(bitmap, &runs.to_bitmap());
            }
            (this @ Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                BitXorAssign::bitxor_assign(bitmap, &runs.to_bitmap());
                *this = rhs;
            }
        }
    }
}

impl BitXorAssign<&Store> for Store {
    fn bitxor_assign(&mut self, rhs: &Store) {
        match (self, rhs) {
            (&mut Array(ref mut vec1), Array(vec2)) => {
                let this = mem::take(vec1);
                *vec1 = BitXor::bitxor(&this, vec2);
            }
            (&mut Bitmap(ref mut bits1), Bitmap(bits2)) => {
                BitXorAssign::bitxor_assign(bits1, bits2);
            }
            (this @ &mut Array(..), Bitmap(bits2)) => {
                let mut lhs: Store = Bitmap(bits2.clone());
                BitXorAssign::bitxor_assign(&mut lhs, &*this);
                *this = lhs;
            }
            (&mut Bitmap(ref mut bits1), Array(vec2)) => {
                BitXorAssign::bitxor_assign(bits1, vec2);
            }
            (Run(runs1), Run(runs2)) => {
                *runs1 = BitXor::bitxor(&*runs1, runs2);
            }
            (Run(runs1), Array(array)) => BitXorAssign::bitxor_assign(runs1, array),
            (this @ Array(..), Run(runs1)) => {
                let Array(array) = &this else { unreachable!() };
                let mut runs1 = runs1.clone();
                BitXorAssign::bitxor_assign(&mut runs1, array);
                *this = Run(runs1);
            }
            (Bitmap(bitmap), Run(runs)) => {
                BitXorAssign::bitxor_assign(bitmap, &runs.to_bitmap());
            }
            (this @ Run(..), Bitmap(bitmap)) => {
                let Run(runs) = &this else { unreachable!() };
                let mut new_bitmap = runs.to_bitmap();
                BitXorAssign::bitxor_assign(&mut new_bitmap, bitmap);
                *this = Bitmap(new_bitmap);
            }
        }
    }
}

impl<'a> IntoIterator for &'a Store {
    type Item = u16;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        match self {
            Array(vec) => Iter::Array(vec.iter()),
            Bitmap(bits) => Iter::BitmapBorrowed(bits.iter()),
            Run(intervals) => Iter::RunBorrowed(intervals.iter()),
        }
    }
}

impl IntoIterator for Store {
    type Item = u16;
    type IntoIter = Iter<'static>;
    fn into_iter(self) -> Iter<'static> {
        match self {
            Array(vec) => Iter::Vec(vec.into_iter()),
            Bitmap(bits) => Iter::BitmapOwned(bits.into_iter()),
            Run(intervals) => Iter::RunOwned(intervals.into_iter()),
        }
    }
}

impl PartialEq for Store {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Array(vec1), Array(vec2)) => vec1 == vec2,
            (Bitmap(bits1), Bitmap(bits2)) => {
                bits1.len() == bits2.len()
                    && bits1.iter().zip(bits2.iter()).all(|(i1, i2)| i1 == i2)
            }
            (Run(intervals1), Run(intervals2)) => intervals1 == intervals2,
            (Run(run), Array(array)) | (Array(array), Run(run)) => {
                run.len() == array.len() && array.iter().all(|&i| run.contains(i))
            }
            (Run(run), Bitmap(bitmap)) | (Bitmap(bitmap), Run(run)) => {
                run.len() == bitmap.len()
                    && run.iter_intervals().all(|&iv| bitmap.contains_range(iv.start()..=iv.end()))
            }
            // Invariant: Array len <= 4096, bitmap len > 4096.
            (Array(_), Bitmap(_)) | (Bitmap(_), Array(_)) => false,
        }
    }
}

// reflexivity: a == a.
impl Eq for Store {}

impl Iter<'_> {
    /// Advance the iterator to the first value greater than or equal to `n`.
    pub(crate) fn advance_to(&mut self, n: u16) {
        match self {
            Iter::Array(inner) => {
                let skip = inner.as_slice().partition_point(|&i| i < n);
                if let Some(nth) = skip.checked_sub(1) {
                    inner.nth(nth);
                }
            }
            Iter::Vec(inner) => {
                let skip = inner.as_slice().partition_point(|&i| i < n);
                if let Some(nth) = skip.checked_sub(1) {
                    inner.nth(nth);
                }
            }
            Iter::BitmapBorrowed(inner) => inner.advance_to(n),
            Iter::BitmapOwned(inner) => inner.advance_to(n),
            Iter::RunOwned(inner) => inner.advance_to(n),
            Iter::RunBorrowed(inner) => inner.advance_to(n),
        }
    }

    pub(crate) fn advance_back_to(&mut self, n: u16) {
        match self {
            Iter::Array(inner) => {
                let slice = inner.as_slice();
                let from_front = slice.partition_point(|&i| i <= n);
                let skip = slice.len() - from_front;
                if let Some(nth) = skip.checked_sub(1) {
                    inner.nth_back(nth);
                }
            }
            Iter::Vec(inner) => {
                let slice = inner.as_slice();
                let from_front = slice.partition_point(|&i| i <= n);
                let skip = slice.len() - from_front;
                if let Some(nth) = skip.checked_sub(1) {
                    inner.nth_back(nth);
                }
            }
            Iter::BitmapBorrowed(inner) => inner.advance_back_to(n),
            Iter::BitmapOwned(inner) => inner.advance_back_to(n),
            Iter::RunOwned(inner) => inner.advance_back_to(n),
            Iter::RunBorrowed(inner) => inner.advance_back_to(n),
        }
    }

    pub(crate) fn next_range(&mut self) -> Option<RangeInclusive<u16>> {
        match self {
            Iter::Array(inner) => {
                let slice = inner.as_slice();
                let len = array_store::first_contiguous_range_len(slice);
                let mut range = None;
                if len != 0 {
                    let first = slice[0];
                    inner.nth(len - 1);
                    range = Some(first..=first + (len - 1) as u16);
                }
                range
            }
            Iter::Vec(inner) => {
                let slice = inner.as_slice();
                let len = array_store::first_contiguous_range_len(slice);
                let mut range = None;
                if len != 0 {
                    let first = slice[0];
                    inner.nth(len - 1);
                    range = Some(first..=first + (len - 1) as u16);
                }
                range
            }
            Iter::BitmapBorrowed(inner) => inner.next_range(),
            Iter::BitmapOwned(inner) => inner.next_range(),
            Iter::RunBorrowed(inner) => inner.next_range(),
            Iter::RunOwned(inner) => inner.next_range(),
        }
    }

    pub(crate) fn next_range_back(&mut self) -> Option<RangeInclusive<u16>> {
        match self {
            Iter::Array(inner) => {
                let slice = inner.as_slice();
                let len = array_store::last_contiguous_range_len(slice);
                let mut range = None;
                if len != 0 {
                    let last = slice[slice.len() - 1];
                    inner.nth_back(len - 1);
                    range = Some(last - (len - 1) as u16..=last);
                }
                range
            }
            Iter::Vec(inner) => {
                let slice = inner.as_slice();
                let len = array_store::last_contiguous_range_len(slice);
                let mut range = None;
                if len != 0 {
                    let last = slice[slice.len() - 1];
                    inner.nth_back(len - 1);
                    range = Some(last - (len - 1) as u16..=last);
                }
                range
            }
            Iter::BitmapBorrowed(inner) => inner.next_range_back(),
            Iter::BitmapOwned(inner) => inner.next_range_back(),
            Iter::RunBorrowed(inner) => inner.next_range_back(),
            Iter::RunOwned(inner) => inner.next_range_back(),
        }
    }

    pub(crate) fn peek(&self) -> Option<u16> {
        match self {
            Iter::Array(inner) => inner.as_slice().first().copied(),
            Iter::Vec(inner) => inner.as_slice().first().copied(),
            Iter::BitmapBorrowed(inner) => inner.peek(),
            Iter::BitmapOwned(inner) => inner.peek(),
            Iter::RunBorrowed(inner) => inner.peek(),
            Iter::RunOwned(inner) => inner.peek(),
        }
    }
    pub(crate) fn peek_back(&self) -> Option<u16> {
        match self {
            Iter::Array(inner) => inner.as_slice().last().copied(),
            Iter::Vec(inner) => inner.as_slice().last().copied(),
            Iter::BitmapBorrowed(inner) => inner.peek_back(),
            Iter::BitmapOwned(inner) => inner.peek_back(),
            Iter::RunBorrowed(inner) => inner.peek_back(),
            Iter::RunOwned(inner) => inner.peek_back(),
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        match self {
            Iter::Array(inner) => inner.next().cloned(),
            Iter::Vec(inner) => inner.next(),
            Iter::BitmapBorrowed(inner) => inner.next(),
            Iter::BitmapOwned(inner) => inner.next(),
            Iter::RunOwned(inner) => inner.next(),
            Iter::RunBorrowed(inner) => inner.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Array(inner) => inner.size_hint(),
            Iter::Vec(inner) => inner.size_hint(),
            Iter::BitmapBorrowed(inner) => inner.size_hint(),
            Iter::BitmapOwned(inner) => inner.size_hint(),
            Iter::RunOwned(inner) => inner.size_hint(),
            Iter::RunBorrowed(inner) => inner.size_hint(),
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        match self {
            Iter::Array(inner) => inner.count(),
            Iter::Vec(inner) => inner.count(),
            Iter::BitmapBorrowed(inner) => inner.count(),
            Iter::BitmapOwned(inner) => inner.count(),
            Iter::RunOwned(inner) => inner.count(),
            Iter::RunBorrowed(inner) => inner.count(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Iter::Array(inner) => inner.nth(n).copied(),
            Iter::Vec(inner) => inner.nth(n),
            Iter::BitmapBorrowed(inner) => inner.nth(n),
            Iter::BitmapOwned(inner) => inner.nth(n),
            Iter::RunOwned(inner) => inner.nth(n),
            Iter::RunBorrowed(inner) => inner.nth(n),
        }
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Array(inner) => inner.next_back().cloned(),
            Iter::Vec(inner) => inner.next_back(),
            Iter::BitmapBorrowed(inner) => inner.next_back(),
            Iter::BitmapOwned(inner) => inner.next_back(),
            Iter::RunOwned(inner) => inner.next_back(),
            Iter::RunBorrowed(inner) => inner.next_back(),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}
