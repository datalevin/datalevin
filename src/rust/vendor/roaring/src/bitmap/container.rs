use core::fmt;
use core::ops::{
    BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, RangeInclusive, Sub, SubAssign,
};

use super::store::{self, ArrayStore, Interval, IntervalStore, Store, BITMAP_BYTES};
use super::util;

pub const ARRAY_LIMIT: u64 = 4096;
#[cfg(test)]
pub const RUN_MAX_SIZE: u64 = 2048;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[derive(PartialEq, Eq, Clone)]
pub(crate) struct Container {
    pub key: u16,
    pub store: Store,
}

#[derive(Clone)]
pub(crate) struct Iter<'a> {
    pub key: u16,
    inner: store::Iter<'a>,
}

impl Container {
    pub fn new(key: u16) -> Container {
        Container { key, store: Store::new() }
    }

    pub fn new_with_range(key: u16, range: RangeInclusive<u16>) -> Container {
        if range.len() <= 2 {
            let mut array = ArrayStore::new();
            array.insert_range(range);
            Self { key, store: Store::Array(array) }
        } else {
            Self {
                key,
                store: Store::Run(IntervalStore::new_with_range(
                    // This is ok, since range must be non empty
                    Interval::new_unchecked(*range.start(), *range.end()),
                )),
            }
        }
    }

    pub fn full(key: u16) -> Container {
        Container { key, store: Store::full() }
    }

    pub fn from_lsb0_bytes(key: u16, bytes: &[u8], byte_offset: usize) -> Option<Self> {
        Some(Container { key, store: Store::from_lsb0_bytes(bytes, byte_offset)? })
    }
}

impl Container {
    pub fn len(&self) -> u64 {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    #[inline]
    pub fn insert(&mut self, index: u16) -> bool {
        if self.store.insert(index) {
            self.ensure_correct_store();
            true
        } else {
            false
        }
    }

    pub fn insert_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        if range.is_empty() {
            return 0;
        }
        match &self.store {
            Store::Bitmap(bitmap) => {
                let added_amount = range.len() as u64
                    - bitmap.intersection_len_interval(&Interval::new_unchecked(
                        *range.start(),
                        *range.end(),
                    ));
                let union_cardinality = bitmap.len() + added_amount;
                if union_cardinality == 1 << 16 {
                    self.store = Store::Run(IntervalStore::full());
                    added_amount
                } else {
                    self.store.insert_range(range)
                }
            }
            Store::Array(array) => {
                let added_amount = range.len() as u64
                    - array.intersection_len_interval(&Interval::new_unchecked(
                        *range.start(),
                        *range.end(),
                    ));
                let union_cardinality = array.len() + added_amount;
                if union_cardinality == 1 << 16 {
                    self.store = Store::Run(IntervalStore::full());
                    added_amount
                } else if union_cardinality <= ARRAY_LIMIT {
                    self.store.insert_range(range)
                } else {
                    self.store = self.store.to_bitmap();
                    self.store.insert_range(range)
                }
            }
            Store::Run(_) => self.store.insert_range(range),
        }
    }

    /// Pushes `index` at the end of the container only if `index` is the new max.
    ///
    /// Returns whether the `index` was effectively pushed.
    pub fn push(&mut self, index: u16) -> bool {
        if self.store.push(index) {
            self.ensure_correct_store();
            true
        } else {
            false
        }
    }

    ///
    /// Pushes `index` at the end of the container.
    /// It is up to the caller to have validated index > self.max()
    ///
    /// # Panics
    ///
    /// If debug_assertions enabled and index is > self.max()
    pub(crate) fn push_unchecked(&mut self, index: u16) {
        self.store.push_unchecked(index);
        self.ensure_correct_store();
    }

    pub fn remove(&mut self, index: u16) -> bool {
        if self.store.remove(index) {
            self.ensure_correct_store();
            true
        } else {
            false
        }
    }

    pub fn remove_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        let result = self.store.remove_range(range);
        self.ensure_correct_store();
        result
    }

    pub fn remove_smallest(&mut self, n: u64) {
        match &self.store {
            Store::Bitmap(bits) => {
                if bits.len() - n <= ARRAY_LIMIT {
                    let mut replace_array = Vec::with_capacity((bits.len() - n) as usize);
                    replace_array.extend(bits.iter().skip(n as usize));
                    self.store = Store::Array(store::ArrayStore::from_vec_unchecked(replace_array));
                } else {
                    self.store.remove_smallest(n)
                }
            }
            Store::Array(_) => self.store.remove_smallest(n),
            Store::Run(_) => self.store.remove_smallest(n),
        };
    }

    pub fn remove_biggest(&mut self, n: u64) {
        match &self.store {
            Store::Bitmap(bits) => {
                if bits.len() - n <= ARRAY_LIMIT {
                    let mut replace_array = Vec::with_capacity((bits.len() - n) as usize);
                    replace_array.extend(bits.iter().take((bits.len() - n) as usize));
                    self.store = Store::Array(store::ArrayStore::from_vec_unchecked(replace_array));
                } else {
                    self.store.remove_biggest(n)
                }
            }
            Store::Array(_) => self.store.remove_biggest(n),
            Store::Run(_) => self.store.remove_biggest(n),
        };
    }

    pub fn contains(&self, index: u16) -> bool {
        self.store.contains(index)
    }

    pub fn contains_range(&self, range: RangeInclusive<u16>) -> bool {
        self.store.contains_range(range)
    }

    pub fn is_full(&self) -> bool {
        self.store.is_full()
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.store.is_disjoint(&other.store)
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.len() <= other.len() && self.store.is_subset(&other.store)
    }

    pub fn intersection_len(&self, other: &Self) -> u64 {
        self.store.intersection_len(&other.store)
    }

    pub fn min(&self) -> Option<u16> {
        self.store.min()
    }

    #[inline]
    pub fn max(&self) -> Option<u16> {
        self.store.max()
    }

    pub fn rank(&self, index: u16) -> u64 {
        self.store.rank(index)
    }

    pub(crate) fn ensure_correct_store(&mut self) -> bool {
        let new_store = match &self.store {
            Store::Bitmap(ref bits) if bits.len() <= ARRAY_LIMIT => {
                Store::Array(bits.to_array_store()).into()
            }
            Store::Array(ref vec) if vec.len() > ARRAY_LIMIT => {
                Store::Bitmap(vec.to_bitmap_store()).into()
            }
            _ => None,
        };
        if let Some(new_store) = new_store {
            self.store = new_store;
            true
        } else {
            false
        }
    }

    pub fn optimize(&mut self) -> bool {
        match &mut self.store {
            Store::Bitmap(_) => {
                let num_runs = self.store.count_runs();
                let size_as_run = IntervalStore::serialized_byte_size(num_runs);
                if BITMAP_BYTES <= size_as_run {
                    return false;
                }
                self.store = self.store.to_run();
                true
            }
            Store::Array(array) => {
                let size_as_array = array.byte_size();
                let num_runs = self.store.count_runs();
                let size_as_run = IntervalStore::serialized_byte_size(num_runs);
                if size_as_array <= size_as_run {
                    return false;
                }
                self.store = self.store.to_run();
                true
            }
            Store::Run(runs) => {
                let size_as_run = runs.byte_size();
                let card = runs.len();
                let size_as_array = ArrayStore::serialized_byte_size(card);
                let min_size_non_run = size_as_array.min(BITMAP_BYTES);
                if size_as_run <= min_size_non_run {
                    return false;
                }
                if card <= ARRAY_LIMIT {
                    self.store = Store::Array(runs.to_array());
                    return true;
                }
                self.store = Store::Bitmap(runs.to_bitmap());
                true
            }
        }
    }

    pub fn remove_run_compression(&mut self) -> bool {
        match &mut self.store {
            Store::Bitmap(_) | Store::Array(_) => false,
            Store::Run(runs) => {
                let card = runs.len();
                if card <= ARRAY_LIMIT {
                    self.store = Store::Array(runs.to_array());
                } else {
                    self.store = Store::Bitmap(runs.to_bitmap());
                }
                true
            }
        }
    }
}

impl BitOr<&Container> for &Container {
    type Output = Container;

    fn bitor(self, rhs: &Container) -> Container {
        let store = BitOr::bitor(&self.store, &rhs.store);
        let mut container = Container { key: self.key, store };
        container.ensure_correct_store();
        container
    }
}

impl BitOrAssign<Container> for Container {
    fn bitor_assign(&mut self, rhs: Container) {
        BitOrAssign::bitor_assign(&mut self.store, rhs.store);
        self.ensure_correct_store();
    }
}

impl BitOrAssign<&Container> for Container {
    fn bitor_assign(&mut self, rhs: &Container) {
        BitOrAssign::bitor_assign(&mut self.store, &rhs.store);
        self.ensure_correct_store();
    }
}

impl BitAnd<&Container> for &Container {
    type Output = Container;

    fn bitand(self, rhs: &Container) -> Container {
        let store = BitAnd::bitand(&self.store, &rhs.store);
        let mut container = Container { key: self.key, store };
        container.ensure_correct_store();
        container
    }
}

impl BitAndAssign<Container> for Container {
    fn bitand_assign(&mut self, rhs: Container) {
        BitAndAssign::bitand_assign(&mut self.store, rhs.store);
        self.ensure_correct_store();
    }
}

impl BitAndAssign<&Container> for Container {
    fn bitand_assign(&mut self, rhs: &Container) {
        BitAndAssign::bitand_assign(&mut self.store, &rhs.store);
        self.ensure_correct_store();
    }
}

impl Sub<&Container> for &Container {
    type Output = Container;

    fn sub(self, rhs: &Container) -> Container {
        let store = Sub::sub(&self.store, &rhs.store);
        let mut container = Container { key: self.key, store };
        container.ensure_correct_store();
        container
    }
}

impl SubAssign<&Container> for Container {
    fn sub_assign(&mut self, rhs: &Container) {
        SubAssign::sub_assign(&mut self.store, &rhs.store);
        self.ensure_correct_store();
    }
}

impl BitXor<&Container> for &Container {
    type Output = Container;

    fn bitxor(self, rhs: &Container) -> Container {
        let store = BitXor::bitxor(&self.store, &rhs.store);
        let mut container = Container { key: self.key, store };
        container.ensure_correct_store();
        container
    }
}

impl BitXorAssign<Container> for Container {
    fn bitxor_assign(&mut self, rhs: Container) {
        BitXorAssign::bitxor_assign(&mut self.store, rhs.store);
        self.ensure_correct_store();
    }
}

impl BitXorAssign<&Container> for Container {
    fn bitxor_assign(&mut self, rhs: &Container) {
        BitXorAssign::bitxor_assign(&mut self.store, &rhs.store);
        self.ensure_correct_store();
    }
}

impl<'a> IntoIterator for &'a Container {
    type Item = u32;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        let store: &Store = &self.store;
        Iter { key: self.key, inner: store.into_iter() }
    }
}

impl IntoIterator for Container {
    type Item = u32;
    type IntoIter = Iter<'static>;

    fn into_iter(self) -> Iter<'static> {
        Iter { key: self.key, inner: self.store.into_iter() }
    }
}

impl Iterator for Iter<'_> {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        self.inner.next().map(|i| util::join(self.key, i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.inner.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner.nth(n).map(|i| util::join(self.key, i))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|i| util::join(self.key, i))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl Iter<'_> {
    pub(crate) fn peek(&self) -> Option<u32> {
        self.inner.peek().map(|i| util::join(self.key, i))
    }

    pub(crate) fn peek_back(&self) -> Option<u32> {
        self.inner.peek_back().map(|i| util::join(self.key, i))
    }

    pub(crate) fn advance_to(&mut self, index: u16) {
        self.inner.advance_to(index);
    }

    pub(crate) fn advance_back_to(&mut self, index: u16) {
        self.inner.advance_back_to(index);
    }

    /// Returns the range of consecutive set bits from the current position to the end of the current run
    ///
    /// After this call, the iterator will be positioned at the first item after the returned range.
    /// Returns `None` if the iterator is exhausted.
    pub(crate) fn next_range(&mut self) -> Option<RangeInclusive<u32>> {
        self.inner
            .next_range()
            .map(|r| util::join(self.key, *r.start())..=util::join(self.key, *r.end()))
    }

    /// Returns the range of consecutive set bits from the start of the current run to the current back position
    ///
    /// After this call, the back of the iterator will be positioned at the last item before the returned range.
    /// Returns `None` if the iterator is exhausted.
    pub(crate) fn next_range_back(&mut self) -> Option<RangeInclusive<u32>> {
        self.inner
            .next_range_back()
            .map(|r| util::join(self.key, *r.start())..=util::join(self.key, *r.end()))
    }
}

impl fmt::Debug for Container {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        format!("Container<{:?} @ {:?}>", self.len(), self.key).fmt(formatter)
    }
}
