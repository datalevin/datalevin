use alloc::collections::{btree_map, BTreeMap};
use core::cmp::Ordering;
use core::iter;
use core::ops::Add;

use super::util;
use crate::bitmap::IntoIter as IntoIter32;
use crate::bitmap::Iter as Iter32;
use crate::{NonSortedIntegers, RoaringBitmap, RoaringTreemap};

#[derive(Clone)]
struct To64Iter<'a> {
    hi: u32,
    inner: Iter32<'a>,
}

impl To64Iter<'_> {
    fn advance_to(&mut self, n: u32) {
        self.inner.advance_to(n)
    }

    fn advance_back_to(&mut self, n: u32) {
        self.inner.advance_back_to(n)
    }
}

impl Iterator for To64Iter<'_> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        self.inner.next().map(|n| util::join(self.hi, n))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline]
    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.inner.fold(init, move |b, lo| f(b, ((self.hi as u64) << 32) + (lo as u64)))
    }
}

impl DoubleEndedIterator for To64Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|n| util::join(self.hi, n))
    }

    #[inline]
    fn rfold<B, F>(self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.inner.rfold(init, move |b, lo| f(b, ((self.hi as u64) << 32) + (lo as u64)))
    }
}

fn to64iter(t: (u32, &RoaringBitmap)) -> To64Iter<'_> {
    To64Iter { hi: t.0, inner: t.1.iter() }
}

#[derive(Clone)]
struct To64IntoIter {
    hi: u32,
    inner: IntoIter32,
}

impl Iterator for To64IntoIter {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        self.inner.next().map(|n| util::join(self.hi, n))
    }

    #[inline]
    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.inner.fold(init, move |b, lo| f(b, ((self.hi as u64) << 32) + (lo as u64)))
    }
}

impl DoubleEndedIterator for To64IntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|n| util::join(self.hi, n))
    }

    #[inline]
    fn rfold<B, F>(self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.inner.rfold(init, move |b, lo| f(b, ((self.hi as u64) << 32) + (lo as u64)))
    }
}

fn to64intoiter(t: (u32, RoaringBitmap)) -> To64IntoIter {
    To64IntoIter { hi: t.0, inner: t.1.into_iter() }
}

type InnerIntoIter = iter::FlatMap<
    btree_map::IntoIter<u32, RoaringBitmap>,
    To64IntoIter,
    fn((u32, RoaringBitmap)) -> To64IntoIter,
>;

/// An iterator for `RoaringTreemap`.
#[derive(Clone)]
pub struct Iter<'a> {
    outer: BitmapIter<'a>,
    front: Option<To64Iter<'a>>,
    back: Option<To64Iter<'a>>,
}

/// An iterator for `RoaringTreemap`.
pub struct IntoIter {
    inner: InnerIntoIter,
    size_hint: u64,
}

impl Iter<'_> {
    fn new(map: &'_ BTreeMap<u32, RoaringBitmap>) -> Iter<'_> {
        let outer = BitmapIter::new(map);
        Iter { outer, front: None, back: None }
    }

    /// Advance the iterator to the first position where the item has a value >= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringTreemap>();
    /// let mut iter = bitmap.iter();
    /// iter.advance_to(2);
    ///
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn advance_to(&mut self, n: u64) {
        let (key, index) = util::split(n);

        if let Some(ref mut front) = self.front {
            match front.hi.cmp(&key) {
                Ordering::Less => {}
                Ordering::Equal => {
                    front.advance_to(index);
                    return;
                }
                Ordering::Greater => return,
            }
            self.front = None;
        }

        self.outer.advance_to(key);

        let Some(next) = self.outer.next() else {
            // if the current front iterator is empty or not yet initialized,
            // but the outer bitmap iterator is empty, then consume the back
            // iterator from the front if it is not also exhausted
            if let Some(ref mut back) = self.back {
                match back.hi.cmp(&key) {
                    Ordering::Less => self.back = None,
                    Ordering::Equal => back.advance_to(index),
                    Ordering::Greater => {}
                }
            }
            return;
        };

        let mut front = to64iter(next);
        if front.hi == key {
            front.advance_to(index);
        }
        self.front = Some(front);
    }

    /// Advance the back of the iterator to the first position where the item has a value <= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringTreemap>();
    /// let mut iter = bitmap.iter();
    /// iter.advance_back_to(1);
    ///
    /// assert_eq!(iter.next_back(), Some(1));
    /// assert_eq!(iter.next_back(), None);
    /// ```
    pub fn advance_back_to(&mut self, n: u64) {
        let (key, index) = util::split(n);

        if let Some(ref mut back) = self.back {
            match back.hi.cmp(&key) {
                Ordering::Less => return,
                Ordering::Equal => {
                    back.advance_back_to(index);
                    return;
                }
                Ordering::Greater => {}
            }
            self.back = None;
        }

        self.outer.advance_back_to(key);

        let Some(next_back) = self.outer.next_back() else {
            // if the current back iterator is empty or not yet initialized,
            // but the outer bitmap iterator is empty, then consume the front
            // iterator from the back if it is not also exhausted
            if let Some(ref mut front) = self.front {
                match front.hi.cmp(&key) {
                    Ordering::Less => {}
                    Ordering::Equal => front.advance_back_to(index),
                    Ordering::Greater => self.front = None,
                }
            }
            return;
        };

        let mut back = to64iter(next_back);
        if back.hi == key {
            back.advance_back_to(index);
        }
        self.back = Some(back);
    }
}

impl IntoIter {
    fn new(map: BTreeMap<u32, RoaringBitmap>) -> IntoIter {
        let size_hint = map.values().map(|r| r.len()).sum();
        let i = map.into_iter().flat_map(to64intoiter as _);
        IntoIter { inner: i, size_hint }
    }
}

impl Iterator for Iter<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if let Some(ref mut front) = &mut self.front {
            if let Some(inner) = front.next() {
                return Some(inner);
            }
        }

        let Some(outer_next) = self.outer.next() else {
            // if the current front iterator is empty or not yet initialized,
            // but the outer bitmap iterator is empty, then consume the back
            // iterator from the front if it is not also exhausted
            if let Some(ref mut back) = &mut self.back {
                if let Some(next) = back.next() {
                    return Some(next);
                }
            }
            return None;
        };

        self.front = Some(to64iter(outer_next));
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let front_size_hint = self.front.as_ref().map_or(0, |f| f.size_hint().0);
        let back_size_hint = self.back.as_ref().map_or(0, |b| b.size_hint().0);

        let size_hint = front_size_hint
            .saturating_add(back_size_hint)
            .saturating_add(self.outer.remaining() as usize);

        (size_hint, Some(size_hint))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(ref mut back) = &mut self.back {
            if let Some(inner) = back.next_back() {
                return Some(inner);
            }
        }

        let Some(outer_next_back) = self.outer.next_back() else {
            // if the current back iterator is empty or not yet initialized,
            // but the outer bitmap iterator is empty, then consume the front
            // iterator from the back if it is not also exhausted
            if let Some(ref mut front) = &mut self.front {
                if let Some(next_back) = front.next_back() {
                    return Some(next_back);
                }
            }
            return None;
        };

        self.back = Some(to64iter(outer_next_back));
        self.next_back()
    }
}

#[cfg(target_pointer_width = "64")]
impl ExactSizeIterator for Iter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

impl Iterator for IntoIter {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.size_hint = self.size_hint.saturating_sub(1);
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.size_hint < usize::MAX as u64 {
            (self.size_hint as usize, Some(self.size_hint as usize))
        } else {
            (usize::MAX, None)
        }
    }

    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.inner.fold(init, f)
    }
}

impl DoubleEndedIterator for IntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.size_hint = self.size_hint.saturating_sub(1);
        self.inner.next_back()
    }

    #[inline]
    fn rfold<Acc, Fold>(self, init: Acc, fold: Fold) -> Acc
    where
        Fold: FnMut(Acc, Self::Item) -> Acc,
    {
        self.inner.rfold(init, fold)
    }
}

#[cfg(target_pointer_width = "64")]
impl ExactSizeIterator for IntoIter {
    fn len(&self) -> usize {
        self.size_hint as usize
    }
}

impl RoaringTreemap {
    /// Iterator over each value stored in the RoaringTreemap, guarantees values are ordered by
    /// value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringTreemap>();
    /// let mut iter = bitmap.iter();
    ///
    /// assert_eq!(iter.next(), Some(1));
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&'_ self) -> Iter<'_> {
        Iter::new(&self.map)
    }

    /// Iterator over pairs of partition number and the corresponding RoaringBitmap.
    /// The partition number is defined by the 32 most significant bits of the bit index.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::{RoaringBitmap, RoaringTreemap};
    /// use core::iter::FromIterator;
    ///
    /// let original = (0..6000).collect::<RoaringTreemap>();
    /// let mut bitmaps = original.bitmaps();
    ///
    /// assert_eq!(bitmaps.next(), Some((0, &(0..6000).collect::<RoaringBitmap>())));
    /// assert_eq!(bitmaps.next(), None);
    /// ```
    pub fn bitmaps(&'_ self) -> BitmapIter<'_> {
        BitmapIter::new(&self.map)
    }

    /// Construct a RoaringTreemap from an iterator of partition number and RoaringBitmap pairs.
    /// The partition number is defined by the 32 most significant bits of the bit index.
    /// Note that repeated partitions, if present, will replace previously set partitions.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    /// use core::iter::FromIterator;
    ///
    /// let original = (0..6000).collect::<RoaringTreemap>();
    /// let clone = RoaringTreemap::from_bitmaps(original.bitmaps().map(|(p, b)| (p, b.clone())));
    ///
    /// assert_eq!(clone, original);
    /// ```
    pub fn from_bitmaps<I: IntoIterator<Item = (u32, RoaringBitmap)>>(iterator: I) -> Self {
        RoaringTreemap { map: iterator.into_iter().collect() }
    }
}

impl<'a> IntoIterator for &'a RoaringTreemap {
    type Item = u64;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl IntoIterator for RoaringTreemap {
    type Item = u64;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter::new(self.map)
    }
}

impl<const N: usize> From<[u64; N]> for RoaringTreemap {
    fn from(arr: [u64; N]) -> Self {
        RoaringTreemap::from_iter(arr)
    }
}

impl FromIterator<u64> for RoaringTreemap {
    fn from_iter<I: IntoIterator<Item = u64>>(iterator: I) -> RoaringTreemap {
        let mut rb = RoaringTreemap::new();
        rb.extend(iterator);
        rb
    }
}

impl<'a> FromIterator<&'a u64> for RoaringTreemap {
    fn from_iter<I: IntoIterator<Item = &'a u64>>(iterator: I) -> RoaringTreemap {
        let mut rb = RoaringTreemap::new();
        rb.extend(iterator);
        rb
    }
}

impl Extend<u64> for RoaringTreemap {
    fn extend<I: IntoIterator<Item = u64>>(&mut self, iterator: I) {
        for value in iterator {
            self.insert(value);
        }
    }
}

impl<'a> Extend<&'a u64> for RoaringTreemap {
    fn extend<I: IntoIterator<Item = &'a u64>>(&mut self, iterator: I) {
        for value in iterator {
            self.insert(*value);
        }
    }
}

impl RoaringTreemap {
    /// Create the set from a sorted iterator. Values must be sorted and deduplicated.
    ///
    /// The values of the iterator must be ordered and strictly greater than the greatest value
    /// in the set. If a value in the iterator doesn't satisfy this requirement, it is not added
    /// and the append operation is stopped.
    ///
    /// Returns `Ok` with the requested `RoaringTreemap`, `Err` with the number of elements
    /// we tried to append before an error occurred.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::from_sorted_iter(0..10).unwrap();
    ///
    /// assert!(rb.iter().eq(0..10));
    /// ```
    pub fn from_sorted_iter<I: IntoIterator<Item = u64>>(
        iterator: I,
    ) -> Result<RoaringTreemap, NonSortedIntegers> {
        let mut rt = RoaringTreemap::new();
        rt.append(iterator).map(|_| rt)
    }

    /// Extend the set with a sorted iterator.
    ///
    /// The values of the iterator must be ordered and strictly greater than the greatest value
    /// in the set. If a value in the iterator doesn't satisfy this requirement, it is not added
    /// and the append operation is stopped.
    ///
    /// Returns `Ok` with the number of elements appended to the set, `Err` with
    /// the number of elements we effectively appended before an error occurred.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringTreemap;
    ///
    /// let mut rb = RoaringTreemap::new();
    /// rb.append(0..10);
    ///
    /// assert!(rb.iter().eq(0..10));
    /// ```
    pub fn append<I: IntoIterator<Item = u64>>(
        &mut self,
        iterator: I,
    ) -> Result<u64, NonSortedIntegers> {
        let mut iterator = iterator.into_iter();
        let mut prev = match (iterator.next(), self.max()) {
            (None, _) => return Ok(0),
            (Some(first), Some(max)) if first <= max => {
                return Err(NonSortedIntegers { valid_until: 0 })
            }
            (Some(first), _) => first,
        };

        // It is now guaranteed that so long as the values of the iterator are
        // monotonically increasing they must also be the greatest in the set.

        self.push_unchecked(prev);

        let mut count = 1;
        for value in iterator {
            if value <= prev {
                return Err(NonSortedIntegers { valid_until: count });
            } else {
                self.push_unchecked(value);
                prev = value;
                count += 1;
            }
        }

        Ok(count)
    }
}

/// An iterator of `RoaringBitmap`s for `RoaringTreemap`.
#[derive(Clone)]
pub struct BitmapIter<'a> {
    treemap: &'a BTreeMap<u32, RoaringBitmap>,
    range: btree_map::Range<'a, u32, RoaringBitmap>,
    latest_front_idx: Option<u32>,
    latest_back_idx: Option<u32>,
}

impl<'a> BitmapIter<'a> {
    fn new(treemap: &'a BTreeMap<u32, RoaringBitmap>) -> Self {
        let range = treemap.range(..);
        Self { treemap, range, latest_back_idx: None, latest_front_idx: None }
    }

    fn advance_to(&mut self, new_front_idx: u32) {
        match self.latest_back_idx {
            Some(latest_back_idx) => match self.latest_front_idx {
                Some(last_idx) if last_idx >= new_front_idx => {}
                _ => {
                    // if asked to advance to beyond the back iterator,
                    // update the self.range iterator to be empty
                    if new_front_idx >= latest_back_idx {
                        self.range = self.treemap.range(0..1);
                        self.range.next_back();
                    } else {
                        // otherwise shrink the remaining range from the front
                        self.range = self.treemap.range(new_front_idx..latest_back_idx);
                    }

                    // self.range = self.treemap.range(new_front_idx..latest_back_idx);
                }
            },
            None => match self.latest_front_idx {
                Some(latest_idx) if latest_idx >= new_front_idx => {}
                _ => {
                    self.range = self.treemap.range(new_front_idx..);
                }
            },
        }
    }

    fn advance_back_to(&mut self, new_back_idx: u32) {
        match self.latest_front_idx {
            Some(latest_front_idx) => match self.latest_back_idx {
                // do nothing if asked to advance back to a higher index than the back is already at
                Some(latest_back_idx) if latest_back_idx <= new_back_idx => {}
                _ => {
                    // if asked to advance back to beyond the front iterator,
                    // update the self.range iterator to be empty
                    if new_back_idx <= latest_front_idx {
                        self.range = self.treemap.range(0..1);
                        self.range.next_back();
                    } else {
                        // otherwise shrink the remaining range from the back
                        self.range = self.treemap.range((latest_front_idx + 1)..new_back_idx);
                    }
                }
            },
            None => match self.latest_back_idx {
                Some(latest_back_idx) if latest_back_idx <= new_back_idx => {}
                _ => {
                    self.range = self.treemap.range(..=new_back_idx);
                }
            },
        }
    }

    fn remaining(&self) -> u64 {
        let range = self.range.clone();
        range.fold(0, |acc, (_, bitmap)| acc.add(bitmap.len()))
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = (u32, &'a RoaringBitmap);

    fn next(&mut self) -> Option<Self::Item> {
        match self.range.next().map(|(&p, b)| (p, b)) {
            None => {
                self.latest_front_idx = None;
                None
            }
            Some((next_idx, next_map)) => {
                self.latest_front_idx = Some(next_idx);
                Some((next_idx, next_map))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl FromIterator<(u32, RoaringBitmap)> for RoaringTreemap {
    fn from_iter<I: IntoIterator<Item = (u32, RoaringBitmap)>>(iterator: I) -> RoaringTreemap {
        Self::from_bitmaps(iterator)
    }
}

impl DoubleEndedIterator for BitmapIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.range.next_back().map(|(&p, b)| (p, b)) {
            None => {
                self.latest_back_idx = None;
                None
            }
            Some((next_back_idx, next_back_map)) => {
                self.latest_back_idx = Some(next_back_idx);
                Some((next_back_idx, next_back_map))
            }
        }
    }
}
