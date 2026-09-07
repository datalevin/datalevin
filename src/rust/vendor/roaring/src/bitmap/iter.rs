use alloc::vec;
use core::iter::FusedIterator;
use core::ops::RangeBounds;
use core::slice;

use super::container::Container;
use super::{container, util};
use crate::{NonSortedIntegers, RoaringBitmap};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// An iterator for `RoaringBitmap`.
#[derive(Clone)]
pub struct Iter<'a> {
    front: Option<container::Iter<'a>>,
    containers: slice::Iter<'a, Container>,
    back: Option<container::Iter<'a>>,
}

/// An iterator for `RoaringBitmap`.
#[derive(Clone)]
pub struct IntoIter {
    front: Option<container::Iter<'static>>,
    containers: vec::IntoIter<Container>,
    back: Option<container::Iter<'static>>,
}

#[inline]
fn and_then_or_clear<T, U>(opt: &mut Option<T>, f: impl FnOnce(&mut T) -> Option<U>) -> Option<U> {
    let x = f(opt.as_mut()?);
    if x.is_none() {
        *opt = None;
    }
    x
}

fn advance_to_impl<'a, It>(
    n: u32,
    front_iter: &mut Option<container::Iter<'a>>,
    containers: &mut It,
    back_iter: &mut Option<container::Iter<'a>>,
) where
    It: Iterator,
    It: AsRef<[Container]>,
    It::Item: IntoIterator<IntoIter = container::Iter<'a>>,
{
    let (key, index) = util::split(n);
    if let Some(iter) = front_iter {
        match key.cmp(&iter.key) {
            core::cmp::Ordering::Less => return,
            core::cmp::Ordering::Equal => {
                iter.advance_to(index);
                return;
            }
            core::cmp::Ordering::Greater => {
                *front_iter = None;
            }
        }
    }
    let containers_slice = containers.as_ref();
    let containers_len = containers_slice.len();
    let to_skip = match containers_slice.binary_search_by_key(&key, |c| c.key) {
        Ok(n) => {
            let container = containers.nth(n).expect("binary search returned a valid index");
            let mut container_iter = container.into_iter();
            container_iter.advance_to(index);
            *front_iter = Some(container_iter);
            return;
        }
        Err(n) => n,
    };

    if let Some(n) = to_skip.checked_sub(1) {
        containers.nth(n);
    }
    if to_skip != containers_len {
        // There are still containers with keys greater than the key we are looking for,
        // the key we're looking _can't_ be in the back iterator.
        return;
    }
    if let Some(iter) = back_iter {
        match key.cmp(&iter.key) {
            core::cmp::Ordering::Less => {}
            core::cmp::Ordering::Equal => {
                iter.advance_to(index);
            }
            core::cmp::Ordering::Greater => {
                *back_iter = None;
            }
        }
    }
}

fn next_range_impl<'a, It>(
    front_iter: &mut Option<container::Iter<'a>>,
    containers: &mut It,
    back_iter: &mut Option<container::Iter<'a>>,
) -> Option<core::ops::RangeInclusive<u32>>
where
    It: Iterator + Clone,
    It: AsRef<[Container]>,
    It::Item: IntoIterator<IntoIter = container::Iter<'a>>,
{
    let range = loop {
        if let Some(r) = and_then_or_clear(front_iter, container::Iter::next_range) {
            break r;
        }
        *front_iter = match containers.next() {
            Some(inner) => Some(inner.into_iter()),
            None => return and_then_or_clear(back_iter, container::Iter::next_range),
        }
    };
    let (range_start, mut range_end) = (*range.start(), *range.end());
    while range_end & 0xFFFF == 0xFFFF {
        let Some(after_end) = range_end.checked_add(1) else {
            return Some(range_start..=range_end);
        };
        let (next_key, _) = util::split(after_end);

        if containers.as_ref().first().is_some_and(|c| c.key == next_key && c.contains(0)) {
            let mut iter = containers.next().unwrap().into_iter();
            let next_range = iter.next_range().unwrap();
            *front_iter = Some(iter);
            debug_assert_eq!(*next_range.start(), after_end);
            range_end = *next_range.end();
        } else {
            if let Some(iter) = back_iter {
                if iter.peek() == Some(after_end) {
                    let next_range = iter.next_range().unwrap();
                    debug_assert_eq!(*next_range.start(), after_end);
                    range_end = *next_range.end();
                }
            }
            break;
        }
    }

    Some(range_start..=range_end)
}

fn next_range_back_impl<'a, It>(
    front_iter: &mut Option<container::Iter<'a>>,
    containers: &mut It,
    back_iter: &mut Option<container::Iter<'a>>,
) -> Option<core::ops::RangeInclusive<u32>>
where
    It: DoubleEndedIterator,
    It: AsRef<[Container]>,
    It::Item: IntoIterator<IntoIter = container::Iter<'a>>,
{
    let range = loop {
        if let Some(r) = and_then_or_clear(back_iter, container::Iter::next_range_back) {
            break r;
        }
        *back_iter = match containers.next_back() {
            Some(inner) => Some(inner.into_iter()),
            None => return and_then_or_clear(front_iter, container::Iter::next_range_back),
        }
    };
    let (mut range_start, range_end) = (*range.start(), *range.end());
    while range_start & 0xFFFF == 0 {
        let Some(before_start) = range_start.checked_sub(1) else {
            return Some(range_start..=range_end);
        };
        let (prev_key, _) = util::split(before_start);

        if containers.as_ref().last().is_some_and(|c| c.key == prev_key && c.contains(u16::MAX)) {
            let mut iter = containers.next_back().unwrap().into_iter();
            let next_range = iter.next_range_back().unwrap();
            *back_iter = Some(iter);
            debug_assert_eq!(*next_range.end(), before_start);
            range_start = *next_range.start();
        } else {
            if let Some(iter) = front_iter {
                if iter.key == prev_key && iter.peek_back() == Some(before_start) {
                    let next_range = iter.next_range_back().unwrap();
                    debug_assert_eq!(*next_range.end(), before_start);
                    range_start = *next_range.start();
                }
            }
            break;
        }
    }

    Some(range_start..=range_end)
}

fn advance_back_to_impl<'a, It>(
    n: u32,
    front_iter: &mut Option<container::Iter<'a>>,
    containers: &mut It,
    back_iter: &mut Option<container::Iter<'a>>,
) where
    It: DoubleEndedIterator,
    It: AsRef<[Container]>,
    It::Item: IntoIterator<IntoIter = container::Iter<'a>>,
{
    let (key, index) = util::split(n);
    if let Some(iter) = back_iter {
        match key.cmp(&iter.key) {
            core::cmp::Ordering::Greater => return,
            core::cmp::Ordering::Equal => {
                iter.advance_back_to(index);
                return;
            }
            core::cmp::Ordering::Less => {
                *back_iter = None;
            }
        }
    }
    let containers_slice = containers.as_ref();
    let containers_len = containers_slice.len();
    let to_skip = match containers_slice.binary_search_by_key(&key, |c| c.key) {
        Ok(n) => {
            // n must be less than containers_len, so this can never underflow
            let n = containers_len - n - 1;
            let container = containers.nth_back(n).expect("binary search returned a valid index");
            let mut container_iter = container.into_iter();
            container_iter.advance_back_to(index);
            *back_iter = Some(container_iter);
            return;
        }
        Err(n) => containers_len - n,
    };

    if let Some(n) = to_skip.checked_sub(1) {
        containers.nth_back(n);
    }
    if to_skip != containers_len {
        // There are still containers with keys less than the key we are looking for,
        // the key we're looking _can't_ be in the front iterator.
        return;
    }
    if let Some(iter) = front_iter {
        match key.cmp(&iter.key) {
            core::cmp::Ordering::Greater => {}
            core::cmp::Ordering::Equal => {
                iter.advance_back_to(index);
            }
            core::cmp::Ordering::Less => {
                *front_iter = None;
            }
        }
    }
}

impl Iter<'_> {
    fn new(containers: &'_ [Container]) -> Iter<'_> {
        Iter { front: None, containers: containers.iter(), back: None }
    }

    fn empty() -> Self {
        Self::new(&[])
    }

    /// Advance the iterator to the first position where the item has a value >= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringBitmap>();
    /// let mut iter = bitmap.iter();
    /// iter.advance_to(2);
    ///
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn advance_to(&mut self, n: u32) {
        advance_to_impl(n, &mut self.front, &mut self.containers, &mut self.back);
    }

    /// Advance the back of the iterator to the first position where the item has a value <= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringBitmap>();
    /// let mut iter = bitmap.iter();
    /// iter.advance_back_to(1);
    ///
    /// assert_eq!(iter.next_back(), Some(1));
    /// assert_eq!(iter.next_back(), None);
    /// ```
    pub fn advance_back_to(&mut self, n: u32) {
        advance_back_to_impl(n, &mut self.front, &mut self.containers, &mut self.back);
    }

    /// Returns the range of consecutive set bits from the current position to the end of the current run
    ///
    /// After this call, the iterator will be positioned at the first item after the returned range.
    /// Returns `None` if the iterator is exhausted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let bm = RoaringBitmap::from([1, 2, 4, 5]);
    /// let mut iter = bm.iter();
    /// assert_eq!(iter.next_range(), Some(1..=2));
    /// assert_eq!(iter.next(), Some(4));
    /// assert_eq!(iter.next_range(), Some(5..=5));
    /// ```
    pub fn next_range(&mut self) -> Option<core::ops::RangeInclusive<u32>> {
        next_range_impl(&mut self.front, &mut self.containers, &mut self.back)
    }

    /// Returns the range of consecutive set bits from the start of the current run to the current back position
    ///
    /// After this call, the back of the iterator will be positioned at the last item before the returned range.
    /// Returns `None` if the iterator is exhausted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let bm = RoaringBitmap::from([1, 2, 4, 5]);
    /// let mut iter = bm.iter();
    /// assert_eq!(iter.next_range_back(), Some(4..=5));
    /// assert_eq!(iter.next_back(), Some(2));
    /// assert_eq!(iter.next_range_back(), Some(1..=1));
    /// ```
    pub fn next_range_back(&mut self) -> Option<core::ops::RangeInclusive<u32>> {
        next_range_back_impl(&mut self.front, &mut self.containers, &mut self.back)
    }
}

impl IntoIter {
    fn new(containers: Vec<Container>) -> IntoIter {
        IntoIter { front: None, containers: containers.into_iter(), back: None }
    }

    fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Advance the iterator to the first position where the item has a value >= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringBitmap>();
    /// let mut iter = bitmap.iter();
    /// iter.advance_to(2);
    ///
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn advance_to(&mut self, n: u32) {
        advance_to_impl(n, &mut self.front, &mut self.containers, &mut self.back);
    }

    /// Advance the back of the iterator to the first position where the item has a value <= `n`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringBitmap>();
    /// let mut iter = bitmap.into_iter();
    /// iter.advance_back_to(1);
    ///
    /// assert_eq!(iter.next_back(), Some(1));
    /// assert_eq!(iter.next_back(), None);
    /// ```
    pub fn advance_back_to(&mut self, n: u32) {
        advance_back_to_impl(n, &mut self.front, &mut self.containers, &mut self.back);
    }

    /// Returns the range of consecutive set bits from the current position to the end of the current run
    ///
    /// After this call, the iterator will be positioned at the first item after the returned range.
    /// Returns `None` if the iterator is exhausted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let bm = RoaringBitmap::from([1, 2, 4, 5]);
    /// let mut iter = bm.into_iter();
    /// assert_eq!(iter.next_range(), Some(1..=2));
    /// assert_eq!(iter.next(), Some(4));
    /// assert_eq!(iter.next_range(), Some(5..=5));
    /// ```
    pub fn next_range(&mut self) -> Option<core::ops::RangeInclusive<u32>> {
        next_range_impl(&mut self.front, &mut self.containers, &mut self.back)
    }

    /// Returns the range of consecutive set bits from the start of the current run to the current back position
    ///
    /// After this call, the back of the iterator will be positioned at the last item before the returned range.
    /// Returns `None` if the iterator is exhausted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let bm = RoaringBitmap::from([1, 2, 4, 5]);
    /// let mut iter = bm.into_iter();
    /// assert_eq!(iter.next_range_back(), Some(4..=5));
    /// assert_eq!(iter.next_back(), Some(2));
    /// assert_eq!(iter.next_range_back(), Some(1..=1));
    /// ```
    pub fn next_range_back(&mut self) -> Option<core::ops::RangeInclusive<u32>> {
        next_range_back_impl(&mut self.front, &mut self.containers, &mut self.back)
    }
}

fn size_hint_impl(
    front: &Option<container::Iter<'_>>,
    containers: &impl AsRef<[Container]>,
    back: &Option<container::Iter<'_>>,
) -> (usize, Option<usize>) {
    let first_size = front.as_ref().map_or(0, |it| it.len());
    let last_size = back.as_ref().map_or(0, |it| it.len());
    let mut size = first_size + last_size;
    for container in containers.as_ref() {
        match size.checked_add(container.len() as usize) {
            Some(new_size) => size = new_size,
            None => return (usize::MAX, None),
        }
    }
    (size, Some(size))
}

impl Iterator for Iter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            if let Some(x) = and_then_or_clear(&mut self.front, Iterator::next) {
                return Some(x);
            }
            self.front = match self.containers.next() {
                Some(inner) => Some(inner.into_iter()),
                None => return and_then_or_clear(&mut self.back, Iterator::next),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        size_hint_impl(&self.front, &self.containers, &self.back)
    }

    #[inline]
    fn fold<B, F>(mut self, mut init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if let Some(iter) = &mut self.front {
            init = iter.fold(init, &mut f);
        }
        init = self.containers.fold(init, |acc, container| {
            let iter = <&Container>::into_iter(container);
            iter.fold(acc, &mut f)
        });
        if let Some(iter) = &mut self.back {
            init = iter.fold(init, &mut f);
        };
        init
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        let mut count = self.front.map_or(0, Iterator::count);
        count += self.containers.map(|container| container.len() as usize).sum::<usize>();
        count += self.back.map_or(0, Iterator::count);
        count
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut n = n;
        let nth_advance = |it: &mut container::Iter| {
            let len = it.len();
            if n < len {
                it.nth(n)
            } else {
                n -= len;
                None
            }
        };
        if let Some(x) = and_then_or_clear(&mut self.front, nth_advance) {
            return Some(x);
        }
        for container in self.containers.by_ref() {
            let len = container.len() as usize;
            if n < len {
                let mut front_iter = container.into_iter();
                let result = front_iter.nth(n);
                self.front = Some(front_iter);
                return result;
            }
            n -= len;
        }
        and_then_or_clear(&mut self.back, |it| it.nth(n))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(x) = and_then_or_clear(&mut self.back, DoubleEndedIterator::next_back) {
                return Some(x);
            }
            self.back = match self.containers.next_back() {
                Some(inner) => Some(inner.into_iter()),
                None => return and_then_or_clear(&mut self.front, DoubleEndedIterator::next_back),
            }
        }
    }

    #[inline]
    fn rfold<Acc, Fold>(mut self, mut init: Acc, mut fold: Fold) -> Acc
    where
        Fold: FnMut(Acc, Self::Item) -> Acc,
    {
        if let Some(iter) = &mut self.back {
            init = iter.rfold(init, &mut fold);
        }
        init = self.containers.rfold(init, |acc, container| {
            let iter = container.into_iter();
            iter.rfold(acc, &mut fold)
        });
        if let Some(iter) = &mut self.front {
            init = iter.rfold(init, &mut fold);
        };
        init
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let mut n = n;
        let nth_advance = |it: &mut container::Iter| {
            let len = it.len();
            if n < len {
                it.nth_back(n)
            } else {
                n -= len;
                None
            }
        };
        if let Some(x) = and_then_or_clear(&mut self.back, nth_advance) {
            return Some(x);
        }
        for container in self.containers.by_ref().rev() {
            let len = container.len() as usize;
            if n < len {
                let mut front_iter = container.into_iter();
                let result = front_iter.nth_back(n);
                self.back = Some(front_iter);
                return result;
            }
            n -= len;
        }
        and_then_or_clear(&mut self.front, |it| it.nth_back(n))
    }
}

#[cfg(target_pointer_width = "64")]
impl ExactSizeIterator for Iter<'_> {}
impl FusedIterator for Iter<'_> {}

impl Iterator for IntoIter {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            if let Some(x) = and_then_or_clear(&mut self.front, Iterator::next) {
                return Some(x);
            }
            match self.containers.next() {
                Some(inner) => self.front = Some(inner.into_iter()),
                None => return and_then_or_clear(&mut self.back, Iterator::next),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        size_hint_impl(&self.front, &self.containers, &self.back)
    }

    #[inline]
    fn fold<B, F>(mut self, mut init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        if let Some(iter) = &mut self.front {
            init = iter.fold(init, &mut f);
        }
        init = self.containers.fold(init, |acc, container| {
            let iter = <Container>::into_iter(container);
            iter.fold(acc, &mut f)
        });
        if let Some(iter) = &mut self.back {
            init = iter.fold(init, &mut f);
        };
        init
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        let mut count = self.front.map_or(0, Iterator::count);
        count += self.containers.map(|container| container.len() as usize).sum::<usize>();
        count += self.back.map_or(0, Iterator::count);
        count
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut n = n;
        let nth_advance = |it: &mut container::Iter| {
            let len = it.len();
            if n < len {
                it.nth(n)
            } else {
                n -= len;
                None
            }
        };
        if let Some(x) = and_then_or_clear(&mut self.front, nth_advance) {
            return Some(x);
        }
        for container in self.containers.by_ref() {
            let len = container.len() as usize;
            if n < len {
                let mut front_iter = container.into_iter();
                let result = front_iter.nth(n);
                self.front = Some(front_iter);
                return result;
            }
            n -= len;
        }
        and_then_or_clear(&mut self.back, |it| it.nth(n))
    }
}

impl DoubleEndedIterator for IntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(x) = and_then_or_clear(&mut self.back, DoubleEndedIterator::next_back) {
                return Some(x);
            }
            match self.containers.next_back() {
                Some(inner) => self.back = Some(inner.into_iter()),
                None => return and_then_or_clear(&mut self.front, DoubleEndedIterator::next_back),
            }
        }
    }

    #[inline]
    fn rfold<Acc, Fold>(mut self, mut init: Acc, mut fold: Fold) -> Acc
    where
        Fold: FnMut(Acc, Self::Item) -> Acc,
    {
        if let Some(iter) = &mut self.back {
            init = iter.rfold(init, &mut fold);
        }
        init = self.containers.rfold(init, |acc, container| {
            let iter = container.into_iter();
            iter.rfold(acc, &mut fold)
        });
        if let Some(iter) = &mut self.front {
            init = iter.rfold(init, &mut fold);
        };
        init
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let mut n = n;
        let nth_advance = |it: &mut container::Iter| {
            let len = it.len();
            if n < len {
                it.nth_back(n)
            } else {
                n -= len;
                None
            }
        };
        if let Some(x) = and_then_or_clear(&mut self.back, nth_advance) {
            return Some(x);
        }
        for container in self.containers.by_ref().rev() {
            let len = container.len() as usize;
            if n < len {
                let mut front_iter = container.into_iter();
                let result = front_iter.nth_back(n);
                self.back = Some(front_iter);
                return result;
            }
            n -= len;
        }
        and_then_or_clear(&mut self.front, |it| it.nth_back(n))
    }
}

#[cfg(target_pointer_width = "64")]
impl ExactSizeIterator for IntoIter {}
impl FusedIterator for IntoIter {}

impl RoaringBitmap {
    /// Iterator over each value stored in the RoaringBitmap, guarantees values are ordered by value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use core::iter::FromIterator;
    ///
    /// let bitmap = (1..3).collect::<RoaringBitmap>();
    /// let mut iter = bitmap.iter();
    ///
    /// assert_eq!(iter.next(), Some(1));
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&'_ self) -> Iter<'_> {
        Iter::new(&self.containers)
    }

    /// Iterator over values within a range stored in the RoaringBitmap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use core::ops::Bound;
    /// use roaring::RoaringBitmap;
    ///
    /// let bitmap = RoaringBitmap::from([0, 1, 2, 3, 4, 5, 10, 11, 12, 20, 21, u32::MAX]);
    /// let mut iter = bitmap.range(10..20);
    ///
    /// assert_eq!(iter.next(), Some(10));
    /// assert_eq!(iter.next(), Some(11));
    /// assert_eq!(iter.next(), Some(12));
    /// assert_eq!(iter.next(), None);
    ///
    /// let mut iter = bitmap.range(100..);
    /// assert_eq!(iter.next(), Some(u32::MAX));
    /// assert_eq!(iter.next(), None);
    ///
    /// let mut iter = bitmap.range((Bound::Excluded(0), Bound::Included(10)));
    /// assert_eq!(iter.next(), Some(1));
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), Some(3));
    /// assert_eq!(iter.next(), Some(4));
    /// assert_eq!(iter.next(), Some(5));
    /// assert_eq!(iter.next(), Some(10));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn range<R>(&self, range: R) -> Iter<'_>
    where
        R: RangeBounds<u32>,
    {
        let range = match util::convert_range_to_inclusive(range) {
            Ok(range) => range,
            Err(util::ConvertRangeError::Empty) => return Iter::empty(),
            Err(util::ConvertRangeError::StartGreaterThanEnd) => {
                panic!("range start is greater than range end")
            }
            Err(util::ConvertRangeError::StartAndEndEqualExcluded) => {
                panic!("range start and end are equal and excluded")
            }
        };
        let (start, end) = (*range.start(), *range.end());
        let mut iter = self.iter();
        if start != 0 {
            iter.advance_to(start);
        }
        if end != u32::MAX {
            iter.advance_back_to(end);
        }
        iter
    }

    /// Iterator over values within a range stored in the RoaringBitmap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use core::ops::Bound;
    /// use roaring::RoaringBitmap;
    ///
    /// fn bitmap() -> RoaringBitmap {
    ///     RoaringBitmap::from([0, 1, 2, 3, 4, 5, 10, 11, 12, 20, 21, u32::MAX])
    /// }
    ///
    /// let mut iter = bitmap().into_range(10..20);
    ///
    /// assert_eq!(iter.next(), Some(10));
    /// assert_eq!(iter.next(), Some(11));
    /// assert_eq!(iter.next(), Some(12));
    /// assert_eq!(iter.next(), None);
    ///
    /// let mut iter = bitmap().into_range(100..);
    /// assert_eq!(iter.next(), Some(u32::MAX));
    /// assert_eq!(iter.next(), None);
    ///
    /// let mut iter = bitmap().into_range((Bound::Excluded(0), Bound::Included(10)));
    /// assert_eq!(iter.next(), Some(1));
    /// assert_eq!(iter.next(), Some(2));
    /// assert_eq!(iter.next(), Some(3));
    /// assert_eq!(iter.next(), Some(4));
    /// assert_eq!(iter.next(), Some(5));
    /// assert_eq!(iter.next(), Some(10));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn into_range<R>(self, range: R) -> IntoIter
    where
        R: RangeBounds<u32>,
    {
        let range = match util::convert_range_to_inclusive(range) {
            Ok(range) => range,
            Err(util::ConvertRangeError::Empty) => return IntoIter::empty(),
            Err(util::ConvertRangeError::StartGreaterThanEnd) => {
                panic!("range start is greater than range end")
            }
            Err(util::ConvertRangeError::StartAndEndEqualExcluded) => {
                panic!("range start and end are equal and excluded")
            }
        };
        let (start, end) = (*range.start(), *range.end());
        let mut iter = self.into_iter();
        if start != 0 {
            iter.advance_to(start);
        }
        if end != u32::MAX {
            iter.advance_back_to(end);
        }
        iter
    }
}

impl<'a> IntoIterator for &'a RoaringBitmap {
    type Item = u32;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl IntoIterator for RoaringBitmap {
    type Item = u32;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter::new(self.containers)
    }
}

impl<const N: usize> From<[u32; N]> for RoaringBitmap {
    fn from(arr: [u32; N]) -> Self {
        RoaringBitmap::from_iter(arr)
    }
}

impl FromIterator<u32> for RoaringBitmap {
    fn from_iter<I: IntoIterator<Item = u32>>(iterator: I) -> RoaringBitmap {
        let mut rb = RoaringBitmap::new();
        rb.extend(iterator);
        rb
    }
}

impl<'a> FromIterator<&'a u32> for RoaringBitmap {
    fn from_iter<I: IntoIterator<Item = &'a u32>>(iterator: I) -> RoaringBitmap {
        let mut rb = RoaringBitmap::new();
        rb.extend(iterator);
        rb
    }
}

impl Extend<u32> for RoaringBitmap {
    /// Inserts multiple values and returns the count of new additions.
    /// This is expected to be faster than calling [`RoaringBitmap::insert`] on each value.
    ///
    /// The provided integers values don't have to be in sorted order, but it may be preferable
    /// to sort them from a performance point of view.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.extend([1, 2, 3, 4, 1500, 1508, 1507, 1509]);
    /// assert!(rb.contains(2));
    /// assert!(rb.contains(1508));
    /// assert!(!rb.contains(5));
    /// ```
    #[inline]
    fn extend<I: IntoIterator<Item = u32>>(&mut self, values: I) {
        let mut values = values.into_iter();
        let value = match values.next() {
            Some(value) => value,
            None => return,
        };

        let (mut currenthb, lowbit) = util::split(value);
        let mut current_container_index = self.find_container_by_key(currenthb);
        let mut current_cont = &mut self.containers[current_container_index];
        current_cont.insert(lowbit);

        for val in values {
            let (newhb, lowbit) = util::split(val);
            if currenthb == newhb {
                // easy case, this could be quite frequent
                current_cont.insert(lowbit);
            } else {
                currenthb = newhb;
                current_container_index = self.find_container_by_key(currenthb);
                current_cont = &mut self.containers[current_container_index];
                current_cont.insert(lowbit);
            }
        }
    }
}

impl<'a> Extend<&'a u32> for RoaringBitmap {
    /// Inserts multiple values and returns the count of new additions.
    /// This is expected to be faster than calling [`RoaringBitmap::insert`] on each value.
    ///
    /// The provided integers values don't have to be in sorted order, but it may be preferable
    /// to sort them from a performance point of view.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// rb.extend([1, 2, 3, 4, 1500, 1508, 1507, 1509]);
    /// assert!(rb.contains(2));
    /// assert!(rb.contains(1508));
    /// assert!(!rb.contains(5));
    /// ```
    #[inline]
    fn extend<I: IntoIterator<Item = &'a u32>>(&mut self, values: I) {
        self.extend(values.into_iter().copied());
    }
}

impl RoaringBitmap {
    /// Create the set from a sorted iterator. Values must be sorted and deduplicated.
    ///
    /// The values of the iterator must be ordered and strictly greater than the greatest value
    /// in the set. If a value in the iterator doesn't satisfy this requirement, it is not added
    /// and the append operation is stopped.
    ///
    /// Returns `Ok` with the requested `RoaringBitmap`, `Err` with the number of elements
    /// that were correctly appended before failure.
    ///
    /// # Example: Create a set from an ordered list of integers.
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::from_sorted_iter(0..10).unwrap();
    ///
    /// assert!(rb.iter().eq(0..10));
    /// ```
    ///
    /// # Example: Try to create a set from a non-ordered list of integers.
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let integers = 0..10u32;
    /// let error = RoaringBitmap::from_sorted_iter(integers.rev()).unwrap_err();
    ///
    /// assert_eq!(error.valid_until(), 1);
    /// ```
    pub fn from_sorted_iter<I: IntoIterator<Item = u32>>(
        iterator: I,
    ) -> Result<RoaringBitmap, NonSortedIntegers> {
        let mut rb = RoaringBitmap::new();
        rb.append(iterator).map(|_| rb)
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
    /// use roaring::RoaringBitmap;
    ///
    /// let mut rb = RoaringBitmap::new();
    /// assert_eq!(rb.append(0..10), Ok(10));
    ///
    /// assert!(rb.iter().eq(0..10));
    /// ```
    pub fn append<I: IntoIterator<Item = u32>>(
        &mut self,
        iterator: I,
    ) -> Result<u64, NonSortedIntegers> {
        // Name shadowed to prevent accidentally referencing the param
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
