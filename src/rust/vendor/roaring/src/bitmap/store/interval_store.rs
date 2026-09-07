use alloc::vec::Vec;
use core::ops::{
    BitAnd, BitAndAssign, BitOrAssign, BitXor, BitXorAssign, RangeInclusive, SubAssign,
};
use core::slice::Iter;
use core::{cmp::Ordering, ops::ControlFlow};

use super::{ArrayStore, BitmapStore};

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct IntervalStore(Vec<Interval>);

pub(crate) const RUN_NUM_BYTES: usize = 2;
pub(crate) const RUN_ELEMENT_BYTES: usize = 4;

impl Default for IntervalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl IntervalStore {
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub fn new_with_range(range: Interval) -> Self {
        Self(alloc::vec![range])
    }

    pub fn full() -> Self {
        Self(alloc::vec![Interval::new_unchecked(0, u16::MAX)])
    }

    pub fn byte_size(&self) -> usize {
        Self::serialized_byte_size(self.run_amount())
    }

    pub fn serialized_byte_size(run_amount: u64) -> usize {
        RUN_NUM_BYTES + (RUN_ELEMENT_BYTES * run_amount as usize)
    }

    #[cfg(feature = "std")]
    pub fn from_vec_unchecked(vec: Vec<Interval>) -> Self {
        #[cfg(debug_assertions)]
        {
            for win in vec.windows(2) {
                let [cur_interval, next] = [win[0], win[1]];
                assert!(cur_interval.end + 1 < next.start);
                assert!(cur_interval.start <= cur_interval.end);
            }
        }
        Self(vec)
    }

    pub(crate) fn push_interval_unchecked(&mut self, interval: Interval) {
        debug_assert!(self.0.last().map(|f| f.end < interval.start).unwrap_or(true));
        debug_assert!(interval.start <= interval.end);
        self.0.push(interval)
    }

    #[inline]
    pub fn insert(&mut self, index: u16) -> bool {
        // All intervals before idx are _fully_ before our index (iv.end < index)
        let idx = self.0.partition_point(|iv| iv.end < index);
        let (before, maybe_after) = self.0.split_at_mut(idx);
        if let Some(next) = maybe_after.first_mut() {
            // Check if the next interval actually already contains our index
            // Because of partition_point, we know already know end >= index
            if next.start <= index {
                // index is already in the interval
                return false;
            }
            // `next` is instead the first interval _after_ our index,
            // check if we should grow that interval down by one
            // Because we know from above that next.start > index, adding 1 is safe
            if next.start == index + 1 {
                next.start -= 1;

                // Check if the previous interval will now be continuous with this interval
                if let Some(prev) = before.last_mut() {
                    // From the partition point: prev.end < index, subtracting 1 is safe
                    if prev.end == index - 1 {
                        prev.end = next.end;
                        self.0.remove(idx);
                    }
                }
                return true;
            }
        }
        if let Some(prev) = before.last_mut() {
            // Because we know from the partition point that prev.end < index, adding 1 is safe
            if prev.end + 1 == index {
                // Merge with previous interval
                prev.end += 1;
                // If we had needed to merge with the next interval, we would have handled that in
                // the previous if statement, so we're done here
                return true;
            }
        }
        self.0.insert(idx, Interval::new_unchecked(index, index));
        true
    }

    #[inline]
    pub fn insert_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        if range.is_empty() {
            return 0;
        }
        let mut interval = Interval { start: *range.start(), end: *range.end() };
        // All intervals in `start_idx..end_idx` are fully contained in our interval.
        let mut start_idx = self.0.partition_point(|iv| iv.start < interval.start);
        let mut end_idx =
            self.0[start_idx..].partition_point(|iv| iv.end <= interval.end) + start_idx;

        if let Some(prev) = self.0[..start_idx].last() {
            // If the previous interval contains our start, or would be contiguous with us, expand
            // to include it
            // from partition point, we know prev.start < interval.start
            if prev.end >= interval.start - 1 {
                // We need to merge with the previous interval
                interval.start = prev.start;
                interval.end = interval.end.max(prev.end);
                start_idx -= 1;
            }
        }
        if let Some(next) = self.0.get(end_idx) {
            // from partition point, we know next.end > interval.end
            if next.start <= interval.end + 1 {
                // We need to merge with the next interval
                interval.end = next.end;
                interval.start = interval.start.min(next.start);
                end_idx += 1;
            }
        }

        let mut added_count = interval.run_len();
        // Replace the first interval to be replaced with an interval covering the new range
        // and remove the rest
        // Otherwise, just insert a new interval
        if let [first, rest @ ..] = &mut self.0[start_idx..end_idx] {
            added_count -= first.run_len();
            added_count -= rest.iter().map(|iv| iv.run_len()).sum::<u64>();
            *first = interval;
            self.0.drain(start_idx + 1..end_idx);
        } else {
            // No intervals to merge with, we can just insert
            self.0.insert(start_idx, interval);
        }
        added_count
    }

    pub fn push(&mut self, index: u16) -> bool {
        if let Some(last_interval) = self.0.last_mut() {
            if last_interval.end.checked_add(1).map(|f| f == index).unwrap_or(false) {
                last_interval.end = index;
                true
            } else if last_interval.end < index {
                self.0.push(Interval::new_unchecked(index, index));
                true
            } else {
                false
            }
        } else {
            self.0.push(Interval::new_unchecked(index, index));
            true
        }
    }

    pub fn remove(&mut self, index: u16) -> bool {
        self.0
            .binary_search_by(|iv| cmp_index_interval(index, *iv).reverse())
            .map(|loc| {
                // loc always points to an interval
                if index == self.0[loc].start && index == self.0[loc].end {
                    // Remove entire run if it only contains this value
                    self.0.remove(loc);
                } else if index == self.0[loc].end {
                    // Value is last in this interval
                    self.0[loc].end = index - 1;
                } else if index == self.0[loc].start {
                    // Value is first in this interval
                    self.0[loc].start = index + 1;
                } else {
                    // Value lies inside the interval, we need to split it
                    // First construct a new interval with the right part
                    let new_interval = Interval::new_unchecked(index + 1, self.0[loc].end);
                    // Then shrink the current interval
                    self.0[loc].end = index - 1;
                    // Then insert the new interval leaving gap where value was removed
                    self.0.insert(loc + 1, new_interval);
                }
            })
            .is_ok()
    }

    pub fn remove_range(&mut self, range: RangeInclusive<u16>) -> u64 {
        if range.is_empty() {
            return 0;
        }

        let mut interval = Interval::new_unchecked(*range.start(), *range.end());
        // All intervals in `start_idx..end_idx` are fully contained in our interval.
        let start_idx = self.0.partition_point(|iv| iv.start < interval.start);
        let end_idx = self.0[start_idx..].partition_point(|iv| iv.end <= interval.end) + start_idx;
        let mut removed_count = 0;
        let mut add_needed = false;
        if let Some(prev) = self.0[..start_idx].last_mut() {
            // If the previous interval contains our start, remove it
            // from partition point, we know prev.start < interval.start
            if prev.end >= interval.start {
                // We need to remove from the previous interval
                removed_count +=
                    Interval::new_unchecked(interval.start, prev.end.min(interval.end)).run_len();
                let new_end = interval.start - 1;
                add_needed = prev.end > interval.end;
                if add_needed {
                    interval.start = interval.end + 1;
                    interval.end = prev.end;
                }
                prev.end = new_end;
            }
        }
        if let Some(next) = self.0.get_mut(end_idx) {
            // from partition point, we know next.end > interval.end
            if next.start <= interval.end {
                // We need to remove everything til interval.end
                removed_count +=
                    Interval::new_unchecked(next.start.max(interval.start), interval.end).run_len();
                next.start = interval.end + 1;
            }
        }

        // Replace the first interval to be replaced with an interval covering the new range
        // and remove the rest
        // Otherwise, just insert a new interval
        if let [first, rest @ ..] = &mut self.0[start_idx..end_idx] {
            removed_count += first.run_len();
            removed_count += rest.iter().map(|iv| iv.run_len()).sum::<u64>();
            self.0.drain(start_idx..end_idx);
        } else if add_needed {
            // We are removing a range contained in a single interval
            // As such we must add a new interval
            self.0.insert(start_idx, interval);
        }
        removed_count
    }

    pub fn remove_smallest(&mut self, mut amount: u64) {
        let mut remove_to = 0;
        let mut last_interval = None;
        for (i, interval) in self.0.iter_mut().enumerate() {
            let too_much = interval.run_len() < amount;
            if too_much {
                amount -= interval.run_len();
            }
            remove_to = i;
            last_interval = Some(interval);
            if !too_much {
                break;
            }
        }
        if let Some(last_interval) = last_interval {
            // `<=`, not `<`: when the amount left equals this interval's run
            // length the whole interval is consumed and must be dropped, not
            // shrunk to `start = end + 1` (an inverted interval) (#359).
            if last_interval.run_len() <= amount {
                remove_to += 1;
            } else {
                last_interval.start += amount as u16;
            }
        }
        self.0.drain(..remove_to);
    }

    pub fn remove_biggest(&mut self, mut amount: u64) {
        let mut remove_to = 0;
        let mut last_interval = None;
        for (i, interval) in self.0.iter_mut().enumerate().rev() {
            let too_much = interval.run_len() < amount;
            if too_much {
                amount -= interval.run_len();
            }
            remove_to = i;
            last_interval = Some(interval);
            if !too_much {
                break;
            }
        }
        if let Some(last_interval) = last_interval {
            // `>`, not `>=`: when the amount left equals this interval's run
            // length the whole interval is consumed and is removed by the
            // `drain` below; shrinking it would set `end = start - 1` (an
            // inverted interval) (#359).
            if last_interval.run_len() > amount {
                remove_to += 1;
                last_interval.end -= amount as u16;
            }
        }
        self.0.drain(remove_to..);
    }

    pub fn contains(&self, index: u16) -> bool {
        self.0.binary_search_by(|iv| cmp_index_interval(index, *iv).reverse()).is_ok()
    }

    pub fn contains_range(&self, range: RangeInclusive<u16>) -> bool {
        let interval = Interval::new_unchecked(*range.start(), *range.end());
        let start = self.0.binary_search_by(|iv| cmp_index_interval(interval.start, *iv).reverse());
        let end = self.0.binary_search_by(|iv| cmp_index_interval(interval.end, *iv).reverse());
        match (start, end) {
            // both start and end are inside an interval,
            // check if this interval is that same interval.
            // If this is not the case then this range is not contained in this store
            (Ok(start_id), Ok(end_id)) => start_id == end_id,
            _ => false,
        }
    }

    fn step_walk<
        'a,
        R,
        C: FnMut(Interval, Interval, R) -> ControlFlow<R, R>,
        E: FnMut(
            (Option<Interval>, Option<Interval>),
            (Iter<'a, Interval>, Iter<'a, Interval>),
            R,
        ) -> R,
    >(
        &'a self,
        other: &'a Self,
        mut calc: C,
        mut else_op: E,
        mut buffer: R,
    ) -> R {
        let (mut i1, mut i2) = (self.0.iter(), other.0.iter());
        let (mut iv1, mut iv2) = (i1.next(), i2.next());
        loop {
            match (iv1, iv2) {
                (Some(v1), Some(v2)) => {
                    match calc(*v1, *v2, buffer) {
                        ControlFlow::Continue(new_buffer) => buffer = new_buffer,
                        ControlFlow::Break(end) => return end,
                    }

                    // We increase the iterator based on which one is furthest behind.
                    // Or both if they are equal to each other.
                    match v1.end.cmp(&v2.end) {
                        Ordering::Less => iv1 = i1.next(),
                        Ordering::Greater => iv2 = i2.next(),
                        Ordering::Equal => {
                            iv1 = i1.next();
                            iv2 = i2.next();
                        }
                    }
                }
                (value1, value2) => {
                    return else_op((value1.copied(), value2.copied()), (i1, i2), buffer)
                }
            }
        }
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.step_walk(
            other,
            |interval1, interval2, _| {
                if interval1.overlaps(&interval2) {
                    ControlFlow::Break(false)
                } else {
                    ControlFlow::Continue(true)
                }
            },
            |_, _, _| true,
            false,
        )
    }

    pub(crate) fn is_disjoint_array(&self, array: &ArrayStore) -> bool {
        array.iter().all(|&i| !self.contains(i))
    }

    pub(crate) fn is_disjoint_bitmap(&self, array: &BitmapStore) -> bool {
        // TODO: make this better
        array.iter().all(|i| !self.contains(i))
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.iter().all(|interval| other.contains_range(interval.start..=interval.end))
    }

    pub(crate) fn is_subset_array(&self, other: &ArrayStore) -> bool {
        self.0.iter().all(|interval| other.contains_range(interval.start..=interval.end))
    }

    pub(crate) fn is_subset_bitmap(&self, other: &BitmapStore) -> bool {
        self.0.iter().all(|interval| other.contains_range(interval.start..=interval.end))
    }

    pub fn intersection_len(&self, other: &Self) -> u64 {
        self.step_walk(
            other,
            |interval1, interval2, buffer| {
                ControlFlow::Continue(
                    interval1.overlapping_interval(&interval2).map(|f| f.run_len()).unwrap_or(0)
                        + buffer,
                )
            },
            |_, _, buffer| buffer,
            0,
        )
    }

    pub(crate) fn intersection_len_bitmap(&self, other: &BitmapStore) -> u64 {
        self.0.iter().map(|f| other.intersection_len_interval(f)).sum()
    }

    pub(crate) fn intersection_len_array(&self, other: &ArrayStore) -> u64 {
        other.iter().map(|&f| self.contains(f) as u64).sum()
    }

    pub fn len(&self) -> u64 {
        self.0.iter().map(|iv| iv.run_len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn min(&self) -> Option<u16> {
        self.0.first().map(|f| f.start)
    }

    pub fn max(&self) -> Option<u16> {
        self.0.last().map(|f| f.end)
    }

    pub fn rank(&self, value: u16) -> u64 {
        let mut rank = 0;
        for iv in self.0.iter() {
            if iv.end <= value {
                rank += iv.run_len();
            } else if iv.start <= value {
                rank += Interval::new_unchecked(iv.start, value).run_len();
            } else {
                break;
            }
        }
        rank
    }

    pub fn select(&self, mut n: u16) -> Option<u16> {
        for iv in self.0.iter() {
            let run_len = iv.run_len();
            if run_len <= n.into() {
                n -= iv.run_len() as u16; // this conversion never overflows since run_len is
                                          // smaller then a u16
            } else {
                return Some(iv.start + n);
            }
        }
        None
    }

    pub fn run_amount(&self) -> u64 {
        self.0.len() as u64
    }

    pub fn to_bitmap(&self) -> BitmapStore {
        let mut bits = BitmapStore::new();
        for iv in self.0.iter() {
            bits.insert_range(iv.start..=iv.end);
        }
        bits
    }

    pub fn to_array(&self) -> ArrayStore {
        let mut array = ArrayStore::new();
        for iv in self.0.iter() {
            array.insert_range(iv.start..=iv.end);
        }
        array
    }

    pub(crate) fn iter(&'_ self) -> RunIterBorrowed<'_> {
        self.into_iter()
    }

    pub(crate) fn iter_intervals(&'_ self) -> core::slice::Iter<'_, Interval> {
        self.0.iter()
    }

    pub(crate) fn internal_validate(&self) -> Result<(), &'static str> {
        if self.0.is_empty() {
            return Err("run container with zero runs");
        }
        let mut last_end: Option<u16> = None;
        for run in &self.0 {
            if run.start > run.end {
                return Err("empty run container");
            }
            if let Some(last_end) = last_end.replace(run.end) {
                if last_end >= run.start {
                    return Err("overlapping or unordered runs");
                }
                if last_end.saturating_add(1) >= run.start {
                    return Err("contiguous runs");
                }
            }
        }

        Ok(())
    }
}

impl From<IntervalStore> for BitmapStore {
    fn from(value: IntervalStore) -> Self {
        value.to_bitmap()
    }
}

impl From<IntervalStore> for ArrayStore {
    fn from(value: IntervalStore) -> Self {
        value.to_array()
    }
}

impl BitOrAssign for IntervalStore {
    fn bitor_assign(&mut self, mut rhs: Self) {
        let (add_intervals, take_intervals, self_is_add) =
            if self.len() > rhs.len() { (self, &mut rhs, true) } else { (&mut rhs, self, false) };
        for iv in take_intervals.iter_intervals() {
            add_intervals.insert_range(iv.start..=iv.end);
        }
        if !self_is_add {
            core::mem::swap(add_intervals, take_intervals);
        }
    }
}

impl BitOrAssign<&ArrayStore> for IntervalStore {
    fn bitor_assign(&mut self, rhs: &ArrayStore) {
        for &i in rhs.iter() {
            self.insert(i);
        }
    }
}

impl BitOrAssign<&Self> for IntervalStore {
    fn bitor_assign(&mut self, rhs: &Self) {
        for iv in rhs.iter_intervals() {
            self.insert_range(iv.start..=iv.end);
        }
    }
}

impl BitAnd for &IntervalStore {
    type Output = IntervalStore;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.step_walk(
            rhs,
            |iv1, iv2, mut buf: IntervalStore| {
                if let Some(new_iv) = iv1.overlapping_interval(&iv2) {
                    buf.insert_range(new_iv.start..=new_iv.end);
                }
                ControlFlow::Continue(buf)
            },
            |_, _, buf| buf,
            IntervalStore::new(),
        )
    }
}

impl BitAndAssign<&IntervalStore> for ArrayStore {
    fn bitand_assign(&mut self, rhs: &IntervalStore) {
        self.retain(|f| rhs.contains(f));
    }
}

impl SubAssign<&Self> for IntervalStore {
    fn sub_assign(&mut self, rhs: &Self) {
        for iv in rhs.iter_intervals() {
            self.remove_range(iv.start..=iv.end);
        }
    }
}

impl BitXor for &IntervalStore {
    type Output = IntervalStore;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut union = self.clone();
        union |= rhs;
        let intersection = self & rhs;
        union -= &intersection;
        union
    }
}

impl BitXorAssign<&ArrayStore> for IntervalStore {
    fn bitxor_assign(&mut self, rhs: &ArrayStore) {
        rhs.iter().for_each(|&f| {
            if self.contains(f) {
                self.remove(f);
            } else {
                self.insert(f);
            }
        })
    }
}

pub(crate) type RunIterOwned = RunIter<alloc::vec::IntoIter<Interval>>;
pub(crate) type RunIterBorrowed<'a> = RunIter<core::slice::Iter<'a, Interval>>;

impl IntoIterator for IntervalStore {
    type Item = u16;
    type IntoIter = RunIter<alloc::vec::IntoIter<Interval>>;

    fn into_iter(self) -> Self::IntoIter {
        RunIter::new(self.0.into_iter())
    }
}

impl<'a> IntoIterator for &'a IntervalStore {
    type Item = u16;
    type IntoIter = RunIter<core::slice::Iter<'a, Interval>>;

    fn into_iter(self) -> Self::IntoIter {
        RunIter::new(self.0.iter())
    }
}

pub(crate) trait SliceIterator<I>: Iterator + DoubleEndedIterator {
    fn as_slice(&self) -> &[I];
}

impl<I> SliceIterator<I> for alloc::vec::IntoIter<I> {
    fn as_slice(&self) -> &[I] {
        alloc::vec::IntoIter::as_slice(self)
    }
}

impl<'a, I> SliceIterator<I> for core::slice::Iter<'a, I> {
    fn as_slice(&self) -> &'a [I] {
        core::slice::Iter::as_slice(self)
    }
}

#[derive(Clone)]
pub(crate) struct RunIter<I: SliceIterator<Interval>> {
    forward_offset: u16,
    backward_offset: u16,
    intervals: I,
}

impl<I: SliceIterator<Interval>> RunIter<I> {
    fn new(intervals: I) -> Self {
        Self { forward_offset: 0, backward_offset: 0, intervals }
    }

    fn move_next(&mut self) {
        if let Some(value) = self.forward_offset.checked_add(1) {
            self.forward_offset = value;
        } else {
            self.intervals.next();
            self.forward_offset = 0;
            return;
        }
        let only_interval = self.intervals.as_slice().len() == 1;
        let total_offset = u64::from(self.forward_offset)
            + if only_interval { u64::from(self.backward_offset) } else { 0 };
        if Some(total_offset) >= self.intervals.as_slice().first().map(|f| f.run_len()) {
            self.intervals.next();
            self.forward_offset = 0;
            if only_interval {
                self.backward_offset = 0;
            }
        }
    }

    fn move_next_back(&mut self) {
        if let Some(value) = self.backward_offset.checked_add(1) {
            self.backward_offset = value;
        } else {
            self.intervals.next_back();
            self.backward_offset = 0;
            return;
        }
        let only_interval = self.intervals.as_slice().len() == 1;
        let total_offset = u64::from(self.backward_offset)
            + if only_interval { u64::from(self.forward_offset) } else { 0 };
        if Some(total_offset) >= self.intervals.as_slice().last().map(|f| f.run_len()) {
            self.intervals.next_back();
            self.backward_offset = 0;
            if only_interval {
                self.forward_offset = 0;
            }
        }
    }

    fn remaining_size(&self) -> usize {
        let total_size = self.intervals.as_slice().iter().map(|f| f.run_len()).sum::<u64>();
        let total_offset = u64::from(self.forward_offset) + u64::from(self.backward_offset);
        debug_assert!(total_size >= total_offset);
        total_size.saturating_sub(total_offset) as usize
    }

    /// Advance the iterator to the first value greater than or equal to `n`.
    pub(crate) fn advance_to(&mut self, n: u16) {
        if n == 0 {
            return;
        }
        if self
            .intervals
            .as_slice()
            .first()
            .map(|f| f.start + self.forward_offset > n)
            .unwrap_or(true)
        {
            return;
        }
        match self.intervals.as_slice().binary_search_by(|iv| cmp_index_interval(n, *iv).reverse())
        {
            Ok(index) => {
                if let Some(value) = index.checked_sub(1) {
                    self.intervals.nth(value);
                }
                let first_interval = self.intervals.as_slice().first().unwrap();
                self.forward_offset = n - first_interval.start;
                if self.intervals.as_slice().len() == 1
                    && u64::from(self.forward_offset) + u64::from(self.backward_offset)
                        >= first_interval.run_len()
                {
                    // If we are now the only interval, and we've now met the forward offset,
                    // consume the final interval
                    _ = self.intervals.next();
                    self.forward_offset = 0;
                    self.backward_offset = 0;
                }
            }
            Err(index) => {
                if index == self.intervals.as_slice().len() {
                    // Consume the whole iterator
                    self.intervals.nth(index);
                    self.forward_offset = 0;
                    self.backward_offset = 0;
                    return;
                }
                if let Some(value) = index.checked_sub(1) {
                    self.intervals.nth(value);
                    self.forward_offset = 0;
                }
            }
        }
    }

    /// Advance the back of iterator to the first value less than or equal to `n`.
    pub(crate) fn advance_back_to(&mut self, n: u16) {
        if n == u16::MAX {
            return;
        }
        if self
            .intervals
            .as_slice()
            .last()
            .map(|f| f.end - self.backward_offset < n)
            .unwrap_or(true)
        {
            return;
        }
        match self.intervals.as_slice().binary_search_by(|iv| cmp_index_interval(n, *iv).reverse())
        {
            Ok(index) => {
                let backward_index = self.intervals.as_slice().len() - index - 1;
                if let Some(value) = backward_index.checked_sub(1) {
                    self.intervals.nth_back(value);
                }
                let last_interval = self.intervals.as_slice().last().unwrap();
                self.backward_offset = last_interval.end - n;
                if self.intervals.as_slice().len() == 1
                    && u64::from(self.forward_offset) + u64::from(self.backward_offset)
                        >= last_interval.run_len()
                {
                    // If we are now the only interval, and we've now met the forward offset,
                    // consume the final interval
                    _ = self.intervals.next_back();
                    self.forward_offset = 0;
                    self.backward_offset = 0;
                }
            }
            Err(index) => {
                if index == 0 {
                    // Consume the whole iterator
                    self.intervals.nth_back(self.intervals.as_slice().len());
                    self.forward_offset = 0;
                    self.backward_offset = 0;
                    return;
                }
                let backward_index = self.intervals.as_slice().len() - index;
                if let Some(value) = backward_index.checked_sub(1) {
                    self.intervals.nth_back(value);
                    self.backward_offset = 0;
                }
            }
        }
    }

    pub(crate) fn next_range(&mut self) -> Option<RangeInclusive<u16>> {
        let interval = self.intervals.as_slice().first()?;
        let end_offset =
            if self.intervals.as_slice().len() == 1 { self.backward_offset } else { 0 };
        let result = interval.start + self.forward_offset..=interval.end - end_offset;
        _ = self.intervals.next();
        self.forward_offset = 0;
        if self.intervals.as_slice().is_empty() {
            self.backward_offset = 0;
        }
        Some(result)
    }

    pub(crate) fn next_range_back(&mut self) -> Option<RangeInclusive<u16>> {
        let interval = self.intervals.as_slice().last()?;
        let start_offset =
            if self.intervals.as_slice().len() == 1 { self.forward_offset } else { 0 };
        let result = interval.start + start_offset..=interval.end - self.backward_offset;
        _ = self.intervals.next_back();
        self.backward_offset = 0;
        if self.intervals.as_slice().is_empty() {
            self.forward_offset = 0;
        }
        Some(result)
    }

    pub(crate) fn peek(&self) -> Option<u16> {
        let result = self.intervals.as_slice().first()?.start + self.forward_offset;
        Some(result)
    }

    pub(crate) fn peek_back(&self) -> Option<u16> {
        let result = self.intervals.as_slice().last()?.end - self.backward_offset;
        Some(result)
    }
}

impl<I: SliceIterator<Interval>> Iterator for RunIter<I> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        let result = self.intervals.as_slice().first()?.start + self.forward_offset;
        self.move_next();
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_size = self.remaining_size();
        (remaining_size, Some(remaining_size))
    }

    fn count(self) -> usize {
        self.remaining_size()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n > usize::from(u16::MAX) {
            // Consume the whole iterator
            self.intervals.nth(self.intervals.as_slice().len());
            self.forward_offset = 0;
            self.backward_offset = 0;
            return None;
        }
        let mut to_skip = n as u64;
        loop {
            let full_first_interval_len = self.intervals.as_slice().first()?.run_len();
            let consumed_len = u64::from(self.forward_offset)
                + if self.intervals.as_slice().len() == 1 {
                    u64::from(self.backward_offset)
                } else {
                    0
                };
            let to_remove = (full_first_interval_len - consumed_len).min(to_skip);
            to_skip -= to_remove;
            self.forward_offset += to_remove as u16;
            if consumed_len + to_remove >= full_first_interval_len {
                self.move_next();
            }
            if to_skip == 0 {
                break;
            }
        }
        self.next()
    }
}

impl<I: SliceIterator<Interval>> DoubleEndedIterator for RunIter<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let result = self.intervals.as_slice().last()?.end - self.backward_offset;
        self.move_next_back();
        Some(result)
    }
}

impl<I: SliceIterator<Interval>> ExactSizeIterator for RunIter<I> {}

/// This interval is inclusive to end.
#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
pub(crate) struct Interval {
    start: u16,
    end: u16,
}

impl From<RangeInclusive<u16>> for Interval {
    fn from(value: RangeInclusive<u16>) -> Self {
        Interval::new_unchecked(*value.start(), *value.end())
    }
}

impl IntoIterator for Interval {
    type Item = u16;
    type IntoIter = RangeInclusive<u16>;

    fn into_iter(self) -> Self::IntoIter {
        self.start..=self.end
    }
}

impl IntoIterator for &'_ Interval {
    type Item = u16;
    type IntoIter = RangeInclusive<u16>;

    fn into_iter(self) -> Self::IntoIter {
        self.start..=self.end
    }
}

pub(crate) fn cmp_index_interval(index: u16, iv: Interval) -> Ordering {
    if index < iv.start {
        Ordering::Less
    } else if index > iv.end {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

impl Interval {
    pub fn new_unchecked(start: u16, end: u16) -> Self {
        debug_assert!(start <= end);
        Self { start, end }
    }

    pub fn start(&self) -> u16 {
        self.start
    }

    pub fn end(&self) -> u16 {
        self.end
    }

    pub fn overlaps(&self, interval: &Self) -> bool {
        interval.start <= self.end && self.start <= interval.end
    }

    pub fn overlapping_interval(&self, other: &Self) -> Option<Self> {
        if self.overlaps(other) {
            Some(Self::new_unchecked(self.start.max(other.start), self.end.min(other.end)))
        } else {
            None
        }
    }

    pub fn run_len(&self) -> u64 {
        u64::from(self.end - self.start) + 1
    }

    pub fn is_full(&self) -> bool {
        self.start == 0 && self.end == u16::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_empty() {
        let mut interval_store = IntervalStore(alloc::vec![]);
        assert!(interval_store.insert(1));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 1, end: 1 }]))
    }

    #[test]
    fn insert_consecutive_begin() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 0, end: 0 },]);
        assert!(interval_store.insert(1));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 0, end: 1 }]))
    }

    #[test]
    fn insert_consecutive_end() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 1, end: 1 },]);
        assert!(interval_store.insert(0));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 0, end: 1 }]))
    }

    #[test]
    fn insert_consecutive_begin_end() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 0 },
            Interval { start: 2, end: 2 },
        ]);
        interval_store.insert(1);
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 0, end: 2 }]))
    }

    #[test]
    fn insert_arbitrary() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 3 },
            Interval { start: 9, end: 10 },
        ]);
        interval_store.insert(5);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 0, end: 3 },
                Interval { start: 5, end: 5 },
                Interval { start: 9, end: 10 },
            ])
        )
    }

    #[test]
    fn insert_u16_max() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 0, end: 3 },]);
        interval_store.insert(u16::MAX);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 0, end: 3 },
                Interval { start: u16::MAX, end: u16::MAX },
            ])
        )
    }

    #[test]
    fn insert_u16_max_consecutive() {
        let mut interval_store =
            IntervalStore(alloc::vec![Interval { start: 0, end: u16::MAX - 1 },]);
        interval_store.insert(u16::MAX);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval { start: 0, end: u16::MAX },])
        )
    }

    #[test]
    fn insert_consecutive_end_with_extra() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 65079, end: 65079 },
            Interval { start: 65179, end: 65179 },
        ]);
        assert!(interval_store.insert(65080));
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 65079, end: 65080 },
                Interval { start: 65179, end: 65179 },
            ])
        )
    }

    #[test]
    fn insert_range_empty() {
        let mut interval_store = IntervalStore(alloc::vec![]);
        assert_eq!(interval_store.insert_range(1..=2), Interval::new_unchecked(1, 2).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 1, end: 2 },]));
    }

    #[test]
    fn insert_range_overlap_begin() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 1, end: 20 }]);
        assert_eq!(interval_store.insert_range(5..=50), Interval::new_unchecked(21, 50).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 1, end: 50 },]));
    }

    #[test]
    fn insert_range_overlap_end() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 10, end: 20 }]);
        assert_eq!(interval_store.insert_range(5..=15), Interval::new_unchecked(5, 9).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 5, end: 20 },]));
    }

    #[test]
    fn insert_range_overlap_begin_end() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 40, end: 60 },
        ]);
        assert_eq!(interval_store.insert_range(15..=50), Interval::new_unchecked(21, 39).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 10, end: 60 },]));
    }

    #[test]
    fn insert_range_concescutive_begin() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 10, end: 20 },]);
        assert_eq!(interval_store.insert_range(21..=50), Interval::new_unchecked(21, 50).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 10, end: 50 },]));
    }

    #[test]
    fn insert_range_concescutive_begin_overlap_end() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 40, end: 60 },
        ]);
        assert_eq!(interval_store.insert_range(21..=50), Interval::new_unchecked(21, 39).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 10, end: 60 },]));
    }

    #[test]
    fn insert_range_concescutive_end() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 70 },]);
        assert_eq!(interval_store.insert_range(21..=49), Interval::new_unchecked(21, 49).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 21, end: 70 },]));
    }

    #[test]
    fn insert_range_concescutive_begin_end() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
        ]);
        assert_eq!(interval_store.insert_range(21..=49), Interval::new_unchecked(21, 49).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 10, end: 70 },]));
    }

    #[test]
    fn insert_range_no_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
        ]);
        assert_eq!(interval_store.insert_range(25..=30), Interval::new_unchecked(25, 30).run_len());
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 10, end: 20 },
                Interval { start: 25, end: 30 },
                Interval { start: 50, end: 70 },
            ])
        );
    }

    #[test]
    fn insert_range_u16_max_no_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
        ]);
        assert_eq!(
            interval_store.insert_range(90..=u16::MAX),
            Interval::new_unchecked(90, u16::MAX).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 10, end: 20 },
                Interval { start: 50, end: 70 },
                Interval { start: 90, end: u16::MAX },
            ])
        );
    }

    #[test]
    fn insert_range_u16_max_overlap_begin() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
        ]);
        assert_eq!(
            interval_store.insert_range(70..=u16::MAX),
            Interval::new_unchecked(71, u16::MAX).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 10, end: 20 },
                Interval { start: 50, end: u16::MAX },
            ])
        );
    }

    #[test]
    fn insert_range_u16_max_overlap_all() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
        ]);
        assert_eq!(
            interval_store.insert_range(0..=u16::MAX),
            Interval::new_unchecked(0, u16::MAX).run_len()
                - Interval::new_unchecked(10, 20).run_len()
                - Interval::new_unchecked(50, 70).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval { start: 0, end: u16::MAX },])
        );
    }

    #[test]
    fn insert_range_overlap_some() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 10, end: 20 },
            Interval { start: 50, end: 70 },
            Interval { start: 500, end: 700 },
        ]);
        assert_eq!(
            interval_store.insert_range(0..=100),
            Interval::new_unchecked(0, 100).run_len()
                - Interval::new_unchecked(10, 20).run_len()
                - Interval::new_unchecked(50, 70).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 0, end: 100 },
                Interval { start: 500, end: 700 },
            ])
        );
    }

    #[test]
    fn insert_range_begin_overlap_concescutive_end() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(2, 10),
            Interval::new_unchecked(12, 700),
        ]);
        assert_eq!(interval_store.insert_range(2..=11), 1);
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval::new_unchecked(2, 700)]));
    }

    #[test]
    fn insert_range_pin_1() {
        let mut interval_store = IntervalStore(alloc::vec![Interval::new_unchecked(65079, 65079)]);
        assert_eq!(interval_store.insert_range(65080..=65080), 1);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval::new_unchecked(65079, 65080)])
        );
    }

    #[test]
    fn push_empty() {
        let mut interval_store = IntervalStore(alloc::vec![]);
        assert!(interval_store.push(80));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 80, end: 80 },]));
    }

    #[test]
    fn push_new_max() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 70 },]);
        assert!(interval_store.push(80));
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 50, end: 70 },
                Interval { start: 80, end: 80 },
            ])
        );
    }

    #[test]
    fn push_new_max_consecutive() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 70 },]);
        assert!(interval_store.push(71));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 50, end: 71 },]));
    }

    #[test]
    fn push_existing() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 70 },]);
        assert!(!interval_store.push(60));
        assert_eq!(interval_store, interval_store);
    }

    #[test]
    fn push_non_existing_non_max() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 70 },]);
        assert!(!interval_store.push(10));
        assert_eq!(interval_store, interval_store);
    }

    #[test]
    fn push_existing_u16_max() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: u16::MAX },]);
        assert!(!interval_store.push(u16::MAX));
        assert_eq!(interval_store, interval_store);
    }

    #[test]
    fn push_new_u16_max() {
        let mut interval_store =
            IntervalStore(alloc::vec![Interval { start: 50, end: u16::MAX - 1 },]);
        assert!(interval_store.push(u16::MAX));
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval { start: 50, end: u16::MAX },])
        );
    }

    #[test]
    fn remove_end_of_interval() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 60 },]);
        assert!(interval_store.remove(60));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 50, end: 59 },]));
    }

    #[test]
    fn remove_begin_of_interval() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 60 },]);
        assert!(interval_store.remove(50));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 51, end: 60 },]));
    }

    #[test]
    fn remove_middle() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 1, end: 3 },]);
        assert!(interval_store.remove(2));
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 1, end: 1 },
                Interval { start: 3, end: 3 },
            ])
        );
    }

    #[test]
    fn remove_nothing() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 60 },]);
        assert!(!interval_store.remove(90));
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 50, end: 60 },]));
    }

    #[test]
    fn remove_u16_max() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: u16::MAX },]);
        assert!(interval_store.remove(u16::MAX));
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval { start: 50, end: u16::MAX - 1 },])
        );
    }

    #[test]
    fn remove_interval() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 50, end: 50 },]);
        assert!(interval_store.remove(50));
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_exact_one() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        assert_eq!(interval_store.remove_range(40..=60), 21);
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_one_with_extra_1() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        assert_eq!(interval_store.remove_range(40..=70), 21);
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_one_with_extra_2() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 400, end: 600 },
            Interval { start: 4000, end: 6000 },
        ]);
        assert_eq!(interval_store.remove_range(40..=70), 21);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(400, 600),
                Interval::new_unchecked(4000, 6000),
            ])
        );
    }

    #[test]
    fn remove_range_exact_many() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 80, end: 90 },
            Interval { start: 100, end: 200 },
        ]);
        assert_eq!(
            interval_store.remove_range(40..=200),
            Interval::new_unchecked(40, 60).run_len()
                + Interval::new_unchecked(80, 90).run_len()
                + Interval::new_unchecked(100, 200).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_begin_exact_overlap_end_one() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 70, end: 90 },
        ]);
        assert_eq!(
            interval_store.remove_range(40..=80),
            Interval::new_unchecked(40, 60).run_len() + Interval::new_unchecked(70, 80).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 81, end: 90 },]));
    }

    #[test]
    fn remove_range_begin_overlap_end_exact_one() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 70, end: 90 },
        ]);
        assert_eq!(
            interval_store.remove_range(50..=90),
            Interval::new_unchecked(70, 90).run_len() + Interval::new_unchecked(50, 60).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 40, end: 49 },]));
    }

    #[test]
    fn remove_range_begin_no_overlap_end_exact_one_1() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 70, end: 90 },
        ]);
        assert_eq!(
            interval_store.remove_range(30..=90),
            Interval::new_unchecked(70, 90).run_len() + Interval::new_unchecked(40, 60).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_begin_no_overlap_end_exact_one_2() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 70, end: 90 },
            Interval { start: 700, end: 900 },
        ]);
        assert_eq!(
            interval_store.remove_range(30..=90),
            Interval::new_unchecked(70, 90).run_len() + Interval::new_unchecked(40, 60).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval::new_unchecked(700, 900),]));
    }

    #[test]
    fn remove_range_both_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 70, end: 90 },
        ]);
        assert_eq!(
            interval_store.remove_range(50..=80),
            Interval::new_unchecked(70, 80).run_len() + Interval::new_unchecked(50, 60).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval { start: 40, end: 49 },
                Interval { start: 81, end: 90 },
            ])
        );
    }

    #[test]
    fn remove_range_begin_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        assert_eq!(
            interval_store.remove_range(50..=100),
            Interval::new_unchecked(50, 60).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 40, end: 49 },]));
    }

    #[test]
    fn remove_range_begin_overlap_many() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 80, end: 100 },
            Interval { start: 200, end: 500 },
        ]);
        assert_eq!(
            interval_store.remove_range(50..=1000),
            Interval::new_unchecked(50, 60).run_len()
                + Interval::new_unchecked(80, 100).run_len()
                + Interval::new_unchecked(200, 500).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 40, end: 49 },]));
    }

    #[test]
    fn remove_range_end_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        assert_eq!(interval_store.remove_range(20..=50), Interval::new_unchecked(40, 50).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 51, end: 60 },]));
    }

    #[test]
    fn remove_range_end_overlap_many() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 100, end: 500 },
            Interval { start: 800, end: 900 },
        ]);
        assert_eq!(
            interval_store.remove_range(20..=850),
            Interval::new_unchecked(40, 60).run_len()
                + Interval::new_unchecked(100, 500).run_len()
                + Interval::new_unchecked(800, 850).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval { start: 851, end: 900 },]));
    }

    #[test]
    fn remove_range_no_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        assert_eq!(interval_store.remove_range(20..=80), Interval::new_unchecked(40, 60).run_len());
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_no_overlap_many() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 40, end: 60 },
            Interval { start: 400, end: 600 },
            Interval { start: 4000, end: 6000 },
        ]);
        assert_eq!(
            interval_store.remove_range(20..=60000),
            Interval::new_unchecked(40, 60).run_len()
                + Interval::new_unchecked(400, 600).run_len()
                + Interval::new_unchecked(4000, 6000).run_len()
        );
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_complete_overlap() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 51, end: 6000 },]);
        assert_eq!(
            interval_store.remove_range(500..=600),
            Interval::new_unchecked(500, 600).run_len()
        );
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(51, 499),
                Interval::new_unchecked(601, 6000),
            ])
        );
    }

    #[test]
    fn remove_range_nothing() {
        let mut interval_store = IntervalStore(alloc::vec![]);
        assert_eq!(interval_store.remove_range(50000..=60000), 0);
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_range_with_extra() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(38161, 38162),
            Interval::new_unchecked(40562, 40562),
        ]);
        assert_eq!(interval_store.remove_range(38162..=38163), 1);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(38161, 38161),
                Interval::new_unchecked(40562, 40562),
            ])
        );
    }

    #[test]
    fn remove_smallest_one() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        interval_store.remove_smallest(500);
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_smallest_many_1() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 99 },
            Interval { start: 400, end: 600 },
            Interval { start: 4000, end: 6000 },
        ]);
        interval_store.remove_smallest(200);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(500, 600),
                Interval::new_unchecked(4000, 6000),
            ])
        );
    }

    #[test]
    fn remove_smallest_many_2() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 99 },
            Interval { start: 400, end: 599 },
            Interval { start: 4000, end: 6000 },
        ]);
        interval_store.remove_smallest(500);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![Interval::new_unchecked(4200, 6000),])
        );
    }

    #[test]
    fn remove_biggest_one() {
        let mut interval_store = IntervalStore(alloc::vec![Interval { start: 40, end: 60 },]);
        interval_store.remove_biggest(500);
        assert_eq!(interval_store, IntervalStore(alloc::vec![]));
    }

    #[test]
    fn remove_biggest_many_1() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 99 },
            Interval { start: 400, end: 600 },
            Interval { start: 5901, end: 6000 },
        ]);
        interval_store.remove_biggest(200);
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(0, 99),
                Interval::new_unchecked(400, 500),
            ])
        );
    }

    #[test]
    fn remove_biggest_many_2() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 6000 },
            Interval { start: 8401, end: 8600 },
            Interval { start: 9901, end: 10000 },
        ]);
        interval_store.remove_biggest(500);
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval::new_unchecked(1, 5800),]));
    }

    #[test]
    fn remove_smallest_exact_interval_boundary() {
        // #359: removing exactly the run length of the first interval must drop
        // it entirely, not shrink it past its end into an inverted interval.
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 0, end: 2 },
            Interval { start: 4, end: 4 },
        ]);
        interval_store.remove_smallest(3); // == run_len of [0, 2]
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval::new_unchecked(4, 4)]));
    }

    #[test]
    fn remove_biggest_exact_interval_boundary() {
        // #359: removing exactly the run length of the last interval must drop
        // it entirely, not shrink its end below its start into an inverted
        // interval.
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 2, end: 2 },
            Interval { start: 4, end: 8 },
        ]);
        interval_store.remove_biggest(5); // == run_len of [4, 8]
        assert_eq!(interval_store, IntervalStore(alloc::vec![Interval::new_unchecked(2, 2)]));
    }

    #[test]
    fn contains_index_1() {
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(interval_store.contains(5));
        assert!(interval_store.contains(16000));
    }

    #[test]
    fn contains_index_2() {
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(!interval_store.contains(0));
    }

    #[test]
    fn contains_range_1() {
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(interval_store.contains_range(1..=500));
    }

    #[test]
    fn contains_range_2() {
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(!interval_store.contains_range(1..=1500));
    }

    #[test]
    fn contains_range_3() {
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(interval_store.contains_range(1..=1));
    }

    #[test]
    fn is_disjoint_1() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval { start: 601, end: 1200 },]);
        assert!(!interval_store_1.is_disjoint(&interval_store_1));
        assert!(!interval_store_2.is_disjoint(&interval_store_2));
        assert!(interval_store_1.is_disjoint(&interval_store_2));
        assert!(interval_store_2.is_disjoint(&interval_store_1));
    }

    #[test]
    fn is_disjoint_2() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval { start: 600, end: 1200 },]);
        assert!(!interval_store_1.is_disjoint(&interval_store_1));
        assert!(!interval_store_2.is_disjoint(&interval_store_2));
        assert!(!interval_store_1.is_disjoint(&interval_store_2));
        assert!(!interval_store_2.is_disjoint(&interval_store_1));
    }

    #[test]
    fn is_disjoint_3() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval { start: 15800, end: 15905 },]);
        assert!(!interval_store_1.is_disjoint(&interval_store_1));
        assert!(!interval_store_2.is_disjoint(&interval_store_2));
        assert!(!interval_store_1.is_disjoint(&interval_store_2));
        assert!(!interval_store_2.is_disjoint(&interval_store_1));
    }

    #[test]
    fn is_disjoint_array_store_1() {
        let array_store = ArrayStore::from_vec_unchecked(alloc::vec![0, 60, 200, 500,]);
        let interval_store = IntervalStore(alloc::vec![Interval { start: 70, end: 199 },]);
        assert!(interval_store.is_disjoint_array(&array_store));
    }

    #[test]
    fn is_disjoint_array_store_2() {
        let array_store = ArrayStore::from_vec_unchecked(alloc::vec![0, 60, 200, 500,]);
        let interval_store = IntervalStore(alloc::vec![Interval { start: 1, end: 400 },]);
        assert!(!interval_store.is_disjoint_array(&array_store));
    }

    #[test]
    fn is_disjoint_bitmap_store_1() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in [500, 5001, 20, 40] {
            bitmap_store.insert(to_set);
        }
        let interval_store = IntervalStore(alloc::vec![
            Interval { start: 1000, end: 4000 },
            Interval { start: 8000, end: 10000 },
        ]);
        assert!(interval_store.is_disjoint_bitmap(&bitmap_store));
    }

    #[test]
    fn is_disjoint_bitmap_store_2() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in [500, 5001, 20, 40] {
            bitmap_store.insert(to_set);
        }
        let interval_store = IntervalStore(alloc::vec![Interval { start: 1, end: 400 },]);
        assert!(!interval_store.is_disjoint_bitmap(&bitmap_store));
    }

    #[test]
    fn is_subset_1() {
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 1500, end: 1600 },]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(interval_store_1.is_subset(&interval_store_1));
        assert!(interval_store_2.is_subset(&interval_store_2));
        assert!(interval_store_1.is_subset(&interval_store_2));
        assert!(!interval_store_2.is_subset(&interval_store_1));
    }

    #[test]
    fn is_subset_2() {
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 50, end: 700 },]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 600 },
            Interval { start: 1401, end: 1600 },
            Interval { start: 15901, end: 16000 },
        ]);
        assert!(interval_store_1.is_subset(&interval_store_1));
        assert!(interval_store_2.is_subset(&interval_store_2));
        assert!(!interval_store_1.is_subset(&interval_store_2));
        assert!(!interval_store_2.is_subset(&interval_store_1));
    }

    #[test]
    fn overlapping_interval_1() {
        let interval1 = Interval::new_unchecked(0, 100);
        let interval2 = Interval::new_unchecked(50, 300);

        assert_eq!(
            interval1.overlapping_interval(&interval2),
            Some(Interval::new_unchecked(50, 100))
        )
    }

    #[test]
    fn overlapping_interval_2() {
        let interval1 = Interval::new_unchecked(50, 300);
        let interval2 = Interval::new_unchecked(0, 100);

        assert_eq!(
            interval1.overlapping_interval(&interval2),
            Some(Interval::new_unchecked(50, 100))
        )
    }

    #[test]
    fn overlapping_interval_3() {
        let interval1 = Interval::new_unchecked(0, 100);
        let interval2 = Interval::new_unchecked(500, 700);

        assert_eq!(interval1.overlapping_interval(&interval2), None)
    }

    #[test]
    fn intersection_len_1() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 11, end: 20 },
            Interval { start: 51, end: 80 },
            Interval { start: 111, end: 120 },
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 20 },
            Interval { start: 41, end: 80 },
            Interval { start: 101, end: 120 },
        ]);
        assert_eq!(
            interval_store_1.intersection_len(&interval_store_2),
            Interval::new_unchecked(11, 20).run_len()
                + Interval::new_unchecked(51, 80).run_len()
                + Interval::new_unchecked(111, 120).run_len()
        )
    }

    #[test]
    fn intersection_len_2() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 11, end: 20 },
            Interval { start: 51, end: 80 },
            Interval { start: 111, end: 120 },
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval { start: 1, end: 80 },
            Interval { start: 101, end: 120 },
        ]);
        let intersect_len = Interval::new_unchecked(11, 20).run_len()
            + Interval::new_unchecked(51, 80).run_len()
            + Interval::new_unchecked(111, 120).run_len();
        assert_eq!(interval_store_1.intersection_len(&interval_store_2), intersect_len);
        assert_eq!(interval_store_2.intersection_len(&interval_store_1), intersect_len);
    }

    #[test]
    fn intersection_len_3() {
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 1, end: 2000 },]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval { start: 1001, end: 3000 },]);
        let intersect_len = Interval::new_unchecked(1001, 2000).run_len();
        assert_eq!(interval_store_1.intersection_len(&interval_store_2), intersect_len);
        assert_eq!(interval_store_2.intersection_len(&interval_store_1), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_1() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in [500, 5001, 20, 40, 60] {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 20, end: 600 },]);
        let intersect_len = 4;
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_2() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in 0..=200 {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 20, end: 600 },]);
        let intersect_len = Interval::new_unchecked(20, 200).run_len();
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_3() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in 0..=20000 {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 20, end: 6000 },
            Interval { start: 5000, end: 33333 },
        ]);
        let intersect_len = Interval::new_unchecked(20, 6000).run_len()
            + Interval::new_unchecked(5000, 20000).run_len();
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_4() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in 0..=20000 {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 64, end: 6400 },
            Interval { start: 7680, end: 64000 },
        ]);
        let intersect_len = Interval::new_unchecked(64, 6400).run_len()
            + Interval::new_unchecked(7680, 20000).run_len();
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_5() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in 0..=20005 {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 64, end: 6400 },
            Interval { start: 7680, end: 64000 },
        ]);
        let intersect_len = Interval::new_unchecked(64, 6400).run_len()
            + Interval::new_unchecked(7680, 20005).run_len();
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_bitmap_6() {
        let mut bitmap_store = BitmapStore::new();
        for to_set in 0..=20005 {
            bitmap_store.insert(to_set);
        }
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 64, end: 64 },]);
        let intersect_len = Interval::new_unchecked(64, 64).run_len();
        assert_eq!(interval_store_1.intersection_len_bitmap(&bitmap_store), intersect_len);
    }

    #[test]
    fn intersection_len_array_1() {
        let array_store = ArrayStore::from_vec_unchecked(alloc::vec![20, 40, 60, 500, 5001]);
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 20, end: 600 },]);
        let intersect_len = 4;
        assert_eq!(interval_store_1.intersection_len_array(&array_store), intersect_len);
    }

    #[test]
    fn intersection_len_array_2() {
        let array_store = ArrayStore::from_vec_unchecked(Vec::from_iter(0..200));
        let interval_store_1 = IntervalStore(alloc::vec![Interval { start: 20, end: 600 },]);
        let intersect_len = 200 - 20;
        assert_eq!(interval_store_1.intersection_len_array(&array_store), intersect_len);
    }

    #[test]
    fn len_1() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval { start: 20, end: 600 },
            Interval { start: 5000, end: 8000 },
        ]);
        assert_eq!(
            interval_store_1.len(),
            Interval::new_unchecked(20, 600).run_len()
                + Interval::new_unchecked(5000, 8000).run_len()
        );
    }

    #[test]
    fn is_empty() {
        let mut interval_store = IntervalStore(alloc::vec![
            Interval { start: 20, end: 600 },
            Interval { start: 5000, end: 8000 },
        ]);
        assert!(!interval_store.is_empty());
        interval_store.remove_range(0..=u16::MAX);
        assert!(interval_store.is_empty());
    }

    #[test]
    fn min_0() {
        let interval_store = IntervalStore(alloc::vec![Interval::new_unchecked(20, u16::MAX)]);
        assert_eq!(interval_store.min(), Some(20));
    }

    #[test]
    fn min_1() {
        let interval_store = IntervalStore(alloc::vec![]);
        assert_eq!(interval_store.min(), None);
    }

    #[test]
    fn max_0() {
        let interval_store = IntervalStore(alloc::vec![Interval::new_unchecked(20, u16::MAX)]);
        assert_eq!(interval_store.max(), Some(u16::MAX));
    }

    #[test]
    fn max_1() {
        let interval_store = IntervalStore(alloc::vec![]);
        assert_eq!(interval_store.max(), None);
    }

    #[test]
    fn rank() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 200),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 10000),
        ]);
        assert_eq!(
            interval_store.rank(5020),
            Interval::new_unchecked(0, 200).run_len()
                + Interval::new_unchecked(5000, 5020).run_len()
        );
        assert_eq!(interval_store.rank(u16::MAX), interval_store.len());
    }

    #[test]
    fn select() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 10000),
        ]);
        assert_eq!(interval_store.select(0), Some(0));
        assert_eq!(interval_store.select(1), Some(2));
        assert_eq!(interval_store.select(10), Some(11));
        assert_eq!(interval_store.select(11), Some(5000));
        assert_eq!(interval_store.select(11 + 3), Some(5003));
        assert_eq!(interval_store.select(11 + 2001), Some(8000));
    }

    #[test]
    fn union_1() {
        let mut interval_store_1 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 10000),
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 10),
            Interval::new_unchecked(12, 7000),
            Interval::new_unchecked(65000, 65050),
        ]);
        interval_store_1 |= interval_store_2;
        assert_eq!(
            interval_store_1,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(0, 0),
                Interval::new_unchecked(2, 7000),
                Interval::new_unchecked(8000, 10000),
                Interval::new_unchecked(65000, 65050),
            ])
        )
    }

    #[test]
    fn union_array() {
        let mut values = alloc::vec![0, 1, 2, 3, 4, 2000, 5000, u16::MAX];
        values.sort();
        let array = ArrayStore::from_vec_unchecked(values);
        let mut interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 10000),
        ]);
        interval_store |= &array;
        assert_eq!(
            interval_store,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(0, 11),
                Interval::new_unchecked(2000, 2000),
                Interval::new_unchecked(5000, 7000),
                Interval::new_unchecked(8000, 10000),
                Interval::new_unchecked(u16::MAX, u16::MAX),
            ])
        )
    }

    #[test]
    fn intersection() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 10000),
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(5, 50),
            Interval::new_unchecked(4000, 10000),
        ]);
        assert_eq!(
            &interval_store_1 & &interval_store_2,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(0, 0),
                Interval::new_unchecked(5, 11),
                Interval::new_unchecked(5000, 7000),
                Interval::new_unchecked(8000, 10000),
            ])
        );
        assert_eq!(&interval_store_1 & &interval_store_1, interval_store_1);
    }

    #[test]
    fn difference() {
        let mut interval_store_1 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 11000),
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(5, 50),
            Interval::new_unchecked(4000, 10000),
        ]);
        interval_store_1 -= &interval_store_2;
        assert_eq!(
            interval_store_1,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(2, 4),
                Interval::new_unchecked(10001, 11000),
            ])
        )
    }

    #[test]
    fn symmetric_difference_0() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(2, 11),
            Interval::new_unchecked(5000, 7000),
            Interval::new_unchecked(8000, 11000),
            Interval::new_unchecked(40000, 50000),
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 0),
            Interval::new_unchecked(5, 50),
            Interval::new_unchecked(4000, 10000),
        ]);
        assert_eq!(
            &interval_store_1 ^ &interval_store_2,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(2, 4),
                Interval::new_unchecked(12, 50),
                Interval::new_unchecked(4000, 4999),
                Interval::new_unchecked(7001, 7999),
                Interval::new_unchecked(10001, 11000),
                Interval::new_unchecked(40000, 50000),
            ])
        );
    }

    #[test]
    fn symmetric_difference_1() {
        let interval_store_1 = IntervalStore(alloc::vec![Interval::new_unchecked(0, 50),]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval::new_unchecked(100, 200),]);
        assert_eq!(
            &interval_store_1 ^ &interval_store_2,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(0, 50),
                Interval::new_unchecked(100, 200),
            ])
        );
    }

    #[test]
    fn symmetric_difference_2() {
        let interval_store_1 = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let interval_store_2 = IntervalStore(alloc::vec![Interval::new_unchecked(0, 6000),]);
        assert_eq!(
            &interval_store_1 ^ &interval_store_2,
            IntervalStore(alloc::vec![
                Interval::new_unchecked(51, 499),
                Interval::new_unchecked(601, 799),
                Interval::new_unchecked(1001, 6000),
            ])
        );
    }

    #[test]
    fn iter_next() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.into_iter();

        let size = (Interval::new_unchecked(0, 50).run_len()
            + Interval::new_unchecked(500, 600).run_len()
            + Interval::new_unchecked(800, 1000).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next() {
            assert_eq!(i, value as usize);
            i += 1;
            if i >= 51 {
                break;
            }
            let size = (Interval::new_unchecked(i as u16, 50).run_len()
                + Interval::new_unchecked(500, 600).run_len()
                + Interval::new_unchecked(800, 1000).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let size = (Interval::new_unchecked(500, 600).run_len()
            + Interval::new_unchecked(800, 1000).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next() {
            assert_eq!(i + 500, value as usize);
            i += 1;
            if i >= 101 {
                break;
            }
            let size = (Interval::new_unchecked((i + 500) as u16, 600).run_len()
                + Interval::new_unchecked(800, 1000).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let size = Interval::new_unchecked(800, 1000).run_len() as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next() {
            if i >= 201 {
                break;
            }
            assert_eq!(i + 800, value as usize);
            i += 1;
            if i >= 201 {
                break;
            }
            let size = (Interval::new_unchecked((i + 800) as u16, 1000).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }
        assert_eq!(iter.size_hint(), (0, Some(0)));

        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn iter_next_back() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.into_iter();

        let size = (Interval::new_unchecked(0, 50).run_len()
            + Interval::new_unchecked(500, 600).run_len()
            + Interval::new_unchecked(800, 1000).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(1000 - i, value as usize);
            i += 1;
            if i >= 201 {
                break;
            }
            let size = (Interval::new_unchecked(0, 50).run_len()
                + Interval::new_unchecked(500, 600).run_len()
                + Interval::new_unchecked(800, (1000 - i) as u16).run_len())
                as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(600 - i, value as usize);
            i += 1;
            if i >= 101 {
                break;
            }
            let size = (Interval::new_unchecked(0, 50).run_len()
                + Interval::new_unchecked(500, (600 - i) as u16).run_len())
                as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(50 - i, value as usize);
            i += 1;
            if i >= 51 {
                break;
            }
            let size = (Interval::new_unchecked(0, (50 - i) as u16).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn iter_next_and_next_back() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.into_iter();

        let size = (Interval::new_unchecked(0, 50).run_len()
            + Interval::new_unchecked(500, 600).run_len()
            + Interval::new_unchecked(800, 1000).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(1000 - i, value as usize);
            i += 1;
            if i >= 201 {
                break;
            }
            let size = (Interval::new_unchecked(0, 50).run_len()
                + Interval::new_unchecked(500, 600).run_len()
                + Interval::new_unchecked(800, (1000 - i) as u16).run_len())
                as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let size = (Interval::new_unchecked(0, 50).run_len()
            + Interval::new_unchecked(500, 600).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(600 - i, value as usize);
            i += 1;
            if i >= 101 {
                break;
            }
            let size = (Interval::new_unchecked(0, 50).run_len()
                + Interval::new_unchecked(500, (600 - i) as u16).run_len())
                as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let size = (Interval::new_unchecked(0, 50).run_len()) as usize;
        assert_eq!(iter.size_hint(), (size, Some(size)));

        let mut i = 0;
        while let Some(value) = iter.next() {
            assert_eq!(i, value as usize);
            i += 1;
            if i >= 51 {
                break;
            }
            let size = (Interval::new_unchecked(i as u16, 50).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn iter_u16_max() {
        let interval_store = IntervalStore(alloc::vec![Interval::new_unchecked(0, u16::MAX),]);
        let mut iter = interval_store.iter();

        let mut i = 0;
        while let Some(value) = iter.next() {
            assert_eq!(i, value as usize);
            i += 1;
            if i >= u16::MAX as usize {
                break;
            }
            let size = (Interval::new_unchecked(i as u16, u16::MAX).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }

        let mut iter = interval_store.iter();

        let mut i = 0;
        while let Some(value) = iter.next_back() {
            assert_eq!(u16::MAX as usize - i, value as usize);
            i += 1;
            if i >= u16::MAX as usize {
                break;
            }
            let size = (Interval::new_unchecked(0, u16::MAX - i as u16).run_len()) as usize;
            assert_eq!(iter.size_hint(), (size, Some(size)));
        }
        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(u16::MAX as usize), Some(u16::MAX));
    }

    #[test]
    fn iter_nth() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(50), Some(50));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(51), Some(500));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(100), Some(549));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(152), Some(800));

        let mut iter = interval_store.iter();
        assert_eq!(
            iter.nth(
                (Interval::new_unchecked(0, 50).run_len()
                    + Interval::new_unchecked(500, 600).run_len()
                    + Interval::new_unchecked(800, 1000).run_len()
                    - 1) as usize
            ),
            Some(1000)
        );

        let mut iter = interval_store.iter();
        iter.next();
        iter.next();
        iter.next();
        assert_eq!(iter.nth(152), Some(803));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(u16::MAX as usize), None);
    }

    // Regression test for https://github.com/RoaringBitmap/roaring-rs/issues/352
    #[test]
    #[allow(clippy::iter_nth_zero)]
    fn iter_nth_single_element_intervals() {
        // A RunStore made of single-element intervals {0,0}, {2,2}, ..., {246,246}.
        // nth(123) must return 246 (index 123 in the sequence 0, 2, 4, ...).
        // The buggy code returned 244 (index 122).
        let intervals: alloc::vec::Vec<_> =
            (0u16..=246).step_by(2).map(|v| Interval::new_unchecked(v, v)).collect();
        let interval_store = IntervalStore(intervals);

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(0), Some(0));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(1), Some(2));

        let mut iter = interval_store.iter();
        assert_eq!(iter.nth(123), Some(246));
    }

    #[test]
    fn iter_advance_to() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.iter();
        iter.advance_to(20);
        assert_eq!(iter.next(), Some(20));
        iter.advance_to(800);
        assert_eq!(iter.next(), Some(800));
        iter.advance_to(u16::MAX);
        assert_eq!(iter.next(), None);

        let mut iter = interval_store.iter();
        iter.advance_to(100);
        assert_eq!(iter.next(), Some(500));
        iter.advance_to(800);
        assert_eq!(iter.next(), Some(800));
        iter.advance_to(900);
        assert_eq!(iter.next(), Some(900));
        iter.advance_to(800);
        assert_eq!(iter.next(), Some(901));
        let mut iter = interval_store.iter();
        iter.next();
        iter.next();
        iter.next();
        iter.advance_to(499);
        assert_eq!(iter.next(), Some(500));

        let mut iter = interval_store.iter();
        iter.advance_to(100);
        assert_eq!(iter.next(), Some(500));
    }

    #[test]
    fn iter_advance_back_to() {
        let interval_store = IntervalStore(alloc::vec![
            Interval::new_unchecked(0, 50),
            Interval::new_unchecked(500, 600),
            Interval::new_unchecked(800, 1000),
        ]);
        let mut iter = interval_store.iter();
        iter.advance_back_to(u16::MAX);
        assert_eq!(iter.next_back(), Some(1000));
        iter.advance_back_to(800);
        assert_eq!(iter.next_back(), Some(800));
        iter.advance_back_to(20);
        assert_eq!(iter.next_back(), Some(20));

        let mut iter = interval_store.iter();
        iter.advance_back_to(800);
        assert_eq!(iter.next_back(), Some(800));
        iter.advance_back_to(900);
        assert_eq!(iter.next_back(), Some(600));
        iter.advance_back_to(550);
        assert_eq!(iter.next_back(), Some(550));
        iter.advance_back_to(20);
        assert_eq!(iter.next_back(), Some(20));
        let mut iter = interval_store.iter();
        iter.next_back();
        iter.next_back();
        iter.next_back();
        iter.advance_back_to(700);
        assert_eq!(iter.next_back(), Some(600));
        let mut iter = interval_store.iter();
        iter.advance_back_to(400);
        assert_eq!(iter.next_back(), Some(50));
    }
}
