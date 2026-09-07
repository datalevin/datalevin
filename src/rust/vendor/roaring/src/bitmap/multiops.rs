use core::{
    cmp::Reverse,
    convert::Infallible,
    mem,
    ops::{BitOrAssign, BitXorAssign},
};

use alloc::borrow::Cow;

use crate::{MultiOps, RoaringBitmap};

use super::{container::Container, store::Store};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// When collecting bitmaps for optimizing the computation. If we don't know how many
// elements are in the iterator we collect 10 elements.
const BASE_COLLECT: usize = 10;

/// If an iterator contain 50 elements or less we collect everything because it'll be so
/// much faster without impacting the memory usage too much (in most cases).
const MAX_COLLECT: usize = 50;

impl<I> MultiOps<RoaringBitmap> for I
where
    I: IntoIterator<Item = RoaringBitmap>,
{
    type Output = RoaringBitmap;

    fn union(self) -> Self::Output {
        try_multi_or_owned(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn intersection(self) -> Self::Output {
        try_multi_and_owned(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn difference(self) -> Self::Output {
        try_multi_sub_owned(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn symmetric_difference(self) -> Self::Output {
        try_multi_xor_owned(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }
}

impl<I, E> MultiOps<Result<RoaringBitmap, E>> for I
where
    I: IntoIterator<Item = Result<RoaringBitmap, E>>,
{
    type Output = Result<RoaringBitmap, E>;

    fn union(self) -> Self::Output {
        try_multi_or_owned(self)
    }

    fn intersection(self) -> Self::Output {
        try_multi_and_owned(self)
    }

    fn difference(self) -> Self::Output {
        try_multi_sub_owned(self)
    }

    fn symmetric_difference(self) -> Self::Output {
        try_multi_xor_owned(self)
    }
}

impl<'a, I> MultiOps<&'a RoaringBitmap> for I
where
    I: IntoIterator<Item = &'a RoaringBitmap>,
{
    type Output = RoaringBitmap;

    fn union(self) -> Self::Output {
        try_multi_or_ref(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn intersection(self) -> Self::Output {
        try_multi_and_ref(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn difference(self) -> Self::Output {
        try_multi_sub_ref(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }

    fn symmetric_difference(self) -> Self::Output {
        try_multi_xor_ref(self.into_iter().map(Ok::<_, Infallible>)).unwrap()
    }
}

impl<'a, I, E: 'a> MultiOps<Result<&'a RoaringBitmap, E>> for I
where
    I: IntoIterator<Item = Result<&'a RoaringBitmap, E>>,
{
    type Output = Result<RoaringBitmap, E>;

    fn union(self) -> Self::Output {
        try_multi_or_ref(self)
    }

    fn intersection(self) -> Self::Output {
        try_multi_and_ref(self)
    }

    fn difference(self) -> Self::Output {
        try_multi_sub_ref(self)
    }

    fn symmetric_difference(self) -> Self::Output {
        try_multi_xor_ref(self)
    }
}

#[inline]
fn try_multi_and_owned<E>(
    bitmaps: impl IntoIterator<Item = Result<RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();

    // We're going to take a bunch of elements at the start of the iterator and sort
    // them to reduce the size of our bitmap faster.
    let mut start = collect_starting_elements(iter.by_ref())?;
    start.sort_unstable_by_key(|bitmap| bitmap.containers.len());
    let mut start = start.into_iter();

    if let Some(mut lhs) = start.next() {
        for rhs in start.map(Ok).chain(iter) {
            if lhs.is_empty() {
                return Ok(lhs);
            }
            lhs &= rhs?;
        }

        Ok(lhs)
    } else {
        Ok(RoaringBitmap::new())
    }
}

#[inline]
fn try_multi_and_ref<'a, E>(
    bitmaps: impl IntoIterator<Item = Result<&'a RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();

    // We're going to take a bunch of elements at the start of the iterator and sort
    // them to reduce the size of our bitmap faster.
    let mut start = collect_starting_elements(iter.by_ref())?;
    start.sort_unstable_by_key(|bitmap| bitmap.containers.len());
    let mut start = start.into_iter();

    if let Some(mut lhs) = start.next().cloned() {
        for rhs in start.map(Ok).chain(iter) {
            if lhs.is_empty() {
                return Ok(lhs);
            }
            lhs &= rhs?;
        }
        Ok(lhs)
    } else {
        Ok(RoaringBitmap::new())
    }
}

#[inline]
fn try_multi_sub_owned<E>(
    bitmaps: impl IntoIterator<Item = Result<RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();
    match iter.next().transpose()? {
        Some(mut lhs) => {
            for rhs in iter {
                if lhs.is_empty() {
                    return Ok(lhs);
                }
                lhs -= rhs?;
            }
            Ok(lhs)
        }
        None => Ok(RoaringBitmap::default()),
    }
}

#[inline]
fn try_multi_sub_ref<'a, E>(
    bitmaps: impl IntoIterator<Item = Result<&'a RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();
    match iter.next().transpose()?.cloned() {
        Some(mut lhs) => {
            for rhs in iter {
                if lhs.is_empty() {
                    return Ok(lhs);
                }
                lhs -= rhs?;
            }

            Ok(lhs)
        }
        None => Ok(RoaringBitmap::default()),
    }
}

#[inline]
fn try_multi_or_owned<E>(
    bitmaps: impl IntoIterator<Item = Result<RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();

    // We're going to take a bunch of elements at the start of the iterator and
    // move the biggest one first to grow faster.
    let mut start = collect_starting_elements(iter.by_ref())?;
    start.sort_unstable_by_key(|bitmap| Reverse(bitmap.containers.len()));
    let start_size = start.len();
    let mut start = start.into_iter();

    let mut containers = if let Some(c) = start.next() {
        if c.is_empty() {
            // everything must be empty if the max is empty
            start.by_ref().nth(start_size);
        }
        c.containers
    } else {
        return Ok(RoaringBitmap::new());
    };

    for bitmap in start.map(Ok).chain(iter) {
        merge_container_owned(&mut containers, bitmap?.containers, BitOrAssign::bitor_assign);
    }

    containers.retain_mut(|container| {
        if !container.is_empty() {
            container.ensure_correct_store();
            true
        } else {
            false
        }
    });

    Ok(RoaringBitmap { containers })
}

#[inline]
fn try_multi_xor_owned<E>(
    bitmaps: impl IntoIterator<Item = Result<RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    let mut iter = bitmaps.into_iter();
    let mut containers = match iter.next().transpose()? {
        None => Vec::new(),
        Some(v) => v.containers,
    };

    for bitmap in iter {
        merge_container_owned(&mut containers, bitmap?.containers, BitXorAssign::bitxor_assign);
    }

    containers.retain_mut(|container| {
        if !container.is_empty() {
            container.ensure_correct_store();
            true
        } else {
            false
        }
    });

    Ok(RoaringBitmap { containers })
}

fn merge_container_owned(
    lhs: &mut Vec<Container>,
    rhs: Vec<Container>,
    op: impl Fn(&mut Store, Store),
) {
    for mut rhs in rhs {
        match lhs.binary_search_by_key(&rhs.key, |c| c.key) {
            Err(loc) => lhs.insert(loc, rhs),
            Ok(loc) => {
                let lhs = &mut lhs[loc];
                match (&lhs.store, &rhs.store) {
                    (Store::Array(..), Store::Array(..)) => lhs.store = lhs.store.to_bitmap(),
                    (Store::Array(..), Store::Bitmap(..)) => mem::swap(lhs, &mut rhs),
                    _ => (),
                };
                op(&mut lhs.store, rhs.store);
            }
        }
    }
}

#[inline]
fn try_multi_or_ref<'a, E: 'a>(
    bitmaps: impl IntoIterator<Item = Result<&'a RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    // This algorithm operates on bitmaps. It must deal with arrays for which there are not (yet)
    // any others with the same key.
    //
    //   1. Eager cloning would create useless intermediate values that might become bitmaps
    //   2. Eager promoting forces disjoint containers to converted back to arrays at the end
    //
    // This strategy uses COW to lazily promote arrays to bitmaps as they are operated on.
    // More memory efficient, negligible wall time difference benchmarks

    // Phase 1. Borrow all the containers from the first element.
    let mut iter = bitmaps.into_iter();
    let mut start = collect_starting_elements(iter.by_ref())?;
    let start_size = start.len();

    start.sort_unstable_by_key(|bitmap| Reverse(bitmap.containers.len()));
    let mut start = start.into_iter();
    let mut containers = match start.next() {
        Some(c) => {
            let c: Vec<Cow<Container>> = c.containers.iter().map(Cow::Borrowed).collect();
            if c.is_empty() {
                // everything must be empty if the max is empty
                start.by_ref().nth(start_size);
            }
            c
        }
        None => {
            return Ok(RoaringBitmap::new());
        }
    };

    // Phase 2: Operate on the remaining containers
    for bitmap in start.map(Ok).chain(iter) {
        merge_container_ref(&mut containers, &bitmap?.containers, |a, b| *a |= b);
    }

    // Phase 3: Clean up
    let containers: Vec<_> = containers
        .into_iter()
        .filter(|container| !container.is_empty())
        .map(|c| {
            // Any borrowed bitmaps or arrays left over get cloned here
            let mut container = c.into_owned();
            container.ensure_correct_store();
            container
        })
        .collect();

    Ok(RoaringBitmap { containers })
}

#[inline]
fn try_multi_xor_ref<'a, E: 'a>(
    bitmaps: impl IntoIterator<Item = Result<&'a RoaringBitmap, E>>,
) -> Result<RoaringBitmap, E> {
    //
    // This algorithm operates on bitmaps. It must deal with arrays for which there are not (yet)
    // any others with the same key.
    //
    //   1. Eager cloning would create useless intermediate values that might become bitmaps
    //   2. Eager promoting forces disjoint containers to converted back to arrays at the end
    //
    // This strategy uses COW to lazily promote arrays to bitmaps as they are operated on.
    // More memory efficient, negligible wall time difference benchmarks

    // Phase 1. Borrow all the containers from the first element.
    let mut iter = bitmaps.into_iter();
    let mut containers: Vec<Cow<Container>> = match iter.next().transpose()? {
        None => Vec::new(),
        Some(v) => v.containers.iter().map(Cow::Borrowed).collect(),
    };

    // Phase 2: Operate on the remaining containers
    for bitmap in iter {
        merge_container_ref(&mut containers, &bitmap?.containers, |a, b| *a ^= b);
    }

    // Phase 3: Clean up
    let containers: Vec<_> = containers
        .into_iter()
        .filter(|container| !container.is_empty())
        .map(|c| {
            // Any borrowed bitmaps or arrays left over get cloned here
            let mut container = c.into_owned();
            container.ensure_correct_store();
            container
        })
        .collect();

    Ok(RoaringBitmap { containers })
}

fn merge_container_ref<'a>(
    containers: &mut Vec<Cow<'a, Container>>,
    rhs: &'a [Container],
    op: impl Fn(&mut Store, &Store),
) {
    for rhs in rhs {
        match containers.binary_search_by_key(&rhs.key, |c| c.key) {
            Err(loc) => {
                // A container not currently in containers. Borrow it.
                containers.insert(loc, Cow::Borrowed(rhs))
            }
            Ok(loc) => {
                // A container that is in containers. Operate on it.
                let lhs = &mut containers[loc];
                match (&lhs.store, &rhs.store) {
                    (Store::Array(..), Store::Array(..)) => {
                        // We had borrowed an array. Without cloning it, create a new bitmap
                        // Add all the elements to the new bitmap
                        let mut store = lhs.store.to_bitmap();
                        op(&mut store, &rhs.store);
                        *lhs = Cow::Owned(Container { key: lhs.key, store });
                    }
                    (Store::Array(..), Store::Bitmap(..)) => {
                        // We had borrowed an array. Copy the rhs bitmap, add lhs to it
                        let mut store = rhs.store.clone();
                        op(&mut store, &lhs.store);
                        *lhs = Cow::Owned(Container { key: lhs.key, store });
                    }
                    (Store::Bitmap(..), _) => {
                        // This might be a owned or borrowed bitmap.
                        // If it was borrowed it will clone-on-write
                        op(&mut lhs.to_mut().store, &rhs.store);
                    }
                    (Store::Run(..), Store::Run(..)) => {
                        op(&mut lhs.to_mut().store, &rhs.store);
                    }
                    (Store::Run(..), _) => {
                        op(&mut lhs.to_mut().store, &rhs.store);
                    }
                    (Store::Array(..), Store::Run(..)) => {
                        op(&mut lhs.to_mut().store, &rhs.store);
                    }
                };
            }
        }
    }
}

#[inline]
fn collect_starting_elements<I, El, Er>(iter: I) -> Result<Vec<El>, Er>
where
    I: IntoIterator<Item = Result<El, Er>>,
{
    let iter = iter.into_iter();
    let mut to_collect = iter.size_hint().1.unwrap_or(BASE_COLLECT);
    if to_collect > MAX_COLLECT {
        to_collect = BASE_COLLECT;
    }

    let mut ret = Vec::with_capacity(to_collect);
    for el in iter.take(to_collect) {
        ret.push(el?);
    }

    Ok(ret)
}
