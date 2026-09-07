use alloc::collections::{binary_heap::PeekMut, BTreeMap, BinaryHeap};
use core::{borrow::Borrow, cmp::Ordering, mem};

use crate::{MultiOps, RoaringBitmap, RoaringTreemap};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

impl<I> MultiOps<RoaringTreemap> for I
where
    I: IntoIterator<Item = RoaringTreemap>,
{
    type Output = RoaringTreemap;

    fn union(self) -> Self::Output {
        try_simple_multi_op_owned::<_, _, UnionOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn intersection(self) -> Self::Output {
        try_ordered_multi_op_owned::<_, _, IntersectionOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn difference(self) -> Self::Output {
        try_ordered_multi_op_owned::<_, _, DifferenceOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn symmetric_difference(self) -> Self::Output {
        try_simple_multi_op_owned::<_, _, SymmetricDifferenceOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }
}

impl<I, E> MultiOps<Result<RoaringTreemap, E>> for I
where
    I: IntoIterator<Item = Result<RoaringTreemap, E>>,
{
    type Output = Result<RoaringTreemap, E>;

    fn union(self) -> Self::Output {
        try_simple_multi_op_owned::<_, _, UnionOp>(self)
    }

    fn intersection(self) -> Self::Output {
        try_ordered_multi_op_owned::<_, _, IntersectionOp>(self)
    }

    fn difference(self) -> Self::Output {
        try_ordered_multi_op_owned::<_, _, DifferenceOp>(self)
    }

    fn symmetric_difference(self) -> Self::Output {
        try_simple_multi_op_owned::<_, _, SymmetricDifferenceOp>(self)
    }
}

#[inline]
fn try_simple_multi_op_owned<E, I, O: Op>(treemaps: I) -> Result<RoaringTreemap, E>
where
    I: IntoIterator<Item = Result<RoaringTreemap, E>>,
{
    let treemaps = treemaps.into_iter().collect::<Result<Vec<_>, _>>()?;

    let mut heap: BinaryHeap<_> = treemaps
        .into_iter()
        .filter_map(|treemap| {
            let mut iter = treemap.map.into_iter();
            iter.next().map(|(key, bitmap)| PeekedRoaringBitmap { key, bitmap, iter })
        })
        .collect();

    let mut bitmaps = Vec::new();
    let mut map = BTreeMap::new();

    while let Some(mut peek) = heap.peek_mut() {
        let (key, bitmap) = match peek.iter.next() {
            Some((next_key, next_bitmap)) => {
                let key = peek.key;
                peek.key = next_key;
                let bitmap = mem::replace(&mut peek.bitmap, next_bitmap);
                (key, bitmap)
            }
            None => {
                let popped = PeekMut::pop(peek);
                (popped.key, popped.bitmap)
            }
        };

        if let Some((first_key, _)) = bitmaps.first() {
            if *first_key != key {
                let current_key = *first_key;
                let computed_bitmap = O::op_owned(bitmaps.drain(..).map(|(_, rb)| rb));
                if !computed_bitmap.is_empty() {
                    map.insert(current_key, computed_bitmap);
                }
            }
        }

        bitmaps.push((key, bitmap));
    }

    if let Some((first_key, _)) = bitmaps.first() {
        let current_key = *first_key;
        let computed_bitmap = O::op_owned(bitmaps.drain(..).map(|(_, rb)| rb));
        if !computed_bitmap.is_empty() {
            map.insert(current_key, computed_bitmap);
        }
    }

    Ok(RoaringTreemap { map })
}

#[inline]
fn try_ordered_multi_op_owned<E, I, O: Op>(treemaps: I) -> Result<RoaringTreemap, E>
where
    I: IntoIterator<Item = Result<RoaringTreemap, E>>,
{
    let mut treemaps = treemaps.into_iter();
    let mut treemap = match treemaps.next().transpose()? {
        Some(treemap) => treemap,
        None => return Ok(RoaringTreemap::new()),
    };
    let mut treemaps = treemaps.collect::<Result<Vec<_>, _>>()?;

    // for each key in the first treemap we're going to find and
    // accumulate all the corresponding bitmaps
    let keys: Vec<_> = treemap.map.keys().copied().collect();
    for k in keys {
        // the unwrap is safe since we're iterating on our keys
        let current_bitmap = treemap.map.remove(&k).unwrap();
        let new_bitmap =
            O::op_owned(core::iter::once(current_bitmap).chain(
                treemaps.iter_mut().map(|treemap| treemap.map.remove(&k).unwrap_or_default()),
            ));
        if !new_bitmap.is_empty() {
            treemap.map.insert(k, new_bitmap);
        }
    }

    Ok(treemap)
}

#[inline]
fn try_ordered_multi_op_ref<'a, E: 'a, I, O: Op>(treemaps: I) -> Result<RoaringTreemap, E>
where
    I: IntoIterator<Item = Result<&'a RoaringTreemap, E>>,
{
    let mut treemaps = treemaps.into_iter();
    let treemap = match treemaps.next().transpose()? {
        Some(treemap) => treemap,
        None => return Ok(RoaringTreemap::new()),
    };
    let treemaps = treemaps.collect::<Result<Vec<_>, _>>()?;

    let mut ret = RoaringTreemap::new();

    // for each keys in the first treemap we're going find and accumulate all the corresponding bitmaps
    let keys: Vec<_> = treemap.map.keys().copied().collect();
    let empty_bitmap = RoaringBitmap::new();
    for k in keys {
        // the unwrap is safe since we're iterating on our keys
        let current_bitmap = treemap.map.get(&k).unwrap();
        let new_bitmap = O::op_ref(
            core::iter::once(current_bitmap)
                .chain(treemaps.iter().map(|treemap| treemap.map.get(&k).unwrap_or(&empty_bitmap))),
        );
        if !new_bitmap.is_empty() {
            ret.map.insert(k, new_bitmap);
        }
    }

    Ok(ret)
}

#[inline]
fn try_simple_multi_op_ref<'a, E: 'a, I, O: Op>(treemaps: I) -> Result<RoaringTreemap, E>
where
    I: IntoIterator<Item = Result<&'a RoaringTreemap, E>>,
{
    let treemaps = treemaps.into_iter().collect::<Result<Vec<_>, E>>()?;

    let mut heap: BinaryHeap<_> = treemaps
        .into_iter()
        .filter_map(|treemap| {
            let mut iter = treemap.map.iter();
            iter.next().map(|(&key, bitmap)| PeekedRoaringBitmap { key, bitmap, iter })
        })
        .collect();

    let mut bitmaps = Vec::new();
    let mut map = BTreeMap::new();

    while let Some(mut peek) = heap.peek_mut() {
        let (key, bitmap) = match peek.iter.next() {
            Some((&next_key, next_bitmap)) => {
                let key = peek.key;
                peek.key = next_key;
                let bitmap = mem::replace(&mut peek.bitmap, next_bitmap);
                (key, bitmap)
            }
            None => {
                let popped = PeekMut::pop(peek);
                (popped.key, popped.bitmap)
            }
        };

        if let Some((first_key, _)) = bitmaps.first() {
            if *first_key != key {
                let current_key = *first_key;
                let computed_bitmap = O::op_ref(bitmaps.drain(..).map(|(_, rb)| rb));
                if !computed_bitmap.is_empty() {
                    map.insert(current_key, computed_bitmap);
                }
            }
        }

        bitmaps.push((key, bitmap));
    }

    if let Some((first_key, _)) = bitmaps.first() {
        let current_key = *first_key;
        let computed_bitmap = O::op_ref(bitmaps.drain(..).map(|(_, rb)| rb));
        if !computed_bitmap.is_empty() {
            map.insert(current_key, computed_bitmap);
        }
    }

    Ok(RoaringTreemap { map })
}

trait Op {
    fn op_owned<I: IntoIterator<Item = RoaringBitmap>>(iter: I) -> RoaringBitmap;
    fn op_ref<'a, I: IntoIterator<Item = &'a RoaringBitmap>>(iter: I) -> RoaringBitmap;
}

enum UnionOp {}

impl Op for UnionOp {
    fn op_owned<J: IntoIterator<Item = RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.union()
    }

    fn op_ref<'a, J: IntoIterator<Item = &'a RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.union()
    }
}

enum IntersectionOp {}

impl Op for IntersectionOp {
    fn op_owned<J: IntoIterator<Item = RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.intersection()
    }

    fn op_ref<'a, J: IntoIterator<Item = &'a RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.intersection()
    }
}

enum DifferenceOp {}

impl Op for DifferenceOp {
    fn op_owned<J: IntoIterator<Item = RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.difference()
    }

    fn op_ref<'a, J: IntoIterator<Item = &'a RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.difference()
    }
}

enum SymmetricDifferenceOp {}

impl Op for SymmetricDifferenceOp {
    fn op_owned<J: IntoIterator<Item = RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.symmetric_difference()
    }

    fn op_ref<'a, J: IntoIterator<Item = &'a RoaringBitmap>>(iter: J) -> RoaringBitmap {
        iter.symmetric_difference()
    }
}

impl<'a, I> MultiOps<&'a RoaringTreemap> for I
where
    I: IntoIterator<Item = &'a RoaringTreemap>,
{
    type Output = RoaringTreemap;

    fn union(self) -> Self::Output {
        try_simple_multi_op_ref::<_, _, UnionOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn intersection(self) -> Self::Output {
        try_ordered_multi_op_ref::<_, _, IntersectionOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn difference(self) -> Self::Output {
        try_ordered_multi_op_ref::<_, _, DifferenceOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }

    fn symmetric_difference(self) -> Self::Output {
        try_simple_multi_op_ref::<_, _, SymmetricDifferenceOp>(
            self.into_iter().map(Ok::<_, core::convert::Infallible>),
        )
        .unwrap()
    }
}

impl<'a, I, E: 'a> MultiOps<Result<&'a RoaringTreemap, E>> for I
where
    I: IntoIterator<Item = Result<&'a RoaringTreemap, E>>,
{
    type Output = Result<RoaringTreemap, E>;

    fn union(self) -> Self::Output {
        try_simple_multi_op_ref::<_, _, UnionOp>(self)
    }

    fn intersection(self) -> Self::Output {
        try_ordered_multi_op_ref::<_, _, IntersectionOp>(self)
    }

    fn difference(self) -> Self::Output {
        try_ordered_multi_op_ref::<_, _, DifferenceOp>(self)
    }

    fn symmetric_difference(self) -> Self::Output {
        try_simple_multi_op_ref::<_, _, SymmetricDifferenceOp>(self)
    }
}

struct PeekedRoaringBitmap<R, I> {
    key: u32,
    bitmap: R,
    iter: I,
}

impl<R: Borrow<RoaringBitmap>, I> Ord for PeekedRoaringBitmap<R, I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).reverse()
    }
}

impl<R: Borrow<RoaringBitmap>, I> PartialOrd for PeekedRoaringBitmap<R, I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R: Borrow<RoaringBitmap>, I> Eq for PeekedRoaringBitmap<R, I> {}

impl<R: Borrow<RoaringBitmap>, I> PartialEq for PeekedRoaringBitmap<R, I> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
