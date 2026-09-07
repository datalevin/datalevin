use proptest::collection::btree_set;
use proptest::prelude::*;
use roaring::RoaringBitmap;
use std::ops::Bound;

#[test]
fn range_array() {
    let mut rb = RoaringBitmap::new();
    rb.insert(0);
    rb.insert(1);
    rb.insert(10);
    rb.insert(100_000);
    rb.insert(999_999);
    rb.insert(1_000_000);

    let expected = vec![1, 10, 100_000, 999_999];
    let actual: Vec<u32> = rb.range(1..=999_999).collect();
    assert_eq!(expected, actual);
}

#[test]
fn range_bitmap() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();

    let expected = vec![10, 11, 12];
    let actual: Vec<u32> = rb.range(0..13).collect();
    assert_eq!(expected, actual);
}

#[test]
fn empty_range() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();

    let mut it = rb.range(0..0);
    assert_eq!(it.next(), None);
    let mut it = rb.range(..0);
    assert_eq!(it.next(), None);
    it = rb.range(13..13);
    assert_eq!(it.next(), None);
    it = rb.range((Bound::Excluded(1), Bound::Included(1)));
    assert_eq!(it.next(), None);
    it = rb.range(u32::MAX..u32::MAX);
    assert_eq!(it.next(), None);
    it = rb.range((Bound::Excluded(u32::MAX), Bound::Included(u32::MAX)));
    assert_eq!(it.next(), None);
    it = rb.range((Bound::Excluded(u32::MAX), Bound::Unbounded));
    assert_eq!(it.next(), None);
}

#[test]
#[should_panic(expected = "range start is greater than range end")]
fn invalid_range() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();
    #[allow(clippy::reversed_empty_ranges)]
    let _ = rb.range(13..0);
}

#[test]
#[should_panic(expected = "range start and end are equal and excluded")]
fn invalid_range_equal_excluded() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();
    let _ = rb.range((Bound::Excluded(13), Bound::Excluded(13)));
}

#[test]
#[should_panic(expected = "range start is greater than range end")]
fn into_invalid_range() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();
    #[allow(clippy::reversed_empty_ranges)]
    let _ = rb.into_range(13..0);
}

#[test]
#[should_panic(expected = "range start and end are equal and excluded")]
fn into_invalid_range_equal_excluded() {
    let rb = RoaringBitmap::from_sorted_iter(10..5000).unwrap();
    let _ = rb.into_range((Bound::Excluded(13), Bound::Excluded(13)));
}

proptest! {
    #[test]
    fn proptest_range(
        values in btree_set(..=262_143_u32, ..=1000),
        range_a in 0u32..262_143,
        range_b in 0u32..262_143,
    ){
        let range = range_a.min(range_b)..=range_a.max(range_b);

        let bitmap = RoaringBitmap::from_sorted_iter(values.iter().cloned()).unwrap();
        let expected: Vec<u32> = values.range(range.clone()).copied().collect();
        let actual: Vec<u32> = bitmap.range(range.clone()).collect();

        assert_eq!(expected, actual);
    }
}
