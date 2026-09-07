extern crate roaring;
mod iter;
use roaring::RoaringTreemap;

use iter::outside_in;
use proptest::arbitrary::any;
use proptest::collection::btree_set;
use proptest::proptest;

#[test]
fn range() {
    let original = (0..2000).collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from_iter(&original);
    let clone2 = RoaringTreemap::from_iter(original.clone());

    assert_eq!(clone, original);
    assert_eq!(clone2, original);
}

#[test]
fn array() {
    let original = (0..5).collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from([0, 1, 2, 3, 4]);

    assert_eq!(clone, original);
}

#[test]
fn bitmap() {
    let original = (0..6000).collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from_iter(&original);
    let clone2 = RoaringTreemap::from_iter(original.clone());

    assert_eq!(clone, original);
    assert_eq!(clone2, original);
}

#[test]
fn arrays() {
    let original = ((0..2000).chain(1_000_000..1_002_000).chain(2_000_000..2_001_000))
        .collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from_iter(&original);
    let clone2 = RoaringTreemap::from_iter(original.clone());

    assert_eq!(clone, original);
    assert_eq!(clone2, original);
}

#[test]
fn bitmaps() {
    let original = ((0..6000).chain(1_000_000..1_012_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from_iter(&original);
    let clone2 = RoaringTreemap::from_iter(original.clone());

    assert_eq!(clone, original);
    assert_eq!(clone2, original);
}

#[test]
fn bitmaps_iterator() {
    let original = ((0..6000).chain(1_000_000..1_012_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let clone = RoaringTreemap::from_bitmaps(original.bitmaps().map(|(p, b)| (p, b.clone())));
    let clone2 = original.bitmaps().map(|(p, b)| (p, b.clone())).collect::<RoaringTreemap>();

    assert_eq!(clone, original);
    assert_eq!(clone2, original);
}

proptest! {
    #[test]
    fn iter(values in btree_set(any::<u64>(), ..=10_000)) {
        let bitmap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();

        assert!(values.into_iter().eq(bitmap));
    }
}

proptest! {
    #[test]
    fn fold(values in btree_set(any::<u64>(), ..=10_000)) {
        let bitmap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();
        let mut val_iter = values.into_iter();
        // `Iterator::all` uses currently unimplementable `try_fold`, we test `fold`
        #[allow(clippy::unnecessary_fold)]
        let r = bitmap.into_iter().fold(true, |b, i| {
            b && i == val_iter.next().unwrap()
        });
        assert!(r)
    }
}

#[test]
fn rev() {
    let values = (1..3)
        .chain(1_000_000..1_012_003)
        .chain(2_000_001..2_000_003)
        .chain(2_000_000_000_001..2_000_000_000_003);
    let bitmap = RoaringTreemap::from_iter(values.clone());

    assert!(values.into_iter().rev().eq(bitmap.iter().rev()));
}

proptest! {
    #[test]
    fn rev_iter(values in btree_set(any::<u64>(), ..=10_000)) {
        let bitmap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();

        assert!(values.into_iter().rev().eq(bitmap.iter().rev()));
    }
}

#[test]
fn from_iter() {
    // This test verifies that the public API allows conversion from iterators
    // with u64 as well as &u64 elements.
    let vals = vec![1, 5, 1_000_000_000_000_000];
    let a = RoaringTreemap::from_iter(vals.iter());
    let b = RoaringTreemap::from_iter(vals);
    assert_eq!(a, b);
}

#[test]
fn interleaved() {
    let values = (1..3)
        .chain(1_000_000..1_012_003)
        .chain(2_000_001..2_000_003)
        .chain(2_000_000_000_001..2_000_000_000_003);
    let bitmap = RoaringTreemap::from_iter(values.clone());

    assert!(outside_in(values).eq(outside_in(bitmap)));
}

proptest! {
    #[test]
    fn interleaved_iter(values in btree_set(any::<u64>(), 50_000..=100_000)) {
        let bitmap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();

        assert!(outside_in(values).eq(outside_in(bitmap)));
    }
}
