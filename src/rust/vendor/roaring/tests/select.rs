extern crate roaring;

use proptest::collection::btree_set;
use proptest::prelude::*;
use roaring::RoaringBitmap;

#[test]
fn select() {
    let bitmap = (0..2000).collect::<RoaringBitmap>();

    assert_eq!(bitmap.select(0), Some(0));
}

#[test]
fn select_array() {
    let bitmap = (0..2000).collect::<RoaringBitmap>();

    assert_eq!(bitmap.select(0), Some(0));
    assert_eq!(bitmap.select(100), Some(100));
    assert_eq!(bitmap.select(1000), Some(1000));
    assert_eq!(bitmap.select(1999), Some(1999));
    assert_eq!(bitmap.select(2000), None);
}

#[test]
fn select_bitmap() {
    let bitmap = (0..100_000).collect::<RoaringBitmap>();

    assert_eq!(bitmap.select(0), Some(0));
    assert_eq!(bitmap.select(63), Some(63));
    assert_eq!(bitmap.select(1000), Some(1000));
    assert_eq!(bitmap.select(65535), Some(65535));
}

#[test]
fn select_empty() {
    let bitmap = RoaringBitmap::new();

    assert_eq!(bitmap.select(0), None);
    assert_eq!(bitmap.select(1024), None);
    assert_eq!(bitmap.select(u32::MAX), None);
}

proptest! {
    #[test]
    fn proptest_select(values in btree_set(any::<u32>(), 1000)) {
        let bitmap = RoaringBitmap::from_sorted_iter(values.iter().cloned()).unwrap();
        for (i, value) in values.iter().cloned().enumerate() {
            prop_assert_eq!(bitmap.select(i as u32), Some(value));
        }
    }
}
