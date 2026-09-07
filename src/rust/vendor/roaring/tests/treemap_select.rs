extern crate roaring;

use proptest::collection::btree_set;
use proptest::prelude::*;
use roaring::RoaringTreemap;

#[test]
fn select() {
    let bitmap = (0..2000).collect::<RoaringTreemap>();

    assert_eq!(bitmap.select(0), Some(0));
}

#[test]
fn select_multiple_bitmap() {
    let mut bitmap = (0..100_000).collect::<RoaringTreemap>();
    bitmap.append(u32::MAX as u64..u32::MAX as u64 + 100_000).expect("sorted integers");

    assert_eq!(bitmap.select(0), Some(0));
    assert_eq!(bitmap.select(99_999), Some(99_999));
    assert_eq!(bitmap.select(100_000), Some(u32::MAX as u64));
    assert_eq!(bitmap.select(199_999), Some(u32::MAX as u64 + 99_999));
    assert_eq!(bitmap.select(200_000), None);
    assert_eq!(bitmap.select(u64::MAX), None);
}

#[test]
fn select_empty() {
    let bitmap = RoaringTreemap::new();

    assert_eq!(bitmap.select(0), None);
    assert_eq!(bitmap.select(1024), None);
    assert_eq!(bitmap.select(u64::MAX), None);
}

proptest! {
    #[test]
    fn proptest_select(values in btree_set(any::<u64>(), 1000)) {
        let bitmap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();
        for (i, value) in values.iter().cloned().enumerate() {
            prop_assert_eq!(bitmap.select(i as u64), Some(value));
        }
    }
}
