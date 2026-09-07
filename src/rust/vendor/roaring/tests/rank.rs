extern crate roaring;

use proptest::collection::{btree_set, vec};
use proptest::prelude::*;
use roaring::RoaringBitmap;

#[test]
fn rank() {
    let mut bitmap = RoaringBitmap::from_sorted_iter(0..2000).unwrap();
    bitmap.insert_range(200_000..210_000);

    // No matching container
    assert_eq!(bitmap.rank(80_000), 2000);
    assert_eq!(bitmap.rank(u32::MAX), 12_000);

    // Array container at key
    assert_eq!(bitmap.rank(0), 1);
    assert_eq!(bitmap.rank(100), 101);
    assert_eq!(bitmap.rank(2000), 2000);

    // Bitmap container at key
    assert_eq!(bitmap.rank(200_000), 2001);
    assert_eq!(bitmap.rank(210_000), 12_000);
}

#[test]
fn rank_array() {
    let bitmap = RoaringBitmap::from_sorted_iter(0..2000).unwrap();

    // No matching container
    assert_eq!(bitmap.rank(u32::MAX), 2000);

    // Has container (array)
    assert_eq!(bitmap.rank(0), 1);
    assert_eq!(bitmap.rank(100), 101);
    assert_eq!(bitmap.rank(2000), 2000);
    assert_eq!(bitmap.rank(3000), 2000);
}

#[test]
fn rank_bitmap() {
    let bitmap = RoaringBitmap::from_sorted_iter(0..5000).unwrap();

    // key: 0, bit: 0
    assert_eq!(bitmap.rank(0), 1);

    // key: 0, bit: 63 (mask of all ones)
    assert_eq!(bitmap.rank(63), 64);

    // key: 1023, bit: 0
    assert_eq!(bitmap.rank(65535), 5000);

    // key: 1023, bit: 63 (mask of all ones)
    assert_eq!(bitmap.rank(65472), 5000);

    assert_eq!(bitmap.rank(1), 2);
    assert_eq!(bitmap.rank(100), 101);
    assert_eq!(bitmap.rank(1000), 1001);
    assert_eq!(bitmap.rank(4999), 5000);
}

proptest! {
    #[test]
    fn proptest_rank(
        values in btree_set(..=262_143_u32, ..=1000),
        checks in vec(..=262_143_u32, ..=100)
    ){
        let bitmap = RoaringBitmap::from_sorted_iter(values.iter().cloned()).unwrap();
        for i in checks {
            let expected = values.iter().take_while(|&&x| x <= i).count() as u64;
            assert_eq!(bitmap.rank(i), expected);
        }
    }
}
