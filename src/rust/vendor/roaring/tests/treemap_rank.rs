extern crate roaring;

use core::ops::RangeInclusive;
use proptest::collection::{btree_set, vec};
use proptest::prelude::*;
use roaring::RoaringTreemap;

const BITMAP_MAX: u64 = u32::MAX as u64;

#[test]
fn rank_roaring_bitmaps() {
    // A treemap with two roaring bitmaps.
    // The lower one contains one array container with the highest 1000 values
    // The higher one contains one bitmap at with the lowest 5000 values
    let treemap = RoaringTreemap::from_sorted_iter(BITMAP_MAX - 1000..BITMAP_MAX + 5000).unwrap();

    // start of treemap
    assert_eq!(treemap.rank(0), 0);

    // low boundary
    assert_eq!(treemap.rank(BITMAP_MAX - 1002), 0);
    assert_eq!(treemap.rank(BITMAP_MAX - 1001), 0);
    assert_eq!(treemap.rank(BITMAP_MAX - 1000), 1);

    // middle range (spans two roaring bitmaps)
    assert_eq!(treemap.rank(BITMAP_MAX - 1), 1000);
    assert_eq!(treemap.rank(BITMAP_MAX), 1001);
    assert_eq!(treemap.rank(BITMAP_MAX + 1), 1002);

    // high boundary
    assert_eq!(treemap.rank(BITMAP_MAX + 4998), 5999);
    assert_eq!(treemap.rank(BITMAP_MAX + 4999), 6000);
    assert_eq!(treemap.rank(BITMAP_MAX + 5000), 6000);

    // end of treemap
    assert_eq!(treemap.rank(u64::MAX), 6000);
}

// A range that spans 2 roaring bitmaps with 2 containers each
const PROP_RANGE: RangeInclusive<u64> = BITMAP_MAX - (1 << 17)..=BITMAP_MAX + (1 << 17);

proptest! {
    #[test]
    fn proptest_rank(
        values in btree_set(PROP_RANGE, ..=1000),
        checks in vec(PROP_RANGE, ..=100)
    ){
        let treemap = RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap();
        for i in checks {
            let expected = values.iter().take_while(|&&x| x <= i).count() as u64;
            assert_eq!(treemap.rank(i), expected);
        }
    }
}
