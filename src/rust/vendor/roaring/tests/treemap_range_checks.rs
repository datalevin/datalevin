use proptest::collection::hash_set;
use proptest::prelude::*;
use roaring::RoaringTreemap;

// The bucket boundary is at every multiple of 2^32. Values with the same high 32 bits
// share a bucket; adjacent high keys are adjacent buckets.
const BUCKET: u64 = 1 << 32;

#[test]
fn empty_range_always_contained() {
    let rb = RoaringTreemap::new();
    assert!(rb.contains_range(7..7));
    assert!(rb.contains_range(0..0));
    assert!(rb.contains_range(u64::MAX..u64::MAX));
}

#[test]
fn empty_range_cardinality_is_zero() {
    let rb = RoaringTreemap::new();
    assert_eq!(rb.range_cardinality(7..7), 0);
    assert_eq!(rb.range_cardinality(u64::MAX..u64::MAX), 0);
}

#[test]
fn empty_treemap_not_contained() {
    let rb = RoaringTreemap::new();
    assert!(!rb.contains_range(0..1));
    assert!(!rb.contains_range(0..=u64::MAX));
    assert_eq!(rb.range_cardinality(0..=u64::MAX), 0);
}

#[test]
fn single_bucket_contained() {
    let mut rb = RoaringTreemap::new();
    rb.insert_range(10..20);
    assert!(rb.contains_range(10..20));
    assert!(rb.contains_range(11..19));
    assert!(!rb.contains_range(9..20));
    assert!(!rb.contains_range(10..21));
    assert_eq!(rb.range_cardinality(10..20), 10);
    assert_eq!(rb.range_cardinality(10..15), 5);
    assert_eq!(rb.range_cardinality(0..10), 0);
    assert_eq!(rb.range_cardinality(20..30), 0);
}

#[test]
fn cross_bucket_boundary() {
    // Values straddle the hi=0 / hi=1 bucket boundary
    let lo_max = u32::MAX as u64;
    let hi_min = BUCKET;

    let mut rb = RoaringTreemap::new();
    rb.insert(lo_max);
    rb.insert(hi_min);

    assert!(rb.contains_range(lo_max..=lo_max));
    assert!(rb.contains_range(hi_min..=hi_min));
    // Range spanning the two values — only those two exist, so the full span isn't contained
    assert!(!rb.contains_range(lo_max - 1..=hi_min));
    assert!(!rb.contains_range(lo_max..=hi_min + 1));

    // But range_cardinality counts the values present
    assert_eq!(rb.range_cardinality(lo_max..=hi_min), 2);
    assert_eq!(rb.range_cardinality(lo_max..=lo_max), 1);
    assert_eq!(rb.range_cardinality(hi_min..=hi_min), 1);
    assert_eq!(rb.range_cardinality(lo_max - 1..lo_max), 0);

    // Insert the full span and verify containment
    rb.insert_range(lo_max..=hi_min);
    assert!(rb.contains_range(lo_max..=hi_min));
    assert_eq!(rb.range_cardinality(lo_max..=hi_min), 2);
}

#[test]
fn multi_bucket_gap_not_contained() {
    // Insert values in buckets 0 and 2, leaving bucket 1 empty.
    let mut rb = RoaringTreemap::new();
    rb.insert_range(0..BUCKET); // hi=0, full
    rb.insert_range(2 * BUCKET..3 * BUCKET); // hi=2, full

    assert!(!rb.contains_range(0..3 * BUCKET));
    assert!(rb.contains_range(0..BUCKET));
    assert!(rb.contains_range(2 * BUCKET..3 * BUCKET));
    assert_eq!(rb.range_cardinality(0..3 * BUCKET), 2 * BUCKET);
}

#[test]
fn u64_max_boundary() {
    let mut rb = RoaringTreemap::new();
    rb.insert(u64::MAX);
    assert!(rb.contains_range(u64::MAX..=u64::MAX));
    assert!(!rb.contains_range(u64::MAX - 1..=u64::MAX));
    assert_eq!(rb.range_cardinality(u64::MAX..=u64::MAX), 1);
    assert_eq!(rb.range_cardinality(u64::MAX - 1..u64::MAX), 0);

    // Insert the last two values
    rb.insert(u64::MAX - 1);
    assert!(rb.contains_range(u64::MAX - 1..=u64::MAX));
    assert_eq!(rb.range_cardinality(u64::MAX - 1..=u64::MAX), 2);
}

#[test]
fn unbounded_range() {
    // Use a start value in the last bucket (hi = u32::MAX) to avoid allocating
    // billions of buckets, which would happen if the high word of start is small.
    let last_bucket_start = (u32::MAX as u64) << 32; // hi=u32::MAX, lo=0
    let mut rb = RoaringTreemap::new();
    rb.insert_range(last_bucket_start..);
    assert!(rb.contains_range(last_bucket_start..));
    assert!(rb.contains_range(last_bucket_start + 1..=u64::MAX));
    assert!(!rb.contains_range(last_bucket_start - 1..=u64::MAX));
}

proptest! {
    #[test]
    fn proptest_range(
        // Keep values well within a single bucket to avoid very slow tests from
        // inserting billions of values across bucket boundaries.
        start in ..=262_143_u64,
        len in ..=262_143_u64,
        extra in hash_set(..=462_143_u64, ..=100),
    ) {
        let end = start + len;
        let range = start..end;
        let inverse_empty_range = (start + len)..start;

        let mut rb = RoaringTreemap::new();
        rb.insert_range(range.clone());
        assert!(rb.contains_range(range.clone()));
        assert!(rb.contains_range(inverse_empty_range.clone()));
        assert_eq!(rb.range_cardinality(range.clone()), len);

        for &val in &extra {
            rb.insert(val);
            assert!(rb.contains_range(range.clone()));
            assert!(rb.contains_range(inverse_empty_range.clone()));
            assert_eq!(rb.range_cardinality(range.clone()), len);
        }

        for (i, &val) in extra.iter().filter(|&&x| range.contains(&x)).enumerate() {
            rb.remove(val);
            assert!(!rb.contains_range(range.clone()));
            assert!(rb.contains_range(inverse_empty_range.clone()));
            assert_eq!(rb.range_cardinality(range.clone()), len - i as u64 - 1);
        }
    }

    #[test]
    fn proptest_range_boundaries(
        start in 1..=262_143_u64,
        len in 0..=262_143_u64,
    ) {
        let mut rb = RoaringTreemap::new();
        let end = start + len;
        let half = start + len / 2;
        rb.insert_range(start..end);

        assert!(rb.contains_range(start..end));
        assert!(rb.contains_range(start + 1..end));
        assert!(rb.contains_range(start..end.saturating_sub(1)));
        assert!(rb.contains_range(start + 1..end.saturating_sub(1)));

        assert!(!rb.contains_range(start - 1..end));
        assert!(!rb.contains_range(start - 1..end.saturating_sub(1)));
        assert!(!rb.contains_range(start..end + 1));
        assert!(!rb.contains_range(start + 1..end + 1));
        assert!(!rb.contains_range(start - 1..end + 1));

        assert!(!rb.contains_range(start - 1..half));
        assert!(!rb.contains_range(half..end + 1));
    }

    #[test]
    fn proptest_cross_bucket(
        // start_lo: low 32 bits of start value, in bucket hi=0
        start_lo in 0_u32..=u32::MAX / 2,
        // end_lo: low 32 bits of end value, in bucket hi=1
        end_lo in u32::MAX / 2..=u32::MAX,
    ) {
        let start = start_lo as u64;
        let end = BUCKET | end_lo as u64;

        let mut rb = RoaringTreemap::new();
        rb.insert_range(start..=end);

        assert!(rb.contains_range(start..=end));
        assert_eq!(
            rb.range_cardinality(start..=end),
            (u32::MAX as u64 - start_lo as u64 + 1) + (end_lo as u64 + 1),
        );

        // One element past the end should break containment
        if end < u64::MAX {
            assert!(!rb.contains_range(start..=end + 1));
        }
        if start > 0 {
            assert!(!rb.contains_range(start - 1..=end));
        }
    }
}
