use proptest::collection::hash_set;
use proptest::prelude::*;
use roaring::RoaringBitmap;

#[test]
fn u32_max() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert(u32::MAX);
    assert!(bitmap.contains_range(u32::MAX..=u32::MAX));
    assert!(!bitmap.contains_range(u32::MAX - 1..=u32::MAX));

    bitmap.insert_range(4_000_000_000..);
    assert!(bitmap.contains_range(4_000_000_000..));
    assert!(bitmap.contains_range(4_000_000_000..u32::MAX));
    assert!(bitmap.contains_range(4_000_000_000..=u32::MAX));
    assert!(bitmap.contains_range(4_100_000_000..=u32::MAX));
}

proptest! {
    #[test]
    fn proptest_range(
        start in ..=262_143_u32,
        len in ..=262_143_u32,
        extra in hash_set(..=462_143_u32, ..=100),
    ){
        let end = start + len;
        let range = start..end;
        let inverse_empty_range = (start+len)..start;

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(range.clone());
        assert!(bitmap.contains_range(range.clone()));
        assert!(bitmap.contains_range(inverse_empty_range.clone()));
        assert_eq!(bitmap.range_cardinality(range.clone()) as usize, range.len());

        for &val in &extra {
            bitmap.insert(val);
            assert!(bitmap.contains_range(range.clone()));
            assert!(bitmap.contains_range(inverse_empty_range.clone()));
            assert_eq!(bitmap.range_cardinality(range.clone()) as usize, range.len());
        }

        for (i, &val) in extra.iter().filter(|x| range.contains(x)).enumerate() {
            bitmap.remove(val);
            assert!(!bitmap.contains_range(range.clone()));
            assert!(bitmap.contains_range(inverse_empty_range.clone()));
            assert_eq!(bitmap.range_cardinality(range.clone()) as usize, range.len() - i - 1);
        }
    }

    #[test]
    fn proptest_range_boundaries(
        // Ensure we can always subtract one from start
        start in 1..=262_143_u32,
        len in 0..=262_143_u32,
    ) {
        let mut bitmap = RoaringBitmap::new();
        let end = start + len;
        let half = start + len / 2;
        bitmap.insert_range(start..end);

        assert!(bitmap.contains_range(start..end));

        assert!(bitmap.contains_range(start+1..end));
        assert!(bitmap.contains_range(start..end - 1));
        assert!(bitmap.contains_range(start+1..end - 1));

        assert!(!bitmap.contains_range(start - 1..end));
        assert!(!bitmap.contains_range(start - 1..end - 1));
        assert!(!bitmap.contains_range(start..end + 1));
        assert!(!bitmap.contains_range(start + 1..end + 1));
        assert!(!bitmap.contains_range(start - 1..end + 1));

        assert!(!bitmap.contains_range(start - 1..half));
        assert!(!bitmap.contains_range(half..end + 1));
    }
}
