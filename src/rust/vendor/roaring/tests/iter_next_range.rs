use roaring::RoaringBitmap;

#[test]
fn next_range_basic() {
    let bm = RoaringBitmap::from([1, 2, 4, 5]);
    let mut iter = bm.iter();

    // First consecutive range: 1..=2
    assert_eq!(iter.next_range(), Some(1..=2));

    // Iterator should now point at 4
    assert_eq!(iter.next(), Some(4));

    // Second consecutive range: 5..=5 (single element)
    assert_eq!(iter.next_range(), Some(5..=5));

    // Iterator should now be exhausted
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_range(), None);
}

#[test]
fn next_range_back_basic() {
    let bm = RoaringBitmap::from([1, 2, 4, 5]);
    let mut iter = bm.iter();

    // Last consecutive range from back: 4..=5
    assert_eq!(iter.next_range_back(), Some(4..=5));

    // Iterator back should now point at 2
    assert_eq!(iter.next_back(), Some(2));

    // Previous consecutive range from back: 1..=1 (single element)
    assert_eq!(iter.next_range_back(), Some(1..=1));

    // Iterator should now be exhausted from back
    assert_eq!(iter.next_back(), None);
    assert_eq!(iter.next_range_back(), None);
}

#[test]
fn next_range_single_elements() {
    // All single-element ranges
    let bm = RoaringBitmap::from([1, 3, 5, 7]);
    let mut iter = bm.iter();

    assert_eq!(iter.next_range(), Some(1..=1));
    assert_eq!(iter.next(), Some(3));

    assert_eq!(iter.next_range(), Some(5..=5));
    assert_eq!(iter.next(), Some(7));

    assert_eq!(iter.next_range(), None);
    assert_eq!(iter.next(), None);
}

#[test]
fn next_range_long_consecutive() {
    // Long consecutive sequence
    let bm = RoaringBitmap::from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let mut iter = bm.iter();

    // Should get the entire range
    assert_eq!(iter.next_range(), Some(1..=10));

    // Iterator should be exhausted after consuming the range
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_range(), None);
}

#[test]
fn next_range_partial_consumption() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 5, 10, 11, 12]);
    let mut iter = bm.iter();

    // Consume some elements first
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(2));

    // Should get remaining range from current position
    assert_eq!(iter.next_range(), Some(3..=5));

    // Continue with next range
    assert_eq!(iter.next(), Some(10));
    assert_eq!(iter.next_range(), Some(11..=12));
}

#[test]
fn next_range_back_partial_consumption() {
    let bm = RoaringBitmap::from([1, 2, 3, 10, 11, 12]);
    let mut iter = bm.iter();

    // Consume some elements from back first
    assert_eq!(iter.next_back(), Some(12));
    assert_eq!(iter.next_back(), Some(11));

    // Should get remaining range from back position
    assert_eq!(iter.next_range_back(), Some(10..=10));

    // Continue with previous range
    assert_eq!(iter.next_back(), Some(3));
    assert_eq!(iter.next_range_back(), Some(1..=2));
}

#[test]
fn next_range_empty_bitmap() {
    let bm = RoaringBitmap::new();
    let mut iter = bm.iter();

    assert_eq!(iter.next_range(), None);
    assert_eq!(iter.next_range_back(), None);
}

#[test]
fn next_range_single_element_bitmap() {
    let bm = RoaringBitmap::from([42]);
    let mut iter = bm.iter();

    assert_eq!(iter.next_range(), Some(42..=42));
    assert_eq!(iter.next(), None);

    // Reset for back test
    let mut iter = bm.iter();
    assert_eq!(iter.next_range_back(), Some(42..=42));
    assert_eq!(iter.next_back(), None);
}

#[test]
fn next_range_mixed_operations() {
    let bm = RoaringBitmap::from([1, 2, 3, 10, 11, 12, 20]);
    let mut iter = bm.iter();

    // Mix forward and backward operations
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next_back(), Some(20));

    // Get remaining range from front (from current position 2)
    assert_eq!(iter.next_range(), Some(2..=3));

    // Get remaining range from back (should be 10..=12)
    assert_eq!(iter.next_range_back(), Some(10..=12));

    // Both ranges consumed, iterator should be empty
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
}

#[test]
fn next_range_multi_container() {
    // Test across container boundaries
    let bm = RoaringBitmap::from([1, 2, 0x1_0000, 0x1_0001, 0x1_0002]);
    let mut iter = bm.iter();

    // First container range
    assert_eq!(iter.next_range(), Some(1..=2));

    // Second container range
    assert_eq!(iter.next(), Some(0x1_0000));
    assert_eq!(iter.next_range(), Some(0x1_0001..=0x1_0002));

    assert_eq!(iter.next(), None);
}

#[test]
fn next_range_u32_max_boundary() {
    // Test behavior at u32::MAX boundary
    let bm = RoaringBitmap::from([u32::MAX - 2, u32::MAX - 1, u32::MAX]);
    let mut iter = bm.iter();

    // Should handle u32::MAX correctly with RangeInclusive
    assert_eq!(iter.next_range(), Some((u32::MAX - 2)..=u32::MAX));

    assert_eq!(iter.next(), None);
}

#[test]
fn next_range_advance_to_integration() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 5, 10, 11, 12, 13]);
    let mut iter = bm.iter();

    // Advance to middle of a consecutive range
    iter.advance_to(3);

    // Should get remaining part of the range
    assert_eq!(iter.next_range(), Some(3..=5));

    // Continue with next range
    assert_eq!(iter.next(), Some(10));
    assert_eq!(iter.next_range(), Some(11..=13));
}

#[test]
fn next_range_advance_back_to_integration() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 5, 10, 11, 12, 13]);
    let mut iter = bm.iter();

    // Advance back to middle of a consecutive range
    iter.advance_back_to(12);

    // Should get range from start to current back position
    assert_eq!(iter.next_range_back(), Some(10..=12));

    // Continue with previous range
    assert_eq!(iter.next_back(), Some(5));
    assert_eq!(iter.next_range_back(), Some(1..=4));
}

// Test IntoIter variants
#[test]
fn into_iter_next_range_basic() {
    let bm = RoaringBitmap::from([1, 2, 4, 5]);
    let mut iter = bm.into_iter();

    assert_eq!(iter.next_range(), Some(1..=2));
    assert_eq!(iter.next(), Some(4));
    assert_eq!(iter.next_range(), Some(5..=5));
}

#[test]
fn into_iter_next_range_back_basic() {
    let bm = RoaringBitmap::from([1, 2, 4, 5]);
    let mut iter = bm.into_iter();

    assert_eq!(iter.next_range_back(), Some(4..=5));
    assert_eq!(iter.next_back(), Some(2));
    assert_eq!(iter.next_range_back(), Some(1..=1));
}

#[test]
fn next_range_exhausted_iterator() {
    let bm = RoaringBitmap::from([1, 2, 3]);
    let mut iter = bm.iter();

    // Consume all elements
    iter.next();
    iter.next();
    iter.next();

    // Iterator should be exhausted
    assert_eq!(iter.next_range(), None);
    assert_eq!(iter.next_range_back(), None);
}

#[test]
fn next_range_overlapping_calls() {
    let bm = RoaringBitmap::from([1, 2, 3, 10, 11]);
    let mut iter = bm.iter();

    // Get first range
    assert_eq!(iter.next_range(), Some(1..=3));

    // Iterator advanced past first range, get second range
    assert_eq!(iter.next_range(), Some(10..=11));

    // No more ranges
    assert_eq!(iter.next_range(), None);
}

#[test]
fn next_range_very_sparse() {
    // Very sparse bitmap
    let bm = RoaringBitmap::from([0, 1000, 2000, 3000]);
    let mut iter = bm.iter();

    // Each element should be its own range
    assert_eq!(iter.next_range(), Some(0..=0));
    assert_eq!(iter.next(), Some(1000));

    assert_eq!(iter.next_range(), Some(2000..=2000));
    assert_eq!(iter.next(), Some(3000));

    assert_eq!(iter.next_range(), None);
}

#[test]
fn next_range_dense_bitmap() {
    // Dense bitmap with large consecutive ranges
    let mut bm = RoaringBitmap::new();
    // Add ranges: 0-99, 200-299, 500-599
    for i in 0..100 {
        bm.insert(i);
    }
    for i in 200..300 {
        bm.insert(i);
    }
    for i in 500..600 {
        bm.insert(i);
    }

    let mut iter = bm.iter();

    assert_eq!(iter.next_range(), Some(0..=99));
    assert_eq!(iter.next(), Some(200));

    assert_eq!(iter.next_range(), Some(201..=299));
    assert_eq!(iter.next(), Some(500));

    assert_eq!(iter.next_range(), Some(501..=599));
    assert_eq!(iter.next(), None);
}

#[test]
fn next_range_multi_container_range() {
    // Single element bitmap
    let mut bm = RoaringBitmap::new();
    bm.insert_range(0..=0x4_0000);
    let mut iter = bm.iter();

    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next_range(), Some(2..=0x4_0000));

    assert_eq!(iter.next_range(), None);
    assert_eq!(iter.next(), None);
}

// Tests for bitmap store - these should trigger the todo!() implementations
#[test]
fn next_range_bitmap_store_forced() {
    // Create a sparse pattern that exceeds ARRAY_LIMIT but is inefficient as runs
    let mut bm = RoaringBitmap::new();

    // Add alternating ranges to create many gaps - inefficient as runs
    for i in (0..20000).step_by(4) {
        bm.insert(i); // bit at i
        bm.insert(i + 1); // bit at i+1
                          // gaps at i+2, i+3
    }

    // Force removal of run compression to ensure bitmap store
    bm.remove_run_compression();

    let mut iter = bm.iter();

    // First consecutive range should be 0..=1
    assert_eq!(iter.next_range(), Some(0..=1));

    // Iterator should now point at 4
    assert_eq!(iter.next(), Some(4));

    // Second consecutive range: 5..=5 (single element)
    assert_eq!(iter.next_range(), Some(5..=5));
}

#[test]
fn next_range_back_bitmap_store_forced() {
    // Create a sparse pattern that exceeds ARRAY_LIMIT but is inefficient as runs
    let mut bm = RoaringBitmap::new();

    // Add alternating ranges to create many gaps
    for i in (0..20000).step_by(4) {
        bm.insert(i);
        bm.insert(i + 1);
    }

    // Force removal of run compression
    bm.remove_run_compression();

    let mut iter = bm.iter();

    // Last consecutive range from back should be the last pair
    // The last elements should be 19996, 19997
    assert_eq!(iter.next_range_back(), Some(19996..=19997));
}

#[test]
fn next_range_bitmap_store_dense_with_gaps() {
    // Create a dense bitmap with strategic gaps to force bitmap store
    let mut bm = RoaringBitmap::new();

    // Add most elements but with regular gaps to make runs inefficient
    for i in 0..10000 {
        if i % 3 != 0 {
            // Skip every 3rd element
            bm.insert(i);
        }
    }

    // Force bitmap representation
    bm.remove_run_compression();

    let mut iter = bm.iter();

    // First consecutive range should be 1..=2
    assert_eq!(iter.next_range(), Some(1..=2));

    // Next element should be 4
    assert_eq!(iter.next(), Some(4));

    // Next range should be 5..=5
    assert_eq!(iter.next_range(), Some(5..=5));
}

#[test]
fn next_range_bitmap_store_partial_consumption() {
    // Create bitmap that forces bitmap store
    let mut bm = RoaringBitmap::new();

    // Add elements in groups of 2 with gaps
    for i in (1000..8000).step_by(3) {
        bm.insert(i);
        bm.insert(i + 1);
    }

    bm.remove_run_compression();

    let mut iter = bm.iter();

    // Consume first few elements
    assert_eq!(iter.next(), Some(1000));
    assert_eq!(iter.next(), Some(1001));

    // Should get next range starting at 1003
    assert_eq!(iter.next_range(), Some(1003..=1004));
}

#[test]
fn next_range_bitmap_store_mixed_operations() {
    let mut bm = RoaringBitmap::new();

    // Create pattern that forces bitmap store
    for i in (0..10000).step_by(3) {
        bm.insert(i);
        bm.insert(i + 1);
    }

    bm.remove_run_compression();

    // The pattern will be: 0,1 gap 3,4 gap 6,7 gap ... 9996,9997 gap 9999
    // Last iteration: i=9999, so we insert 9999 and 10000
    // But 10000 might be in a different container, so let's find the actual last element
    let last_element = bm.iter().next_back().unwrap();

    let mut iter = bm.iter();

    // Mix forward and backward operations
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next_back(), Some(last_element));

    // Get remaining range from front
    assert_eq!(iter.next_range(), Some(1..=1));

    // Continue to next range
    assert_eq!(iter.next(), Some(3));
    assert_eq!(iter.next_range(), Some(4..=4));
}

#[test]
fn next_range_bitmap_store_single_elements() {
    // Create very sparse bitmap that forces bitmap store
    let mut bm = RoaringBitmap::new();

    // Add individual elements spread far apart
    for i in (0..20000).step_by(5) {
        bm.insert(i);
    }

    bm.remove_run_compression();

    let mut iter = bm.iter();

    // Each element should be its own single-element range
    assert_eq!(iter.next_range(), Some(0..=0));
    assert_eq!(iter.next(), Some(5));
    assert_eq!(iter.next_range(), Some(10..=10));
    assert_eq!(iter.next(), Some(15));
    assert_eq!(iter.next_range(), Some(20..=20));
}

#[test]
fn next_range_bitmap_store_alternating_pattern() {
    // Create alternating pattern that's inefficient for run encoding
    let mut bm = RoaringBitmap::new();

    // Every other bit set in a large range
    for i in (0..10000).step_by(2) {
        bm.insert(i);
    }

    bm.remove_run_compression();

    let mut iter = bm.iter();

    // Each bit should be its own range due to alternating pattern
    assert_eq!(iter.next_range(), Some(0..=0));
    assert_eq!(iter.next(), Some(2));
    assert_eq!(iter.next_range(), Some(4..=4));
    assert_eq!(iter.next(), Some(6));
    assert_eq!(iter.next_range(), Some(8..=8));
}

#[test]
fn next_range_bitmap_store_with_small_clusters() {
    // Create small clusters of bits separated by gaps
    let mut bm = RoaringBitmap::new();

    // Add clusters of 3 bits separated by gaps of 5
    for base in (0..15000).step_by(8) {
        bm.insert(base);
        bm.insert(base + 1);
        bm.insert(base + 2);
        // gap of 5 (base+3, base+4, base+5, base+6, base+7)
    }

    bm.remove_run_compression();

    let mut iter = bm.iter();

    // First cluster: 0..=2
    assert_eq!(iter.next_range(), Some(0..=2));

    // Next cluster starts at 8
    assert_eq!(iter.next(), Some(8));
    assert_eq!(iter.next_range(), Some(9..=10));

    // Next cluster starts at 16
    assert_eq!(iter.next(), Some(16));
    assert_eq!(iter.next_range(), Some(17..=18));
}

#[test]
fn range_partial_consume() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x3FFF);
    let mut iter = bitmap.iter();
    iter.next();
    assert_eq!(iter.next_range_back(), Some(1..=0x3FFF));
}

#[test]
fn range_with_initial_next() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(69311..=180090);
    let mut iter = bitmap.iter();
    assert_eq!(iter.next(), Some(69311));
    assert_eq!(iter.next_range_back(), Some(69312..=180090));
}

#[test]
fn range_with_gap() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0x2_0000..=0x2_FFFF);
    bitmap.remove(0x2_1000);
    bitmap.remove_run_compression();
    let mut iter = bitmap.iter();
    assert_eq!(iter.next_range(), Some(0x2_0000..=0x2_0FFF));
    assert_eq!(iter.next(), Some(0x2_1001));
}

#[test]
fn range_back_after_next() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x3_FFFF);
    bitmap.remove(0x0_3000);
    let mut iter = bitmap.iter();
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next_range_back(), Some(0x0_3001..=0x3_FFFF));
}
