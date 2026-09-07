extern crate roaring;
use roaring::RoaringTreemap;

#[test]
fn iter_basic() {
    let bm = RoaringTreemap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.iter();
    i.advance_to(10);
    for n in 11..=14 {
        assert_eq!(i.next(), Some(n))
    }
    assert_eq!(i.next(), None);
}

#[test]
fn to_missing_container() {
    let bm = RoaringTreemap::from([1, 0x2_0001, 0x2_0002]);
    let mut i = bm.iter();
    i.advance_to(0x1_0000);
    assert_eq!(i.next(), Some(0x2_0001));
    assert_eq!(i.next(), Some(0x2_0002));
    assert_eq!(i.next(), None);
}

#[test]
fn to_next_bitmap() {
    let bm =
        RoaringTreemap::from([1u64, 0x2_0001u64 + u32::MAX as u64, 0x2_0002u64 + u32::MAX as u64]);
    let mut i = bm.iter();
    i.advance_to(0x1_0000);
    assert_eq!(i.next(), Some(0x2_0001u64 + u32::MAX as u64));
    assert_eq!(i.next(), Some(0x2_0002u64 + u32::MAX as u64));
    assert_eq!(i.next(), None);
}

#[test]
fn advance_to_later_bitmap_after_iteration_started() {
    let later_bitmap = 1u64 << 32;
    let bm = RoaringTreemap::from([1, 3, later_bitmap]);
    let mut i = bm.iter();

    assert_eq!(i.next(), Some(1));
    i.advance_to(later_bitmap);

    assert_eq!(i.next(), Some(later_bitmap));
    assert_eq!(i.next(), None);
}

#[test]
fn advance_to_missing_bitmap() {
    let later_bitmap = 1u64 << 32;
    let bm = RoaringTreemap::from([later_bitmap]);
    let mut i = bm.iter();

    i.advance_to(3);

    assert_eq!(i.next(), Some(later_bitmap));
    assert_eq!(i.next(), None);
}

#[test]
fn iter_back_basic() {
    let bm = RoaringTreemap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.iter();
    i.advance_back_to(10);
    assert_eq!(i.next(), Some(1));
    assert_eq!(i.next(), Some(2));
    assert_eq!(i.next_back(), Some(4));
    assert_eq!(i.next_back(), Some(3));

    assert_eq!(i.next(), None);
    assert_eq!(i.next_back(), None);
}

#[test]
fn advance_back_to_earlier_bitmap_after_iteration_started() {
    let later_bitmap = 1u64 << 32;
    let bm = RoaringTreemap::from([1, later_bitmap, later_bitmap + 2]);
    let mut i = bm.iter();

    assert_eq!(i.next_back(), Some(later_bitmap + 2));
    i.advance_back_to(1);

    assert_eq!(i.next_back(), Some(1));
    assert_eq!(i.next_back(), None);
}

#[test]
fn advance_back_to_missing_bitmap() {
    let later_bitmap = 1u64 << 32;
    let bm = RoaringTreemap::from([3]);
    let mut i = bm.iter();

    i.advance_back_to(later_bitmap);

    assert_eq!(i.next_back(), Some(3));
    assert_eq!(i.next_back(), None);
}

#[test]
fn iter_advance_past_end() {
    let bm = RoaringTreemap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.iter();
    i.advance_to(15);
    assert_eq!(i.next(), None);
    assert_eq!(i.size_hint(), (0, Some(0)));
}

#[test]
fn iter_multi_container() {
    let bm = RoaringTreemap::from([1, 2, 3, 100000, 100001]);
    let mut i = bm.iter();
    i.advance_to(3);
    assert_eq!(i.size_hint(), (3, Some(3)));
    assert_eq!(i.next(), Some(3));
    assert_eq!(i.size_hint(), (2, Some(2)));
    assert_eq!(i.next(), Some(100000));
    assert_eq!(i.size_hint(), (1, Some(1)));
    assert_eq!(i.next(), Some(100001));
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None);
    assert_eq!(i.size_hint(), (0, Some(0)));
}

#[test]
fn iter_multi_container_multi_bitmap() {
    let bm = RoaringTreemap::from([
        1,
        2,
        3,
        100000,
        100001,
        1u64 + u32::MAX as u64,
        2u64 + u32::MAX as u64,
        3u64 + u32::MAX as u64,
        100000u64 + u32::MAX as u64,
        100001u64 + u32::MAX as u64,
    ]);
    let mut i = bm.iter();
    i.advance_to(3);
    assert_eq!(i.size_hint(), (8, Some(8)));
    assert_eq!(i.next(), Some(3));
    assert_eq!(i.size_hint(), (7, Some(7)));
    assert_eq!(i.next(), Some(100000));
    assert_eq!(i.size_hint(), (6, Some(6)));
    assert_eq!(i.next(), Some(100001));
    assert_eq!(i.size_hint(), (5, Some(5)));
    assert_eq!(i.next(), Some(1u64 + u32::MAX as u64));
    assert_eq!(i.size_hint(), (4, Some(4)));
    assert_eq!(i.next(), Some(2u64 + u32::MAX as u64));
    assert_eq!(i.size_hint(), (3, Some(3)));
    assert_eq!(i.next(), Some(3u64 + u32::MAX as u64));
    assert_eq!(i.size_hint(), (2, Some(2)));
    assert_eq!(i.next(), Some(100000u64 + u32::MAX as u64));
    assert_eq!(i.size_hint(), (1, Some(1)));
    assert_eq!(i.next(), Some(100001u64 + u32::MAX as u64));
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None);
    assert_eq!(i.size_hint(), (0, Some(0)));
}

#[test]
fn iter_empty() {
    let bm = RoaringTreemap::new();
    let mut i = bm.iter();
    i.advance_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

#[test]
fn iter_back_empty() {
    let bm = RoaringTreemap::new();
    let mut i = bm.iter();
    i.advance_back_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

/*#[test]
fn into_iter_basic() {
    let bm = RoaringTreemap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.into_iter();
    i.advance_to(10);
    let mut expected_size_hint = 4;
    assert_eq!(i.size_hint(), (expected_size_hint, Some(expected_size_hint)));
    for n in 11..=14 {
        assert_eq!(i.next(), Some(n));
        expected_size_hint -= 1;
        assert_eq!(i.size_hint(), (expected_size_hint, Some(expected_size_hint)));
    }
    assert_eq!(i.next(), None);
}*/

/*#[test]
fn into_iter_multi_container() {
    let bm = RoaringTreemap::from([1, 2, 3, 100000, 100001]);
    let mut i = bm.into_iter();
    i.advance_to(3);
    assert_eq!(i.size_hint(), (3, Some(3)));
    assert_eq!(i.next(), Some(3));
    assert_eq!(i.next(), Some(100000));
    assert_eq!(i.next(), Some(100001));
    assert_eq!(i.next(), None);
}*/

/*#[test]
fn into_iter_empty() {
    let bm = RoaringTreemap::new();
    let mut i = bm.into_iter();
    i.advance_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}*/

/*#[test]
fn into_iter_back_empty() {
    let bm = RoaringTreemap::new();
    let mut i = bm.into_iter();
    i.advance_back_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}*/

#[test]
fn advance_to_with_tail_iter() {
    let bm = RoaringTreemap::from([1, 2, 3, 100000, 100001]);
    let mut i = bm.iter();
    i.next_back();
    i.advance_to(100000);
    assert_eq!(i.size_hint(), (1, Some(1)));
    assert_eq!(i.next(), Some(100000));
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None);
}

#[test]
fn advance_to_end() {
    let bitmap = RoaringTreemap::from([u64::MAX]);
    let mut iter = bitmap.iter();
    iter.advance_to(u64::MAX);
    assert_eq!(Some(u64::MAX), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn advance_bitset() {
    let mut bitmap = RoaringTreemap::new();
    for i in (0..=0x2_0000).step_by(2) {
        bitmap.insert(i);
    }
    let mut iter = bitmap.iter();
    iter.advance_to(0x1_0000 - 4);
    // 0x1_0000 + 5 is not in the bitmap, so the next value will be the first value less than that
    iter.advance_back_to(0x1_0000 + 5);
    assert_eq!(iter.next(), Some(0x1_0000 - 4));
    assert_eq!(iter.next_back(), Some(0x1_0000 + 4));

    assert_eq!(iter.next(), Some(0x1_0000 - 2));
    assert_eq!(iter.next(), Some(0x1_0000));
    assert_eq!(iter.next(), Some(0x1_0000 + 2));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
}

#[test]
fn advance_bitset_current_word() {
    let mut bitmap = RoaringTreemap::new();
    for i in (0..=0x2_0000).step_by(2) {
        bitmap.insert(i);
    }
    let mut iter = bitmap.iter();
    iter.advance_to(4);
    iter.advance_back_to(0x2_0000 - 4);
    for i in (4..=(0x2_0000 - 4)).step_by(2) {
        assert_eq!(iter.next(), Some(i));
    }
    assert_eq!(iter.next(), None);
}

#[test]
fn advance_bitset_to_end_word() {
    let mut bitmap = RoaringTreemap::new();
    for i in (0..=0x2_0000).step_by(2) {
        bitmap.insert(i);
    }
    let mut iter = bitmap.iter();
    iter.advance_to(0x1_0000 - 4);
    for i in ((0x1_0000 - 4)..=0x2_0000).step_by(2) {
        assert_eq!(iter.next(), Some(i));
    }
    assert_eq!(iter.next(), None);
}

#[test]
fn advance_bitset_back_to_start_word() {
    let mut bitmap = RoaringTreemap::new();
    for i in (0..=0x2_0000).step_by(2) {
        bitmap.insert(i);
    }
    let mut iter = bitmap.iter();
    iter.advance_back_to(0x1_0000 - 4);
    for i in (0..=(0x1_0000 - 4)).step_by(2) {
        assert_eq!(iter.next(), Some(i));
    }
    assert_eq!(iter.next(), None);
}
