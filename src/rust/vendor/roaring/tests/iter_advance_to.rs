use roaring::RoaringBitmap;

#[test]
fn iter_basic() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.iter();
    i.advance_to(10);
    for n in 11..=14 {
        assert_eq!(i.next(), Some(n))
    }
    assert_eq!(i.next(), None);
}

#[test]
fn to_missing_container() {
    let bm = RoaringBitmap::from([1, 0x2_0001, 0x2_0002]);
    let mut i = bm.iter();
    i.advance_to(0x1_0000);
    assert_eq!(i.next(), Some(0x2_0001));
    assert_eq!(i.next(), Some(0x2_0002));
    assert_eq!(i.next(), None);
}

#[test]
fn iter_back_basic() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 11, 12, 13, 14]);
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
fn iter_advance_past_end() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 11, 12, 13, 14]);
    let mut i = bm.iter();
    i.advance_to(15);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None);
}

#[test]
fn iter_multi_container() {
    let bm = RoaringBitmap::from([1, 2, 3, 100000, 100001]);
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
fn iter_empty() {
    let bm = RoaringBitmap::new();
    let mut i = bm.iter();
    i.advance_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

#[test]
fn iter_back_empty() {
    let bm = RoaringBitmap::new();
    let mut i = bm.iter();
    i.advance_back_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

#[test]
fn into_iter_basic() {
    let bm = RoaringBitmap::from([1, 2, 3, 4, 11, 12, 13, 14]);
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
}

#[test]
fn into_iter_multi_container() {
    let bm = RoaringBitmap::from([1, 2, 3, 100000, 100001]);
    let mut i = bm.into_iter();
    i.advance_to(3);
    assert_eq!(i.size_hint(), (3, Some(3)));
    assert_eq!(i.next(), Some(3));
    assert_eq!(i.next(), Some(100000));
    assert_eq!(i.next(), Some(100001));
    assert_eq!(i.next(), None);
}

#[test]
fn into_iter_empty() {
    let bm = RoaringBitmap::new();
    let mut i = bm.into_iter();
    i.advance_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

#[test]
fn into_iter_back_empty() {
    let bm = RoaringBitmap::new();
    let mut i = bm.into_iter();
    i.advance_back_to(31337);
    assert_eq!(i.size_hint(), (0, Some(0)));
    assert_eq!(i.next(), None)
}

#[test]
fn advance_to_with_tail_iter() {
    let bm = RoaringBitmap::from([1, 2, 3, 100000, 100001]);
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
    let bitmap = RoaringBitmap::from([u32::MAX]);
    let mut iter = bitmap.iter();
    iter.advance_to(u32::MAX);
    assert_eq!(Some(u32::MAX), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn advance_bitset() {
    let mut bitmap = RoaringBitmap::new();
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
    let mut bitmap = RoaringBitmap::new();
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
    let mut bitmap = RoaringBitmap::new();
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
    let mut bitmap = RoaringBitmap::new();
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

#[test]
fn advance_run_past_the_end() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x35B00);
    let mut iter = bitmap.iter();
    iter.advance_to(0x35B01);
    assert_eq!(iter.next(), None);
}

#[test]
fn advance_run_back_before_start() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(500..=0x35B00);
    let mut iter = bitmap.iter();
    iter.advance_back_to(499);
    assert_eq!(iter.next_back(), None);
}

#[test]
fn advance_run_back_reduces_forward_iter() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x4000);
    let mut iter = bitmap.iter();
    iter.advance_back_to(1);

    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), None);
}

#[test]
fn advance_run_front_and_back_past_each_other() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x4000);
    let mut iter = bitmap.iter();
    iter.advance_back_to(100);
    iter.advance_to(300);
    assert_eq!(iter.next(), None);
}

#[test]
fn advance_run_both_sides_past_each_other() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..0x1000);
    let mut iter = bitmap.iter();
    iter.advance_back_to(100);
    iter.advance_to(0xFFFF);
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.nth_back(0), None);
}

#[test]
fn advance_run_with_nth() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(36141..=224407);
    let mut iter = bitmap.iter();
    iter.advance_back_to(101779);
    assert_eq!(iter.nth(100563), None);
}

#[test]
fn advance_to_with_next_len() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(100..0x4000);
    let mut iter = bitmap.iter();
    iter.advance_back_to(100);
    assert_eq!(iter.next(), Some(100));
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.nth_back(0), None);
}

#[test]
fn tmp() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(196363..=262143);
    let mut iter = bitmap.iter();
    assert_eq!(iter.next_back(), Some(262143));
    iter.advance_to(228960);
    assert_eq!(iter.nth(36643), None);
}

#[test]
fn advance_bitset_front_and_back_past_each_other() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0x4000);
    bitmap.remove_run_compression();
    let mut iter = bitmap.iter();
    iter.advance_back_to(100);
    iter.advance_to(300);
    assert_eq!(iter.next(), None);
}

#[test]
fn combine_with_nth() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(0..=0xFFFF);
    bitmap.remove_run_compression();
    let mut iter = bitmap.iter();

    // Use nth to skip to a specific position
    assert_eq!(iter.nth(100), Some(100));
    iter.advance_back_to(50);
    assert_eq!(iter.next_back(), None);
}
