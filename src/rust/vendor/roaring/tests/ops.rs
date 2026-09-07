extern crate roaring;
use roaring::RoaringBitmap;

#[test]
fn or() {
    let mut rb1 = (1..4).collect::<RoaringBitmap>();
    let rb2 = (3..6).collect::<RoaringBitmap>();
    let rb3 = (1..6).collect::<RoaringBitmap>();

    assert_eq!(rb3, &rb1 | &rb2);
    assert_eq!(rb3, &rb1 | rb2.clone());
    assert_eq!(rb3, rb1.clone() | &rb2);
    assert_eq!(rb3, rb1.clone() | rb2.clone());
    assert_eq!(rb3.len(), rb1.union_len(&rb2));

    rb1 |= &rb2;
    rb1 |= rb2;

    assert_eq!(rb3, rb1);
}

#[test]
fn and() {
    let mut rb1 = (1..4).collect::<RoaringBitmap>();
    let rb2 = (3..6).collect::<RoaringBitmap>();
    let rb3 = (3..4).collect::<RoaringBitmap>();

    assert_eq!(rb3, &rb1 & &rb2);
    assert_eq!(rb3, &rb1 & rb2.clone());
    assert_eq!(rb3, rb1.clone() & &rb2);
    assert_eq!(rb3, rb1.clone() & rb2.clone());
    assert_eq!(rb3.len(), rb1.intersection_len(&rb2));

    rb1 &= &rb2;
    rb1 &= rb2;

    assert_eq!(rb3, rb1);
}

#[test]
fn sub() {
    let mut rb1 = (1..4000).collect::<RoaringBitmap>();
    let rb2 = (3..5000).collect::<RoaringBitmap>();
    let rb3 = (1..3).collect::<RoaringBitmap>();

    assert_eq!(rb3, &rb1 - &rb2);
    assert_eq!(rb3, &rb1 - rb2.clone());
    assert_eq!(rb3, rb1.clone() - &rb2);
    assert_eq!(rb3, rb1.clone() - rb2.clone());
    assert_eq!(rb3.len(), rb1.difference_len(&rb2));

    rb1 -= &rb2;
    rb1 -= rb2;

    assert_eq!(rb3, rb1);
}

// See issue #327
#[test]
fn subtraction_preserves_zero_element() {
    let mut a = RoaringBitmap::from([0, 35, 80, 104, 138, 214, 235, 258]);
    let b = RoaringBitmap::from([9, 35, 42, 51, 111, 134, 231, 239]);

    a -= b;

    // The bug: element 0 should still be present but was being removed
    assert!(a.contains(0), "Element 0 should be present after subtraction");

    // Verify the complete result
    let expected: Vec<u32> = vec![0, 80, 104, 138, 214, 235, 258];
    let actual: Vec<u32> = a.iter().collect();
    assert_eq!(actual, expected, "Subtraction result should match expected values");
}

#[test]
fn xor() {
    let mut rb1 = (1..4).collect::<RoaringBitmap>();
    let rb2 = (3..6).collect::<RoaringBitmap>();
    let rb3 = (1..3).chain(4..6).collect::<RoaringBitmap>();
    let rb4 = (0..0).collect::<RoaringBitmap>();

    assert_eq!(rb3, &rb1 ^ &rb2);
    assert_eq!(rb3, &rb1 ^ rb2.clone());
    assert_eq!(rb3, rb1.clone() ^ &rb2);
    assert_eq!(rb3, rb1.clone() ^ rb2.clone());
    assert_eq!(rb3.len(), rb1.symmetric_difference_len(&rb2));

    rb1 ^= &rb2;

    assert_eq!(rb3, rb1);

    rb1 ^= rb3;

    assert_eq!(rb4, rb1);
}
