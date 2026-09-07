extern crate roaring;
use roaring::RoaringTreemap;

#[test]
fn or() {
    let mut rb1 = (1..4).collect::<RoaringTreemap>();
    let rb2 = (3..6).collect::<RoaringTreemap>();
    let rb3 = (1..6).collect::<RoaringTreemap>();

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
    let mut rb1 = (1..4).collect::<RoaringTreemap>();
    let rb2 = (3..6).collect::<RoaringTreemap>();
    let rb3 = (3..4).collect::<RoaringTreemap>();

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
    let mut rb1 = (1..4).collect::<RoaringTreemap>();
    let rb2 = (3..6).collect::<RoaringTreemap>();
    let rb3 = (1..3).collect::<RoaringTreemap>();

    assert_eq!(rb3, &rb1 - &rb2);
    assert_eq!(rb3, &rb1 - rb2.clone());
    assert_eq!(rb3, rb1.clone() - &rb2);
    assert_eq!(rb3, rb1.clone() - rb2.clone());
    assert_eq!(rb3.len(), rb1.difference_len(&rb2));

    rb1 -= &rb2;
    rb1 -= rb2;

    assert_eq!(rb3, rb1);
}

#[test]
fn xor() {
    let mut rb1 = (1..4).collect::<RoaringTreemap>();
    let rb2 = (3..6).collect::<RoaringTreemap>();
    let rb3 = ((1..3).chain(4..6)).collect::<RoaringTreemap>();
    let rb4 = (0..0).collect::<RoaringTreemap>();

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
