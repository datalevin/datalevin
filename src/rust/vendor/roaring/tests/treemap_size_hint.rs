extern crate roaring;
use roaring::RoaringTreemap;

#[test]
fn array() {
    let bitmap = (0..2000).collect::<RoaringTreemap>();
    let mut iter = bitmap.iter();
    assert_eq!((2000, Some(2000)), iter.size_hint());
    iter.by_ref().take(1000).for_each(drop);
    assert_eq!((1000, Some(1000)), iter.size_hint());
    iter.by_ref().for_each(drop);
    assert_eq!((0, Some(0)), iter.size_hint());
}

#[test]
fn bitmap() {
    let bitmap = (0..6000).collect::<RoaringTreemap>();
    let mut iter = bitmap.iter();
    assert_eq!((6000, Some(6000)), iter.size_hint());
    iter.by_ref().take(3000).for_each(drop);
    assert_eq!((3000, Some(3000)), iter.size_hint());
    iter.by_ref().for_each(drop);
    assert_eq!((0, Some(0)), iter.size_hint());
}

#[test]
fn arrays() {
    let bitmap = ((0..2000).chain(1_000_000..1_002_000).chain(2_000_000..2_001_000))
        .collect::<RoaringTreemap>();
    let mut iter = bitmap.iter();
    assert_eq!((5000, Some(5000)), iter.size_hint());
    iter.by_ref().take(3000).for_each(drop);
    assert_eq!((2000, Some(2000)), iter.size_hint());
    iter.by_ref().for_each(drop);
    assert_eq!((0, Some(0)), iter.size_hint());
}

#[test]
fn bitmaps() {
    let bitmap = ((0..6000).chain(1_000_000..1_012_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let mut iter = bitmap.iter();
    assert_eq!((28000, Some(28000)), iter.size_hint());
    iter.by_ref().take(2000).for_each(drop);
    assert_eq!((26000, Some(26000)), iter.size_hint());
    iter.by_ref().take(5000).for_each(drop);
    assert_eq!((21000, Some(21000)), iter.size_hint());
    iter.by_ref().take(20000).for_each(drop);
    assert_eq!((1000, Some(1000)), iter.size_hint());
    iter.by_ref().for_each(drop);
    assert_eq!((0, Some(0)), iter.size_hint());
}
