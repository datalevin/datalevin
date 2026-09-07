extern crate roaring;
use roaring::RoaringTreemap;

#[test]
fn array_not() {
    let sup = (0..2000).collect::<RoaringTreemap>();
    let sub = (1000..3000).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn array() {
    let sup = (0..4000).collect::<RoaringTreemap>();
    let sub = (2000..3000).collect::<RoaringTreemap>();
    assert!(sub.is_subset(&sup));
}

#[test]
fn array_bitmap_not() {
    let sup = (0..2000).collect::<RoaringTreemap>();
    let sub = (1000..15000).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn bitmap_not() {
    let sup = (0..6000).collect::<RoaringTreemap>();
    let sub = (4000..10000).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn bitmap() {
    let sup = (0..20000).collect::<RoaringTreemap>();
    let sub = (5000..15000).collect::<RoaringTreemap>();
    assert!(sub.is_subset(&sup));
}

#[test]
fn bitmap_array_not() {
    let sup = (0..20000).collect::<RoaringTreemap>();
    let sub = (19000..21000).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn bitmap_array() {
    let sup = (0..20000).collect::<RoaringTreemap>();
    let sub = (18000..20000).collect::<RoaringTreemap>();
    assert!(sub.is_subset(&sup));
}

#[test]
fn arrays_not() {
    let sup = ((0..2000).chain(1_000_000..1_002_000)).collect::<RoaringTreemap>();
    let sub = ((100_000..102_000).chain(1_100_000..1_102_000)).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn arrays() {
    let sup = ((0..3000).chain(100_000..103_000)).collect::<RoaringTreemap>();
    let sub = ((0..2000).chain(100_000..102_000)).collect::<RoaringTreemap>();
    assert!(sub.is_subset(&sup));
}

#[test]
fn bitmaps_not() {
    let sup = ((0..6000).chain(1_000_000..1_006_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let sub = ((100_000..106_000).chain(1_100_000..1_106_000)).collect::<RoaringTreemap>();
    assert!(!sub.is_subset(&sup));
}

#[test]
fn bitmaps() {
    let sup = ((0..1_000_000).chain(2_000_000..2_010_000)).collect::<RoaringTreemap>();
    let sub = ((0..10_000).chain(500_000..510_000)).collect::<RoaringTreemap>();
    assert!(sub.is_subset(&sup));
}
