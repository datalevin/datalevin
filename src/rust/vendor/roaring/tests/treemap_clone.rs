extern crate roaring;
use roaring::RoaringTreemap;

#[test]
#[allow(clippy::redundant_clone)]
fn array() {
    let original = (0..2000).collect::<RoaringTreemap>();
    let clone = original.clone();

    assert_eq!(clone, original);
}

#[test]
#[allow(clippy::redundant_clone)]
fn bitmap() {
    let original = (0..6000).collect::<RoaringTreemap>();
    let clone = original.clone();

    assert_eq!(clone, original);
}

#[test]
#[allow(clippy::redundant_clone)]
fn arrays() {
    let original = ((0..2000).chain(1_000_000..1_002_000).chain(2_000_000..2_001_000))
        .collect::<RoaringTreemap>();
    let clone = original.clone();

    assert_eq!(clone, original);
}

#[test]
#[allow(clippy::redundant_clone)]
fn bitmaps() {
    let original = ((0..6000).chain(1_000_000..1_012_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let clone = original.clone();

    assert_eq!(clone, original);
}
