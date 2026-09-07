extern crate roaring;
use roaring::RoaringTreemap;

#[test]
fn array() {
    let mut bitmap1 = (0..2000).collect::<RoaringTreemap>();
    let bitmap2 = (1000..3000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..1000).chain(2000..3000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn no_symmetric_difference() {
    let mut bitmap1 = (0..2).collect::<RoaringTreemap>();
    let bitmap2 = (0..2).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, RoaringTreemap::new());
}

#[test]
fn array_and_bitmap() {
    let mut bitmap1 = (0..2000).collect::<RoaringTreemap>();
    let bitmap2 = (1000..8000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..1000).chain(2000..8000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap_to_bitmap() {
    let mut bitmap1 = (0..12000).collect::<RoaringTreemap>();
    let bitmap2 = (6000..18000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..6000).chain(12000..18000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap_to_array() {
    let mut bitmap1 = (0..6000).collect::<RoaringTreemap>();
    let bitmap2 = (2000..7000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..2000).chain(6000..7000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap_and_array_to_bitmap() {
    let mut bitmap1 = (0..12000).collect::<RoaringTreemap>();
    let bitmap2 = (11000..14000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..11000).chain(12000..14000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap_and_array_to_array() {
    let mut bitmap1 = (0..6000).collect::<RoaringTreemap>();
    let bitmap2 = (3000..7000).collect::<RoaringTreemap>();
    let bitmap3 = ((0..3000).chain(6000..7000)).collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn arrays() {
    let mut bitmap1 = ((0..2000).chain(1_000_000..1_002_000).chain(3_000_000..3_001_000))
        .collect::<RoaringTreemap>();
    let bitmap2 = ((1000..3000).chain(1_001_000..1_003_000).chain(2_000_000..2_000_001))
        .collect::<RoaringTreemap>();
    let bitmap3 = ((0..1000)
        .chain(1_000_000..1_001_000)
        .chain(2000..3000)
        .chain(1_002_000..1_003_000)
        .chain(2_000_000..2_000_001)
        .chain(3_000_000..3_001_000))
    .collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmaps() {
    let mut bitmap1 = ((0..6000).chain(1_000_000..1_012_000).chain(3_000_000..3_010_000))
        .collect::<RoaringTreemap>();
    let bitmap2 = ((3000..7000).chain(1_006_000..1_018_000).chain(2_000_000..2_010_000))
        .collect::<RoaringTreemap>();
    let bitmap3 = ((0..3000)
        .chain(1_000_000..1_006_000)
        .chain(6000..7000)
        .chain(1_012_000..1_018_000)
        .chain(2_000_000..2_010_000)
        .chain(3_000_000..3_010_000))
    .collect::<RoaringTreemap>();

    bitmap1 ^= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}
