extern crate roaring;
use roaring::RoaringBitmap;

#[test]
fn array_to_array() {
    let mut bitmap1 = (0..2000).collect::<RoaringBitmap>();
    let bitmap2 = (1000..3000).collect::<RoaringBitmap>();
    let bitmap3 = (0..3000).collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn array_to_bitmap() {
    let mut bitmap1 = (0..4000).collect::<RoaringBitmap>();
    let bitmap2 = (4000..8000).collect::<RoaringBitmap>();
    let bitmap3 = (0..8000).collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn array_and_bitmap() {
    let mut bitmap1 = (0..2000).collect::<RoaringBitmap>();
    let bitmap2 = (1000..8000).collect::<RoaringBitmap>();
    let bitmap3 = (0..8000).collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap() {
    let mut bitmap1 = (0..12000).collect::<RoaringBitmap>();
    let bitmap2 = (6000..18000).collect::<RoaringBitmap>();
    let bitmap3 = (0..18000).collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmap_and_array() {
    let mut bitmap1 = (0..12000).collect::<RoaringBitmap>();
    let bitmap2 = (10000..13000).collect::<RoaringBitmap>();
    let bitmap3 = (0..13000).collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn arrays() {
    let mut bitmap1 = (0..2000)
        .chain(1_000_000..1_002_000)
        .chain(3_000_000..3_001_000)
        .collect::<RoaringBitmap>();
    let bitmap2 = (1000..3000)
        .chain(1_001_000..1_003_000)
        .chain(2_000_000..2_001_000)
        .collect::<RoaringBitmap>();
    let bitmap3 = (0..3000)
        .chain(1_000_000..1_003_000)
        .chain(2_000_000..2_001_000)
        .chain(3_000_000..3_001_000)
        .collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}

#[test]
fn bitmaps() {
    let mut bitmap1 = (0..6000)
        .chain(1_000_000..1_012_000)
        .chain(3_000_000..3_010_000)
        .collect::<RoaringBitmap>();
    let bitmap2 = (3000..9000)
        .chain(1_006_000..1_018_000)
        .chain(2_000_000..2_010_000)
        .collect::<RoaringBitmap>();
    let bitmap3 = (0..9000)
        .chain(1_000_000..1_018_000)
        .chain(2_000_000..2_010_000)
        .chain(3_000_000..3_010_000)
        .collect::<RoaringBitmap>();

    bitmap1 |= bitmap2;

    assert_eq!(bitmap1, bitmap3);
}
