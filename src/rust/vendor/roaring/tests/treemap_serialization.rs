#![cfg(feature = "std")]

use roaring::RoaringTreemap;

fn serialize_deserialize<Dataset, I>(dataset: Dataset)
where
    Dataset: IntoIterator<Item = u64, IntoIter = I>,
    I: Iterator<Item = u64>,
{
    let rb = RoaringTreemap::from_iter(dataset);

    let mut buffer = vec![];
    rb.serialize_into(&mut buffer).unwrap();

    assert_eq!(buffer.len(), rb.serialized_size());

    let new_rb = RoaringTreemap::deserialize_from(&buffer[..]).unwrap();

    assert_eq!(rb, new_rb);
}

#[test]
fn empty() {
    serialize_deserialize(vec![])
}

#[test]
fn basic() {
    serialize_deserialize(vec![1, 2, 3, 4, 5, 100, 1000])
}

#[test]
fn basic_2() {
    serialize_deserialize(vec![1, 2, 3, 4, 5, 100, 1000, 10000, 100000, 1000000])
}

#[test]
fn basic_3() {
    let u32max = u32::MAX as u64;
    serialize_deserialize(
        vec![1, 2, 3, 4, 5, 100, 1000, 10000, 100000, 1000000, u32max + 10, u32max << 10]
            .into_iter()
            .chain(u32max..(u32max + 2 * (1 << 16))),
    )
}
