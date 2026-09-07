use crate::bitmap::container::{Container, ARRAY_LIMIT};
use crate::bitmap::store::{
    ArrayStore, BitmapStore, Interval, Store, BITMAP_BYTES, BITMAP_LENGTH, RUN_ELEMENT_BYTES,
    RUN_NUM_BYTES,
};
use crate::RoaringBitmap;
use bytemuck::cast_slice_mut;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use core::convert::Infallible;
use std::error::Error;
use std::io;

use super::store::IntervalStore;

pub(crate) const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
pub(crate) const SERIAL_COOKIE: u16 = 12347;
pub(crate) const NO_OFFSET_THRESHOLD: usize = 4;

// Sizes of header structures
pub(crate) const COOKIE_BYTES: usize = 4;
pub(crate) const SIZE_BYTES: usize = 4;
pub(crate) const DESCRIPTION_BYTES: usize = 4;
pub(crate) const OFFSET_BYTES: usize = 4;

impl RoaringBitmap {
    /// Return the size in bytes of the serialized output.
    /// This is compatible with the official C/C++, Java and Go implementations.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let rb1: RoaringBitmap = (1..4).collect();
    /// let mut bytes = Vec::with_capacity(rb1.serialized_size());
    /// rb1.serialize_into(&mut bytes).unwrap();
    /// let rb2 = RoaringBitmap::deserialize_from(&bytes[..]).unwrap();
    ///
    /// assert_eq!(rb1, rb2);
    /// ```
    pub fn serialized_size(&self) -> usize {
        let mut has_run_containers = false;
        let size = self.containers.len();
        let container_sizes: usize = self
            .containers
            .iter()
            .map(|container| match container.store {
                Store::Array(ref values) => values.byte_size(),
                Store::Bitmap(..) => BITMAP_BYTES,
                Store::Run(ref intervals) => {
                    has_run_containers = true;
                    intervals.byte_size()
                }
            })
            .sum();

        // header + container sizes
        header_size(size, has_run_containers) + container_sizes
    }

    /// Serialize this bitmap into [the standard Roaring on-disk format][format].
    /// This is compatible with the official C/C++, Java and Go implementations.
    ///
    /// [format]: https://github.com/RoaringBitmap/RoaringFormatSpec
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let rb1: RoaringBitmap = (1..4).collect();
    /// let mut bytes = vec![];
    /// rb1.serialize_into(&mut bytes).unwrap();
    /// let rb2 = RoaringBitmap::deserialize_from(&bytes[..]).unwrap();
    ///
    /// assert_eq!(rb1, rb2);
    /// ```
    pub fn serialize_into<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        let has_run_containers = self.containers.iter().any(|c| matches!(c.store, Store::Run(_)));
        let size = self.containers.len();

        // Depending on if run containers are present or not write the appropriate header
        if has_run_containers {
            // The new format stores the container count in the most significant bits of the header
            let cookie = SERIAL_COOKIE as u32 | ((size as u32 - 1) << 16);
            writer.write_u32::<LittleEndian>(cookie)?;
            // It is then followed by a bitset indicating which containers are run containers
            let run_container_bitmap_size = size.div_ceil(8);
            let mut run_container_bitmap = vec![0; run_container_bitmap_size];
            for (i, container) in self.containers.iter().enumerate() {
                if let Store::Run(_) = container.store {
                    run_container_bitmap[i / 8] |= 1 << (i % 8);
                }
            }
            writer.write_all(&run_container_bitmap)?;
        } else {
            // Write old format, cookie followed by container count
            writer.write_u32::<LittleEndian>(SERIAL_COOKIE_NO_RUNCONTAINER)?;
            writer.write_u32::<LittleEndian>(size as u32)?;
        }

        // Write the container descriptions
        for container in &self.containers {
            writer.write_u16::<LittleEndian>(container.key)?;
            writer.write_u16::<LittleEndian>((container.len() - 1) as u16)?;
        }

        let mut offset = header_size(size, has_run_containers) as u32;
        let has_offsets = if has_run_containers { size >= OFFSET_BYTES } else { true };
        if has_offsets {
            for container in &self.containers {
                writer.write_u32::<LittleEndian>(offset)?;
                match container.store {
                    Store::Array(ref values) => {
                        offset += values.len() as u32 * 2;
                    }
                    Store::Bitmap(..) => {
                        offset += 8 * 1024;
                    }
                    Store::Run(ref intervals) => {
                        offset += (RUN_NUM_BYTES
                            + (intervals.run_amount() as usize * RUN_ELEMENT_BYTES))
                            as u32;
                    }
                }
            }
        }

        for container in &self.containers {
            match container.store {
                Store::Array(ref values) => {
                    #[cfg(target_endian = "little")]
                    writer.write_all(bytemuck::cast_slice(values.as_slice()))?;
                    #[cfg(target_endian = "big")]
                    for &value in values.iter() {
                        writer.write_u16::<LittleEndian>(value)?;
                    }
                }
                Store::Bitmap(ref bits) => {
                    #[cfg(target_endian = "little")]
                    writer.write_all(bytemuck::cast_slice(bits.as_array()))?;
                    #[cfg(target_endian = "big")]
                    for &value in bits.as_array() {
                        writer.write_u64::<LittleEndian>(value)?;
                    }
                }
                Store::Run(ref intervals) => {
                    writer.write_u16::<LittleEndian>(intervals.run_amount() as u16)?;
                    for iv in intervals.iter_intervals() {
                        writer.write_u16::<LittleEndian>(iv.start())?;
                        writer.write_u16::<LittleEndian>(iv.end() - iv.start())?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Deserialize a bitmap into memory from [the standard Roaring on-disk
    /// format][format]. This is compatible with the official C/C++, Java and
    /// Go implementations. This method checks that all of the internal values
    /// are valid. If deserializing from a trusted source consider
    /// [RoaringBitmap::deserialize_unchecked_from]
    ///
    /// [format]: https://github.com/RoaringBitmap/RoaringFormatSpec
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let rb1: RoaringBitmap = (1..4).collect();
    /// let mut bytes = vec![];
    /// rb1.serialize_into(&mut bytes).unwrap();
    /// let rb2 = RoaringBitmap::deserialize_from(&bytes[..]).unwrap();
    ///
    /// assert_eq!(rb1, rb2);
    /// ```
    pub fn deserialize_from<R: io::Read>(reader: R) -> io::Result<RoaringBitmap> {
        RoaringBitmap::deserialize_from_impl(reader, ArrayStore::try_from, BitmapStore::try_from)
    }

    /// Deserialize a bitmap into memory from [the standard Roaring on-disk
    /// format][format]. This is compatible with the official C/C++, Java and
    /// Go implementations. This method is memory safe but will not check if
    /// the data is a valid bitmap.
    ///
    /// [format]: https://github.com/RoaringBitmap/RoaringFormatSpec
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    ///
    /// let rb1: RoaringBitmap = (1..4).collect();
    /// let mut bytes = vec![];
    /// rb1.serialize_into(&mut bytes).unwrap();
    /// let rb2 = RoaringBitmap::deserialize_unchecked_from(&bytes[..]).unwrap();
    ///
    /// assert_eq!(rb1, rb2);
    /// ```
    pub fn deserialize_unchecked_from<R: io::Read>(reader: R) -> io::Result<RoaringBitmap> {
        RoaringBitmap::deserialize_from_impl::<R, _, Infallible, _, Infallible>(
            reader,
            |values| Ok(ArrayStore::from_vec_unchecked(values)),
            |len, values| Ok(BitmapStore::from_unchecked(len, values)),
        )
    }

    fn deserialize_from_impl<R, A, AErr, B, BErr>(
        mut reader: R,
        a: A,
        b: B,
    ) -> io::Result<RoaringBitmap>
    where
        R: io::Read,
        A: Fn(Vec<u16>) -> Result<ArrayStore, AErr>,
        AErr: Error + Send + Sync + 'static,
        B: Fn(u64, Box<[u64; 1024]>) -> Result<BitmapStore, BErr>,
        BErr: Error + Send + Sync + 'static,
    {
        // First read the cookie to determine which version of the format we are reading
        let (size, has_offsets, has_run_containers) = {
            let cookie = reader.read_u32::<LittleEndian>()?;
            if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
                (reader.read_u32::<LittleEndian>()? as usize, true, false)
            } else if (cookie as u16) == SERIAL_COOKIE {
                let size = ((cookie >> 16) + 1) as usize;
                (size, size >= NO_OFFSET_THRESHOLD, true)
            } else {
                return Err(io::Error::other("unknown cookie value"));
            }
        };

        // Read the run container bitmap if necessary
        let run_container_bitmap = if has_run_containers {
            let mut bitmap = vec![0u8; size.div_ceil(8)];
            reader.read_exact(&mut bitmap)?;
            Some(bitmap)
        } else {
            None
        };

        if size > u16::MAX as usize + 1 {
            return Err(io::Error::other("size is greater than supported"));
        }

        // Read the container descriptions
        let mut description_bytes = vec![0u8; size * DESCRIPTION_BYTES];
        reader.read_exact(&mut description_bytes)?;
        let mut description_bytes = &description_bytes[..];

        if has_offsets {
            let mut offsets = vec![0u8; size * OFFSET_BYTES];
            reader.read_exact(&mut offsets)?;
            drop(offsets); // Not useful when deserializing into memory
        }

        let mut containers = Vec::with_capacity(size);

        let mut last_key = None::<u16>;
        // Read each container
        for i in 0..size {
            let key = description_bytes.read_u16::<LittleEndian>()?;
            if let Some(last_key) = last_key.replace(key) {
                if key <= last_key {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "container keys are not sorted",
                    ));
                }
            }
            let cardinality = u64::from(description_bytes.read_u16::<LittleEndian>()?) + 1;

            // If the run container bitmap is present, check if this container is a run container
            let is_run_container =
                run_container_bitmap.as_ref().is_some_and(|bm| bm[i / 8] & (1 << (i % 8)) != 0);

            let store = if is_run_container {
                let runs = reader.read_u16::<LittleEndian>()?;
                if runs == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "run container with zero runs",
                    ));
                }
                let mut intervals = vec![[0, 0]; runs as usize];
                reader.read_exact(cast_slice_mut(&mut intervals))?;
                intervals.iter_mut().for_each(|[s, len]| {
                    *s = u16::from_le(*s);
                    *len = u16::from_le(*len);
                });

                let mut last_end = None::<u16>;
                let store = IntervalStore::from_vec_unchecked(
                    intervals
                        .into_iter()
                        .map(|[s, len]| -> Result<Interval, io::ErrorKind> {
                            let end = s.checked_add(len).ok_or(io::ErrorKind::InvalidData)?;
                            if let Some(last_end) = last_end.replace(end) {
                                if s <= last_end.saturating_add(1) {
                                    // Range overlaps or would be contiguous with the previous range
                                    return Err(io::ErrorKind::InvalidData);
                                }
                            }
                            Ok(Interval::new_unchecked(s, end))
                        })
                        .collect::<Result<_, _>>()?,
                );
                Store::Run(store)
            } else if cardinality <= ARRAY_LIMIT {
                let mut values = vec![0; cardinality as usize];
                reader.read_exact(cast_slice_mut(&mut values))?;
                values.iter_mut().for_each(|n| *n = u16::from_le(*n));
                let array = a(values).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Store::Array(array)
            } else {
                let mut values = Box::new([0; BITMAP_LENGTH]);
                reader.read_exact(cast_slice_mut(&mut values[..]))?;
                values.iter_mut().for_each(|n| *n = u64::from_le(*n));
                let bitmap = b(cardinality, values)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Store::Bitmap(bitmap)
            };

            containers.push(Container { key, store });
        }

        Ok(RoaringBitmap { containers })
    }
}

fn header_size(size: usize, has_run_containers: bool) -> usize {
    if has_run_containers {
        // New format encodes the size (number of containers) into the 4 byte cookie
        // Additionally a bitmap is included marking which containers are run containers
        let run_container_bitmap_size = size.div_ceil(8);
        // New format conditionally includes offsets if there are 4 or more containers
        if size >= NO_OFFSET_THRESHOLD {
            COOKIE_BYTES + ((DESCRIPTION_BYTES + OFFSET_BYTES) * size) + run_container_bitmap_size
        } else {
            COOKIE_BYTES + (DESCRIPTION_BYTES * size) + run_container_bitmap_size
        }
    } else {
        // Old format encodes cookie followed by container count
        // It also always includes the offsets
        COOKIE_BYTES + SIZE_BYTES + ((DESCRIPTION_BYTES + OFFSET_BYTES) * size)
    }
}

#[cfg(test)]
mod test {
    use crate::{bitmap::store::BITMAP_LENGTH, RoaringBitmap};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_serialization(
            bitmap in RoaringBitmap::arbitrary(),
        ) {
            let mut buffer = Vec::new();
            bitmap.serialize_into(&mut buffer).unwrap();
            prop_assert_eq!(bitmap, RoaringBitmap::deserialize_from(buffer.as_slice()).unwrap());
        }
    }

    #[test]
    fn test_from_lsb0_bytes() {
        const CONTAINER_OFFSET: u32 = u64::BITS * BITMAP_LENGTH as u32;
        const CONTAINER_OFFSET_IN_BYTES: u32 = CONTAINER_OFFSET / 8;
        let mut bytes = vec![0xff; CONTAINER_OFFSET_IN_BYTES as usize];
        bytes.extend([0x00; CONTAINER_OFFSET_IN_BYTES as usize]);
        bytes.extend([0b00000001, 0b00000010, 0b00000011, 0b00000100]);

        let offset = 32;
        let rb = RoaringBitmap::from_lsb0_bytes(offset, &bytes);
        for i in 0..offset {
            assert!(!rb.contains(i), "{i} should not be in the bitmap");
        }
        for i in offset..offset + CONTAINER_OFFSET {
            assert!(rb.contains(i), "{i} should be in the bitmap");
        }
        for i in offset + CONTAINER_OFFSET..offset + CONTAINER_OFFSET * 2 {
            assert!(!rb.contains(i), "{i} should not be in the bitmap");
        }
        for bit in [0, 9, 16, 17, 26] {
            let i = bit + offset + CONTAINER_OFFSET * 2;
            assert!(rb.contains(i), "{i} should be in the bitmap");
        }

        assert_eq!(rb.len(), CONTAINER_OFFSET as u64 + 5);

        // Ensure the empty container is not created
        let mut bytes = vec![0x00u8; CONTAINER_OFFSET_IN_BYTES as usize];
        bytes.extend([0xff]);
        let rb = RoaringBitmap::from_lsb0_bytes(0, &bytes);
        assert_eq!(rb.min(), Some(CONTAINER_OFFSET));

        let rb = RoaringBitmap::from_lsb0_bytes(8, &bytes);
        assert_eq!(rb.min(), Some(CONTAINER_OFFSET + 8));

        // Ensure we can set the last byte in an array container
        let bytes = [0x80];
        let rb = RoaringBitmap::from_lsb0_bytes(0xFFFFFFF8, &bytes);
        assert_eq!(rb.len(), 1);
        assert!(rb.contains(u32::MAX));

        // Ensure we can set the last byte in a bitmap container
        let bytes = vec![0xFF; 0x1_0000 / 8];
        let rb = RoaringBitmap::from_lsb0_bytes(0xFFFF0000, &bytes);
        assert_eq!(rb.len(), 0x1_0000);
        assert!(rb.contains(u32::MAX));
    }

    #[test]
    fn test_from_lsb0_bytes_not_multiple_of_8() {
        const CONTAINER_OFFSET: u32 = u64::BITS * BITMAP_LENGTH as u32;
        const CONTAINER_OFFSET_IN_BYTES: u32 = CONTAINER_OFFSET / 8;

        let mut bytes = vec![0b0101_1001];
        bytes.extend([0x00; CONTAINER_OFFSET_IN_BYTES as usize]);
        bytes.extend([0b00000001, 0b00000010, 0b00000011, 0b00000100]);

        let mut indices = vec![0, 3, 4, 6];
        indices.extend([0, 9, 16, 17, 26].map(|i| 8 + CONTAINER_OFFSET + i));

        for offset in 0..8 {
            let rb = RoaringBitmap::from_lsb0_bytes(offset, &bytes);
            for i in indices.iter().map(|&i| i + offset) {
                assert!(rb.contains(i), "{i} should be in the bitmap");
            }
        }
    }

    #[test]
    #[should_panic(expected = "<= 2^32")]
    fn test_from_lsb0_bytes_overflow() {
        let bytes = [0x01, 0x01];
        RoaringBitmap::from_lsb0_bytes(u32::MAX - 7, &bytes);
    }

    #[test]
    fn test_deserialize_overflow_s_plus_len() {
        let data = vec![59, 48, 0, 0, 255, 130, 254, 59, 48, 2, 0, 41, 255, 255, 166, 197, 4, 0, 2];
        let res = RoaringBitmap::deserialize_from(data.as_slice());
        assert!(res.is_err());
    }
}
