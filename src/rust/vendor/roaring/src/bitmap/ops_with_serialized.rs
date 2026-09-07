use bytemuck::cast_slice_mut;
use byteorder::{LittleEndian, ReadBytesExt};
use core::convert::Infallible;
use std::error::Error;
use std::io::{self, SeekFrom};
use std::mem;
use std::ops::RangeInclusive;

use crate::bitmap::container::Container;
use crate::bitmap::serialization::{
    NO_OFFSET_THRESHOLD, SERIAL_COOKIE, SERIAL_COOKIE_NO_RUNCONTAINER,
};
use crate::RoaringBitmap;

use super::container::ARRAY_LIMIT;
use super::store::{ArrayStore, BitmapStore, Store, BITMAP_LENGTH};

impl RoaringBitmap {
    /// Computes the intersection between a materialized [`RoaringBitmap`] and a serialized one.
    ///
    /// This is faster and more space efficient when you only need the intersection result.
    /// It reduces the number of deserialized internal container and therefore
    /// the number of allocations and copies of bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use roaring::RoaringBitmap;
    /// use std::io::Cursor;
    ///
    /// let rb1: RoaringBitmap = (1..4).collect();
    /// let rb2: RoaringBitmap = (3..5).collect();
    ///
    /// // Let's say the rb2 bitmap is serialized
    /// let mut bytes = Vec::new();
    /// rb2.serialize_into(&mut bytes).unwrap();
    /// let rb2_bytes = Cursor::new(bytes);
    ///
    /// assert_eq!(
    ///     rb1.intersection_with_serialized_unchecked(rb2_bytes).unwrap(),
    ///     rb1 & rb2,
    /// );
    /// ```
    pub fn intersection_with_serialized_unchecked<R>(&self, other: R) -> io::Result<RoaringBitmap>
    where
        R: io::Read + io::Seek,
    {
        RoaringBitmap::intersection_with_serialized_impl::<R, _, Infallible, _, Infallible>(
            self,
            other,
            |values| Ok(ArrayStore::from_vec_unchecked(values)),
            |len, values| Ok(BitmapStore::from_unchecked(len, values)),
        )
    }

    fn intersection_with_serialized_impl<R, A, AErr, B, BErr>(
        &self,
        mut reader: R,
        a: A,
        b: B,
    ) -> io::Result<RoaringBitmap>
    where
        R: io::Read + io::Seek,
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
        let mut descriptions = vec![[0; 2]; size];
        reader.read_exact(cast_slice_mut(&mut descriptions))?;
        descriptions.iter_mut().for_each(|[ref mut key, ref mut len]| {
            *key = u16::from_le(*key);
            *len = u16::from_le(*len);
        });

        if has_offsets {
            let mut offsets = vec![0; size];
            reader.read_exact(cast_slice_mut(&mut offsets))?;
            offsets.iter_mut().for_each(|offset| *offset = u32::from_le(*offset));
            return self.intersection_with_serialized_impl_with_offsets(
                reader,
                a,
                b,
                &descriptions,
                &offsets,
                run_container_bitmap.as_deref(),
            );
        }

        // Read each container and skip the useless ones
        let mut containers = Vec::new();
        for (i, &[key, len_minus_one]) in descriptions.iter().enumerate() {
            let container = match self.containers.binary_search_by_key(&key, |c| c.key) {
                Ok(index) => self.containers.get(index),
                Err(_) => None,
            };
            let cardinality = u64::from(len_minus_one) + 1;

            // If the run container bitmap is present, check if this container is a run container
            let is_run_container =
                run_container_bitmap.as_ref().is_some_and(|bm| bm[i / 8] & (1 << (i % 8)) != 0);

            let store = if is_run_container {
                let runs = reader.read_u16::<LittleEndian>()?;
                match container {
                    Some(_) => {
                        let mut intervals = vec![[0, 0]; runs as usize];
                        reader.read_exact(cast_slice_mut(&mut intervals))?;
                        intervals.iter_mut().for_each(|[s, len]| {
                            *s = u16::from_le(*s);
                            *len = u16::from_le(*len);
                        });

                        let cardinality = intervals.iter().map(|[_, len]| *len as usize).sum();
                        let mut store = Store::with_capacity(cardinality);
                        intervals.into_iter().try_for_each(
                            |[s, len]| -> Result<(), io::ErrorKind> {
                                let end = s.checked_add(len).ok_or(io::ErrorKind::InvalidData)?;
                                store.insert_range(RangeInclusive::new(s, end));
                                Ok(())
                            },
                        )?;
                        store
                    }
                    None => {
                        let runs_size = mem::size_of::<u16>() * 2 * runs as usize;
                        reader.seek(SeekFrom::Current(runs_size as i64))?;
                        continue;
                    }
                }
            } else if cardinality <= ARRAY_LIMIT {
                match container {
                    Some(_) => {
                        let mut values = vec![0; cardinality as usize];
                        reader.read_exact(cast_slice_mut(&mut values))?;
                        values.iter_mut().for_each(|n| *n = u16::from_le(*n));
                        let array =
                            a(values).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        Store::Array(array)
                    }
                    None => {
                        let array_size = mem::size_of::<u16>() * cardinality as usize;
                        reader.seek(SeekFrom::Current(array_size as i64))?;
                        continue;
                    }
                }
            } else {
                match container {
                    Some(_) => {
                        let mut values = Box::new([0; BITMAP_LENGTH]);
                        reader.read_exact(cast_slice_mut(&mut values[..]))?;
                        values.iter_mut().for_each(|n| *n = u64::from_le(*n));
                        let bitmap = b(cardinality, values)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        Store::Bitmap(bitmap)
                    }
                    None => {
                        let bitmap_size = mem::size_of::<u64>() * BITMAP_LENGTH;
                        reader.seek(SeekFrom::Current(bitmap_size as i64))?;
                        continue;
                    }
                }
            };

            if let Some(container) = container {
                let mut other_container = Container { key, store };
                other_container &= container;
                if !other_container.is_empty() {
                    containers.push(other_container);
                }
            }
        }

        Ok(RoaringBitmap { containers })
    }

    fn intersection_with_serialized_impl_with_offsets<R, A, AErr, B, BErr>(
        &self,
        mut reader: R,
        a: A,
        b: B,
        descriptions: &[[u16; 2]],
        offsets: &[u32],
        run_container_bitmap: Option<&[u8]>,
    ) -> io::Result<RoaringBitmap>
    where
        R: io::Read + io::Seek,
        A: Fn(Vec<u16>) -> Result<ArrayStore, AErr>,
        AErr: Error + Send + Sync + 'static,
        B: Fn(u64, Box<[u64; 1024]>) -> Result<BitmapStore, BErr>,
        BErr: Error + Send + Sync + 'static,
    {
        let mut containers = Vec::new();
        for container in &self.containers {
            let i = match descriptions.binary_search_by_key(&container.key, |[k, _]| *k) {
                Ok(index) => index,
                Err(_) => continue,
            };

            // Seek to the bytes of the container we want.
            reader.seek(SeekFrom::Start(offsets[i] as u64))?;

            let [key, len_minus_one] = descriptions[i];
            let cardinality = u64::from(len_minus_one) + 1;

            // If the run container bitmap is present, check if this container is a run container
            let is_run_container =
                run_container_bitmap.as_ref().is_some_and(|bm| bm[i / 8] & (1 << (i % 8)) != 0);

            let store = if is_run_container {
                let runs = reader.read_u16::<LittleEndian>().unwrap();
                let mut intervals = vec![[0, 0]; runs as usize];
                reader.read_exact(cast_slice_mut(&mut intervals)).unwrap();
                intervals.iter_mut().for_each(|[s, len]| {
                    *s = u16::from_le(*s);
                    *len = u16::from_le(*len);
                });

                let cardinality = intervals.iter().map(|[_, len]| *len as usize).sum();
                let mut store = Store::with_capacity(cardinality);
                intervals.into_iter().try_for_each(|[s, len]| -> Result<(), io::ErrorKind> {
                    let end = s.checked_add(len).ok_or(io::ErrorKind::InvalidData)?;
                    store.insert_range(RangeInclusive::new(s, end));
                    Ok(())
                })?;
                store
            } else if cardinality <= ARRAY_LIMIT {
                let mut values = vec![0; cardinality as usize];
                reader.read_exact(cast_slice_mut(&mut values)).unwrap();
                values.iter_mut().for_each(|n| *n = u16::from_le(*n));
                let array = a(values).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Store::Array(array)
            } else {
                let mut values = Box::new([0; BITMAP_LENGTH]);
                reader.read_exact(cast_slice_mut(&mut values[..])).unwrap();
                values.iter_mut().for_each(|n| *n = u64::from_le(*n));
                let bitmap = b(cardinality, values)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Store::Bitmap(bitmap)
            };

            let mut other_container = Container { key, store };
            other_container &= container;
            if !other_container.is_empty() {
                containers.push(other_container);
            }
        }

        Ok(RoaringBitmap { containers })
    }
}

#[cfg(test)]
mod test {
    use crate::RoaringBitmap;
    use proptest::prelude::*;
    use std::io::Cursor;

    // fast count tests
    proptest! {
        #[test]
        fn intersection_with_serialized_eq_materialized_intersection(
            a in RoaringBitmap::arbitrary(),
            b in RoaringBitmap::arbitrary()
        ) {
            let mut serialized_bytes_b = Vec::new();
            b.serialize_into(&mut serialized_bytes_b).unwrap();
            let serialized_bytes_b = &serialized_bytes_b[..];

            prop_assert_eq!(a.intersection_with_serialized_unchecked(Cursor::new(serialized_bytes_b)).unwrap(), a & b);
        }
    }
}
