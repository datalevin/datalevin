use serde::de::SeqAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::RoaringBitmap;

impl<'de> Deserialize<'de> for RoaringBitmap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BitmapVisitor;

        impl<'de> Visitor<'de> for BitmapVisitor {
            type Value = RoaringBitmap;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("roaring bitmap")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<RoaringBitmap, E>
            where
                E: serde::de::Error,
            {
                RoaringBitmap::deserialize_from(bytes).map_err(serde::de::Error::custom)
            }

            // in some case bytes will be serialized as a sequence thus we need to accept both
            // even if it means non optimal performance
            fn visit_seq<A>(self, mut seq: A) -> Result<RoaringBitmap, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes: Vec<u8> = Vec::new();
                while let Some(el) = seq.next_element()? {
                    bytes.push(el);
                }
                RoaringBitmap::deserialize_from(&*bytes).map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(BitmapVisitor)
    }
}

impl Serialize for RoaringBitmap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf).map_err(serde::ser::Error::custom)?;

        serializer.serialize_bytes(&buf)
    }
}

#[cfg(test)]
mod test {
    use crate::RoaringBitmap;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_serde_json(
            bitmap in RoaringBitmap::arbitrary(),
        ) {
            let json = serde_json::to_vec(&bitmap).unwrap();
            prop_assert_eq!(bitmap, serde_json::from_slice(&json).unwrap());
        }

        #[test]
        fn test_postcard(
            bitmap in RoaringBitmap::arbitrary(),
        ) {
            let buffer = postcard::to_allocvec(&bitmap).unwrap();
            prop_assert_eq!(bitmap, postcard::from_bytes(&buffer).unwrap());
        }
    }
}
