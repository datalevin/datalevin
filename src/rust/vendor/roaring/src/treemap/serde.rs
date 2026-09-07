use serde::de::SeqAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::RoaringTreemap;

impl<'de> Deserialize<'de> for RoaringTreemap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TreemapVisitor;

        impl<'de> Visitor<'de> for TreemapVisitor {
            type Value = RoaringTreemap;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("roaring bitmap")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<RoaringTreemap, E>
            where
                E: serde::de::Error,
            {
                RoaringTreemap::deserialize_from(bytes).map_err(serde::de::Error::custom)
            }

            // in some case bytes will be serialized as a sequence thus we need to accept both
            // even if it means non optimal performance
            fn visit_seq<A>(self, mut seq: A) -> Result<RoaringTreemap, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes: Vec<u8> = Vec::new();
                while let Some(el) = seq.next_element()? {
                    bytes.push(el);
                }
                RoaringTreemap::deserialize_from(&*bytes).map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_bytes(TreemapVisitor)
    }
}

impl Serialize for RoaringTreemap {
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
    use crate::RoaringTreemap;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_serde_json(
            treemap in RoaringTreemap::arbitrary(),
        ) {
            let json = serde_json::to_vec(&treemap).unwrap();
            prop_assert_eq!(treemap, serde_json::from_slice(&json).unwrap());
        }

        #[test]
        fn test_postcard(
            treemap in RoaringTreemap::arbitrary(),
        ) {
            let buffer = postcard::to_allocvec(&treemap).unwrap();
            prop_assert_eq!(treemap, postcard::from_bytes(&buffer).unwrap());
        }
    }
}
