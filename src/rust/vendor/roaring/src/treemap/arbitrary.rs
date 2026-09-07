#[cfg(test)]
mod test {
    use crate::{RoaringBitmap, RoaringTreemap};
    use proptest::collection::btree_map;
    use proptest::prelude::*;

    impl RoaringTreemap {
        prop_compose! {
            pub(crate) fn arbitrary()(map in btree_map(0u32..=16, RoaringBitmap::arbitrary(), 0usize..=16)) -> RoaringTreemap {
                // we’re NEVER supposed to start with a treemap containing empty bitmaps
                // Since we can’t configure this in arbitrary we’re simply going to ignore the generated empty bitmaps
                let map = map.into_iter().filter(|(_, v)| !v.is_empty()).collect();
               RoaringTreemap { map }
           }
        }
    }
}
