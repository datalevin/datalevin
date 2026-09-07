#[cfg(test)]
mod test {
    use crate::bitmap::container::{Container, RUN_MAX_SIZE};
    use crate::bitmap::store::{ArrayStore, BitmapStore, IntervalStore, Store};
    use crate::RoaringBitmap;
    use core::fmt::{Debug, Formatter};
    use proptest::bits::{BitSetLike, SampledBitSetStrategy};
    use proptest::collection::{vec, SizeRange};
    use proptest::prelude::*;

    #[cfg(not(feature = "std"))]
    use alloc::vec::Vec;

    impl Debug for BitmapStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            if self.len() < 16 {
                write!(f, "BitmapStore<{:?}>", self.iter().collect::<Vec<u16>>())
            } else {
                write!(
                    f,
                    "BitmapStore<{:?} values between {:?} and {:?}>",
                    self.len(),
                    self.min().unwrap(),
                    self.max().unwrap()
                )
            }
        }
    }

    impl BitSetLike for BitmapStore {
        fn new_bitset(max: usize) -> Self {
            assert!(max <= BitmapStore::MAX + 1);
            BitmapStore::new()
        }

        fn len(&self) -> usize {
            BitmapStore::MAX + 1
        }

        fn test(&self, bit: usize) -> bool {
            assert!(bit <= BitmapStore::MAX);
            self.contains(bit as u16)
        }

        fn set(&mut self, bit: usize) {
            assert!(bit <= BitmapStore::MAX);
            self.insert(bit as u16);
        }

        fn clear(&mut self, bit: usize) {
            assert!(bit <= BitmapStore::MAX);
            self.remove(bit as u16);
        }

        fn count(&self) -> usize {
            self.len() as usize
        }
    }

    impl BitmapStore {
        const MAX: usize = u16::MAX as usize;

        pub fn sampled(
            size: impl Into<SizeRange>,
            bits: impl Into<SizeRange>,
        ) -> SampledBitSetStrategy<Self> {
            SampledBitSetStrategy::new(size.into(), bits.into())
        }
    }

    impl BitSetLike for IntervalStore {
        fn new_bitset(max: usize) -> Self {
            assert!(max <= IntervalStore::MAX + 1);
            IntervalStore::new()
        }

        fn len(&self) -> usize {
            IntervalStore::MAX + 1
        }

        fn test(&self, bit: usize) -> bool {
            assert!(bit <= IntervalStore::MAX);
            self.contains(bit as u16)
        }

        fn set(&mut self, bit: usize) {
            assert!(bit <= IntervalStore::MAX);
            self.insert(bit as u16);
        }

        fn clear(&mut self, bit: usize) {
            assert!(bit <= IntervalStore::MAX);
            self.remove(bit as u16);
        }

        fn count(&self) -> usize {
            self.len() as usize
        }
    }

    impl IntervalStore {
        const MAX: usize = u16::MAX as usize;

        pub fn sampled(
            size: impl Into<SizeRange>,
            bits: impl Into<SizeRange>,
        ) -> SampledBitSetStrategy<Self> {
            SampledBitSetStrategy::new(size.into(), bits.into())
        }
    }

    impl Debug for ArrayStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            if self.len() < 16 {
                write!(f, "ArrayStore<{:?}>", self.as_slice())
            } else {
                write!(
                    f,
                    "ArrayStore<{:?} values between {:?} and {:?}>",
                    self.len(),
                    self.min().unwrap(),
                    self.max().unwrap()
                )
            }
        }
    }

    impl BitSetLike for ArrayStore {
        fn new_bitset(max: usize) -> Self {
            assert!(max <= ArrayStore::MAX + 1);
            ArrayStore::new()
        }

        fn len(&self) -> usize {
            ArrayStore::MAX + 1
        }

        fn test(&self, bit: usize) -> bool {
            assert!(bit <= ArrayStore::MAX);
            self.contains(bit as u16)
        }

        fn set(&mut self, bit: usize) {
            assert!(bit <= ArrayStore::MAX);
            self.insert(bit as u16);
        }

        fn clear(&mut self, bit: usize) {
            assert!(bit <= ArrayStore::MAX);
            self.remove(bit as u16);
        }

        fn count(&self) -> usize {
            self.len() as usize
        }
    }

    impl ArrayStore {
        const MAX: usize = u16::MAX as usize;

        pub fn sampled(
            size: impl Into<SizeRange>,
            bits: impl Into<SizeRange>,
        ) -> SampledBitSetStrategy<ArrayStore> {
            SampledBitSetStrategy::new(size.into(), bits.into())
        }
    }

    impl Debug for Store {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            match self {
                Store::Array(a) => write!(f, "Store({a:?})"),
                Store::Bitmap(b) => write!(f, "Store({b:?})"),
                Store::Run(c) => write!(f, "Store({c:?})"),
            }
        }
    }

    impl Store {
        fn arbitrary() -> impl Strategy<Value = Store> {
            prop_oneof![
                ArrayStore::sampled(1..=4096, ..=u16::MAX as usize).prop_map(Store::Array),
                BitmapStore::sampled(4097..u16::MAX as usize, ..=u16::MAX as usize)
                    .prop_map(Store::Bitmap),
                IntervalStore::sampled(1..=RUN_MAX_SIZE as usize, ..=u16::MAX as usize)
                    .prop_map(Store::Run),
            ]
        }
    }

    prop_compose! {
        fn containers(n: usize)
                     (keys in ArrayStore::sampled(..=n, ..=n),
                      stores in vec(Store::arbitrary(), n)) -> RoaringBitmap {
            let containers = keys.into_iter().zip(stores).map(|(key, store)| {
                let mut container = Container { key, store };
                container.ensure_correct_store();
                container
            }).collect::<Vec<Container>>();
            RoaringBitmap { containers }
       }
    }

    impl RoaringBitmap {
        prop_compose! {
            pub(crate) fn arbitrary()(bitmap in (0usize..=16).prop_flat_map(containers)) -> RoaringBitmap {
                bitmap
            }
        }
    }
}
