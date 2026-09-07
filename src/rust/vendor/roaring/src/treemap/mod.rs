use crate::RoaringBitmap;
use alloc::collections::BTreeMap;

mod fmt;
mod multiops;
mod util;

// Order of these modules matters as it determines the `impl` blocks order in
// the docs
mod arbitrary;
mod cmp;
mod inherent;
mod iter;
mod ops;
#[cfg(feature = "serde")]
mod serde;
#[cfg(feature = "std")]
mod serialization;

pub use self::iter::{BitmapIter, IntoIter, Iter};

/// A compressed bitmap with u64 values.
/// Implemented as a `BTreeMap` of `RoaringBitmap`s.
///
/// # Examples
///
/// ```rust
/// use roaring::RoaringTreemap;
///
/// let mut rb = RoaringTreemap::new();
///
/// // insert all primes less than 10
/// rb.insert(2);
/// rb.insert(3);
/// rb.insert(5);
/// rb.insert(7);
/// println!("total bits set to true: {}", rb.len());
/// ```
#[derive(PartialEq, Eq)]
pub struct RoaringTreemap {
    map: BTreeMap<u32, RoaringBitmap>,
}
