mod arbitrary;
mod container;
mod fmt;
mod multiops;
mod proptests;
mod statistics;
mod store;
mod util;

// Order of these modules matters as it determines the `impl` blocks order in
// the docs
mod cmp;
mod inherent;
mod iter;
mod ops;
#[cfg(feature = "std")]
mod ops_with_serialized;
#[cfg(feature = "serde")]
mod serde;
#[cfg(feature = "std")]
mod serialization;

use self::cmp::Pairs;
pub use self::iter::IntoIter;
pub use self::iter::Iter;
pub use self::statistics::Statistics;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A compressed bitmap using the [Roaring bitmap compression scheme](https://roaringbitmap.org/).
///
/// # Examples
///
/// ```rust
/// use roaring::RoaringBitmap;
///
/// let mut rb = RoaringBitmap::new();
///
/// // insert all primes less than 10
/// rb.insert(2);
/// rb.insert(3);
/// rb.insert(5);
/// rb.insert(7);
/// println!("total bits set to true: {}", rb.len());
/// ```
#[derive(PartialEq, Eq)]
pub struct RoaringBitmap {
    containers: Vec<container::Container>,
}
