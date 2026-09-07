//! This is a [Rust][] port of the [Roaring bitmap][] data structure, initially
//! defined as a [Java library][roaring-java] and described in [_Better bitmap
//! performance with Roaring bitmaps_][roaring-paper].
//!
//! [Rust]: https://www.rust-lang.org/
//! [Roaring bitmap]: https://roaringbitmap.org/
//! [roaring-java]: https://github.com/lemire/RoaringBitmap
//! [roaring-paper]: https://arxiv.org/pdf/1402.6407v4

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(feature = "simd", feature(portable_simd))]
#![warn(missing_docs)]
#![warn(unsafe_op_in_unsafe_fn)]
#![warn(variant_size_differences)]
#![allow(unknown_lints)] // For clippy
#![allow(clippy::doc_overindented_list_items)]
#![deny(unnameable_types)]

#[cfg(feature = "std")]
extern crate byteorder;

#[macro_use]
extern crate alloc;

use core::fmt;

/// A compressed bitmap using the [Roaring bitmap compression scheme](https://roaringbitmap.org/).
pub mod bitmap;

/// A compressed bitmap with u64 values.  Implemented as a `BTreeMap` of `RoaringBitmap`s.
pub mod treemap;

pub use bitmap::RoaringBitmap;
pub use treemap::RoaringTreemap;

/// An error type that is returned when a `try_push` in a bitmap did not succeed.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntegerTooSmall;

impl fmt::Display for IntegerTooSmall {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("inserted integer is smaller than the largest integer")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for IntegerTooSmall {}

/// An error type that is returned when an iterator isn't sorted.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonSortedIntegers {
    valid_until: u64,
}

impl NonSortedIntegers {
    /// Returns the number of elements that were
    pub fn valid_until(&self) -> u64 {
        self.valid_until
    }
}

impl fmt::Display for NonSortedIntegers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "integers are ordered up to the {}th element", self.valid_until())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for NonSortedIntegers {}

/// A [`Iterator::collect`] blanket implementation that provides extra methods for [`RoaringBitmap`]
/// and [`RoaringTreemap`].
///
/// When merging multiple bitmap with the same operation it's usually faster to call the
/// method in this trait than to write your own for loop and merging the bitmaps yourself.
///
/// # Examples
/// ```
/// use roaring::{MultiOps, RoaringBitmap};
///
/// let bitmaps = [
///     RoaringBitmap::from_iter(0..10),
///     RoaringBitmap::from_iter(10..20),
///     RoaringBitmap::from_iter(20..30),
/// ];
///
/// // Stop doing this
/// let naive = bitmaps.clone().into_iter().reduce(|a, b| a | b).unwrap_or_default();
///
/// // And start doing this instead, it will be much faster!
/// let iter = bitmaps.union();
///
/// assert_eq!(naive, iter);
/// ```
pub trait MultiOps<T>: IntoIterator<Item = T> {
    /// The type of output from operations.
    type Output;

    /// The `union` between all elements.
    fn union(self) -> Self::Output;

    /// The `intersection` between all elements.
    fn intersection(self) -> Self::Output;

    /// The `difference` between all elements.
    fn difference(self) -> Self::Output;

    /// The `symmetric difference` between all elements.
    fn symmetric_difference(self) -> Self::Output;
}
