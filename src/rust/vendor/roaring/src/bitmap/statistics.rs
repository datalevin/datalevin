use core::mem;

use crate::bitmap::container::Container;
use crate::RoaringBitmap;

use super::store::Store;

/// Detailed statistics on the composition of a bitmap.
#[derive(Clone, Copy, PartialEq, Debug)]
#[non_exhaustive]
pub struct Statistics {
    /// Number of containers in the bitmap
    pub n_containers: u32,
    /// Number of array containers in the bitmap
    pub n_array_containers: u32,
    /// Number of run containers in the bitmap
    pub n_run_containers: u32,
    /// Number of bitset containers in the bitmap
    pub n_bitset_containers: u32,
    /// Number of values stored in array containers
    pub n_values_array_containers: u32,
    /// Number of values stored in run containers
    pub n_values_run_containers: u32,
    /// Number of values stored in bitset containers
    pub n_values_bitset_containers: u64,
    /// Number of bytes used by array containers
    pub n_bytes_array_containers: u64,
    /// Number of bytes used by run containers
    pub n_bytes_run_containers: u64,
    /// Number of bytes used by bitset containers
    pub n_bytes_bitset_containers: u64,
    /// Maximum value stored in the bitmap
    pub max_value: Option<u32>,
    /// Minimum value stored in the bitmap
    pub min_value: Option<u32>,
    /// Number of values stored in the bitmap
    pub cardinality: u64,
}

impl RoaringBitmap {
    /// Returns statistics about the composition of a roaring bitmap.
    ///
    /// ```
    /// use roaring::RoaringBitmap;
    ///
    /// let mut bitmap: RoaringBitmap = (1..100).collect();
    /// let statistics = bitmap.statistics();
    ///
    /// assert_eq!(statistics.n_containers, 1);
    /// assert_eq!(statistics.n_array_containers, 1);
    /// assert_eq!(statistics.n_run_containers, 0);
    /// assert_eq!(statistics.n_bitset_containers, 0);
    /// assert_eq!(statistics.n_values_array_containers, 99);
    /// assert_eq!(statistics.n_values_run_containers, 0);
    /// assert_eq!(statistics.n_values_bitset_containers, 0);
    /// assert_eq!(statistics.n_bytes_array_containers, 512);
    /// assert_eq!(statistics.n_bytes_run_containers, 0);
    /// assert_eq!(statistics.n_bytes_bitset_containers, 0);
    /// assert_eq!(statistics.max_value, Some(99));
    /// assert_eq!(statistics.min_value, Some(1));
    /// assert_eq!(statistics.cardinality, 99);
    /// ```
    pub fn statistics(&self) -> Statistics {
        let mut n_containers = 0;
        let mut n_array_containers = 0;
        let mut n_bitset_containers = 0;
        let mut n_run_containers = 0;
        let mut n_values_array_containers = 0;
        let mut n_values_bitset_containers = 0;
        let mut n_values_run_containers = 0;
        let mut n_bytes_array_containers = 0;
        let mut n_bytes_bitset_containers = 0;
        let mut n_bytes_run_containers = 0;
        let mut cardinality = 0;

        for Container { key: _, store } in &self.containers {
            match store {
                Store::Array(array) => {
                    cardinality += array.len();
                    n_values_array_containers += array.len() as u32;
                    n_bytes_array_containers += (array.capacity() * mem::size_of::<u32>()) as u64;
                    n_array_containers += 1;
                }
                Store::Bitmap(bitmap) => {
                    cardinality += bitmap.len();
                    n_values_bitset_containers += bitmap.len();
                    n_bytes_bitset_containers += bitmap.capacity() as u64;
                    n_bitset_containers += 1;
                }
                Store::Run(runs) => {
                    cardinality += runs.len();
                    n_values_run_containers += runs.len() as u32;
                    n_bytes_run_containers += runs.byte_size() as u64;
                    n_run_containers += 1;
                }
            }
            n_containers += 1;
        }

        Statistics {
            n_containers,
            n_array_containers,
            n_run_containers,
            n_bitset_containers,
            n_values_array_containers,
            n_values_run_containers,
            n_values_bitset_containers,
            n_bytes_array_containers,
            n_bytes_run_containers,
            n_bytes_bitset_containers,
            max_value: self.max(),
            min_value: self.min(),
            cardinality,
        }
    }
}
