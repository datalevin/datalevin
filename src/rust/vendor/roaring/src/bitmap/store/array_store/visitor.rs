#[cfg(feature = "simd")]
use crate::bitmap::store::array_store::vector::swizzle_to_front;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// This visitor pattern allows multiple different algorithms to be written over the same data
/// For example: vectorized algorithms can pass a visitor off to a scalar algorithm to finish off
/// a tail that is not a multiple of the vector width.
///
/// Perhaps more importantly: it separates the set algorithms from the operations performed on
/// their results. Future work can utilize the exiting algorithms to trivially implement
/// computing the cardinality of an operation without materializng a new bitmap.
pub trait BinaryOperationVisitor {
    #[cfg(feature = "simd")]
    fn visit_vector(&mut self, value: core::simd::u16x8, mask: u8);
    fn visit_scalar(&mut self, value: u16);
    fn visit_slice(&mut self, values: &[u16]);
}

/// A simple visitor that stores the computation result to a Vec
/// accessible by calling `into_inner()`
pub struct VecWriter {
    vec: Vec<u16>,
}

impl VecWriter {
    pub fn new(capacity: usize) -> VecWriter {
        let vec = Vec::with_capacity(capacity);
        VecWriter { vec }
    }

    pub fn into_inner(self) -> Vec<u16> {
        // Consider shrinking the vec here.
        // Exactly len could be too aggressive. Len rounded up to next power of 2?
        // Related, but not exact issue: https://github.com/RoaringBitmap/roaring-rs/issues/136
        self.vec
    }
}

impl BinaryOperationVisitor for VecWriter {
    #[cfg(feature = "simd")]
    fn visit_vector(&mut self, value: core::simd::u16x8, mask: u8) {
        let result = swizzle_to_front(value, mask);

        // This idiom is better than subslicing result, as it compiles down to an unaligned vector
        // store instr.
        // A more straightforward, but unsafe way would be ptr::write_unaligned and Vec::set_len
        // Writing a vector at once is why the vectorized algorithms do not operate in place
        // first write the entire vector
        self.vec.extend_from_slice(&result.as_array()[..]);
        // next truncate the masked out values
        self.vec.truncate(self.vec.len() - (result.len() - mask.count_ones() as usize));
    }

    fn visit_scalar(&mut self, value: u16) {
        self.vec.push(value)
    }

    fn visit_slice(&mut self, values: &[u16]) {
        self.vec.extend_from_slice(values);
    }
}

pub struct CardinalityCounter {
    count: usize,
}

impl CardinalityCounter {
    pub fn new() -> CardinalityCounter {
        CardinalityCounter { count: 0 }
    }

    pub fn into_inner(self) -> u64 {
        self.count as u64
    }
}

impl BinaryOperationVisitor for CardinalityCounter {
    #[cfg(feature = "simd")]
    fn visit_vector(&mut self, _value: core::simd::u16x8, mask: u8) {
        self.count += mask.count_ones() as usize;
    }

    fn visit_scalar(&mut self, _value: u16) {
        self.count += 1;
    }

    fn visit_slice(&mut self, values: &[u16]) {
        self.count += values.len();
    }
}
