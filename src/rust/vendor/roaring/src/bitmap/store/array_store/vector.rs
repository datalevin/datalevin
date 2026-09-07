//! Ported from CRoaring and arXiv:1709.07821
//! Lemire et al, Roaring Bitmaps: Implementation of an Optimized Software Library
//!
//! Prior work: Schlegel et al., Fast Sorted-Set Intersection using SIMD Instructions
//!
//! Rust port notes:
//! The x86 PCMPESTRM instruction has been replaced with a simple vector or-shift
//! While several more instructions, this is what is available through LLVM intrinsics
//! and is portable.

#![cfg(feature = "simd")]

use super::scalar;
use core::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use core::simd::{mask16x8, u16x8, u8x16, Mask, Select as _, Simd, SimdElement, ToBytes};

// a one-pass SSE union algorithm
pub fn or(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // De-duplicates `slice` in place
    // Returns the end index of the deduplicated slice.
    // elements after the return value are not guaranteed to be unique or in order
    #[inline]
    fn dedup(slice: &mut [u16]) -> usize {
        let mut pos: usize = 1;
        for i in 1..slice.len() {
            if slice[i] != slice[i - 1] {
                slice[pos] = slice[i];
                pos += 1;
            }
        }
        pos
    }

    #[inline]
    fn handle_vector(old: u16x8, new: u16x8, f: impl FnOnce(u16x8, u8)) {
        let tmp: u16x8 = Shr1::concat_swizzle(new, old);
        let mask = 255 - tmp.simd_eq(new).to_bitmask() as u8;
        f(new, mask);
    }

    if (lhs.len() < 8) || (rhs.len() < 8) {
        scalar::or(lhs, rhs, visitor);
        return;
    }

    let len1: usize = lhs.len() / 8;
    let len2: usize = rhs.len() / 8;

    let v_a: u16x8 = load(lhs);
    let v_b: u16x8 = load(rhs);
    let [mut v_min, mut v_max] = simd_merge_u16(v_a, v_b);

    let mut i = 1;
    let mut j = 1;
    handle_vector(Simd::splat(u16::MAX), v_min, |v, m| visitor.visit_vector(v, m));
    let mut v_prev: u16x8 = v_min;
    if (i < len1) && (j < len2) {
        let mut v: u16x8;
        let mut cur_a: u16 = lhs[8 * i];
        let mut cur_b: u16 = rhs[8 * j];
        loop {
            if cur_a <= cur_b {
                v = load(&lhs[8 * i..]);
                i += 1;
                if i < len1 {
                    cur_a = lhs[8 * i];
                } else {
                    break;
                }
            } else {
                v = load(&rhs[8 * j..]);
                j += 1;
                if j < len2 {
                    cur_b = rhs[8 * j];
                } else {
                    break;
                }
            }
            [v_min, v_max] = simd_merge_u16(v, v_max);
            handle_vector(v_prev, v_min, |v, m| visitor.visit_vector(v, m));
            v_prev = v_min;
        }
        [v_min, v_max] = simd_merge_u16(v, v_max);
        handle_vector(v_prev, v_min, |v, m| visitor.visit_vector(v, m));
        v_prev = v_min;
    }

    debug_assert!(i == len1 || j == len2);

    // we finish the rest off using a scalar algorithm
    // could be improved?
    //
    // copy the small end on a tmp buffer
    let mut buffer: [u16; 16] = [0; 16];
    let mut rem = 0;
    handle_vector(v_prev, v_max, |v, m| {
        store(swizzle_to_front(v, m), buffer.as_mut_slice());
        rem = m.count_ones() as usize;
    });

    let (tail_a, tail_b, tail_len) = if i == len1 {
        (&lhs[8 * i..], &rhs[8 * j..], lhs.len() - 8 * len1)
    } else {
        (&rhs[8 * j..], &lhs[8 * i..], rhs.len() - 8 * len2)
    };

    buffer[rem..rem + tail_len].copy_from_slice(tail_a);
    rem += tail_len;

    if rem == 0 {
        visitor.visit_slice(tail_b)
    } else {
        buffer[..rem].sort_unstable();
        rem = dedup(&mut buffer[..rem]);
        scalar::or(&buffer[..rem], tail_b, visitor);
    }
}

pub fn and(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    let st_a = (lhs.len() / u16x8::LEN) * u16x8::LEN;
    let st_b = (rhs.len() / u16x8::LEN) * u16x8::LEN;

    let mut i: usize = 0;
    let mut j: usize = 0;
    if (i < st_a) && (j < st_b) {
        let mut v_a: u16x8 = load(&lhs[i..]);
        let mut v_b: u16x8 = load(&rhs[j..]);
        loop {
            let mask = matrix_cmp_u16(v_a, v_b).to_bitmask() as u8;
            visitor.visit_vector(v_a, mask);

            let a_max: u16 = lhs[i + u16x8::LEN - 1];
            let b_max: u16 = rhs[j + u16x8::LEN - 1];
            if a_max <= b_max {
                i += u16x8::LEN;
                if i == st_a {
                    break;
                }
                v_a = load(&lhs[i..]);
            }
            if b_max <= a_max {
                j += u16x8::LEN;
                if j == st_b {
                    break;
                }
                v_b = load(&rhs[j..]);
            }
        }
    }

    // intersect the tail using scalar intersection
    scalar::and(&lhs[i..], &rhs[j..], visitor);
}

// a one-pass SSE xor algorithm
pub fn xor(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    /// De-duplicates `slice` in place, removing _both_ duplicates
    /// Returns the end index of the xor-ed slice.
    /// elements after the return value are not guaranteed to be unique or in order
    #[inline]
    fn xor_slice(slice: &mut [u16]) -> usize {
        let mut pos: usize = 1;
        for i in 1..slice.len() {
            if slice[i] != slice[i - 1] {
                slice[pos] = slice[i];
                pos += 1;
            } else {
                pos -= 1; // it is identical to previous, delete it
            }
        }
        pos
    }

    // write vector new, while omitting repeated values assuming that previously
    // written vector was "old"
    #[inline]
    fn handle_vector(old: u16x8, new: u16x8, f: impl FnOnce(u16x8, u8)) {
        let tmp1: u16x8 = Shr2::concat_swizzle(new, old);
        let tmp2: u16x8 = Shr1::concat_swizzle(new, old);
        let eq_l: mask16x8 = tmp2.simd_eq(tmp1);
        let eq_r: mask16x8 = tmp2.simd_eq(new);
        let eq_l_or_r: mask16x8 = eq_l | eq_r;
        let mask: u8 = eq_l_or_r.to_bitmask() as u8;
        f(tmp2, 255 - mask);
    }

    if (lhs.len() < 8) || (rhs.len() < 8) {
        scalar::xor(lhs, rhs, visitor);
        return;
    }

    let len1: usize = lhs.len() / 8;
    let len2: usize = rhs.len() / 8;

    let v_a: u16x8 = load(lhs);
    let v_b: u16x8 = load(rhs);
    let [mut v_min, mut v_max] = simd_merge_u16(v_a, v_b);

    let mut i = 1;
    let mut j = 1;
    handle_vector(Simd::splat(u16::MAX), v_min, |v, m| visitor.visit_vector(v, m));
    let mut v_prev: u16x8 = v_min;
    if (i < len1) && (j < len2) {
        let mut v: u16x8;
        let mut cur_a: u16 = lhs[8 * i];
        let mut cur_b: u16 = rhs[8 * j];
        loop {
            if cur_a <= cur_b {
                v = load(&lhs[8 * i..]);
                i += 1;
                if i < len1 {
                    cur_a = lhs[8 * i];
                } else {
                    break;
                }
            } else {
                v = load(&rhs[8 * j..]);
                j += 1;
                if j < len2 {
                    cur_b = rhs[8 * j];
                } else {
                    break;
                }
            }
            [v_min, v_max] = simd_merge_u16(v, v_max);
            handle_vector(v_prev, v_min, |v, m| visitor.visit_vector(v, m));
            v_prev = v_min;
        }
        [v_min, v_max] = simd_merge_u16(v, v_max);
        handle_vector(v_prev, v_min, |v, m| visitor.visit_vector(v, m));
        v_prev = v_min;
    }

    debug_assert!(i == len1 || j == len2);

    // we finish the rest off using a scalar algorithm
    // could be improved?
    // conditionally stores the last value of laststore as well as all but the
    // last value of vecMax,
    let mut buffer: [u16; 17] = [0; 17];
    // remaining size
    let mut rem = 0;
    handle_vector(v_prev, v_max, |v, m| {
        store(swizzle_to_front(v, m), buffer.as_mut_slice());
        rem = m.count_ones() as usize;
    });

    let arr_max = v_max.as_array();
    let vec7 = arr_max[7];
    let vec6 = arr_max[6];
    if vec6 != vec7 {
        buffer[rem] = vec7;
        rem += 1;
    }

    let (tail_a, tail_b, tail_len) = if i == len1 {
        (&lhs[8 * i..], &rhs[8 * j..], lhs.len() - 8 * len1)
    } else {
        (&rhs[8 * j..], &lhs[8 * i..], rhs.len() - 8 * len2)
    };

    buffer[rem..rem + tail_len].copy_from_slice(tail_a);
    rem += tail_len;

    if rem == 0 {
        visitor.visit_slice(tail_b)
    } else {
        buffer[..rem].sort_unstable();
        rem = xor_slice(&mut buffer[..rem]);
        scalar::xor(&buffer[..rem], tail_b, visitor);
    }
}

pub fn sub(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // we handle the degenerate cases
    if lhs.is_empty() {
        return;
    } else if rhs.is_empty() {
        visitor.visit_slice(lhs);
        return;
    }

    let st_a = (lhs.len() / u16x8::LEN) * u16x8::LEN;
    let st_b = (rhs.len() / u16x8::LEN) * u16x8::LEN;

    let mut i = 0;
    let mut j = 0;
    if (i < st_a) && (j < st_b) {
        let mut v_a: u16x8 = load(&lhs[i..]);
        let mut v_b: u16x8 = load(&rhs[j..]);
        // we have a running mask which indicates which values from a have been
        // spotted in b, these don't get written out.
        let mut runningmask_a_found_in_b: u8 = 0;
        loop {
            // a_found_in_b will contain a mask indicate for each entry in A
            // whether it is seen in B
            let a_found_in_b: u8 = matrix_cmp_u16(v_a, v_b).to_bitmask() as u8;
            runningmask_a_found_in_b |= a_found_in_b;
            // we always compare the last values of A and B
            let a_max: u16 = lhs[i + u16x8::LEN - 1];
            let b_max: u16 = rhs[j + u16x8::LEN - 1];
            if a_max <= b_max {
                // Ok. In this code path, we are ready to write our v_a
                // because there is no need to read more from B, they will
                // all be large values.
                let bitmask_belongs_to_difference = runningmask_a_found_in_b ^ 0xFF;
                visitor.visit_vector(v_a, bitmask_belongs_to_difference);
                i += u16x8::LEN;
                if i == st_a {
                    break;
                }
                runningmask_a_found_in_b = 0;
                v_a = load(&lhs[i..]);
            }
            if b_max <= a_max {
                // in this code path, the current v_b has become useless
                j += u16x8::LEN;
                if j == st_b {
                    break;
                }
                v_b = load(&rhs[j..]);
            }
        }

        debug_assert!(i == st_a || j == st_b);

        // End of main vectorized loop
        // At this point either i_a == st_a, which is the end of the vectorized processing,
        // or i_b == st_b and we are not done processing the vector...
        // so we need to finish it off.
        if i < st_a {
            let remaining_rhs = &rhs[j..];
            if !remaining_rhs.is_empty() {
                let mut buffer: [u16; 8] = [0; 8]; // buffer to do a masked load
                buffer[..remaining_rhs.len()].copy_from_slice(remaining_rhs);
                // Ensure the buffer is filled with a value we should remove: we do not want to
                // end up trying to remove zero values which aren't actually in rhs
                buffer[remaining_rhs.len()..].fill(remaining_rhs[0]);
                v_b = Simd::from_array(buffer);
                let a_found_in_b: u8 = matrix_cmp_u16(v_a, v_b).to_bitmask() as u8;
                runningmask_a_found_in_b |= a_found_in_b;
                let [.., max_va] = *v_a.as_array();
                let used_rhs = remaining_rhs.partition_point(|&b| b <= max_va);
                j += used_rhs;
            }
            let bitmask_belongs_to_difference: u8 = runningmask_a_found_in_b ^ 0xFF;
            visitor.visit_vector(v_a, bitmask_belongs_to_difference);
            i += u16x8::LEN;
        }
    }

    // do the tail using scalar code
    scalar::sub(&lhs[i..], &rhs[j..], visitor);
}

/// compute the min for each lane in `a` and `b`
#[inline]
fn lanes_min_u16<const LANES: usize>(
    lhs: Simd<u16, LANES>,
    rhs: Simd<u16, LANES>,
) -> Simd<u16, LANES> {
    lhs.simd_le(rhs).select(lhs, rhs)
}

/// compute the max for each lane in `a` and `b`
#[inline]
fn lanes_max_u16<const LANES: usize>(
    lhs: Simd<u16, LANES>,
    rhs: Simd<u16, LANES>,
) -> Simd<u16, LANES> {
    lhs.simd_gt(rhs).select(lhs, rhs)
}

#[inline]
pub fn load<U, const LANES: usize>(src: &[U]) -> Simd<U, LANES>
where
    U: SimdElement + PartialOrd,
{
    debug_assert!(src.len() >= LANES);
    unsafe { load_unchecked(src) }
}

/// write `v` to slice `out` without checking bounds
///
/// ### Safety
///   - The caller must ensure `LANES` does not exceed the allocation for `out`
#[inline]
pub unsafe fn load_unchecked<U, const LANES: usize>(src: &[U]) -> Simd<U, LANES>
where
    U: SimdElement + PartialOrd,
{
    unsafe { core::ptr::read_unaligned(src as *const _ as *const Simd<U, LANES>) }
}

/// write `v` to slice `out`
#[inline]
pub fn store<U, const LANES: usize>(v: Simd<U, LANES>, out: &mut [U])
where
    U: SimdElement + PartialOrd,
{
    debug_assert!(out.len() >= LANES);
    unsafe {
        store_unchecked(v, out);
    }
}

/// write `v` to slice `out` without checking bounds
///
/// ### Safety
///   - The caller must ensure `LANES` does not exceed the allocation for `out`
#[inline]
unsafe fn store_unchecked<U, const LANES: usize>(v: Simd<U, LANES>, out: &mut [U])
where
    U: SimdElement + PartialOrd,
{
    unsafe { core::ptr::write_unaligned(out as *mut _ as *mut Simd<U, LANES>, v) }
}

/// Compare all lanes in `a` to all lanes in `b`
///
/// Returns result mask will be set if any lane at `a[i]` is in any lane of `b`
///
/// ### Example
/// ```ignore
/// let a = Simd::from_array([1, 2, 3, 4, 32, 33, 34, 35]);
/// let b = Simd::from_array([2, 4, 6, 8, 10, 12, 14, 16]);
/// let result = matrix_cmp_u16(a, b);
/// assert_eq!(result, Mask::from_array([false, true, false, true, false, false, false, false]));
/// ```
#[inline]
// It would be nice to implement this for all supported lane counts
// However, we currently only support u16x8 so it's not really necessary
fn matrix_cmp_u16(a: Simd<u16, 8>, b: Simd<u16, 8>) -> Mask<i16, 8> {
    a.simd_eq(b)
        | a.simd_eq(b.rotate_elements_left::<1>())
        | a.simd_eq(b.rotate_elements_left::<2>())
        | a.simd_eq(b.rotate_elements_left::<3>())
        | a.simd_eq(b.rotate_elements_left::<4>())
        | a.simd_eq(b.rotate_elements_left::<5>())
        | a.simd_eq(b.rotate_elements_left::<6>())
        | a.simd_eq(b.rotate_elements_left::<7>())
}

use crate::bitmap::store::array_store::visitor::BinaryOperationVisitor;
use core::simd::Swizzle;

/// Append to vectors to an imaginary 16 lane vector,  shift the lanes right by 1, then
/// truncate to the low order 8 lanes
pub struct Shr1;
impl Swizzle<8> for Shr1 {
    const INDEX: [usize; 8] = [15, 0, 1, 2, 3, 4, 5, 6];
}

/// Append to vectors to an imaginary 16 lane vector,  shift the lanes right by 2, then
/// truncate to the low order 8 lanes
pub struct Shr2;
impl Swizzle<8> for Shr2 {
    const INDEX: [usize; 8] = [14, 15, 0, 1, 2, 3, 4, 5];
}

/// Assuming that a and b are sorted, returns an array of the sorted output.
/// Developed originally for merge sort using SIMD instructions.
/// Standard merge. See, e.g., Inoue and Taura, SIMD- and Cache-Friendly
/// Algorithm for Sorting an Array of Structures
#[inline]
fn simd_merge_u16(a: Simd<u16, 8>, b: Simd<u16, 8>) -> [Simd<u16, 8>; 2] {
    let mut tmp: Simd<u16, 8> = lanes_min_u16(a, b);
    let mut max: Simd<u16, 8> = lanes_max_u16(a, b);
    tmp = tmp.rotate_elements_left::<1>();
    let mut min: Simd<u16, 8> = lanes_min_u16(tmp, max);
    for _ in 0..6 {
        max = lanes_max_u16(tmp, max);
        tmp = min.rotate_elements_left::<1>();
        min = lanes_min_u16(tmp, max);
    }
    max = lanes_max_u16(tmp, max);
    min = min.rotate_elements_left::<1>();
    [min, max]
}

/// Move the values in `val` with the corresponding index in `bitmask`
/// set to the front of the return vector, preserving their order.
///
/// The values in the return vector after index bitmask.count_ones() is unspecified.
// Dynamic swizzle is only available for `u8`s.
//
// So we need to convert the `u16x8` to `u8x16`, and then swizzle it two lanes at a time.
//
// e.g. if `bitmask` is `0b0101`, then swizzle the first two bytes (the first u16 lane) to the
// first two positions, and the 5th and 6th bytes (the third u16 lane) to the next two positions.
//
// Note however:
// https://github.com/rust-lang/rust/blob/34097a38afc9efdedf776d3f1c84a190ff334886/library/portable-simd/crates/core_simd/src/swizzle_dyn.rs#L12-L15
// > Note that the current implementation is selected during build-time
// > of the standard library, so `cargo build -Zbuild-std` may be necessary
// > to unlock better performance, especially for larger vectors.
// > A planned compiler improvement will enable using `#[target_feature]` instead.
pub fn swizzle_to_front(val: u16x8, bitmask: u8) -> u16x8 {
    static SWIZZLE_TABLE: [[u8; 16]; 256] = {
        let mut table = [[0; 16]; 256];
        let mut n = 0usize;
        while n < table.len() {
            let mut x = n;
            let mut i = 0;
            while x > 0 {
                let lsb = x.trailing_zeros() as u8;
                x ^= 1 << lsb;
                table[n][i] = lsb * 2; // first byte
                table[n][i + 1] = lsb * 2 + 1; // second byte
                i += 2;
            }
            n += 1;
        }
        table
    };

    // Our swizzle table retains the order of the bytes in the 16 bit lanes, we can
    // stick with native byte order as long as we convert back with native endianness too.
    let val_convert: u8x16 = val.to_ne_bytes();
    let swizzle_idxs = u8x16::from_array(SWIZZLE_TABLE[bitmask as usize]);

    // Because the default `x86_64` target does not enable ssse3 (and without -Zbuild-std`
    // std will not be compiled with it), use a manual swizzle with intrinsics so we can get
    // reasonable performance without requiring the caller to use `-Zbuild-std`.
    #[cfg(all(target_arch = "x86_64", any(target_feature = "ssse3", feature = "std")))]
    {
        let has_ssse3 = {
            #[cfg(target_feature = "ssse3")]
            {
                true
            }
            #[cfg(not(target_feature = "ssse3"))]
            {
                // From above, `feature = std` must be true here, so we can do runtime detection
                std::arch::is_x86_feature_detected!("ssse3")
            }
        };
        if has_ssse3 {
            use core::arch::x86_64::{__m128i, _mm_shuffle_epi8};
            let val_m128 = __m128i::from(val_convert);
            let swizzle_m128 = __m128i::from(swizzle_idxs);
            // SAFETY: We only are in this block if the target supports `ssse3`
            let swizzled_m128 = unsafe { _mm_shuffle_epi8(val_m128, swizzle_m128) };
            return u16x8::from(swizzled_m128);
        }
    }

    let swizzled: u8x16 = val_convert.swizzle_dyn(swizzle_idxs);
    u16x8::from_ne_bytes(swizzled)
}
