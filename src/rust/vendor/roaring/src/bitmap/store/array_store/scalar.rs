//! Scalar arithmetic binary set operations on `ArrayStore`'s inner types

use crate::bitmap::store::array_store::visitor::BinaryOperationVisitor;
use core::cmp::Ordering::*;

#[inline]
pub fn or(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // Traverse both arrays
    let mut i = 0;
    let mut j = 0;
    while i < lhs.len() && j < rhs.len() {
        let a = unsafe { lhs.get_unchecked(i) };
        let b = unsafe { rhs.get_unchecked(j) };
        match a.cmp(b) {
            Less => {
                visitor.visit_scalar(*a);
                i += 1;
            }
            Greater => {
                visitor.visit_scalar(*b);
                j += 1;
            }
            Equal => {
                visitor.visit_scalar(*a);
                i += 1;
                j += 1;
            }
        }
    }

    // Store remaining elements of the arrays
    visitor.visit_slice(&lhs[i..]);
    visitor.visit_slice(&rhs[j..]);
}

#[inline]
pub fn and(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // Traverse both arrays
    let mut i = 0;
    let mut j = 0;
    while i < lhs.len() && j < rhs.len() {
        let a = unsafe { lhs.get_unchecked(i) };
        let b = unsafe { rhs.get_unchecked(j) };
        match a.cmp(b) {
            Less => i += 1,
            Greater => j += 1,
            Equal => {
                visitor.visit_scalar(*a);
                i += 1;
                j += 1;
            }
        }
    }
}

#[inline]
pub fn sub(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // Traverse both arrays
    let mut i = 0;
    let mut j = 0;
    while i < lhs.len() && j < rhs.len() {
        let a = unsafe { lhs.get_unchecked(i) };
        let b = unsafe { rhs.get_unchecked(j) };
        match a.cmp(b) {
            Less => {
                visitor.visit_scalar(*a);
                i += 1;
            }
            Greater => j += 1,
            Equal => {
                i += 1;
                j += 1;
            }
        }
    }

    // Store remaining elements of the left array
    visitor.visit_slice(&lhs[i..]);
}

#[inline]
pub fn xor(lhs: &[u16], rhs: &[u16], visitor: &mut impl BinaryOperationVisitor) {
    // Traverse both arrays
    let mut i = 0;
    let mut j = 0;
    while i < lhs.len() && j < rhs.len() {
        let a = unsafe { lhs.get_unchecked(i) };
        let b = unsafe { rhs.get_unchecked(j) };
        match a.cmp(b) {
            Less => {
                visitor.visit_scalar(*a);
                i += 1;
            }
            Greater => {
                visitor.visit_scalar(*b);
                j += 1;
            }
            Equal => {
                i += 1;
                j += 1;
            }
        }
    }

    // Store remaining elements of the arrays
    visitor.visit_slice(&lhs[i..]);
    visitor.visit_slice(&rhs[j..]);
}
