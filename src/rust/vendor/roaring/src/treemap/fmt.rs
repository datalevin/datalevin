use core::fmt;

use crate::RoaringTreemap;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

impl fmt::Debug for RoaringTreemap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.len() < 16 {
            write!(f, "RoaringTreemap<{:?}>", self.iter().collect::<Vec<u64>>())
        } else {
            write!(
                f,
                "RoaringTreemap<{:?} values between {:?} and {:?}>",
                self.len(),
                self.min().unwrap(),
                self.max().unwrap()
            )
        }
    }
}
