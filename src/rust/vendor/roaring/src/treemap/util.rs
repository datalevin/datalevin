use core::ops::{Bound, RangeBounds, RangeInclusive};

#[inline]
pub fn split(value: u64) -> (u32, u32) {
    ((value >> 32) as u32, value as u32)
}

#[inline]
pub fn join(high: u32, low: u32) -> u64 {
    (u64::from(high) << 32) | u64::from(low)
}

/// Convert a `RangeBounds<u64>` object to `RangeInclusive<u64>`,
pub fn convert_range_to_inclusive<R>(range: R) -> Option<RangeInclusive<u64>>
where
    R: RangeBounds<u64>,
{
    let start: u64 = match range.start_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&u64::MAX) => return None,
        Bound::Excluded(&i) => i + 1,
        Bound::Unbounded => 0,
    };
    let end: u64 = match range.end_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&0) => return None,
        Bound::Excluded(&i) => i - 1,
        Bound::Unbounded => u64::MAX,
    };
    if end < start {
        return None;
    }
    Some(start..=end)
}

#[cfg(test)]
mod test {
    use super::{convert_range_to_inclusive, join, split};

    #[test]
    fn test_split_u64() {
        assert_eq!((0x0000_0000u32, 0x0000_0000u32), split(0x0000_0000_0000_0000u64));
        assert_eq!((0x0000_0000u32, 0x0000_0001u32), split(0x0000_0000_0000_0001u64));
        assert_eq!((0x0000_0000u32, 0xFFFF_FFFEu32), split(0x0000_0000_FFFF_FFFEu64));
        assert_eq!((0x0000_0000u32, 0xFFFF_FFFFu32), split(0x0000_0000_FFFF_FFFFu64));
        assert_eq!((0x0000_0001u32, 0x0000_0000u32), split(0x0000_0001_0000_0000u64));
        assert_eq!((0x0000_0001u32, 0x0000_0001u32), split(0x0000_0001_0000_0001u64));
        assert_eq!((0xFFFF_FFFFu32, 0xFFFF_FFFEu32), split(0xFFFF_FFFF_FFFF_FFFEu64));
        assert_eq!((0xFFFF_FFFFu32, 0xFFFF_FFFFu32), split(0xFFFF_FFFF_FFFF_FFFFu64));
    }

    #[test]
    fn test_join_u64() {
        assert_eq!(0x0000_0000_0000_0000u64, join(0x0000_0000u32, 0x0000_0000u32));
        assert_eq!(0x0000_0000_0000_0001u64, join(0x0000_0000u32, 0x0000_0001u32));
        assert_eq!(0x0000_0000_FFFF_FFFEu64, join(0x0000_0000u32, 0xFFFF_FFFEu32));
        assert_eq!(0x0000_0000_FFFF_FFFFu64, join(0x0000_0000u32, 0xFFFF_FFFFu32));
        assert_eq!(0x0000_0001_0000_0000u64, join(0x0000_0001u32, 0x0000_0000u32));
        assert_eq!(0x0000_0001_0000_0001u64, join(0x0000_0001u32, 0x0000_0001u32));
        assert_eq!(0xFFFF_FFFF_FFFF_FFFEu64, join(0xFFFF_FFFFu32, 0xFFFF_FFFEu32));
        assert_eq!(0xFFFF_FFFF_FFFF_FFFFu64, join(0xFFFF_FFFFu32, 0xFFFF_FFFFu32));
    }

    #[test]
    fn test_convert_range_to_inclusive() {
        assert_eq!(Some(1..=5), convert_range_to_inclusive(1..6));
        assert_eq!(Some(1..=u64::MAX), convert_range_to_inclusive(1..));
        assert_eq!(Some(0..=u64::MAX), convert_range_to_inclusive(..));
        assert_eq!(None, convert_range_to_inclusive(5..5));
        assert_eq!(Some(16..=16), convert_range_to_inclusive(16..=16))
    }
}
