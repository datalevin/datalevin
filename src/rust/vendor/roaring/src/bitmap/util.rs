use core::ops::{Bound, RangeBounds, RangeInclusive};

/// Returns the container key and the index
/// in this container for a given integer.
#[inline]
pub fn split(value: u32) -> (u16, u16) {
    ((value >> 16) as u16, value as u16)
}

/// Returns the original integer from the container
/// key and the index of it in the container.
#[inline]
pub fn join(high: u16, low: u16) -> u32 {
    (u32::from(high) << 16) + u32::from(low)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConvertRangeError {
    Empty,
    StartGreaterThanEnd,
    StartAndEndEqualExcluded,
}

/// Convert a `RangeBounds<u32>` object to `RangeInclusive<u32>`,
pub fn convert_range_to_inclusive<R>(range: R) -> Result<RangeInclusive<u32>, ConvertRangeError>
where
    R: RangeBounds<u32>,
{
    let start_bound = range.start_bound().cloned();
    let end_bound = range.end_bound().cloned();
    match (start_bound, end_bound) {
        (Bound::Excluded(s), Bound::Excluded(e)) if s == e => {
            Err(ConvertRangeError::StartAndEndEqualExcluded)
        }
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e))
            if s > e =>
        {
            Err(ConvertRangeError::StartGreaterThanEnd)
        }
        _ => {
            let start = match start_bound {
                Bound::Included(s) => s,
                Bound::Excluded(s) => s.checked_add(1).ok_or(ConvertRangeError::Empty)?,
                Bound::Unbounded => 0,
            };

            let end = match end_bound {
                Bound::Included(e) => e,
                Bound::Excluded(e) => e.checked_sub(1).ok_or(ConvertRangeError::Empty)?,
                Bound::Unbounded => u32::MAX,
            };

            if start > end {
                // This handles e.g. `x..x`: we've ruled out `start > end` overall, so a value must
                // have been changed via exclusion.
                Err(ConvertRangeError::Empty)
            } else {
                Ok(start..=end)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{convert_range_to_inclusive, join, split, ConvertRangeError};
    use core::ops::Bound;

    #[test]
    fn test_split_u32() {
        assert_eq!((0x0000u16, 0x0000u16), split(0x0000_0000u32));
        assert_eq!((0x0000u16, 0x0001u16), split(0x0000_0001u32));
        assert_eq!((0x0000u16, 0xFFFEu16), split(0x0000_FFFEu32));
        assert_eq!((0x0000u16, 0xFFFFu16), split(0x0000_FFFFu32));
        assert_eq!((0x0001u16, 0x0000u16), split(0x0001_0000u32));
        assert_eq!((0x0001u16, 0x0001u16), split(0x0001_0001u32));
        assert_eq!((0xFFFFu16, 0xFFFEu16), split(0xFFFF_FFFEu32));
        assert_eq!((0xFFFFu16, 0xFFFFu16), split(0xFFFF_FFFFu32));
    }

    #[test]
    fn test_join_u32() {
        assert_eq!(0x0000_0000u32, join(0x0000u16, 0x0000u16));
        assert_eq!(0x0000_0001u32, join(0x0000u16, 0x0001u16));
        assert_eq!(0x0000_FFFEu32, join(0x0000u16, 0xFFFEu16));
        assert_eq!(0x0000_FFFFu32, join(0x0000u16, 0xFFFFu16));
        assert_eq!(0x0001_0000u32, join(0x0001u16, 0x0000u16));
        assert_eq!(0x0001_0001u32, join(0x0001u16, 0x0001u16));
        assert_eq!(0xFFFF_FFFEu32, join(0xFFFFu16, 0xFFFEu16));
        assert_eq!(0xFFFF_FFFFu32, join(0xFFFFu16, 0xFFFFu16));
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn test_convert_range_to_inclusive() {
        assert_eq!(Ok(1..=5), convert_range_to_inclusive(1..6));
        assert_eq!(Ok(1..=u32::MAX), convert_range_to_inclusive(1..));
        assert_eq!(Ok(0..=u32::MAX), convert_range_to_inclusive(..));
        assert_eq!(Ok(16..=16), convert_range_to_inclusive(16..=16));
        assert_eq!(
            Ok(11..=19),
            convert_range_to_inclusive((Bound::Excluded(10), Bound::Excluded(20)))
        );

        assert_eq!(Err(ConvertRangeError::Empty), convert_range_to_inclusive(0..0));
        assert_eq!(Err(ConvertRangeError::Empty), convert_range_to_inclusive(5..5));
        assert_eq!(Err(ConvertRangeError::StartGreaterThanEnd), convert_range_to_inclusive(1..0));
        assert_eq!(Err(ConvertRangeError::StartGreaterThanEnd), convert_range_to_inclusive(10..5));
        assert_eq!(
            Err(ConvertRangeError::Empty),
            convert_range_to_inclusive((Bound::Excluded(u32::MAX), Bound::Included(u32::MAX)))
        );
        assert_eq!(
            Err(ConvertRangeError::StartAndEndEqualExcluded),
            convert_range_to_inclusive((Bound::Excluded(u32::MAX), Bound::Excluded(u32::MAX)))
        );
        assert_eq!(
            Err(ConvertRangeError::Empty),
            convert_range_to_inclusive((Bound::Excluded(0), Bound::Included(0)))
        );
    }
}
