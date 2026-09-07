//! Bound the portable Roaring record before handing it to the standard library.
use super::ErrorKind;

pub(super) fn read(
    bytes: &[u8],
    max_members: usize,
    allocation_budget: usize,
) -> std::result::Result<(roaring::RoaringBitmap, usize, usize), ErrorKind> {
    let u16_at = |i: usize| -> std::result::Result<usize, ErrorKind> {
        Ok(u16::from_le_bytes(
            bytes
                .get(i..i + 2)
                .ok_or(ErrorKind::Truncated)?
                .try_into()
                .unwrap(),
        ) as usize)
    };
    let u32_at = |i: usize| -> std::result::Result<usize, ErrorKind> {
        Ok(u32::from_le_bytes(
            bytes
                .get(i..i + 4)
                .ok_or(ErrorKind::Truncated)?
                .try_into()
                .unwrap(),
        ) as usize)
    };
    let cookie = u32_at(0)?;
    let runs = cookie & 0xffff == 12347;
    if !runs && cookie != 12346 {
        return Err(ErrorKind::InvalidValue);
    }
    let count = if runs { (cookie >> 16) + 1 } else { u32_at(4)? };
    if count > 65536 {
        return Err(ErrorKind::InvalidLength);
    }
    let flags = if runs { 4 } else { 8 };
    let keys = flags + if runs { count.div_ceil(8) } else { 0 };
    let has_offsets = !runs || count >= 4;
    let offsets = keys + count * 4;
    let mut position = offsets + if has_offsets { count * 4 } else { 0 };
    if position > bytes.len() {
        return Err(ErrorKind::Truncated);
    }
    let mut cardinality = 0usize;
    let descriptions = bytes[keys..offsets].as_chunks::<4>().0;
    for (i, description) in descriptions.iter().enumerate() {
        let cardinal = u16::from_le_bytes([description[2], description[3]]) as usize + 1;
        cardinality += cardinal;
        if cardinality > max_members {
            return Err(ErrorKind::LimitExceeded);
        }
        // The checked library reader validates key ordering and container data.
        // Offsets are ignored by that reader, so validate them here.
        if has_offsets && u32_at(offsets + i * 4)? != position {
            return Err(ErrorKind::InvalidValue);
        }
        let run = runs && bytes[flags + i / 8] & (1 << (i % 8)) != 0;
        position += if run {
            2 + u16_at(position)? * 4
        } else if cardinal <= 4096 {
            cardinal * 2
        } else {
            8192
        };
        if position > bytes.len() {
            return Err(ErrorKind::Truncated);
        }
    }
    // Account for the library's container table, descriptions, and temporary
    // run/array storage before it allocates anything from the wire's lengths.
    let allocation = position * 2 + count * 128;
    if allocation > allocation_budget {
        return Err(ErrorKind::LimitExceeded);
    }
    let mut input = &bytes[..position];
    let bitmap = roaring::RoaringBitmap::deserialize_from(&mut input)
        .map_err(|_| ErrorKind::InvalidValue)?;
    if !input.is_empty() || bitmap.len() as usize != cardinality {
        return Err(ErrorKind::InvalidValue);
    }
    Ok((bitmap, position, allocation))
}
