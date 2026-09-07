//! JavaFastPFOR IntCompressor's default SkippableComposition(BinaryPacking,
//! VariableByte). The length is the first int; groups of 32 use little-bit-order
//! packing; four groups share one big-endian width word. The tail uses stop-bit
//! VariableByte, four little-endian bytes per int. No alternative wire format.
use super::ErrorKind;

pub(super) fn compress(input: &[i32]) -> Vec<i32> {
    let mut out = vec![input.len() as i32];
    let packed = input.len() / 32 * 32;
    let mut pos = 0;
    while pos < packed {
        let groups = if packed - pos >= 128 { 4 } else { 1 };
        let mut widths = [0; 4];
        for (g, width) in widths.iter_mut().enumerate().take(groups) {
            *width = 32
                - input[pos + g * 32..pos + (g + 1) * 32]
                    .iter()
                    .fold(0u32, |a, b| a | *b as u32)
                    .leading_zeros();
        }
        out.push(if groups == 4 {
            ((widths[0] << 24) | (widths[1] << 16) | (widths[2] << 8) | widths[3]) as i32
        } else {
            widths[0] as i32
        });
        for &width in widths.iter().take(groups) {
            let start = out.len();
            out.resize(start + width as usize, 0);
            if width != 0 {
                for i in 0..32 {
                    let bit = i * width as usize;
                    let word = start + bit / 32;
                    let shift = bit % 32;
                    let value = input[pos + i] as u32;
                    out[word] |= (value << shift) as i32;
                    if shift + width as usize > 32 {
                        out[word + 1] |= (value >> (32 - shift)) as i32;
                    }
                }
            }
            pos += 32;
        }
    }
    if packed == 0 {
        out.push(0);
    } // SkippableComposition's empty first-codec marker.
    let mut word = 0u32;
    let mut shift = 0;
    for &n in &input[packed..] {
        let mut n = n as u32;
        loop {
            let last = n < 128;
            let byte = (n & 127) | if last { 128 } else { 0 };
            word |= byte << shift;
            shift += 8;
            if shift == 32 {
                out.push(word as i32);
                word = 0;
                shift = 0;
            }
            n >>= 7;
            if last {
                break;
            }
        }
    }
    if shift != 0 {
        out.push(word as i32);
    }
    out
}

/// Caller checks the output length and allocation budget before entry.
pub(super) fn decompress(input: &[i32]) -> std::result::Result<Vec<i32>, ErrorKind> {
    let n = *input.first().ok_or(ErrorKind::Truncated)?;
    let n = usize::try_from(n).map_err(|_| ErrorKind::InvalidLength)?;
    let packed = n / 32 * 32;
    let mut out = Vec::with_capacity(n);
    let mut pos = 1;
    while out.len() < packed {
        let groups = if packed - out.len() >= 128 { 4 } else { 1 };
        let header = *input.get(pos).ok_or(ErrorKind::Truncated)? as u32;
        pos += 1;
        for g in 0..groups {
            let width = if groups == 4 {
                (header >> (24 - g * 8)) & 255
            } else {
                header
            } as usize;
            if width > 32 {
                return Err(ErrorKind::InvalidValue);
            }
            let words = input.get(pos..pos + width).ok_or(ErrorKind::Truncated)?;
            for i in 0..32 {
                let bit = i * width;
                let value = if width == 0 {
                    0
                } else {
                    let word = bit / 32;
                    let shift = bit % 32;
                    let mut value = words[word] as u32 as u64 >> shift;
                    if shift + width > 32 {
                        value |= (words[word + 1] as u32 as u64) << (32 - shift);
                    }
                    (value & ((1u64 << width) - 1)) as u32
                };
                out.push(value as i32);
            }
            pos += width;
        }
    }
    if packed == 0 {
        if input.get(pos) != Some(&0) {
            return Err(ErrorKind::InvalidValue);
        }
        pos += 1;
    }
    let mut byte_pos = pos * 4;
    while out.len() < n {
        let mut value = 0u32;
        for shift in (0..=28).step_by(7) {
            let word = *input.get(byte_pos / 4).ok_or(ErrorKind::Truncated)? as u32;
            let byte = (word >> (byte_pos % 4 * 8)) & 255;
            byte_pos += 1;
            if shift == 28 && byte & 0x70 != 0 {
                return Err(ErrorKind::InvalidValue);
            }
            value |= (byte & 127) << shift;
            if byte & 128 != 0 {
                out.push(value as i32);
                break;
            }
            if shift == 28 {
                return Err(ErrorKind::InvalidValue);
            }
        }
    }
    if byte_pos.div_ceil(4) != input.len() {
        return Err(ErrorKind::TrailingBytes);
    }
    for i in byte_pos..input.len() * 4 {
        if (input[i / 4] as u32 >> (i % 4 * 8)) & 255 != 0 {
            return Err(ErrorKind::InvalidValue);
        }
    }
    Ok(out)
}
