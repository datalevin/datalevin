//! JavaFastPFOR IntCompressor's default SkippableComposition(BinaryPacking,
//! VariableByte). The length is the first int; groups of 32 use little-bit-order
//! packing; four groups share one big-endian width word. The tail uses stop-bit
//! VariableByte, four little-endian bytes per int. No alternative wire format.
use super::ErrorKind;

// The wire is JavaFastPFOR BinaryPacking. Specialize each legal bit width so
// element offsets and cross-word shifts are constants, as in its Java kernels.
macro_rules! each_element {
    ($emit:ident) => {
        $emit!(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31
        );
    };
}

macro_rules! by_width {
    ($width:expr, $kernel:ident, $input:expr, $output:expr) => {
        match $width {
            0 => $kernel::<0>($input, $output),
            1 => $kernel::<1>($input, $output),
            2 => $kernel::<2>($input, $output),
            3 => $kernel::<3>($input, $output),
            4 => $kernel::<4>($input, $output),
            5 => $kernel::<5>($input, $output),
            6 => $kernel::<6>($input, $output),
            7 => $kernel::<7>($input, $output),
            8 => $kernel::<8>($input, $output),
            9 => $kernel::<9>($input, $output),
            10 => $kernel::<10>($input, $output),
            11 => $kernel::<11>($input, $output),
            12 => $kernel::<12>($input, $output),
            13 => $kernel::<13>($input, $output),
            14 => $kernel::<14>($input, $output),
            15 => $kernel::<15>($input, $output),
            16 => $kernel::<16>($input, $output),
            17 => $kernel::<17>($input, $output),
            18 => $kernel::<18>($input, $output),
            19 => $kernel::<19>($input, $output),
            20 => $kernel::<20>($input, $output),
            21 => $kernel::<21>($input, $output),
            22 => $kernel::<22>($input, $output),
            23 => $kernel::<23>($input, $output),
            24 => $kernel::<24>($input, $output),
            25 => $kernel::<25>($input, $output),
            26 => $kernel::<26>($input, $output),
            27 => $kernel::<27>($input, $output),
            28 => $kernel::<28>($input, $output),
            29 => $kernel::<29>($input, $output),
            30 => $kernel::<30>($input, $output),
            31 => $kernel::<31>($input, $output),
            32 => $kernel::<32>($input, $output),
            _ => unreachable!("validated bit width"),
        }
    };
}

#[inline]
fn pack<const WIDTH: usize>(values: &[i32], output: &mut [i32]) {
    let values: &[i32; 32] = values.try_into().unwrap();
    let output: &mut [i32; WIDTH] = output.try_into().unwrap();
    if WIDTH == 0 {
        return;
    }
    if WIDTH == 32 {
        output.copy_from_slice(values);
        return;
    }
    macro_rules! put {
        ($($i:expr),*) => { $(
            let bit = $i * WIDTH;
            let shift = bit % 32;
            let value = values[$i] as u32;
            output[bit / 32] |= (value << shift) as i32;
            if shift + WIDTH > 32 {
                output[bit / 32 + 1] |= (value >> (32 - shift)) as i32;
            }
        )* };
    }
    each_element!(put);
}

#[inline]
fn unpack<const WIDTH: usize>(words: &[i32], output: &mut [i32]) {
    let words: &[i32; WIDTH] = words.try_into().unwrap();
    let output: &mut [i32; 32] = output.try_into().unwrap();
    if WIDTH == 0 {
        output.fill(0);
        return;
    }
    if WIDTH == 32 {
        output.copy_from_slice(words);
        return;
    }
    macro_rules! get {
        ($($i:expr),*) => { $(
            let bit = $i * WIDTH;
            let shift = bit % 32;
            let mut value = words[bit / 32] as u32 >> shift;
            if shift + WIDTH > 32 {
                value |= (words[bit / 32 + 1] as u32) << (32 - shift);
            }
            output[$i] = (value & (u32::MAX >> (32 - WIDTH))) as i32;
        )* };
    }
    each_element!(get);
}

pub(super) fn compress(input: &[i32]) -> Vec<i32> {
    let mut out = Vec::with_capacity(input.len() + input.len() / 32 + 12);
    out.push(input.len() as i32);
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
            by_width!(width, pack, &input[pos..pos + 32], &mut out[start..]);
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
            let start = out.len();
            out.resize(start + 32, 0);
            by_width!(width, unpack, words, &mut out[start..]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_bit_width_matches_independent_bit_stream() {
        for width in 0..=32 {
            let mask = u32::MAX.checked_shr(32 - width).unwrap_or(0);
            let values = std::array::from_fn::<_, 32, _>(|i| {
                (if i == 0 {
                    mask
                } else {
                    (i as u32).wrapping_mul(0x9e3779b9) & mask
                }) as i32
            });
            // Build the expected stream bit by bit, independently of word
            // shifts or the optimized constant-width packing expressions.
            let mut expected = vec![0i32; width as usize];
            for (index, value) in values.iter().enumerate() {
                for bit in 0..width as usize {
                    let position = index * width as usize + bit;
                    if (*value as u32 >> bit) & 1 != 0 {
                        expected[position / 32] |= (1u32 << (position % 32)) as i32;
                    }
                }
            }
            let mut actual = vec![0; width as usize];
            by_width!(width, pack, &values, &mut actual);
            assert_eq!(actual, expected, "width {width}");
            let mut decoded = [i32::MIN; 32];
            by_width!(width, unpack, &expected, &mut decoded);
            assert_eq!(decoded, values, "width {width}");
        }
        assert_eq!(decompress(&[32, 33]), Err(ErrorKind::InvalidValue));
        assert_eq!(decompress(&[32, 32, 0]), Err(ErrorKind::Truncated));
    }
}
