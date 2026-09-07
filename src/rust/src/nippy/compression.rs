use super::*;
use std::io::{Read, Write};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Snappy,
    Zstd,
    Lzma2,
}

/// Uses Nippy's standard NPY header and compressor framing, without encryption.
pub fn freeze(value: &Value, compression: Compression) -> Result<Vec<u8>> {
    let raw = fast_freeze(value)?;
    let err = |_| Error::new(ErrorKind::Compression, 4);
    let mut out = b"NPY".to_vec();
    match compression {
        Compression::None => {
            out.push(0);
            out.extend_from_slice(&raw);
        }
        Compression::Lz4 => {
            out.push(8);
            out.extend_from_slice(&(raw.len() as u32).to_be_bytes());
            out.extend(lz4_flex::block::compress(&raw));
        }
        Compression::Snappy => {
            out.push(1);
            out.extend(
                snap::raw::Encoder::new()
                    .compress_vec(&raw)
                    .map_err(|_| Error::new(ErrorKind::Compression, 4))?,
            );
        }
        Compression::Zstd => {
            out.push(20);
            out.extend(zstd::bulk::compress(&raw, 3).map_err(err)?);
        }
        Compression::Lzma2 => {
            out.push(11);
            out.extend_from_slice(&(raw.len() as u32).to_be_bytes());
            let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 0);
            encoder.write_all(&raw).map_err(err)?;
            out.extend(encoder.finish().map_err(err)?);
        }
    }
    Ok(out)
}

pub fn thaw(bytes: &[u8]) -> Result<Value> {
    thaw_with_limits(bytes, Limits::default())
}

/// Decodes a header-bearing payload. Encrypted/custom compressor headers fail
/// explicitly; a caller must supply those application-specific transforms.
pub fn thaw_with_limits(bytes: &[u8], limits: Limits) -> Result<Value> {
    if bytes.len() > limits.max_bytes {
        return Err(Error::new(ErrorKind::LimitExceeded, 0));
    }
    if bytes.len() < 4 {
        return Err(Error::new(ErrorKind::Truncated, bytes.len()));
    }
    if &bytes[..3] != b"NPY" {
        return Err(Error::new(ErrorKind::InvalidValue, 0));
    }
    let tag = bytes[3];
    let data = &bytes[4..];
    if tag == 0 {
        return fast_thaw_with_limits(data, limits).map_err(|mut e| {
            e.offset += 4;
            e
        });
    }
    let bad = || Error::new(ErrorKind::Compression, 4);
    let bound = |n: usize| -> Result<usize> {
        if n > limits.max_bytes || n > limits.max_allocation_bytes {
            return Err(Error::new(ErrorKind::LimitExceeded, 4));
        }
        Ok(n)
    };
    let prefixed_size = || -> Result<usize> {
        let n = i32::from_be_bytes(data.get(..4).ok_or_else(bad)?.try_into().unwrap());
        bound(usize::try_from(n).map_err(|_| bad())?)
    };
    let raw = match tag {
        8 => {
            let n = prefixed_size()?;
            let mut out = vec![0; n];
            let used = lz4_flex::block::decompress_into(&data[4..], &mut out).map_err(|_| bad())?;
            if used != n {
                return Err(bad());
            }
            out
        }
        1 => {
            let n = bound(snap::raw::decompress_len(data).map_err(|_| bad())?)?;
            let mut out = vec![0; n];
            if snap::raw::Decoder::new()
                .decompress(data, &mut out)
                .map_err(|_| bad())?
                != n
            {
                return Err(bad());
            }
            out
        }
        20 => {
            if zstd::zstd_safe::find_frame_compressed_size(data).map_err(|_| bad())? != data.len() {
                return Err(bad());
            }
            let max = limits.max_bytes.min(limits.max_allocation_bytes);
            let n = match zstd::zstd_safe::get_frame_content_size(data).map_err(|_| bad())? {
                Some(n) => bound(usize::try_from(n).map_err(|_| bad())?)?,
                None => max,
            };
            zstd::bulk::decompress(data, n).map_err(|_| bad())?
        }
        11 => {
            let n = prefixed_size()?;
            let stream =
                xz2::stream::Stream::new_stream_decoder(limits.max_allocation_bytes as u64, 0)
                    .map_err(|_| bad())?;
            let mut decoder = xz2::read::XzDecoder::new_stream(&data[4..], stream);
            let mut out = Vec::with_capacity(n.min(65536));
            (&mut decoder)
                .take(n as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|_| bad())?;
            if out.len() != n || decoder.total_in() as usize != data.len() - 4 {
                return Err(bad());
            }
            out
        }
        _ => return Err(Error::new(ErrorKind::UnsupportedHeader(tag), 3)),
    };
    let limits = Limits {
        max_allocation_bytes: limits.max_allocation_bytes.saturating_sub(raw.len()),
        ..limits
    };
    // Inner offsets address decompressed bytes, not positions in the input.
    // Report the compressed payload's start consistently for this public API.
    fast_thaw_with_limits(&raw, limits).map_err(|mut error| {
        error.offset = 4;
        error
    })
}
