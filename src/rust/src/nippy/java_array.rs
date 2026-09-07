//! Primitive arrays for which Nippy uses Java Object Serialization. Only the
//! standard single-array stream is interpreted; other Serializable data stays
//! opaque. Class descriptors and serialVersionUIDs are checked against JVM
//! fixtures. This does not load classes or implement ObjectInputStream hooks.
use super::{ErrorKind, Value};

fn descriptor(class: &str) -> Option<(&'static [u8; 8], usize)> {
    match class {
        "[Z" => Some((&[0x57, 0x8f, 0x20, 0x39, 0x14, 0xb8, 0x5d, 0xe2], 1)),
        "[S" => Some((&[0xef, 0x83, 0x2e, 0x06, 0xe5, 0x5d, 0xb0, 0xfa], 2)),
        "[C" => Some((&[0xb0, 0x26, 0x66, 0xb0, 0xe2, 0x5d, 0x84, 0xac], 2)),
        "[B" => Some((&[0xac, 0xf3, 0x17, 0xf8, 0x06, 0x08, 0x54, 0xe0], 1)),
        "[I" => Some((&[0x4d, 0xba, 0x60, 0x26, 0x76, 0xea, 0xb2, 0xa5], 4)),
        "[J" => Some((&[0x78, 0x20, 0x04, 0xb5, 0x12, 0xb1, 0x75, 0x93], 8)),
        "[F" => Some((&[0x0b, 0x9c, 0x81, 0x89, 0x22, 0xe0, 0x0c, 0x42], 4)),
        "[D" => Some((&[0x3e, 0xa6, 0x8c, 0x14, 0xab, 0x63, 0x5a, 0x1e], 8)),
        _ => None,
    }
}

pub(super) fn encode(value: &Value) -> Option<(&'static str, Vec<u8>)> {
    let (class, len) = match value {
        Value::BooleanArray(v) => ("[Z", v.len()),
        Value::ShortArray(v) => ("[S", v.len()),
        Value::CharArray(v) => ("[C", v.len()),
        _ => return None,
    };
    let (uid, width) = descriptor(class).unwrap();
    let mut bytes = Vec::with_capacity(27 + len * width);
    bytes.extend_from_slice(&[0xac, 0xed, 0, 5, 0x75, 0x72, 0, 2]);
    bytes.extend_from_slice(class.as_bytes());
    bytes.extend_from_slice(uid);
    bytes.extend_from_slice(&[2, 0, 0, 0x78, 0x70]);
    bytes.extend_from_slice(&(len as i32).to_be_bytes());
    match value {
        Value::BooleanArray(v) => {
            bytes.extend(v.iter().map(|b| u8::from(*b)));
        }
        Value::ShortArray(v) => {
            for n in v {
                bytes.extend_from_slice(&n.to_be_bytes());
            }
        }
        Value::CharArray(v) => {
            for n in v {
                bytes.extend_from_slice(&n.to_be_bytes());
            }
        }
        _ => unreachable!(),
    }
    Some((class, bytes))
}

pub(super) fn decode(
    class: &str,
    bytes: &[u8],
    max_len: usize,
) -> std::result::Result<Option<Value>, ErrorKind> {
    let Some((uid, width)) = descriptor(class) else {
        return Ok(None);
    };
    // Legal custom annotations/reset records need a full Java stream parser;
    // preserve such objects verbatim rather than misinterpreting their bytes.
    if bytes.get(..8) != Some(&[0xac, 0xed, 0, 5, 0x75, 0x72, 0, 2])
        || bytes.get(8..10) != Some(class.as_bytes())
        || bytes.get(10..18) != Some(uid)
        || bytes.get(18..23) != Some(&[2, 0, 0, 0x78, 0x70])
    {
        return Ok(None);
    }
    let n = i32::from_be_bytes(
        bytes
            .get(23..27)
            .ok_or(ErrorKind::Truncated)?
            .try_into()
            .unwrap(),
    );
    let n = usize::try_from(n).map_err(|_| ErrorKind::InvalidLength)?;
    if n > max_len {
        return Err(ErrorKind::LimitExceeded);
    }
    if bytes.len() != 27 + n * width {
        return Err(ErrorKind::InvalidLength);
    }
    let data = &bytes[27..];
    Ok(Some(match class {
        "[Z" => Value::BooleanArray(data.iter().map(|b| *b != 0).collect()),
        "[S" => Value::ShortArray(
            data.as_chunks::<2>()
                .0
                .iter()
                .map(|b| i16::from_be_bytes(*b))
                .collect(),
        ),
        "[C" => Value::CharArray(
            data.as_chunks::<2>()
                .0
                .iter()
                .map(|b| u16::from_be_bytes(*b))
                .collect(),
        ),
        "[B" => Value::Bytes(data.to_vec()),
        "[I" => Value::IntArray(
            data.as_chunks::<4>()
                .0
                .iter()
                .map(|b| i32::from_be_bytes(*b))
                .collect(),
        ),
        "[J" => Value::LongArray(
            data.as_chunks::<8>()
                .0
                .iter()
                .map(|b| i64::from_be_bytes(*b))
                .collect(),
        ),
        "[F" => Value::FloatArray(
            data.as_chunks::<4>()
                .0
                .iter()
                .map(|b| u32::from_be_bytes(*b))
                .collect(),
        ),
        "[D" => Value::DoubleArray(
            data.as_chunks::<8>()
                .0
                .iter()
                .map(|b| u64::from_be_bytes(*b))
                .collect(),
        ),
        _ => unreachable!(),
    }))
}
