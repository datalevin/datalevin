use datalevin_codec::nippy::{
    ErrorKind, Limits, Value, fast_freeze, fast_thaw, fast_thaw_with_limits,
};

fn wire(tag: u8, bytes: &[u8]) -> Option<Vec<u8>> {
    let mut result = vec![tag];
    match tag {
        96 => result.push(u8::try_from(bytes.len()).ok()? ^ 0x80),
        105 => result.push(i8::try_from(bytes.len()).ok()? as u8),
        16 => result.extend_from_slice(&i16::try_from(bytes.len()).ok()?.to_be_bytes()),
        13 => result.extend_from_slice(&i32::try_from(bytes.len()).ok()?.to_be_bytes()),
        _ => unreachable!(),
    }
    result.extend_from_slice(bytes);
    Some(result)
}

fn check_utf8(bytes: &[u8]) {
    for tag in [96, 105, 16, 13] {
        let Some(wire) = wire(tag, bytes) else {
            continue;
        };
        match std::str::from_utf8(bytes) {
            Ok(text) => assert_eq!(fast_thaw(&wire).unwrap(), Value::Text(text.into())),
            Err(_) => {
                let error = fast_thaw(&wire).unwrap_err();
                assert_eq!(error.kind, ErrorKind::InvalidUtf8);
                assert_eq!(error.offset, wire.len());
            }
        }
    }
}

#[test]
fn ascii_and_unicode_match_checked_utf8_at_word_and_length_boundaries() {
    for length in (0..=65).chain([127, 128, 255, 256, 257, 1023, 1024, 1025, 32768, 65536]) {
        let bytes: Vec<_> = (0..length).map(|i| (i * 17 % 128) as u8).collect();
        check_utf8(&bytes);
        for unicode in ["λ", "ಬಾ", "🙂"] {
            let mut mixed = bytes.clone();
            mixed.extend_from_slice(unicode.as_bytes());
            mixed.extend_from_slice(b"suffix");
            check_utf8(&mixed);
        }
    }
}

#[test]
fn ascii_fast_path_does_not_accept_invalid_utf8() {
    for prefix in [0, 7, 8, 15, 16, 31, 32, 63, 64, 255] {
        for byte in 0..=255 {
            let mut bytes = vec![b'x'; prefix];
            bytes.push(byte);
            check_utf8(&bytes);
        }
        for invalid in [
            &b"\xc0\x80"[..], // Overlong encoding.
            &b"\xe0\x80\x80"[..],
            &b"\xed\xa0\x80"[..],     // Surrogate.
            &b"\xf4\x90\x80\x80"[..], // Above U+10FFFF.
            &b"\xf5\x80\x80\x80"[..],
            &b"\xf0\x90\x80"[..], // Incomplete sequence.
            &b"\xe2\x28\xa1"[..], // Invalid continuation.
        ] {
            let mut bytes = vec![b'x'; prefix];
            bytes.extend_from_slice(invalid);
            bytes.extend_from_slice(b"suffix");
            check_utf8(&bytes);
        }
    }
}

#[test]
fn string_fast_path_preserves_allocation_limits_and_truncation() {
    for text in ["", "abc", "a\0b", "λ🙂"] {
        for tag in [96, 105, 16, 13] {
            let wire = wire(tag, text.as_bytes()).unwrap();
            let limits = Limits {
                max_allocation_bytes: std::mem::size_of::<Value>() + text.len(),
                ..Limits::default()
            };
            assert_eq!(
                fast_thaw_with_limits(&wire, limits).unwrap(),
                Value::Text(text.into())
            );
            assert_eq!(
                fast_thaw_with_limits(
                    &wire,
                    Limits {
                        max_allocation_bytes: limits.max_allocation_bytes - 1,
                        ..limits
                    }
                )
                .unwrap_err()
                .kind,
                ErrorKind::LimitExceeded
            );
            for prefix in 0..wire.len() {
                assert_eq!(
                    fast_thaw(&wire[..prefix]).unwrap_err().kind,
                    ErrorKind::Truncated
                );
            }
        }
    }
}

#[test]
fn string_collections_obey_exact_budgets_across_reservation_boundaries() {
    for count in [4, 255, 256, 257, 512, 1024, 1025, 2048] {
        let value = Value::Vector(vec![Value::Text("abc".into()); count]);
        let wire = fast_freeze(&value).unwrap();
        let limits = Limits {
            max_values: count + 1,
            max_allocation_bytes: (count + 1) * std::mem::size_of::<Value>() + count * 3,
            ..Limits::default()
        };
        assert_eq!(fast_thaw_with_limits(&wire, limits).unwrap(), value);
        for insufficient in [
            Limits {
                max_values: count,
                ..limits
            },
            Limits {
                max_allocation_bytes: limits.max_allocation_bytes - 1,
                ..limits
            },
            Limits {
                max_values: 1,
                ..limits
            },
            Limits {
                max_allocation_bytes: std::mem::size_of::<Value>(),
                ..limits
            },
        ] {
            assert_eq!(
                fast_thaw_with_limits(&wire, insufficient).unwrap_err().kind,
                ErrorKind::LimitExceeded
            );
        }
    }
}
