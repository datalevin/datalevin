use std::io::Cursor;

fn compare(members: impl IntoIterator<Item = u32>) {
    let members: Vec<_> = members.into_iter().collect();
    let mut patched: roaring::RoaringBitmap = members.iter().copied().collect();
    let mut upstream: roaring_upstream::RoaringBitmap = members.iter().copied().collect();
    for optimize in [false, true] {
        if optimize {
            patched.optimize();
            upstream.optimize();
        }
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        patched.serialize_into(&mut actual).unwrap();
        upstream.serialize_into(&mut expected).unwrap();
        assert_eq!(actual, expected);
        let decoded = roaring::RoaringBitmap::deserialize_from(Cursor::new(&expected)).unwrap();
        let oracle =
            roaring_upstream::RoaringBitmap::deserialize_from(Cursor::new(&actual)).unwrap();
        assert!(decoded.iter().eq(oracle.iter()));
        assert!(decoded.iter().eq(patched.iter()));
    }
}

#[test]
fn portable_bytes_match_unpatched_roaring() {
    for n in [0, 1, 7, 8, 15, 16, 31, 32, 4095, 4096, 4097, 65536] {
        compare(0..n);
        compare((0..n).map(|i| i * 65537));
    }
    for containers in [1, 3, 4, 63, 64, 255, 256] {
        compare((0..containers).flat_map(|key| (0..33).map(move |i| (key << 16) | (i * 1009))));
    }
    compare([0, 65535, 65536, u32::MAX - 1, u32::MAX]);
}

#[test]
fn malformed_arrays_retain_upstream_validation_and_first_error() {
    for len in [2, 8, 9, 16, 17, 32, 33, 255, 4096] {
        let bitmap: roaring_upstream::RoaringBitmap = (0..len).map(|i| i * 2 + 2).collect();
        let mut valid = Vec::new();
        bitmap.serialize_into(&mut valid).unwrap();
        // No-run cookie, count, descriptor, offset, then little-endian u16s.
        assert_eq!(valid.len(), 16 + len as usize * 2);
        for index in [1, 7, 8, 15, 16, 31, 32, len - 1] {
            if index >= len {
                continue;
            }
            for duplicate in [false, true] {
                let mut bytes = valid.clone();
                let offset = 16 + index as usize * 2;
                let previous = index as u16 * 2;
                let bad = if duplicate { previous } else { previous - 1 };
                bytes[offset..offset + 2].copy_from_slice(&bad.to_le_bytes());
                let actual =
                    roaring::RoaringBitmap::deserialize_from(Cursor::new(&bytes)).unwrap_err();
                let expected =
                    roaring_upstream::RoaringBitmap::deserialize_from(Cursor::new(&bytes))
                        .unwrap_err();
                assert_eq!(actual.kind(), expected.kind());
                assert_eq!(actual.to_string(), expected.to_string());
            }
        }
        for length in [0, 1, 7, 8, 15, valid.len() - 1] {
            assert!(roaring::RoaringBitmap::deserialize_from(&valid[..length]).is_err());
            assert!(roaring_upstream::RoaringBitmap::deserialize_from(&valid[..length]).is_err());
        }
    }
}
