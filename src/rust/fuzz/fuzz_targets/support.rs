use datalevin_codec::nippy::*;

pub fn limits() -> Limits {
    Limits {
        max_bytes: 65536,
        max_depth: 32,
        max_collection_len: 4096,
        max_values: 16384,
        max_allocation_bytes: 1024 * 1024,
    }
}

pub fn check(result: Result<Value>, input_len: usize) {
    match result {
        Ok(value) => {
            let mut bytes = Vec::new();
            Encoder::new(&mut bytes, Limits::default())
                .with_version(WireVersion::V3_9)
                .write_value(&value)
                .expect("decoded value must encode");
            let decoded = fast_thaw(&bytes).expect("encoded value must decode");
            let mut repeated = Vec::new();
            Encoder::new(&mut repeated, Limits::default())
                .with_version(WireVersion::V3_9)
                .write_value(&decoded)
                .unwrap();
            assert_eq!(repeated, bytes);
            let mut caller_owned = vec![0; bytes.len()];
            Encoder::new(&mut caller_owned[..], Limits::default())
                .with_version(WireVersion::V3_9)
                .write_value(&value)
                .unwrap();
            assert_eq!(caller_owned, bytes);
        }
        Err(error) => assert!(error.offset <= input_len),
    }
}
