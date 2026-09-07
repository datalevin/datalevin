use datalevin_codec::nippy::*;
use proptest::prelude::*;
#[path = "../examples/support/mod.rs"]
mod support;

fn read(bytes: &[u8]) -> Result<Value> {
    let mut decoder =
        Decoder::new(bytes, Limits::default())?.with_legacy_deftype("taoensso.nippy.StressType", 2);
    let v = decoder.read_value()?;
    assert_eq!(decoder.position(), bytes.len());
    Ok(v)
}

#[test]
fn checked_in_jvm_corpora_and_independent_values() {
    let expected = support::fixtures();
    for corpus in [
        include_str!("../../../resources/datalevin/nippy/jvm-3.7.0-beta1.tsv"),
        include_str!("../../../resources/datalevin/nippy/jvm-3.9.0.tsv"),
    ] {
        let mut count = 0;
        for line in corpus
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            let (id, hex) = line.split_once('\t').unwrap();
            let bytes = support::unhex(hex).unwrap();
            let value = read(&bytes).unwrap_or_else(|e| panic!("{id}: {e}"));
            if let Some((_, expected)) = expected.iter().find(|(name, _)| *name == id) {
                assert_eq!(&value, expected, "JVM semantic decode: {id}");
            }
            assert_eq!(
                read(&fast_freeze(&value).unwrap()).unwrap(),
                value,
                "Rust re-encode: {id}"
            );
            count += 1;
        }
        assert_eq!(
            count, 121,
            "every native fixture and all stress leaves must be present"
        );
    }
}

#[test]
fn direct_caller_owned_buffers_and_rollback() {
    for (_, value) in support::fixtures() {
        let bytes = fast_freeze(&value).unwrap();
        let mut storage = vec![0xcc; bytes.len() + 8];
        let mut encoder = Encoder::new(&mut storage[4..4 + bytes.len()], Limits::default());
        encoder.write_value(&value).unwrap();
        assert_eq!(encoder.position(), bytes.len());
        assert_eq!(&storage[4..4 + bytes.len()], &bytes);
        assert_eq!(&storage[..4], &[0xcc; 4]);
        assert_eq!(&storage[4 + bytes.len()..], &[0xcc; 4]);
        let mut small = vec![0; bytes.len() - 1];
        assert!(
            Encoder::new(&mut small[..], Limits::default())
                .write_value(&value)
                .is_err()
        );
    }
    let mut out = vec![123];
    assert!(fast_freeze_into(&mut out, &Value::Keyword("k".repeat(32768).into())).is_err());
    assert_eq!(out, [123]);
}

#[test]
fn numeric_and_length_boundaries() {
    for n in [0, 1, 127, 128, 255, 256, 32767, 32768] {
        for v in [
            Value::Text("x".repeat(n)),
            Value::Bytes(vec![5; n]),
            Value::Vector(vec![Value::Null; n]),
        ] {
            assert_eq!(fast_thaw(&fast_freeze(&v).unwrap()).unwrap(), v);
        }
    }
    for n in [
        i64::MIN,
        i64::MAX,
        -4294967296,
        -4294967295,
        -65536,
        -65535,
        -256,
        -255,
        -128,
        -127,
        -1,
        0,
        1,
        127,
        128,
        255,
        256,
        65535,
        65536,
        4294967295,
        4294967296,
    ] {
        assert_eq!(
            fast_thaw(&fast_freeze(&Value::Long(n)).unwrap()).unwrap(),
            Value::Long(n)
        );
    }
}

#[test]
fn malformed_inputs_and_resource_limits() {
    for bytes in [
        vec![],
        vec![3, 3],
        vec![2, 0xff, 0xff, 0xff, 0xff],
        vec![96, 0x81, 0xff],
        vec![59, 59],
        vec![63, 3],
        vec![67, 0xff],
        vec![68, 0x7f, 0xff],
        vec![82, 0, 1],
    ] {
        assert!(fast_thaw(&bytes).is_err(), "{:?}", bytes);
    }
    let mut deep = vec![59]; // cache definition followed by 130 nested vectors
    for _ in 0..130 {
        deep.extend([97, 0x81]);
    }
    deep.push(3);
    assert_eq!(fast_thaw(&deep).unwrap_err().kind, ErrorKind::LimitExceeded);
    let mut bomb = vec![69, 0, 100, 59, 96, 0x7f];
    bomb.extend([b'x'; 255]);
    bomb.extend([59; 99]);
    let limits = Limits {
        max_allocation_bytes: 4096,
        ..Limits::default()
    };
    assert_eq!(
        fast_thaw_with_limits(&bomb, limits).unwrap_err().kind,
        ErrorKind::LimitExceeded
    );
    let huge = [118, 0x7f, 0xff, 0xff, 0xff];
    assert_eq!(fast_thaw(&huge).unwrap_err().kind, ErrorKind::LimitExceeded);
    let bitmap = Value::Bitmap((0..10000).collect());
    assert_eq!(
        fast_thaw_with_limits(&fast_freeze(&bitmap).unwrap(), limits)
            .unwrap_err()
            .kind,
        ErrorKind::LimitExceeded
    );
    for (_, v) in support::fixtures() {
        let bytes = fast_freeze(&v).unwrap();
        for len in 0..bytes.len().min(512) {
            let mut decoder = Decoder::new(&bytes[..len], Limits::default())
                .unwrap()
                .with_legacy_deftype("taoensso.nippy.StressType", 2);
            assert!(decoder.read_value().is_err(), "prefix {len} of {:?}", v);
        }
    }
}

#[test]
fn metadata_and_cache_preserve_nested_values() {
    // cached vector has its own slot reserved before the cached string inside it
    let bytes = support::unhex("715b").unwrap();
    assert!(fast_thaw(&bytes).is_err());
    let bytes = [113, 59, 113, 63, 96, 0x81, b'x', 63, 59];
    let expected = Value::Vector(vec![Value::Vector(vec![Value::Text("x".into()); 2]); 2]);
    assert_eq!(fast_thaw(&bytes).unwrap(), expected);
    let bytes = [113, 59, 3, 59];
    assert_eq!(
        fast_thaw(&bytes).unwrap(),
        Value::Vector(vec![Value::Null; 2])
    );
}

#[test]
fn deftype_versions_require_explicit_legacy_schema() {
    let v = Value::Deftype {
        class: "example.Pair".into(),
        fields: vec![Value::Long(1), Value::Long(2)],
    };
    let legacy = fast_freeze(&v).unwrap();
    assert_eq!(legacy[0], 81);
    assert_eq!(
        fast_thaw(&legacy).unwrap_err().kind,
        ErrorKind::UnsupportedType(81)
    );
    assert_eq!(
        Decoder::new(&legacy, Limits::default())
            .unwrap()
            .with_legacy_deftype("example.Pair", 2)
            .read_value()
            .unwrap(),
        v
    );
    let mut out = Vec::new();
    Encoder::new(&mut out, Limits::default())
        .with_version(WireVersion::V3_9)
        .write_value(&v)
        .unwrap();
    assert_eq!(out[0], 122);
    assert_eq!(fast_thaw(&out).unwrap(), v);
}

#[test]
fn existing_nippy_headers_and_compressor_limits() {
    let v = Value::Vector(vec![Value::Text("compress me".repeat(100)); 32]);
    for c in [
        Compression::None,
        Compression::Lz4,
        Compression::Snappy,
        Compression::Zstd,
        Compression::Lzma2,
    ] {
        let encoded = freeze(&v, c).unwrap();
        assert_eq!(thaw(&encoded).unwrap(), v);
        assert!(thaw(&encoded[..encoded.len() - 1]).is_err());
        assert!(
            thaw_with_limits(
                &encoded,
                Limits {
                    max_bytes: 32,
                    ..Limits::default()
                }
            )
            .is_err()
        );
    }
    for header in [
        2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 255,
    ] {
        assert_eq!(
            thaw(&[b'N', b'P', b'Y', header, 3]).unwrap_err().kind,
            ErrorKind::UnsupportedHeader(header)
        );
    }
}

fn arb_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Long),
        any::<u64>().prop_map(Value::Double),
        ".{0,64}".prop_map(Value::Text),
        "[a-z/]{0,20}".prop_map(|s: String| Value::Keyword(s.into())),
        prop::collection::vec(any::<i32>(), 0..260).prop_map(Value::GrowingIntArray)
    ];
    leaf.prop_recursive(8, 256, 16, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..16).prop_map(Value::Vector),
            prop::collection::vec(inner.clone(), 0..16).prop_map(Value::List),
            prop::collection::vec(inner.clone(), 0..16).prop_map(Value::Set),
            prop::collection::vec((inner.clone(), inner), 0..16).prop_map(Value::Map)
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]
    #[test]
    fn generated_values_roundtrip(value in arb_value()) {
        prop_assert_eq!(fast_thaw(&fast_freeze(&value).unwrap()).unwrap(), value);
    }
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        let limits = Limits { max_bytes: 4096, max_depth: 32, max_collection_len: 256, max_values: 1024, max_allocation_bytes: 65536 };
        for result in [fast_thaw_with_limits(&bytes, limits), thaw_with_limits(&bytes, limits)] {
            match result {
                Ok(v) => prop_assert_eq!(fast_thaw(&fast_freeze(&v).unwrap()).unwrap(), v),
                Err(e) => prop_assert!(e.offset <= bytes.len()),
            }
        }
    }
}
