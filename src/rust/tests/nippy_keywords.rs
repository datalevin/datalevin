use datalevin_codec::nippy::{Decoder, Encoder, Keyword, Limits, Value, fast_freeze, fast_thaw};
use std::hash::{DefaultHasher, Hash, Hasher};

fn hash(keyword: &Keyword) -> u64 {
    let mut state = DefaultHasher::new();
    keyword.hash(&mut state);
    state.finish()
}

#[test]
fn keyword_hashing_is_lazy_shared_and_thread_safe() {
    let keyword = Keyword::from("namespace/λ");
    let clone = keyword.clone();
    assert_eq!(keyword.as_ptr(), clone.as_ptr());
    let expected = hash(&keyword);
    assert_eq!(hash(&clone), expected);
    assert_eq!(hash(&Keyword::from("namespace/λ".to_owned())), expected);
    assert_eq!(keyword.to_string(), "namespace/λ");
    let uninitialized = Keyword::from("namespace/λ");
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let keyword = &uninitialized;
            scope.spawn(move || assert_eq!(hash(keyword), expected));
        }
    });
    assert_eq!(keyword, uninitialized);
}

#[test]
fn keywords_follow_nippy_39_second_occurrence_policy() {
    let value = Value::Vector(
        ["a", "b", "a", "a", "b", "b"]
            .map(|s| Value::Keyword(s.into()))
            .to_vec(),
    );
    // Plain first occurrence; cache definition at the second; then reference.
    let expected = [
        97, 134, 106, 1, b'a', 106, 1, b'b', 59, 106, 1, b'a', 59, 63, 106, 1, b'b', 63,
    ];
    assert_eq!(fast_freeze(&value).unwrap(), expected);
    let decoded = fast_thaw(&expected).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(fast_freeze(&decoded).unwrap(), expected);
    let Value::Vector(items) = decoded else {
        panic!()
    };
    let keyword = |i| match &items[i] {
        Value::Keyword(k) => k,
        _ => panic!(),
    };
    assert_eq!(keyword(2).as_ptr(), keyword(3).as_ptr());
    assert_ne!(keyword(0).as_ptr(), keyword(2).as_ptr());
    assert_eq!(keyword(4).as_ptr(), keyword(5).as_ptr());
    // The returned names own their storage after both input and decoder drop.
    let standalone = {
        let input = vec![59, 106, 1, b'a', 59];
        let mut decoder = Decoder::new(&input, Limits::default()).unwrap();
        decoder.read_value().unwrap();
        decoder.read_value().unwrap()
    };
    assert_eq!(standalone, Value::Keyword("a".into()));
}

#[test]
fn keyword_cache_boundaries_and_stream_sessions() {
    let keywords: Vec<_> = (0..32770)
        .map(|i| Value::Keyword(format!("k{i}").into()))
        .collect();
    let mut encoded = Vec::new();
    let mut encoder = Encoder::new(&mut encoded, Limits::default());
    let mut expected = Vec::new();
    for occurrence in 0..3 {
        for (index, keyword) in keywords.iter().enumerate() {
            encoder.write_value(keyword).unwrap();
            if occurrence > 0 && index < 32768 {
                const TAGS: [u8; 8] = [59, 63, 64, 65, 66, 72, 73, 74];
                if index < 8 {
                    expected.push(TAGS[index]);
                } else if index <= 127 {
                    expected.extend([67, index as u8]);
                } else {
                    expected.push(68);
                    expected.extend_from_slice(&(index as i16).to_be_bytes());
                }
            }
            if occurrence < 2 || index >= 32768 {
                let name = format!("k{index}");
                expected.extend([106, name.len() as u8]);
                expected.extend_from_slice(name.as_bytes());
            }
        }
    }
    assert_eq!(encoder.position(), expected.len());
    assert_eq!(encoded, expected);
    let mut decoder = Decoder::new(&encoded, Limits::default()).unwrap();
    for _ in 0..3 {
        for keyword in &keywords {
            assert_eq!(&decoder.read_value().unwrap(), keyword);
        }
    }
    assert_eq!(decoder.position(), encoded.len());
    assert_eq!(fast_freeze(&keywords[0]).unwrap(), [106, 2, b'k', b'0']);
}
