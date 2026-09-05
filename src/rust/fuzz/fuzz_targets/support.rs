use datalevin_codec::cbor::{
    Error, ErrorKind, Limits, Mode, Value, decode_with_limits, encode, encode_into, encoded_len,
};

pub const FUZZ_LIMITS: Limits = Limits {
    max_input_bytes: 64 * 1024,
    max_depth: 64,
    max_collection_len: 4 * 1024,
    max_string_bytes: 64 * 1024,
    max_bignum_bytes: 4 * 1024,
    max_extension_bytes: 64 * 1024,
};

pub fn verify_value(value: &Value) -> Vec<u8> {
    let canonical = encode(value, Mode::Canonical).expect("a decoded value must re-encode");
    assert_eq!(
        canonical.len(),
        encoded_len(value, Mode::Canonical).expect("a decoded value must have an encoded length"),
        "encoded_len disagrees with encode"
    );

    let mut output = vec![0; canonical.len()];
    let written = encode_into(value, Mode::Canonical, &mut output)
        .expect("an exactly sized output must be sufficient");
    assert_eq!(canonical.len(), written, "encode_into wrote the wrong size");
    assert_eq!(canonical, output, "encode and encode_into disagree");

    let mut short = vec![0; canonical.len() - 1];
    let error = encode_into(value, Mode::Canonical, &mut short)
        .expect_err("a short output must be rejected");
    assert_eq!(ErrorKind::OutputTooSmall, error.kind);

    let decoded = decode_with_limits(&canonical, true, FUZZ_LIMITS)
        .expect("canonical encoder output must decode");
    let reencoded =
        encode(&decoded, Mode::Canonical).expect("a decoded canonical value must re-encode");
    assert_eq!(
        canonical, reencoded,
        "canonical bytes are not a fixed point"
    );
    canonical
}

pub fn verify_error_offset(error: &Error, input_len: usize) {
    assert!(
        error.offset <= input_len,
        "error offset {} is outside an input of length {input_len}",
        error.offset
    );
}
