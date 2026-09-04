#![no_main]

mod support;

use datalevin_codec::cbor::{Mode, decode_storage_with_limits, encode, encode_storage};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    exercise(input, true);
    exercise(input, false);
});

fn exercise(input: &[u8], canonical: bool) {
    let value = match decode_storage_with_limits(input, canonical, support::FUZZ_LIMITS) {
        Ok(value) => value,
        Err(error) => {
            support::verify_error_offset(&error, input.len());
            return;
        }
    };
    let normalized = support::verify_value(&value);
    let storage = encode_storage(&value, Mode::Canonical)
        .expect("a decoded value must have a storage encoding");
    if canonical {
        assert_eq!(input, storage, "accepted storage bytes changed on encode");
    }
    let decoded = decode_storage_with_limits(&storage, true, support::FUZZ_LIMITS)
        .expect("canonical storage output must decode");
    assert_eq!(
        normalized,
        encode(&decoded, Mode::Canonical).expect("a decoded value must re-encode"),
        "storage round trip changed the canonical bare value"
    );
}
