#![no_main]

mod support;

use datalevin_codec::cbor::decode_with_limits;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    exercise(input, true);
    exercise(input, false);
});

fn exercise(input: &[u8], canonical: bool) {
    let value = match decode_with_limits(input, canonical, support::FUZZ_LIMITS) {
        Ok(value) => value,
        Err(error) => {
            support::verify_error_offset(&error, input.len());
            return;
        }
    };
    let normalized = support::verify_value(&value);
    if canonical {
        assert_eq!(
            input, normalized,
            "accepted canonical bytes changed on encode"
        );
    }
}
