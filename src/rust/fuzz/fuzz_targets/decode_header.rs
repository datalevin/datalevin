#![no_main]
mod support;
use datalevin_codec::nippy::thaw_with_limits;
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    support::check(thaw_with_limits(data, support::limits()), data.len());
});
