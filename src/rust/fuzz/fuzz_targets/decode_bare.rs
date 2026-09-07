#![no_main]
mod support;
use datalevin_codec::nippy::fast_thaw_with_limits;
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    support::check(fast_thaw_with_limits(data, support::limits()), data.len());
});
