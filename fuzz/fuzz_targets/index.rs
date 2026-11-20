#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| lsr_fuzzing::index(data));
