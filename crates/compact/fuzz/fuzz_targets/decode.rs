//! Any text: decoding it fails or succeeds, never panics.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let (text, side) = text
            .split_once("\u{0}")
            .map_or((text, None), |(t, s)| (t, Some(s)));
        let _ = tangent_compact::decode(text, side);
    }
});
