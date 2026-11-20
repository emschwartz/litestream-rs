//! A corrupt file never verifies with different pages: every truncation of a
//! small Go file is an error, and every single-bit flip is an error or the
//! same pages. Some flips do verify: the file checksum covers decompressed
//! pages, and LZ4 has bits no decoder reads (the match-length nibble of a
//! block's last, literals-only token; a frame's descriptor checksum and
//! block-size id).

use std::fs;

use crate::common::{decode, fixtures_dir};

fn small_fixtures() -> Vec<Vec<u8>> {
    ["ltx-v0.5.1", "ltx-v0.5.3"]
        .iter()
        .flat_map(|version| {
            [
                "0000000000000003-0000000000000003.ltx",
                "0000000000000004-0000000000000005.ltx",
            ]
            .map(|name| fs::read(fixtures_dir().join(version).join("512").join(name)).unwrap())
        })
        .collect()
}

#[test]
fn every_truncation_is_an_error() {
    for bytes in small_fixtures() {
        for len in 0..bytes.len() {
            assert!(
                decode(&bytes[..len]).is_err(),
                "prefix of {len} bytes verified"
            );
        }
    }
}

#[test]
fn every_bit_flip_is_an_error_or_the_same_file() {
    for bytes in small_fixtures() {
        let original = decode(&bytes).unwrap();
        for at in 0..bytes.len() {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[at] ^= 1 << bit;
                if let Ok(decoded) = decode(&flipped) {
                    assert_eq!(
                        (decoded.header, &decoded.pages),
                        (original.header, &original.pages),
                        "flip at {at} bit {bit}"
                    );
                }
            }
        }
    }
}
