//! The `key` target: parsers whose accepted input must print back unchanged.

use lsr_core::{FileKey, file_name, parse_file_name};
use lsr_ltx::Txid;

use crate::bytes::violation;

/// Parses the input as a transaction id, a file name and a full key. Whatever
/// a parser accepts, its `Display` must reproduce byte for byte: a second
/// spelling of one key would let two objects name one file.
///
/// Input: the text itself; bytes that are not UTF-8 are skipped.
pub fn key(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(txid) = text.parse::<Txid>()
        && txid.to_string() != text
    {
        violation("a parsed Txid prints differently");
    }
    if let Ok(range) = parse_file_name(text)
        && file_name(range) != text
    {
        violation("a parsed file name prints differently");
    }
    if let Ok(key) = text.parse::<FileKey>()
        && key.to_string() != text
    {
        violation("a parsed FileKey prints differently");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_keys_round_trip() {
        key(b"0009/0000000000000001-000000000000000a.ltx");
        key(b"0000000000000001");
        key(b"0000000000000001-0000000000000002.ltx");
    }

    #[test]
    fn rejected_input_is_not_a_failure() {
        key(b"");
        key(b"0009/0000000000000001-000000000000000A.ltx");
        key(&[0xff, 0xfe, 0x00]);
    }
}
