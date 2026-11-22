//! The `flip` target: one changed byte in a file that verifies.

use lsr_ltx::testing::compressed_block_spans;

use crate::bytes::{Reader, violation};
use crate::decode::{End, run};

/// Changes one byte of a file that verifies and holds the decoder to the
/// single-byte property: the changed file fails to verify, or the byte lies
/// inside a compressed LZ4 block (not its last token) and the file decodes to
/// the same header and pages. That exception is LZ4 aliasing, which no check
/// over decompressed pages can see (`lsr_ltx::FrameError`'s module docs).
///
/// Input: a big-endian `u16` offset (reduced modulo the file's length), a mask
/// XORed into that byte (zero counts as one), then the file. A file that does
/// not verify as given is not an input to this property.
pub fn flip(data: &[u8]) {
    let mut reader = Reader::new(data);
    let (Some(at), Some(mask)) = (reader.u16(), reader.u8()) else {
        return;
    };
    let file = reader.rest();
    let original = run(&[file]);
    if !matches!(original.end, End::Verified(_)) {
        return;
    }
    let Some(spans) = compressed_block_spans(file) else {
        violation("a verified file's blocks do not parse");
    };
    let Some(at) = usize::from(at).checked_rem(file.len()) else {
        return;
    };
    let mut changed = file.to_vec();
    if let Some(byte) = changed.get_mut(at) {
        *byte ^= mask.max(1);
    }
    let after = run(&[&changed]);
    if !matches!(after.end, End::Verified(_)) {
        return;
    }
    if after.events != original.events {
        violation(&format!("a change at {at} verified with other pages"));
    }
    if !spans.iter().any(|span| span.contains(&at)) {
        violation(&format!(
            "a change at {at}, outside every LZ4 block, verified"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_and_empty_input_do_not_panic() {
        flip(&[]);
        flip(&[0, 1, 2]);
        flip(&[0, 1, 2, b'L', b'T', b'X', b'1']);
    }
}
