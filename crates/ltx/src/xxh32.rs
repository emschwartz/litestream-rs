//! xxHash32 with seed 0, the checksum the LZ4 frame format uses.
//!
//! Legacy page frames (see [`crate::FrameError`]) carry up to three of them: a
//! header checksum byte over the frame descriptor, an optional checksum per
//! block over its stored bytes, and an optional content checksum over the
//! decompressed page. The LTX file checksum covers the decompressed page only,
//! so these are the only checks on a frame's descriptor and block bytes. The
//! hash is written out here rather than taken from a crate: it is forty lines,
//! and a new runtime dependency for it is not worth the supply chain.

const PRIME1: u32 = 0x9e37_79b1;
const PRIME2: u32 = 0x85eb_ca77;
const PRIME3: u32 = 0xc2b2_ae3d;
const PRIME4: u32 = 0x27d4_eb2f;
const PRIME5: u32 = 0x1656_67b1;

const fn round(acc: u32, lane: u32) -> u32 {
    acc.wrapping_add(lane.wrapping_mul(PRIME2))
        .rotate_left(13)
        .wrapping_mul(PRIME1)
}

fn lane(word: &[u8; 4]) -> u32 {
    u32::from_le_bytes(*word)
}

/// The xxHash32 of `data` with seed 0.
pub fn xxh32(data: &[u8]) -> u32 {
    let (stripes, tail) = data.as_chunks::<16>();
    let mut acc = if stripes.is_empty() {
        PRIME5
    } else {
        let mut v = [
            PRIME1.wrapping_add(PRIME2),
            PRIME2,
            0,
            0_u32.wrapping_sub(PRIME1),
        ];
        for stripe in stripes {
            for (slot, word) in v.iter_mut().zip(stripe.as_chunks::<4>().0) {
                *slot = round(*slot, lane(word));
            }
        }
        let [v1, v2, v3, v4] = v;
        v1.rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18))
    };
    // The length enters modulo 2^32, as the reference implementation adds it.
    let [b0, b1, b2, b3, ..] = data.len().to_le_bytes();
    acc = acc.wrapping_add(lane(&[b0, b1, b2, b3]));
    let (words, bytes) = tail.as_chunks::<4>();
    for word in words {
        acc = acc
            .wrapping_add(lane(word).wrapping_mul(PRIME3))
            .rotate_left(17)
            .wrapping_mul(PRIME4);
    }
    for byte in bytes {
        acc = acc
            .wrapping_add(u32::from(*byte).wrapping_mul(PRIME5))
            .rotate_left(11)
            .wrapping_mul(PRIME1);
    }
    acc ^= acc >> 15;
    acc = acc.wrapping_mul(PRIME2);
    acc ^= acc >> 13;
    acc = acc.wrapping_mul(PRIME3);
    acc ^ (acc >> 16)
}

#[cfg(test)]
mod tests {
    use super::xxh32;

    /// Published reference values: the empty input, the short-input path
    /// (under 16 bytes) and the striped path (16 bytes and more).
    #[test]
    fn matches_the_reference_vectors() {
        assert_eq!(xxh32(b""), 0x02cc_5d05);
        assert_eq!(xxh32(b"a"), 0x550d_7456);
        assert_eq!(xxh32(b"abc"), 0x32d1_53ff);
        assert_eq!(
            xxh32(b"Nobody inspects the spammish repetition"),
            0xe229_3b2f
        );
    }
}
