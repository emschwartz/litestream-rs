//! SQLite's cumulative WAL checksum.
//!
//! Two 32-bit accumulators run over 8-byte units: `s0 += word0 + s1; s1 += word1 + s0`,
//! wrapping. The WAL header's magic number picks whether a word is read big- or
//! little-endian. The header checksums its first 24 bytes from zero; each frame
//! continues from the previous frame's (or the header's) result over its first 8
//! header bytes and then its page.

/// The byte order the WAL's checksum words are read in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ByteOrder {
    /// Magic `0x377f0682`.
    Little,
    /// Magic `0x377f0683`.
    Big,
}

impl ByteOrder {
    fn word(self, bytes: [u8; 4]) -> u32 {
        match self {
            Self::Little => u32::from_le_bytes(bytes),
            Self::Big => u32::from_be_bytes(bytes),
        }
    }
}

/// A pair of running checksum accumulators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WalChecksum(u32, u32);

impl WalChecksum {
    /// The state a header's checksum starts from.
    pub const ZERO: Self = Self(0, 0);

    /// A checksum from its two accumulators.
    #[must_use]
    pub const fn new(s0: u32, s1: u32) -> Self {
        Self(s0, s1)
    }

    /// The first accumulator.
    #[must_use]
    pub const fn s0(self) -> u32 {
        self.0
    }

    /// The second accumulator.
    #[must_use]
    pub const fn s1(self) -> u32 {
        self.1
    }

    /// Continues the checksum over `bytes`; `None` unless the length is a multiple of 8.
    #[must_use]
    pub fn extend(self, order: ByteOrder, bytes: &[u8]) -> Option<Self> {
        let (units, remainder) = bytes.as_chunks::<8>();
        let (mut s0, mut s1) = (self.0, self.1);
        for unit in units {
            let [a, b, c, d, e, f, g, h] = *unit;
            s0 = s0.wrapping_add(order.word([a, b, c, d])).wrapping_add(s1);
            s1 = s1.wrapping_add(order.word([e, f, g, h])).wrapping_add(s0);
        }
        remainder.is_empty().then_some(Self(s0, s1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_a_hand_computed_unit() {
        // One unit, little-endian words 1 and 2: s0 = 1, s1 = 2 + 1.
        let unit = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(
            WalChecksum::ZERO.extend(ByteOrder::Little, &unit),
            Some(WalChecksum::new(1, 3))
        );
        // The same bytes read big-endian: words 0x01000000 and 0x02000000.
        assert_eq!(
            WalChecksum::ZERO.extend(ByteOrder::Big, &unit),
            Some(WalChecksum::new(0x0100_0000, 0x0300_0000))
        );
    }

    #[test]
    fn extending_in_pieces_equals_extending_at_once() {
        let bytes: Vec<u8> = (0..=255).collect();
        let whole = WalChecksum::ZERO.extend(ByteOrder::Little, &bytes);
        let (a, b) = bytes.split_at(64);
        let pieces = WalChecksum::ZERO
            .extend(ByteOrder::Little, a)
            .and_then(|c| c.extend(ByteOrder::Little, b));
        assert_eq!(whole, pieces);
    }

    #[test]
    fn a_length_off_the_unit_is_refused() {
        assert_eq!(WalChecksum::ZERO.extend(ByteOrder::Big, &[0; 7]), None);
        assert_eq!(
            WalChecksum::ZERO.extend(ByteOrder::Big, &[]),
            Some(WalChecksum::ZERO)
        );
    }

    #[test]
    fn accumulators_wrap() {
        let unit = [0xff; 8];
        assert!(WalChecksum::ZERO.extend(ByteOrder::Big, &unit).is_some());
    }
}
