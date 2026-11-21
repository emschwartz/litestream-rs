//! CRC-64 checksums: per page, per database and per file.
//!
//! Every LTX checksum is a CRC-64 (Go's `crc64.ISO` table) with bit 63 forced
//! on, so a stored zero can mean "not tracked". A page's checksum covers its
//! big-endian page number and then its bytes. A database's checksum is the XOR
//! of its pages' checksums, flag reapplied, which makes it incremental: applying
//! a page XORs out the old checksum and XORs in the new one. An empty database
//! is the bare flag. The file checksum is one CRC over the bytes the module docs
//! of [`crate::Decoder`] list.
//!
//! The CRC runs slice-by-16 (sixteen 256-entry tables, 32 KiB built at compile
//! time) rather than the `crc` crate's one-table default. Every page of a
//! restore is hashed at least once, so the CRC's speed bounds the restore's:
//! on a 4.8 GiB snapshot of 16 KiB pages the one-table CRC ran at about
//! 390 MB/s and took 13 of the 16 seconds the decode took; slice-by-16 runs at
//! about 2.6 GB/s. Go's `hash/crc64` uses slicing-by-8 for the same reason.
//! The value is the same CRC either way; a property test pins it against a
//! bit-at-a-time reference.

use std::fmt;

use crc::{CRC_64_GO_ISO, Crc, Digest, Table};

use crate::PageNo;

/// Bit 63, set on every checksum Go stores.
const FLAG: u64 = 0x8000_0000_0000_0000;

/// The table set every LTX CRC uses; see the module docs for why sixteen.
type Tables = Table<16>;

static CRC64: Crc<u64, Tables> = Crc::<u64, Tables>::new(&CRC_64_GO_ISO);

/// A CRC-64 checksum with the LTX flag bit set.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Checksum(u64);

impl Checksum {
    /// The checksum of a database with no pages.
    pub const EMPTY: Self = Self(FLAG);

    /// Returns `None` unless bit 63 is set, as on every checksum Go writes.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Option<Self> {
        if raw & FLAG == 0 {
            None
        } else {
            Some(Self(raw))
        }
    }

    /// The checksum as stored on the wire.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The checksum of one page.
    #[must_use]
    pub fn of_page(pgno: PageNo, data: &[u8]) -> Self {
        let mut digest = CRC64.digest();
        digest.update(&pgno.get().to_be_bytes());
        digest.update(data);
        Self(FLAG | digest.finalize())
    }

    /// Adds a page's checksum to a database checksum, or removes it: XOR is
    /// its own inverse.
    #[must_use]
    pub const fn toggle(self, page: Self) -> Self {
        Self(FLAG | (self.0 ^ page.0))
    }

    /// A database checksum after one page changed from `old` to `new`.
    #[must_use]
    pub const fn replace(self, old: Self, new: Self) -> Self {
        self.toggle(old).toggle(new)
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checksum({self})")
    }
}

/// A running CRC-64 over a byte stream.
pub(crate) struct Crc64(Digest<'static, u64, Tables>);

impl Crc64 {
    pub(crate) fn new() -> Self {
        Self(CRC64.digest())
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub(crate) fn finish(self) -> Checksum {
        Checksum(FLAG | self.0.finalize())
    }
}

impl fmt::Debug for Crc64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Crc64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PageSize;

    fn pgno(n: u32) -> PageNo {
        PageNo::new(n, PageSize::new(4096).unwrap()).unwrap()
    }

    #[test]
    fn raw_checksum_needs_the_flag() {
        assert_eq!(Checksum::from_raw(0), None);
        assert_eq!(Checksum::from_raw(FLAG - 1), None);
        assert_eq!(
            Checksum::from_raw(FLAG | 5).map(Checksum::get),
            Some(FLAG | 5)
        );
    }

    #[test]
    fn page_checksum_matches_go() {
        // Values from ltx.ChecksumPage at ltx v0.5.3.
        assert_eq!(
            Checksum::of_page(pgno(1), &[0; 4096]).get(),
            15_650_729_268_246_115_133
        );
        let ramp: Vec<u8> = (0..=255).cycle().take(512).collect();
        assert_eq!(
            Checksum::of_page(pgno(262_146), &ramp).get(),
            14_277_307_397_040_163_999
        );
    }

    #[test]
    fn page_checksum_always_carries_the_flag() {
        // A raw CRC has bit 63 set about half the time; the flag must be forced
        // on either way (OR), not toggled (XOR), or half the pages lose it.
        for n in 1..=64_u32 {
            let sum = Checksum::of_page(pgno(n), &n.to_le_bytes());
            assert_eq!(sum.get() & FLAG, FLAG, "page {n}");
        }
    }

    #[test]
    fn checksums_print_as_sixteen_hex_digits() {
        let sum = Checksum::from_raw(FLAG | 0xab).unwrap();
        assert_eq!(sum.to_string(), "80000000000000ab");
        assert_eq!(format!("{sum:?}"), "Checksum(80000000000000ab)");
        assert_eq!(format!("{:?}", Crc64::new()), "Crc64");
    }

    #[test]
    fn database_checksum_is_incremental() {
        let a = Checksum::of_page(pgno(1), b"one");
        let b = Checksum::of_page(pgno(2), b"two");
        let b2 = Checksum::of_page(pgno(2), b"too");
        let db = Checksum::EMPTY.toggle(a).toggle(b);
        assert_eq!(db.replace(b, b2), Checksum::EMPTY.toggle(a).toggle(b2));
        assert_eq!(db.toggle(b).toggle(a), Checksum::EMPTY);
        assert_eq!(db.get() & FLAG, FLAG);
    }

    /// CRC-64/GO-ISO one bit at a time, from its definition: reflected
    /// polynomial 0xD800000000000000, initial value and final XOR all ones.
    fn bitwise(bytes: &[u8]) -> u64 {
        let mut crc = u64::MAX;
        for byte in bytes {
            crc ^= u64::from(*byte);
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0xD800_0000_0000_0000
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    #[test]
    fn the_crc_is_go_iso_on_its_check_string() {
        // The catalogue check value of CRC-64/GO-ISO.
        assert_eq!(bitwise(b"123456789"), 0xb909_56c7_75a4_1001);
        let mut crc = Crc64::new();
        crc.update(b"123456789");
        assert_eq!(crc.finish().get(), FLAG | 0xb909_56c7_75a4_1001);
    }

    proptest::proptest! {
        /// The sliced tables give the bitwise CRC for every length and every
        /// split of the input across updates, so their 16-byte strides and
        /// byte-wise tails agree with the definition.
        #[test]
        fn the_sliced_crc_equals_the_bitwise_one(
            bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..2100),
            cuts in proptest::collection::vec(0_usize..2100, 0..6),
        ) {
            let mut cuts: Vec<usize> = cuts.into_iter().map(|c| c.min(bytes.len())).collect();
            cuts.sort_unstable();
            let mut crc = Crc64::new();
            let mut at = 0;
            for cut in cuts.into_iter().chain([bytes.len()]) {
                crc.update(&bytes[at..cut]);
                at = cut;
            }
            proptest::prop_assert_eq!(crc.finish().get(), FLAG | bitwise(&bytes));
        }

        /// A page checksum is the CRC of the big-endian page number then the page.
        #[test]
        fn a_page_checksum_is_the_bitwise_crc_of_number_and_bytes(
            n in 1_u32..1_000_000,
            data in proptest::collection::vec(proptest::num::u8::ANY, 512),
        ) {
            let mut whole = n.to_be_bytes().to_vec();
            whole.extend_from_slice(&data);
            let pgno = PageNo::new(n, PageSize::new(512).unwrap());
            proptest::prop_assume!(pgno.is_some());
            proptest::prop_assert_eq!(
                Checksum::of_page(pgno.unwrap(), &data).get(),
                FLAG | bitwise(&whole)
            );
        }
    }
}
