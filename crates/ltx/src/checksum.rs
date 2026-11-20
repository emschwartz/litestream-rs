//! CRC-64 checksums: per page, per database and per file.
//!
//! Every LTX checksum is a CRC-64 (Go's `crc64.ISO` table) with bit 63 forced
//! on, so a stored zero can mean "not tracked". A page's checksum covers its
//! big-endian page number and then its bytes. A database's checksum is the XOR
//! of its pages' checksums, flag reapplied, which makes it incremental: applying
//! a page XORs out the old checksum and XORs in the new one. An empty database
//! is the bare flag. The file checksum is one CRC over the bytes the module docs
//! of [`crate::Decoder`] list.

use std::fmt;

use crc::{CRC_64_GO_ISO, Crc, Digest};

use crate::PageNo;

/// Bit 63, set on every checksum Go stores.
const FLAG: u64 = 0x8000_0000_0000_0000;

static CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

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
pub(crate) struct Crc64(Digest<'static, u64>);

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
}
