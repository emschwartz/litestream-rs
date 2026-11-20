//! Integer conversions the lint set would otherwise spell as `as`.

// Every conversion below is lossless only because of this bound.
const _: () = assert!(usize::BITS >= 32 && usize::BITS <= 64);

/// A `u32` as a `usize`: lossless on every target this crate builds for.
#[expect(
    clippy::as_conversions,
    reason = "usize is 32 to 64 bits, asserted above, so a u32 always fits"
)]
pub(crate) const fn usize_from(value: u32) -> usize {
    value as usize
}

/// A `usize` as a `u64`: lossless on every target this crate builds for.
#[expect(
    clippy::as_conversions,
    reason = "usize is at most 64 bits, asserted above, so it always fits a u64"
)]
pub(crate) const fn u64_from(value: usize) -> u64 {
    value as u64
}

/// Reads big- and little-endian fields off a buffer the caller sized for the layout.
///
/// A buffer shorter than the fields read yields zeros for the missing bytes;
/// every caller hands in a unit whose length it asked for, so that never happens.
pub(crate) struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(crate) fn take<const N: usize>(&mut self) -> [u8; N] {
        match self.0.split_first_chunk::<N>() {
            Some((head, rest)) => {
                self.0 = rest;
                *head
            }
            None => {
                self.0 = &[];
                [0; N]
            }
        }
    }

    pub(crate) fn u8(&mut self) -> u8 {
        let [byte] = self.take::<1>();
        byte
    }

    pub(crate) fn u16(&mut self) -> u16 {
        u16::from_be_bytes(self.take())
    }

    pub(crate) fn u32(&mut self) -> u32 {
        u32::from_be_bytes(self.take())
    }

    pub(crate) fn u32_le(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    pub(crate) fn u64(&mut self) -> u64 {
        u64::from_be_bytes(self.take())
    }

    pub(crate) fn u64_le(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    pub(crate) fn i64(&mut self) -> i64 {
        i64::from_be_bytes(self.take())
    }
}
