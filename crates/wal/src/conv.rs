//! Integer conversions and field reads the lint set would otherwise spell as `as` or indexing.

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

/// Reads big-endian fields off a buffer the caller sized for the layout.
///
/// A buffer shorter than the fields read yields zeros for the missing bytes;
/// every caller hands in a unit whose length it asked for, so that never happens.
pub(crate) struct Fields<'a>(&'a [u8]);

impl<'a> Fields<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(crate) fn u32(&mut self) -> u32 {
        match self.0.split_first_chunk::<4>() {
            Some((head, rest)) => {
                self.0 = rest;
                u32::from_be_bytes(*head)
            }
            None => {
                self.0 = &[];
                0
            }
        }
    }
}
