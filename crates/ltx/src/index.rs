//! The page index.
//!
//! After the end-of-pages marker comes one entry per page, three unsigned
//! LEB128 varints each (as Go's `binary.PutUvarint`): page number, offset of
//! the page header in the file, and the frame's length including its page
//! header. Entries ascend by page number. A zero varint ends the list, then a
//! u64 big-endian count of the index bytes before it (terminator included),
//! then the trailer. The streaming decoder checks the index against the
//! frames it read.
//!
//! Reading a file out of order (`PageIndex` and `decode_frame`, where only the
//! index region and one frame are fetched) yields a page that no checksum
//! covers: the file checksum spans the whole file. Nothing in production reads
//! a page that way, so both exist only with the `testing` feature, for the
//! fuzz targets and the codec's own tests.

use thiserror::Error;

use crate::{OrderError, TRAILER_LEN};

#[cfg(any(test, feature = "testing"))]
mod lookup;

#[cfg(any(test, feature = "testing"))]
pub use lookup::{IndexEntry, PageIndex, decode_frame};

/// Bytes at the end of a file that locate the index: its size field and the trailer.
pub const INDEX_TAIL_LEN: usize = 8 + TRAILER_LEN;

/// Why a page index failed to decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum IndexError {
    /// A varint runs past 64 bits.
    #[error("page index varint overflows 64 bits")]
    Varint,
    /// A page number does not fit 32 bits.
    #[error("page index page number {value} does not fit 32 bits")]
    PageNumber {
        /// The varint's value.
        value: u64,
    },
    /// An entry's page number is out of place.
    #[error("page index: {0}")]
    Order(#[from] OrderError),
    /// An entry's frame starts inside the header or the frame before it.
    #[error("page {pgno} frame at offset {offset} overlaps what precedes it")]
    Offset {
        /// The page number.
        pgno: u32,
        /// The frame's offset.
        offset: u64,
    },
    /// An entry's frame length is outside what one page can take.
    #[error("page {pgno} frame of {size} bytes is impossible at this page size")]
    FrameSize {
        /// The page number.
        pgno: u32,
        /// The frame length.
        size: u64,
    },
    /// The index size field does not match the index.
    #[error("page index size field {found}, index is {expected} bytes")]
    SizeField {
        /// The size field.
        found: u64,
        /// The index bytes before it.
        expected: u64,
    },
    /// The region ends before the index does.
    #[error("page index truncated")]
    Truncated,
    /// Bytes follow the index terminator inside the region.
    #[error("bytes after the page index terminator")]
    TrailingBytes,
}

/// An unsigned LEB128 varint in progress, decoded as Go's `binary.ReadUvarint`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Varint {
    value: u64,
    count: u32,
}

impl Varint {
    pub(crate) fn push(&mut self, byte: u8) -> Result<Option<u64>, IndexError> {
        if self.count >= 9 && byte > 1 {
            return Err(IndexError::Varint);
        }
        self.value |= u64::from(byte & 0x7f).wrapping_shl(self.count.wrapping_mul(7));
        self.count = self.count.saturating_add(1);
        if byte & 0x80 != 0 {
            return Ok(None);
        }
        let value = self.value;
        *self = Self::default();
        Ok(Some(value))
    }
}

/// Appends `value` as an unsigned LEB128 varint.
pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).unwrap_or_default() | 0x80);
        value = value.wrapping_shr(7);
    }
    out.push(u8::try_from(value).unwrap_or_default());
}

#[cfg(test)]
mod tests {
    use super::lookup::read_uvarint;
    use super::*;

    #[test]
    fn varints_match_go() {
        // Encodings from Go's binary.AppendUvarint.
        for (value, bytes) in [
            (0_u64, vec![0x00]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (300, vec![0xac, 0x02]),
            (
                u64::MAX,
                vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
            ),
        ] {
            let mut out = Vec::new();
            put_uvarint(&mut out, value);
            assert_eq!(out, bytes);
            assert_eq!(read_uvarint(&mut bytes.iter().copied()), Ok(value));
        }
    }

    #[test]
    fn varint_overflow_is_rejected() {
        let eleven = [0xff_u8; 9].into_iter().chain([0x02]);
        assert_eq!(
            read_uvarint(&mut eleven.into_iter()),
            Err(IndexError::Varint)
        );
        let long = [0x80_u8; 10].into_iter().chain([0x00]);
        assert_eq!(read_uvarint(&mut long.into_iter()), Err(IndexError::Varint));
    }

    #[test]
    fn non_canonical_varints_decode_as_go_does() {
        assert_eq!(read_uvarint(&mut [0x80, 0x00].into_iter()), Ok(0));
    }

    #[test]
    fn region_len_reads_the_size_field() {
        let mut tail = [0_u8; INDEX_TAIL_LEN];
        tail[..8].copy_from_slice(&12_u64.to_be_bytes());
        assert_eq!(PageIndex::region_len(&tail), 20);
    }
}
