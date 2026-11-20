//! The page index, and decoding one frame it points at.
//!
//! After the end-of-pages marker comes one entry per page, three unsigned
//! LEB128 varints each (as Go's `binary.PutUvarint`): page number, offset of
//! the page header in the file, and the frame's length including its page
//! header. Entries ascend by page number. A zero varint ends the list, then a
//! u64 big-endian count of the index bytes before it (terminator included),
//! then the trailer. The streaming decoder checks the index against the
//! frames it read; [`PageIndex`] is for reading a file out of order, where only
//! the index region and one frame are fetched.

use std::num::NonZeroU32;

use thiserror::Error;

use crate::conv::{Cursor, u64_from};
use crate::frame::{FrameParser, MIN_FRAME_LEN, Progress, max_frame_len};
use crate::order::PageSequence;
use crate::{DecodeError, HEADER_LEN, Header, OrderError, PageNo, Section, TRAILER_LEN};

/// The largest file offset Go's `int64` offsets can name.
const I64_MAX: u64 = i64::MAX.unsigned_abs();

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

/// Where one page's frame lives in a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pgno: PageNo,
    offset: u64,
    size: u64,
}

impl IndexEntry {
    /// The page number.
    #[must_use]
    pub const fn pgno(&self) -> PageNo {
        self.pgno
    }

    /// Byte offset of the frame's page header.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Frame length, page header included.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

/// A decoded, validated page index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageIndex {
    entries: Vec<IndexEntry>,
}

impl PageIndex {
    /// Bytes the index region takes (varints, terminator, size field), read
    /// from the last [`INDEX_TAIL_LEN`] bytes of a file. The caller checks it
    /// against the file's length before fetching that many bytes.
    #[must_use]
    pub fn region_len(tail: &[u8; INDEX_TAIL_LEN]) -> u64 {
        Cursor::new(tail).u64().saturating_add(8)
    }

    /// Decodes an index region: everything after the end-of-pages marker and
    /// before the trailer.
    ///
    /// # Errors
    ///
    /// The first [`IndexError`] the region violates.
    pub fn decode(region: &[u8], header: &Header) -> Result<Self, IndexError> {
        let (body, size) = region
            .split_last_chunk::<8>()
            .ok_or(IndexError::Truncated)?;
        let found = u64::from_be_bytes(*size);
        if found != u64_from(body.len()) {
            return Err(IndexError::SizeField {
                found,
                expected: u64_from(body.len()),
            });
        }
        let mut bytes = body.iter().copied();
        let mut next = || read_uvarint(&mut bytes);
        let mut seq = PageSequence::new(header);
        let max = u64_from(max_frame_len(header.page_size()));
        let mut end = u64_from(HEADER_LEN);
        let mut entries = Vec::new();
        loop {
            let value = next()?;
            let Ok(pgno) = u32::try_from(value) else {
                return Err(IndexError::PageNumber { value });
            };
            let Some(pgno) = NonZeroU32::new(pgno) else {
                break;
            };
            let pgno = seq.push(pgno)?;
            let (offset, size) = (next()?, next()?);
            if size < u64_from(MIN_FRAME_LEN) || size > max {
                return Err(IndexError::FrameSize {
                    pgno: pgno.get(),
                    size,
                });
            }
            let offset_error = IndexError::Offset {
                pgno: pgno.get(),
                offset,
            };
            if offset < end {
                return Err(offset_error);
            }
            // Go's `pageIndexValidator` rejects `offset > MaxInt64 - size`: past
            // that an offset names no byte of any file a reader can seek to.
            end = offset
                .checked_add(size)
                .filter(|end| *end <= I64_MAX)
                .ok_or(offset_error)?;
            entries.push(IndexEntry { pgno, offset, size });
        }
        if bytes.next().is_some() {
            return Err(IndexError::TrailingBytes);
        }
        seq.finish()?;
        Ok(Self { entries })
    }

    /// The entry for a page, if the file holds it.
    #[must_use]
    pub fn get(&self, pgno: PageNo) -> Option<&IndexEntry> {
        self.entries
            .binary_search_by_key(&pgno, |entry| entry.pgno)
            .ok()
            .and_then(|at| self.entries.get(at))
    }

    /// Every entry, ascending by page number.
    #[must_use]
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }
}

fn read_uvarint(bytes: &mut impl Iterator<Item = u8>) -> Result<u64, IndexError> {
    let mut varint = Varint::default();
    loop {
        let byte = bytes.next().ok_or(IndexError::Truncated)?;
        if let Some(value) = varint.push(byte)? {
            return Ok(value);
        }
    }
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

/// Decodes one frame, as an index entry locates it, into `out`.
///
/// `frame` must be exactly `entry`'s bytes. The frame's page number must be
/// the entry's and fit the header. On any error `out` is zeroed, so a
/// rejected page cannot be read back from it.
///
/// # Errors
///
/// [`DecodeError::EntryMismatch`] when the frame holds another page than
/// `entry`, [`DecodeError::Frame`] for a malformed frame (including the end-of-pages
/// marker), [`DecodeError::Order`] for a page the header rules out,
/// [`DecodeError::Truncated`] when `frame` ends early and
/// [`DecodeError::TrailingBytes`] when it runs long.
pub fn decode_frame(
    frame: &[u8],
    header: &Header,
    entry: &IndexEntry,
    out: &mut Vec<u8>,
) -> Result<PageNo, DecodeError> {
    let result = decode_frame_into(frame, header, entry, out);
    if result.is_err() {
        out.fill(0);
    }
    result
}

fn decode_frame_into(
    frame: &[u8],
    header: &Header,
    entry: &IndexEntry,
    out: &mut Vec<u8>,
) -> Result<PageNo, DecodeError> {
    let mut parser = FrameParser::new(header.page_size());
    let mut rest = frame;
    loop {
        let (unit, tail) = rest
            .split_at_checked(parser.need())
            .ok_or(DecodeError::Truncated {
                section: Section::Page,
            })?;
        rest = tail;
        match parser.consume(unit)? {
            Progress::More => {}
            Progress::EndMarker => return Err(crate::FrameError::EndMarker.into()),
            Progress::PageStart(pgno) => {
                check_entry(pgno, header, entry)?;
            }
            Progress::Page(pgno) => {
                let page_no = check_entry(pgno, header, entry)?;
                if !rest.is_empty() {
                    return Err(DecodeError::TrailingBytes);
                }
                out.clear();
                out.extend_from_slice(parser.page());
                return Ok(page_no);
            }
        }
    }
}

fn check_entry(
    pgno: NonZeroU32,
    header: &Header,
    entry: &IndexEntry,
) -> Result<PageNo, DecodeError> {
    if pgno != entry.pgno.nonzero() {
        return Err(DecodeError::EntryMismatch {
            expected: entry.pgno.get(),
            found: pgno.get(),
        });
    }
    Ok(check_page(pgno, header)?)
}

fn check_page(pgno: NonZeroU32, header: &Header) -> Result<PageNo, OrderError> {
    let commit = header.commit();
    if pgno.get() > commit {
        return Err(OrderError::AboveCommit {
            pgno: pgno.get(),
            commit,
        });
    }
    PageNo::new(pgno.get(), header.page_size()).ok_or(OrderError::LockPage { pgno: pgno.get() })
}

#[cfg(test)]
mod tests {
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
