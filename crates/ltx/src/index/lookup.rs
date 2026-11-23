//! Reading one page out of order: the index region decoded into entries, and
//! the one frame an entry locates. See the parent module for why this is
//! test support only.

use std::num::NonZeroU32;

use super::{INDEX_TAIL_LEN, IndexError, Varint};
use crate::conv::{Cursor, u64_from};
use crate::frame::{FrameParser, MIN_FRAME_LEN, Progress, max_frame_len};
use crate::order::PageSequence;
use crate::{DecodeError, HEADER_LEN, Header, OrderError, PageNo, Section};

/// The largest file offset Go's `int64` offsets can name.
const I64_MAX: u64 = i64::MAX.unsigned_abs();

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

pub(super) fn read_uvarint(bytes: &mut impl Iterator<Item = u8>) -> Result<u64, IndexError> {
    let mut varint = Varint::default();
    loop {
        let byte = bytes.next().ok_or(IndexError::Truncated)?;
        if let Some(value) = varint.push(byte)? {
            return Ok(value);
        }
    }
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
    PageSequence::new(header).fits(pgno)
}
