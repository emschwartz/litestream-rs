//! One page frame: a 6-byte page header, then the page's LZ4 data.
//!
//! The page header is the page number (u32) and flags (u16), big-endian. Six
//! zero bytes end the page section instead. Flag bit 0 says which of the two
//! encodings Go wrote:
//!
//! - **Block** (ltx v0.5.2 and later, Litestream v0.5.16 and later): a u32
//!   big-endian length, then one raw LZ4 block that decompresses to exactly one
//!   page.
//! - **Frame** (ltx v0.5.0 and v0.5.1, Litestream v0.5.0 to v0.5.15): no flag,
//!   no length. A whole LZ4 frame follows, as Go's `pierrec/lz4` writer emits it.
//!   Nothing says where it ends, so this parser walks the frame format itself:
//!   magic, descriptor (FLG, BD, optional content size, header checksum), then
//!   blocks, each a u32 little-endian size whose high bit marks it stored raw,
//!   ended by a zero size and the optional content checksum. Go always wrote one
//!   independent block per page; this parser also takes several, and dependent
//!   ones, within the length budget below.
//!
//! The LTX file checksum covers each page's header, its block length and its
//! decompressed bytes, never the LZ4 bytes (Go's encoder hashes the same, so
//! the format cannot change). What else catches a change to LZ4 bytes:
//!
//! - A legacy frame's xxHash32 checksums, as Go's lz4 reader compares them:
//!   the header checksum over the descriptor, and when the descriptor asks for
//!   them, each block's checksum and the content checksum. Every Go writer
//!   wrote correct ones, so a mismatch is corruption.
//! - Every LZ4 block, compressed in either encoding, must end in a token that
//!   holds only literals and whose match-length nibble is zero
//!   ([`FrameError::BlockEnd`]). No decoder reads that nibble, but every
//!   compressor this crate reads (Go's `pierrec/lz4`, `lz4_flex`) writes zero,
//!   so a nonzero nibble is corruption the page bytes would never show.
//!
//! What remains is aliasing: an LZ4 block is one of many encodings of its page,
//! and a changed offset or match length can copy equal bytes from elsewhere
//! (most often in runs of zeros). Such a change decodes to the same page, so
//! nothing that checks pages can see it, and restore is unaffected; the bytes
//! still differ, so whoever needs the very bytes it wrote compares raw bytes.
//!
//! Every length is bounded before a byte of it is buffered: a block cannot
//! exceed the LZ4 bound of a page, a stored block the bytes the page still
//! lacks, and a whole frame [`max_frame_len`], which is the largest frame a
//! one-block encoder can write. A hostile length fails at once.

use std::num::NonZeroU32;

use lz4_flex::block::{decompress_into, decompress_into_with_dict, get_maximum_output_size};
use thiserror::Error;

use crate::PageSize;
use crate::conv::{Cursor, u64_from, usize_from};
use crate::xxh32::xxh32;

/// Bytes in a page header.
pub(crate) const PAGE_HEADER_LEN: usize = 6;
/// The smallest possible frame: a page header, a length and a one-byte block.
#[cfg(any(test, feature = "testing"))]
pub(crate) const MIN_FRAME_LEN: usize = PAGE_HEADER_LEN + 4 + 1;

const FLAG_SIZE_PREFIX: u16 = 1;
const LZ4_MAGIC: [u8; 4] = 0x184d_2204_u32.to_le_bytes();
const FLG_VERSION_MASK: u8 = 0xc0;
const FLG_VERSION_1: u8 = 0x40;
const FLG_INDEPENDENT: u8 = 0x20;
const FLG_BLOCK_CHECKSUM: u8 = 0x10;
const FLG_CONTENT_SIZE: u8 = 0x08;
const FLG_CONTENT_CHECKSUM: u8 = 0x04;
const FLG_UNSUPPORTED: u8 = 0x03; // reserved bit and dictionary id
const BD_RESERVED: u8 = 0x8f;
const BD_MIN_BLOCK_SIZE: u8 = 0x40; // block max size ids below 4 are reserved
const STORED: u32 = 0x8000_0000;
const XXH32_LEN: usize = 4;
/// Magic, FLG, BD, content size, header checksum.
const DESCRIPTOR_MAX: usize = 4 + 1 + 1 + 8 + 1;

/// The largest LZ4 block one page can compress to.
fn block_bound(page_size: PageSize) -> usize {
    get_maximum_output_size(page_size.bytes())
}

/// The longest frame, page header included, this crate accepts for one page.
#[must_use]
pub(crate) fn max_frame_len(page_size: PageSize) -> usize {
    // page header, descriptor, block size, block, block checksum, end mark, content checksum
    PAGE_HEADER_LEN
        .saturating_add(DESCRIPTOR_MAX)
        .saturating_add(4)
        .saturating_add(block_bound(page_size))
        .saturating_add(XXH32_LEN)
        .saturating_add(4)
        .saturating_add(XXH32_LEN)
}

/// Why a page frame failed to decode.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum FrameError {
    /// The page header sets a flag other than the size prefix.
    #[error("unknown page header flags {flags:#x}")]
    PageHeaderFlags {
        /// The flags field.
        flags: u16,
    },
    /// The page header names page zero with a flag set.
    #[error("page header names page 0")]
    ZeroPageNumber,
    /// A lone frame turned out to be the end-of-pages marker.
    #[error("frame is the end-of-pages marker, not a page")]
    EndMarker,
    /// A block length is zero or larger than the page allows.
    #[error("LZ4 block of {len} bytes, at most {max} allowed")]
    BlockLen {
        /// The block length field.
        len: u64,
        /// The most this block may hold.
        max: u64,
    },
    /// A legacy frame does not start with the LZ4 frame magic.
    #[error("not an LZ4 frame: magic {found:02x?}")]
    FrameMagic {
        /// The four bytes after the page header.
        found: [u8; 4],
    },
    /// The LZ4 frame descriptor is malformed or asks for a dictionary.
    #[error("unsupported LZ4 frame descriptor FLG {flg:#04x} BD {bd:#04x}")]
    FrameDescriptor {
        /// The FLG byte.
        flg: u8,
        /// The BD byte.
        bd: u8,
    },
    /// The LZ4 frame declares a content size other than one page.
    #[error("LZ4 frame declares {found} bytes of content")]
    ContentSize {
        /// The declared content size.
        found: u64,
    },
    /// The frame runs past the longest frame one page can need.
    #[error("page frame longer than {max} bytes")]
    FrameTooLong {
        /// The limit for this page size.
        max: usize,
    },
    /// LZ4 rejected a block.
    #[error("corrupt LZ4 block: {reason}")]
    Lz4 {
        /// LZ4's description of the fault.
        reason: String,
    },
    /// A legacy frame's header checksum does not match its descriptor.
    #[error("LZ4 frame header checksum {stored:#04x}, computed {computed:#04x}")]
    HeaderChecksum {
        /// The checksum byte in the frame.
        stored: u8,
        /// The checksum over the descriptor.
        computed: u8,
    },
    /// A legacy frame's block checksum does not match the block's bytes.
    #[error("LZ4 block checksum {stored:#010x}, computed {computed:#010x}")]
    BlockChecksum {
        /// The checksum in the frame.
        stored: u32,
        /// The checksum over the block as stored.
        computed: u32,
    },
    /// A legacy frame's content checksum does not match the page.
    #[error("LZ4 content checksum {stored:#010x}, computed {computed:#010x}")]
    ContentChecksum {
        /// The checksum in the frame.
        stored: u32,
        /// The checksum over the decompressed page.
        computed: u32,
    },
    /// An LZ4 block does not end in a literals-only token with a zero
    /// match-length nibble, as every compressor writes it.
    #[error("LZ4 block does not end in a literals-only token")]
    BlockEnd,
    /// The frame decompressed to something other than one page.
    #[error("page decompressed to {found} bytes, expected {expected}")]
    PageLen {
        /// Bytes produced.
        found: usize,
        /// The page size.
        expected: usize,
    },
}

/// What one unit of input completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    More,
    PageStart(NonZeroU32),
    Page(NonZeroU32),
    EndMarker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    PageHeader,
    BlockSize,
    Block { len: usize },
    Descriptor,
    DescriptorTail { len: usize },
    LegacyBlockSize,
    LegacyBlock { len: usize, stored: bool },
    ContentChecksum,
}

/// Parses page frames one unit at a time: the caller hands it exactly
/// [`FrameParser::need`] bytes per call.
#[derive(Debug)]
pub(crate) struct FrameParser {
    page: Vec<u8>,
    filled: usize,
    state: State,
    /// Bytes of the current frame consumed so far.
    len: usize,
    max_len: usize,
    bound: usize,
    pgno: NonZeroU32,
    flg: u8,
    bd: u8,
}

impl FrameParser {
    pub(crate) fn new(page_size: PageSize) -> Self {
        Self {
            page: vec![0; page_size.bytes()],
            filled: 0,
            state: State::PageHeader,
            len: 0,
            max_len: max_frame_len(page_size),
            bound: block_bound(page_size),
            pgno: NonZeroU32::MIN,
            flg: 0,
            bd: 0,
        }
    }

    /// Bytes the next call to [`FrameParser::consume`] takes.
    pub(crate) const fn need(&self) -> usize {
        match self.state {
            State::PageHeader => PAGE_HEADER_LEN,
            State::BlockSize | State::LegacyBlockSize | State::ContentChecksum => 4,
            State::Descriptor => 6,
            State::Block { len } | State::DescriptorTail { len } => len,
            State::LegacyBlock { len, .. } => len.saturating_add(self.block_checksum_len()),
        }
    }

    /// Whether the next unit is covered by the file checksum as it stands,
    /// rather than through the decompressed page.
    pub(crate) const fn hashes_unit(&self) -> bool {
        match self.state {
            State::PageHeader | State::BlockSize => true,
            State::Block { .. }
            | State::Descriptor
            | State::DescriptorTail { .. }
            | State::LegacyBlockSize
            | State::LegacyBlock { .. }
            | State::ContentChecksum => false,
        }
    }

    /// The last page decompressed.
    pub(crate) fn page(&self) -> &[u8] {
        &self.page
    }

    /// Length of the frame in progress or just finished, page header included.
    pub(crate) const fn frame_len(&self) -> usize {
        self.len
    }

    pub(crate) fn consume(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        if self.state == State::PageHeader {
            self.len = 0;
        }
        self.len = self.len.saturating_add(unit.len());
        let progress = match self.state {
            State::PageHeader => self.page_header(unit)?,
            State::BlockSize => self.block_size(unit)?,
            State::Block { .. } => self.block(unit)?,
            State::Descriptor => self.descriptor(unit)?,
            State::DescriptorTail { .. } => self.descriptor_tail(unit)?,
            State::LegacyBlockSize => self.legacy_block_size(unit)?,
            State::LegacyBlock { len, stored } => self.legacy_block(unit, len, stored)?,
            State::ContentChecksum => self.content_checksum(unit)?,
        };
        if self.state != State::PageHeader && self.len.saturating_add(self.need()) > self.max_len {
            return Err(FrameError::FrameTooLong { max: self.max_len });
        }
        Ok(progress)
    }

    const fn block_checksum_len(&self) -> usize {
        if self.flg & FLG_BLOCK_CHECKSUM == 0 {
            0
        } else {
            XXH32_LEN
        }
    }

    fn page_header(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let mut cursor = Cursor::new(unit);
        let (pgno, flags) = (cursor.u32(), cursor.u16());
        if flags & !FLAG_SIZE_PREFIX != 0 {
            return Err(FrameError::PageHeaderFlags { flags });
        }
        let Some(pgno) = NonZeroU32::new(pgno) else {
            return if flags == 0 {
                Ok(Progress::EndMarker)
            } else {
                Err(FrameError::ZeroPageNumber)
            };
        };
        self.pgno = pgno;
        self.filled = 0;
        self.state = if flags & FLAG_SIZE_PREFIX == 0 {
            State::Descriptor
        } else {
            State::BlockSize
        };
        Ok(Progress::PageStart(pgno))
    }

    fn block_size(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let len = usize_from(Cursor::new(unit).u32());
        self.state = State::Block {
            len: self.check_block_len(len, self.bound)?,
        };
        Ok(Progress::More)
    }

    fn check_block_len(&self, len: usize, max: usize) -> Result<usize, FrameError> {
        if len == 0 || len > max {
            return Err(FrameError::BlockLen {
                len: u64_from(len),
                max: u64_from(max),
            });
        }
        Ok(len)
    }

    fn block(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        self.filled = decompress_into(unit, &mut self.page).map_err(lz4)?;
        check_block_end(unit)?;
        self.complete()
    }

    fn content_checksum(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let progress = self.complete()?;
        let stored = Cursor::new(unit).u32_le();
        let computed = xxh32(&self.page);
        if stored != computed {
            return Err(FrameError::ContentChecksum { stored, computed });
        }
        Ok(progress)
    }

    fn complete(&mut self) -> Result<Progress, FrameError> {
        if self.filled != self.page.len() {
            return Err(FrameError::PageLen {
                found: self.filled,
                expected: self.page.len(),
            });
        }
        self.state = State::PageHeader;
        Ok(Progress::Page(self.pgno))
    }

    fn descriptor(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let mut cursor = Cursor::new(unit);
        let found: [u8; 4] = cursor.take();
        if found != LZ4_MAGIC {
            return Err(FrameError::FrameMagic { found });
        }
        let (flg, bd) = (cursor.u8(), cursor.u8());
        if flg & FLG_VERSION_MASK != FLG_VERSION_1
            || flg & FLG_UNSUPPORTED != 0
            || bd & BD_RESERVED != 0
            || bd < BD_MIN_BLOCK_SIZE
        {
            return Err(FrameError::FrameDescriptor { flg, bd });
        }
        self.flg = flg;
        self.bd = bd;
        let len = if flg & FLG_CONTENT_SIZE == 0 { 1 } else { 9 };
        self.state = State::DescriptorTail { len };
        Ok(Progress::More)
    }

    fn descriptor_tail(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let (size, stored) = unit.split_last_chunk::<1>().unwrap_or((&[], &[0]));
        if self.flg & FLG_CONTENT_SIZE != 0 {
            let found = Cursor::new(size).u64_le();
            if found != u64_from(self.page.len()) {
                return Err(FrameError::ContentSize { found });
            }
        }
        // The header checksum is the second byte of the xxHash32 of FLG, BD and
        // the content size when present.
        let mut descriptor = [self.flg, self.bd, 0, 0, 0, 0, 0, 0, 0, 0];
        let hashed = descriptor
            .get_mut(2..2_usize.saturating_add(size.len()))
            .map_or(0, |slot| {
                slot.copy_from_slice(size);
                size.len()
            });
        let [_, computed, _, _] = xxh32(
            descriptor
                .get(..2_usize.saturating_add(hashed))
                .unwrap_or_default(),
        )
        .to_le_bytes();
        let [stored] = *stored;
        if stored != computed {
            return Err(FrameError::HeaderChecksum { stored, computed });
        }
        self.state = State::LegacyBlockSize;
        Ok(Progress::More)
    }

    fn legacy_block_size(&mut self, unit: &[u8]) -> Result<Progress, FrameError> {
        let raw = Cursor::new(unit).u32_le();
        if raw == 0 {
            if self.flg & FLG_CONTENT_CHECKSUM == 0 {
                return self.complete();
            }
            self.state = State::ContentChecksum;
            return Ok(Progress::More);
        }
        let stored = raw & STORED != 0;
        let max = if stored {
            self.page.len().saturating_sub(self.filled)
        } else {
            self.bound
        };
        let len = self.check_block_len(usize_from(raw & !STORED), max)?;
        self.state = State::LegacyBlock { len, stored };
        Ok(Progress::More)
    }

    fn legacy_block(
        &mut self,
        unit: &[u8],
        len: usize,
        stored: bool,
    ) -> Result<Progress, FrameError> {
        let (data, checksum) = unit.split_at_checked(len).unwrap_or_default();
        if self.flg & FLG_BLOCK_CHECKSUM != 0 {
            let stored = Cursor::new(checksum).u32_le();
            let computed = xxh32(data);
            if stored != computed {
                return Err(FrameError::BlockChecksum { stored, computed });
            }
        }
        let (before, after) = self.page.split_at_mut(self.filled);
        let produced = if stored {
            after.get_mut(..data.len()).map_or(0, |out| {
                out.copy_from_slice(data);
                data.len()
            })
        } else {
            let produced = if self.flg & FLG_INDEPENDENT == 0 {
                decompress_into_with_dict(data, after, before).map_err(lz4)?
            } else {
                decompress_into(data, after).map_err(lz4)?
            };
            check_block_end(data)?;
            produced
        };
        self.filled = self.filled.saturating_add(produced);
        self.state = State::LegacyBlockSize;
        Ok(Progress::More)
    }
}

/// Reads a length continued in 255-valued bytes after a nibble of 15, from
/// `pos`; returns the length and the position after it, or `None` when the
/// block ends first.
fn extended_len(block: &[u8], nibble: u8, mut pos: usize) -> Option<(usize, usize)> {
    let mut len = usize::from(nibble);
    if nibble == 15 {
        loop {
            let byte = *block.get(pos)?;
            pos = pos.saturating_add(1);
            len = len.saturating_add(usize::from(byte));
            if byte != 255 {
                break;
            }
        }
    }
    Some((len, pos))
}

/// Walks the sequences of a block LZ4 already decompressed and checks that it
/// ends in a token holding only literals, with a zero match-length nibble.
fn check_block_end(block: &[u8]) -> Result<(), FrameError> {
    let mut pos = 0_usize;
    loop {
        let token = *block.get(pos).ok_or(FrameError::BlockEnd)?;
        let (literals, after) =
            extended_len(block, token >> 4, pos.saturating_add(1)).ok_or(FrameError::BlockEnd)?;
        pos = after.saturating_add(literals);
        if pos >= block.len() {
            return if pos == block.len() && token & 0x0f == 0 {
                Ok(())
            } else {
                Err(FrameError::BlockEnd)
            };
        }
        // Two offset bytes, then the match length's continuation.
        let (_, after) =
            extended_len(block, token & 0x0f, pos.saturating_add(2)).ok_or(FrameError::BlockEnd)?;
        pos = after;
    }
}

fn lz4(err: lz4_flex::block::DecompressError) -> FrameError {
    FrameError::Lz4 {
        reason: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PS: u32 = 512;

    fn feed(parser: &mut FrameParser, unit: &[u8]) -> Result<Progress, FrameError> {
        let need = parser.need();
        let result = parser.consume(unit);
        if need != unit.len() {
            return Err(FrameError::BlockLen { len: 0, max: 0 });
        }
        result
    }

    /// A frame of one-byte-or-larger stored blocks that is cut off the moment
    /// it reaches `max` bytes. The parser must refuse it as soon as the next
    /// unit would pass the limit (so memory stays bounded), and not before the
    /// unit that lands exactly on it.
    #[test]
    fn frame_limit_is_enforced_mid_frame_and_exactly_at_the_limit() {
        let size = PageSize::new(PS).unwrap();
        let max = max_frame_len(size);
        let mut parser = FrameParser::new(size);
        let mut page_header = 1_u32.to_be_bytes().to_vec();
        page_header.extend_from_slice(&[0, 0]);
        feed(&mut parser, &page_header).unwrap();
        let mut descriptor = LZ4_MAGIC.to_vec();
        descriptor.extend_from_slice(&[0x64, 0x40]);
        feed(&mut parser, &descriptor).unwrap();
        let [_, header_checksum, _, _] = xxh32(&[0x64, 0x40]).to_le_bytes();
        feed(&mut parser, &[header_checksum]).unwrap();
        assert_eq!(parser.frame_len(), 13);

        // Forty blocks that bring the frame to max - 4 bytes: the next
        // four-byte size field then lands exactly on max, which is allowed.
        let blocks = 40;
        let total = max - 4 - parser.frame_len();
        let last = total - 4 * blocks - (blocks - 1);
        for at in 0..blocks {
            let len = if at + 1 == blocks { last } else { 1 };
            let tag = STORED | u32::try_from(len).unwrap();
            feed(&mut parser, &tag.to_le_bytes()).unwrap();
            feed(&mut parser, &vec![7; len]).unwrap();
        }
        assert_eq!(parser.frame_len(), max - 4);
        let tag = STORED | 1;
        assert_eq!(
            feed(&mut parser, &tag.to_le_bytes()),
            Err(FrameError::FrameTooLong { max })
        );
    }
}
