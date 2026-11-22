//! Where a file's compressed LZ4 blocks lie, for the single-byte-change
//! property: a changed byte outside these spans must fail the decode, and one
//! inside them must fail it or decode to the same pages (see
//! [`crate::FrameError`]'s module docs on aliasing). Each block's last token
//! is left out of the spans: its match-length nibble is checked, and a change
//! to its literal count moves the block's end, so a change there must fail.
//!
//! This walks the layout on its own, apart from the decoder, so a decoder that
//! skips a field cannot also widen what the property forgives.

use std::ops::Range;

use crate::HEADER_LEN;
use crate::conv::{Cursor, usize_from};

const FLG_BLOCK_CHECKSUM: u8 = 0x10;
const FLG_CONTENT_SIZE: u8 = 0x08;
const FLG_CONTENT_CHECKSUM: u8 = 0x04;
const STORED: u32 = 0x8000_0000;

struct Walk<'a> {
    file: &'a [u8],
    pos: usize,
}

impl Walk<'_> {
    fn take(&mut self, len: usize) -> Option<Range<usize>> {
        let end = self.pos.checked_add(len)?;
        self.file.get(self.pos..end)?;
        let range = self.pos..end;
        self.pos = end;
        Some(range)
    }

    fn bytes(&mut self, len: usize) -> Option<&[u8]> {
        let range = self.take(len)?;
        self.file.get(range)
    }

    /// Pushes `block` as two spans, before and after its last token.
    fn push_block(&self, block: Range<usize>, out: &mut Vec<Range<usize>>) -> Option<()> {
        let last = block
            .start
            .checked_add(last_token(self.file.get(block.clone())?)?)?;
        out.push(block.start..last);
        out.push(last.checked_add(1)?..block.end);
        Some(())
    }

    fn legacy_blocks(&mut self, out: &mut Vec<Range<usize>>) -> Option<()> {
        self.take(4)?; // magic
        let flg = *self.bytes(1)?.first()?;
        self.take(1)?; // BD
        if flg & FLG_CONTENT_SIZE != 0 {
            self.take(8)?;
        }
        self.take(1)?; // header checksum
        loop {
            let raw = Cursor::new(self.bytes(4)?).u32_le();
            if raw == 0 {
                break;
            }
            let block = self.take(usize_from(raw & !STORED))?;
            if raw & STORED == 0 {
                self.push_block(block, out)?;
            }
            if flg & FLG_BLOCK_CHECKSUM != 0 {
                self.take(4)?;
            }
        }
        if flg & FLG_CONTENT_CHECKSUM != 0 {
            self.take(4)?;
        }
        Some(())
    }
}

/// A length continued in 255-valued bytes after a nibble of 15: the position
/// after it.
fn skip_len(block: &[u8], nibble: u8, mut pos: usize) -> Option<(usize, usize)> {
    let mut len = usize::from(nibble);
    if nibble == 15 {
        loop {
            let byte = *block.get(pos)?;
            pos = pos.checked_add(1)?;
            len = len.checked_add(usize::from(byte))?;
            if byte != 255 {
                return Some((len, pos));
            }
        }
    }
    Some((len, pos))
}

/// The offset of the token whose literals reach the end of `block`.
fn last_token(block: &[u8]) -> Option<usize> {
    let mut pos = 0_usize;
    loop {
        let token = *block.get(pos)?;
        let (literals, after) = skip_len(block, token >> 4, pos.checked_add(1)?)?;
        let next = after.checked_add(literals)?;
        if next >= block.len() {
            return Some(pos);
        }
        pos = skip_len(block, token & 0x0f, next.checked_add(2)?)?.1;
    }
}

/// The byte ranges of every compressed LZ4 block in `file`, in order, or
/// `None` when the page section does not parse.
#[must_use]
pub fn compressed_block_spans(file: &[u8]) -> Option<Vec<Range<usize>>> {
    let mut walk = Walk {
        file,
        pos: HEADER_LEN,
    };
    let mut out = Vec::new();
    loop {
        let mut cursor = Cursor::new(walk.bytes(6)?);
        let (pgno, flags) = (cursor.u32(), cursor.u16());
        if pgno == 0 {
            return Some(out);
        }
        if flags & 1 == 0 {
            walk.legacy_blocks(&mut out)?;
        } else {
            let len = usize_from(Cursor::new(walk.bytes(4)?).u32());
            let block = walk.take(len)?;
            walk.push_block(block, &mut out)?;
        }
    }
}
