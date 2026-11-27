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

#[cfg(test)]
mod tests {
    use super::compressed_block_spans;
    use crate::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};

    /// A Rust-written file of one 512-byte zero page with 0x55 at offset 100:
    /// one block at 110..132 of four tokens, at 110 (0x1f: one literal, a
    /// match continued in one byte), 115 (0x1f), 120 (0x0f: no literals, a
    /// match continued in two bytes) and 125 (0x60: six literals to the end).
    #[test]
    fn a_rust_block_splits_at_its_last_token() {
        let page_size = PageSize::new(512).unwrap();
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size,
            commit: 1,
            txids: TxidRange::new(Txid::new(1).unwrap(), Txid::new(1).unwrap()).unwrap(),
            timestamp: Timestamp::from_unix_millis(0),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut page = vec![0_u8; 512];
        page[100] = 0x55;
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        encoder
            .write_page(PageNo::new(1, page_size).unwrap(), &page)
            .unwrap();
        let bytes = encoder.finish(None).unwrap().0;
        assert_eq!(
            bytes[110..132],
            [
                0x1f, 0x00, 0x01, 0x00, 0x50, 0x1f, 0x55, 0x64, 0x00, 0x50, 0x0f, 0x02, 0x00, 0xff,
                0x20, 0x60, 0, 0, 0, 0, 0, 0
            ],
            "the encoder's output this test pins"
        );
        assert_eq!(
            compressed_block_spans(&bytes),
            Some(vec![110..125, 126..132])
        );
    }

    /// A Go ltx v0.5.1 file: one legacy frame (FLG 0x64, content checksum on)
    /// whose single compressed block of 1062 bytes starts at 117 and whose last
    /// token, 0xe0 (fourteen literals), is at 1164.
    #[test]
    fn a_go_legacy_block_splits_at_its_last_token() {
        let bytes = include_bytes!(
            "../tests/fixtures/ltx-v0.5.1/4096/0000000000000004-0000000000000005.ltx"
        );
        assert_eq!(bytes[113..117], 1062_u32.to_le_bytes());
        assert_eq!(bytes[1164], 0xe0);
        assert_eq!(
            compressed_block_spans(bytes),
            Some(vec![117..1164, 1165..1179])
        );
    }

    /// Two legacy frames without a content checksum (FLG 0x60): nothing follows
    /// the end mark, so the second frame starts right after it. Each block is
    /// one literal and a match of nibble 2 (no continuation), then a token of
    /// three literals.
    #[test]
    fn legacy_frames_without_a_content_checksum_end_at_the_end_mark() {
        let block = [0x12, b'x', 0x01, 0x00, 0x30, b'a', b'b', b'c'];
        let mut file = vec![0_u8; 100];
        for pgno in [1_u8, 2] {
            file.extend_from_slice(&[0, 0, 0, pgno, 0, 0]);
            file.extend_from_slice(&0x184d_2204_u32.to_le_bytes());
            file.extend_from_slice(&[0x60, 0x40, 0]);
            file.extend_from_slice(&8_u32.to_le_bytes());
            file.extend_from_slice(&block);
            file.extend_from_slice(&[0; 4]);
        }
        file.extend_from_slice(&[0; 6]);
        assert_eq!(
            compressed_block_spans(&file),
            Some(vec![117..121, 122..125, 146..150, 151..154])
        );
    }
}
