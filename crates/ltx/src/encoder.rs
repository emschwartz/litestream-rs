//! The strict encoder: writes only files every Go reader since ltx v0.5.2 accepts.
//!
//! Pages are written in the block encoding (see [`crate::FrameError`]'s
//! module) and the encoder refuses anything Go's encoder refuses: a page of the
//! wrong length, a page out of order or above the commit size, the lock page,
//! an incomplete snapshot, a header with commit 0 (Go writes one only tracked
//! and SQLite never commits an empty WAL-mode database; the decoder still
//! accepts it), a post-apply checksum that does not fit the header,
//! and a tracked snapshot whose post-apply checksum is not its pages'. The
//! index is kept in memory as varint bytes, at most 30 bytes a page.

use std::collections::VecDeque;
use std::io::{self, Write};

use lz4_flex::block::{compress_into, get_maximum_output_size};

use crate::checksum::Crc64;
use crate::conv::u64_from;
use crate::frame::PAGE_HEADER_LEN;
use crate::index::put_uvarint;
use crate::order::PageSequence;
use crate::trailer::check_post_apply;
use crate::{DbChecksum, EncodeError, HEADER_LEN, Header, PageChecksum, PageNo, Trailer};

/// The most one page adds to the index: three varints of at most ten bytes.
const MAX_ENTRY: usize = 30;

/// How much index one allocation holds: the index grows by whole chunks, never by copying.
const INDEX_CHUNK: usize = 16 * 1024;

/// Writes one LTX file to `W`.
#[derive(Debug)]
pub struct Encoder<W> {
    writer: W,
    header: Header,
    seq: PageSequence,
    file: Crc64,
    offset: u64,
    index: VecDeque<Vec<u8>>,
    phase: Phase,
    /// Bytes of index written so far.
    index_written: u64,
    /// The compressor's output for one page, reused so a page costs no allocation.
    block: Vec<u8>,
    /// The database checksum so far, for tracked snapshots only.
    db: Option<DbChecksum>,
}

/// How far the file is written. Only `Pages` takes a page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Pages are being written.
    Pages,
    /// The end-of-pages marker is written, so no page may follow it.
    Index,
    /// A write failed; nothing more can be written.
    Failed,
}

impl<W: Write> Encoder<W> {
    /// Writes the header.
    ///
    /// # Errors
    ///
    /// [`EncodeError::ZeroCommit`] for a header with commit 0, before anything
    /// is written; [`EncodeError::Io`] if the write fails.
    pub fn new(writer: W, header: Header) -> Result<Self, EncodeError> {
        if header.commit() == 0 {
            return Err(EncodeError::ZeroCommit);
        }
        Self::start(writer, header)
    }

    fn start(mut writer: W, header: Header) -> Result<Self, EncodeError> {
        let bytes = header.encode();
        writer.write_all(&bytes)?;
        let mut file = Crc64::new();
        file.update(&bytes);
        Ok(Self {
            writer,
            header,
            seq: PageSequence::new(&header),
            file,
            offset: u64_from(HEADER_LEN),
            index: VecDeque::new(),
            phase: Phase::Pages,
            index_written: 0,
            block: vec![0; get_maximum_output_size(header.page_size().bytes())],
            db: header.snapshot_db_checksum(),
        })
    }

    /// Writes one page.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PageLen`] or [`EncodeError::Order`] reject the page and
    /// leave the encoder usable; [`EncodeError::Io`] poisons it.
    pub fn write_page(&mut self, pgno: PageNo, data: &[u8]) -> Result<(), EncodeError> {
        if self.phase != Phase::Pages {
            return Err(EncodeError::Poisoned);
        }
        let expected = self.header.page_size().bytes();
        if data.len() != expected {
            return Err(EncodeError::PageLen {
                found: data.len(),
                expected,
            });
        }
        let mut seq = self.seq;
        let pgno = seq.push(pgno.nonzero())?;
        let mut block = std::mem::take(&mut self.block);
        let Ok(used) = compress_into(data, &mut block) else {
            // The buffer holds the compressor's worst case for a page, so this cannot happen.
            self.phase = Phase::Failed;
            return Err(EncodeError::Io(io::Error::other(
                "lz4 output buffer too small",
            )));
        };
        let len = u32::try_from(used).unwrap_or(u32::MAX);
        let mut head = [0_u8; PAGE_HEADER_LEN + 4];
        head[0..4].copy_from_slice(&pgno.get().to_be_bytes());
        head[4..6].copy_from_slice(&1_u16.to_be_bytes());
        head[6..10].copy_from_slice(&len.to_be_bytes());
        let packed = block.get(..used).unwrap_or_default();
        self.write(&head)?;
        self.write(packed)?;
        self.seq = seq;
        self.file.update(&head);
        self.file.update(data);
        self.db = self.db.map(|db| db.toggle(PageChecksum::of(pgno, data)));
        let size = u64_from(head.len()).saturating_add(u64_from(packed.len()));
        self.block = block;
        let room = self
            .index
            .back()
            .is_some_and(|chunk| chunk.capacity().saturating_sub(chunk.len()) >= MAX_ENTRY);
        if !room {
            self.index.push_back(Vec::with_capacity(INDEX_CHUNK));
        }
        if let Some(chunk) = self.index.back_mut() {
            put_uvarint(chunk, u64::from(pgno.get()));
            put_uvarint(chunk, self.offset);
            put_uvarint(chunk, size);
        }
        self.offset = self.offset.saturating_add(size);
        Ok(())
    }

    /// Writes the end-of-pages marker, the index and the trailer, flushes, and
    /// returns the writer and the trailer written. `post_apply` must be present
    /// exactly when the header is tracked.
    ///
    /// # Errors
    ///
    /// [`EncodeError::Order`] for an incomplete snapshot,
    /// [`EncodeError::Trailer`] or [`EncodeError::PostApplyMismatch`] for a
    /// post-apply checksum that does not fit, [`EncodeError::Io`] or
    /// [`EncodeError::Poisoned`] for a failed write. Nothing is written on the
    /// first three.
    pub fn finish(mut self, post_apply: Option<DbChecksum>) -> Result<(W, Trailer), EncodeError> {
        if self.phase == Phase::Failed {
            return Err(EncodeError::Poisoned);
        }
        self.seq.finish()?;
        check_post_apply(&self.header, post_apply)?;
        if let (Some(computed), Some(given)) = (self.db, post_apply)
            && computed != given
        {
            return Err(EncodeError::PostApplyMismatch { computed, given });
        }
        // The index goes out a chunk at a time, each freed once written: copying it into one
        // buffer first held it two or three times over at the end of a large snapshot.
        while self.write_index_chunk()? {}
        let mut tail = Vec::with_capacity(32);
        put_uvarint(&mut tail, 0);
        let index_len = self.index_written.saturating_add(u64_from(tail.len()));
        tail.extend_from_slice(&index_len.to_be_bytes());
        tail.extend_from_slice(&post_apply.map_or(0, DbChecksum::get).to_be_bytes());
        self.file.update(&tail);
        let trailer = Trailer::new(post_apply, self.file.finish_file());
        tail.extend_from_slice(&trailer.file_checksum().get().to_be_bytes());
        self.writer.write_all(&tail)?;
        self.writer.flush()?;
        Ok((self.writer, trailer))
    }

    /// Writes the end-of-pages marker the first time, then the next piece of the page index,
    /// and says whether more of the index is left. A caller that buffers in memory calls it
    /// after the last page until it returns `false`, handing the bytes onward between calls, so
    /// the index (about nine bytes a page) is never held a second time in the output buffer;
    /// [`Encoder::finish`] writes whatever is left. No page may be written once it has been
    /// called.
    ///
    /// # Errors
    ///
    /// [`EncodeError::Order`] for an incomplete snapshot (nothing is written),
    /// [`EncodeError::Io`] or [`EncodeError::Poisoned`] for a failed write.
    pub fn write_index_chunk(&mut self) -> Result<bool, EncodeError> {
        if self.phase == Phase::Failed {
            return Err(EncodeError::Poisoned);
        }
        self.seq.finish()?;
        if self.phase == Phase::Pages {
            let marker = [0_u8; PAGE_HEADER_LEN];
            self.file.update(&marker);
            self.write(&marker)?;
            self.phase = Phase::Index;
        }
        let Some(chunk) = self.index.pop_front() else {
            return Ok(false);
        };
        self.file.update(&chunk);
        self.write(&chunk)?;
        self.index_written = self.index_written.saturating_add(u64_from(chunk.len()));
        Ok(!self.index.is_empty())
    }

    /// The writer, so a caller that buffers in memory can hand finished bytes
    /// onward between pages instead of holding the whole file.
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.writer.write_all(bytes).map_err(|err| {
            self.phase = Phase::Failed;
            EncodeError::Io(err)
        })
    }
}

/// A file with commit 0 and no pages, which the decoder accepts and
/// [`Encoder::new`] refuses. For tests of readers that must handle one.
///
/// # Errors
///
/// [`EncodeError::Trailer`] when `post_apply` does not fit the header (a
/// tracked file needs `DbChecksum::EMPTY`, an untracked one none), and
/// [`EncodeError::Order`] when the header is a snapshot with commit above 0.
#[cfg(any(test, feature = "testing"))]
pub fn encode_commit_zero(
    header: Header,
    post_apply: Option<DbChecksum>,
) -> Result<Vec<u8>, EncodeError> {
    Ok(Encoder::start(Vec::new(), header)?.finish(post_apply)?.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decoder, Event, HeaderFields, PageSize, Timestamp, Txid, TxidRange};

    #[test]
    fn commit_zero_file_decodes_to_a_verified_empty_file() {
        let header = Header::new(HeaderFields {
            tracked: true,
            page_size: PageSize::new(512).unwrap(),
            commit: 0,
            txids: TxidRange::single(Txid::MIN),
            timestamp: Timestamp::from_unix_millis(7),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let bytes = encode_commit_zero(header, Some(DbChecksum::EMPTY)).unwrap();
        let mut decoder = Decoder::new();
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let (used, event) = decoder.feed(rest).unwrap();
            assert!(!matches!(event, Some(Event::Page { .. })), "no pages");
            rest = &rest[used..];
        }
        let verified = decoder.finish().unwrap();
        assert_eq!(verified.page_count(), 0);
        assert_eq!(verified.header(), &header);
        assert!(matches!(
            Encoder::new(Vec::new(), header),
            Err(EncodeError::ZeroCommit)
        ));
    }

    #[test]
    fn bytes_drained_between_pages_reassemble_into_the_same_verified_file() {
        let size = PageSize::new(512).unwrap();
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size: size,
            commit: 3,
            txids: TxidRange::single(Txid::MIN),
            timestamp: Timestamp::from_unix_millis(1),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        let mut drained = Vec::new();
        for pgno in 1..=3_u32 {
            let page = vec![u8::try_from(pgno).unwrap(); 512];
            encoder
                .write_page(PageNo::new(pgno, size).unwrap(), &page)
                .unwrap();
            drained.append(encoder.writer_mut());
            assert!(encoder.writer_mut().is_empty());
        }
        let (tail, _) = encoder.finish(None).unwrap();
        drained.extend_from_slice(&tail);

        let mut whole = Encoder::new(Vec::new(), header).unwrap();
        for pgno in 1..=3_u32 {
            let page = vec![u8::try_from(pgno).unwrap(); 512];
            whole
                .write_page(PageNo::new(pgno, size).unwrap(), &page)
                .unwrap();
        }
        let (expected, _) = whole.finish(None).unwrap();
        assert_eq!(drained, expected);

        let mut decoder = Decoder::new();
        let mut rest = drained.as_slice();
        while !rest.is_empty() {
            let (used, _) = decoder.feed(rest).unwrap();
            rest = &rest[used..];
        }
        assert_eq!(decoder.finish().unwrap().page_count(), 3);
    }

    #[test]
    fn page_blocks_are_the_compressors_own_output_whatever_the_reused_buffer_held() {
        let size = PageSize::new(512).unwrap();
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size: size,
            commit: 3,
            txids: TxidRange::single(Txid::MIN),
            timestamp: Timestamp::from_unix_millis(1),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        // An incompressible page, a compressible one, then a short-lived pattern: the buffer
        // holds the previous page's block when the next is compressed.
        let pages: Vec<Vec<u8>> = vec![
            (0..512_u32)
                .map(|i| u8::try_from((i.wrapping_mul(2_654_435_761) >> 13) & 0xff).unwrap())
                .collect(),
            vec![7; 512],
            (0..512_u32).map(|i| u8::try_from(i % 5).unwrap()).collect(),
        ];
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (n, page) in (1..=3_u32).zip(&pages) {
            encoder
                .write_page(PageNo::new(n, size).unwrap(), page)
                .unwrap();
        }
        let (bytes, _) = encoder.finish(None).unwrap();
        let mut at = HEADER_LEN;
        for page in &pages {
            let len = usize::try_from(u32::from_be_bytes(
                bytes[at + 6..at + 10].try_into().unwrap(),
            ))
            .unwrap();
            let block = &bytes[at + PAGE_HEADER_LEN + 4..at + PAGE_HEADER_LEN + 4 + len];
            assert_eq!(block, lz4_flex::block::compress(page).as_slice());
            at += PAGE_HEADER_LEN + 4 + len;
        }
    }
}
