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

use std::io::Write;

use lz4_flex::block::compress;

use crate::checksum::Crc64;
use crate::conv::u64_from;
use crate::frame::PAGE_HEADER_LEN;
use crate::index::put_uvarint;
use crate::order::PageSequence;
use crate::trailer::check_post_apply;
use crate::{Checksum, EncodeError, HEADER_LEN, Header, PageNo, Trailer};

/// Writes one LTX file to `W`.
#[derive(Debug)]
pub struct Encoder<W> {
    writer: W,
    header: Header,
    seq: PageSequence,
    file: Crc64,
    offset: u64,
    index: Vec<u8>,
    /// The database checksum so far, for tracked snapshots only.
    db: Option<Checksum>,
    failed: bool,
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
            index: Vec::new(),
            db: (header.tracked() && header.is_snapshot()).then_some(Checksum::EMPTY),
            failed: false,
        })
    }

    /// Writes one page.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PageLen`] or [`EncodeError::Order`] reject the page and
    /// leave the encoder usable; [`EncodeError::Io`] poisons it.
    pub fn write_page(&mut self, pgno: PageNo, data: &[u8]) -> Result<(), EncodeError> {
        if self.failed {
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
        let block = compress(data);
        let len = u32::try_from(block.len()).unwrap_or(u32::MAX);
        let mut head = [0_u8; PAGE_HEADER_LEN + 4];
        head[0..4].copy_from_slice(&pgno.get().to_be_bytes());
        head[4..6].copy_from_slice(&1_u16.to_be_bytes());
        head[6..10].copy_from_slice(&len.to_be_bytes());
        self.write(&head)?;
        self.write(&block)?;
        self.seq = seq;
        self.file.update(&head);
        self.file.update(data);
        self.db = self.db.map(|db| db.toggle(Checksum::of_page(pgno, data)));
        let size = u64_from(head.len()).saturating_add(u64_from(block.len()));
        put_uvarint(&mut self.index, u64::from(pgno.get()));
        put_uvarint(&mut self.index, self.offset);
        put_uvarint(&mut self.index, size);
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
    pub fn finish(mut self, post_apply: Option<Checksum>) -> Result<(W, Trailer), EncodeError> {
        if self.failed {
            return Err(EncodeError::Poisoned);
        }
        self.seq.finish()?;
        check_post_apply(&self.header, post_apply)?;
        if let (Some(computed), Some(given)) = (self.db, post_apply)
            && computed != given
        {
            return Err(EncodeError::PostApplyMismatch { computed, given });
        }
        let mut tail = vec![0_u8; PAGE_HEADER_LEN];
        tail.append(&mut self.index);
        put_uvarint(&mut tail, 0);
        let index_len = u64_from(tail.len().saturating_sub(PAGE_HEADER_LEN));
        tail.extend_from_slice(&index_len.to_be_bytes());
        tail.extend_from_slice(&post_apply.map_or(0, Checksum::get).to_be_bytes());
        self.file.update(&tail);
        let trailer = Trailer::new(post_apply, self.file.finish());
        tail.extend_from_slice(&trailer.file_checksum().get().to_be_bytes());
        self.writer.write_all(&tail)?;
        self.writer.flush()?;
        Ok((self.writer, trailer))
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.writer.write_all(bytes).map_err(|err| {
            self.failed = true;
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
/// tracked file needs `Checksum::EMPTY`, an untracked one none), and
/// [`EncodeError::Order`] when the header is a snapshot with commit above 0.
#[cfg(feature = "testing")]
pub fn encode_commit_zero(
    header: Header,
    post_apply: Option<Checksum>,
) -> Result<Vec<u8>, EncodeError> {
    Ok(Encoder::start(Vec::new(), header)?.finish(post_apply)?.0)
}
