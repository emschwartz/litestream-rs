//! Scanning a WAL for committed transactions.

use std::io::{self, Read, Seek, SeekFrom};
use std::num::NonZeroU32;

use crate::checksum::WalChecksum;
use crate::conv::{Fields, usize_from};
use crate::error::{OpenError, ReadPageError, ResumeError, WalError};
use crate::frame::{
    CommittedEnd, FRAME_HEADER_LEN, FrameFault, FrameRef, VerifiedFrame, WalOffset, frame_len,
    verify_frame,
};
use crate::header::{HEADER_LEN, Salts, WalHeader};
use lsr_ltx::{PageChecksum, PageSize};

/// Where the next frame starts and the checksum it continues from.
///
/// A cursor taken from `cursor_at_start` or from a scan's result is verified
/// all the way back to the header. One from [`WalReader::resume`] is verified
/// at the frame before it only, which is enough to continue the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameCursor {
    next: WalOffset,
    checksum: WalChecksum,
    salts: Salts,
}

impl FrameCursor {
    /// The cursor before the first frame of the generation `header` names.
    #[must_use]
    pub const fn at_start(header: &WalHeader) -> Self {
        Self {
            next: WalOffset::start(header.page_size()),
            checksum: header.checksum(),
            salts: header.salts(),
        }
    }

    /// Where the next frame starts.
    #[must_use]
    pub const fn next(&self) -> WalOffset {
        self.next
    }

    /// The running checksum the next frame continues from.
    #[must_use]
    pub const fn checksum(&self) -> WalChecksum {
        self.checksum
    }

    /// The generation this cursor belongs to.
    #[must_use]
    pub const fn salts(&self) -> Salts {
        self.salts
    }
}

/// How far a scan may trust the bytes it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanLimit {
    /// Frames below this end are known committed: a bad or missing one is an
    /// error, and no byte at or past the end is read.
    Committed(CommittedEnd),
    /// No end is known: the first invalid frame ends the scan, as in SQLite's own recovery.
    FirstInvalidFrame,
}

/// Why a scan stopped without a committed transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tail {
    /// The WAL ends exactly after the last commit.
    Clean,
    /// The file ends inside a frame.
    PartialFrame,
    /// A frame failed verification.
    InvalidFrame(FrameFault),
    /// Valid frames follow the last commit but no commit frame does: a transaction
    /// still open, or one rolled back.
    Uncommitted {
        /// How many such frames.
        frames: u32,
    },
}

/// A transaction: valid frames up to and including one with a non-zero database size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedTx {
    frames: Vec<FrameRef>,
    commit: NonZeroU32,
    end: FrameCursor,
}

impl CommittedTx {
    /// Every frame of the transaction in WAL order, a page possibly more than once.
    #[must_use]
    pub fn frames(&self) -> &[FrameRef] {
        &self.frames
    }

    /// The database size in pages after the transaction: never zero, since a
    /// commit frame is the one frame that stores a size.
    #[must_use]
    pub const fn commit(&self) -> NonZeroU32 {
        self.commit
    }

    /// The cursor after the commit frame.
    #[must_use]
    pub const fn end(&self) -> &FrameCursor {
        &self.end
    }
}

/// What one step of a scan found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxOutcome {
    /// The next committed transaction.
    Tx(CommittedTx),
    /// No further committed transaction.
    End {
        /// Where the scan stood after its last committed transaction; scanning
        /// again from here sees any frames that arrive later.
        at: FrameCursor,
        /// Why the scan stopped.
        tail: Tail,
    },
}

/// A WAL file opened for reading.
///
/// It holds the header as read at `open`. Open a fresh reader for every scan:
/// a WAL restarted in between is then seen as a new generation.
#[derive(Debug)]
pub struct WalReader<R> {
    reader: R,
    header: WalHeader,
}

/// A buffer for one whole frame.
fn frame_buf(page_size: PageSize) -> Vec<u8> {
    vec![0; usize_from(u32::try_from(frame_len(page_size)).unwrap_or(0))]
}

/// Reads up to `buf.len()` bytes at `at`, fewer only at the end of the file.
fn read_at<R: Read + Seek>(reader: &mut R, at: u64, buf: &mut [u8]) -> io::Result<usize> {
    reader.seek(SeekFrom::Start(at))?;
    let mut filled = 0;
    while let Some(rest) = buf.get_mut(filled..).filter(|rest| !rest.is_empty()) {
        match reader.read(rest) {
            Ok(0) => break,
            Ok(n) => filled = filled.saturating_add(n),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(filled.min(buf.len()))
}

impl<R: Read + Seek> WalReader<R> {
    /// Reads and verifies the header.
    ///
    /// # Errors
    /// [`OpenError`]: an I/O failure, a file shorter than a header, or an invalid header.
    pub fn open(mut reader: R) -> Result<Self, OpenError> {
        let mut bytes = [0; HEADER_LEN];
        let found = read_at(&mut reader, 0, &mut bytes)?;
        if found < HEADER_LEN {
            return Err(OpenError::Truncated { found });
        }
        Ok(Self {
            reader,
            header: WalHeader::decode(&bytes)?,
        })
    }

    /// The verified header.
    #[must_use]
    pub const fn header(&self) -> &WalHeader {
        &self.header
    }

    /// Whether the file's header, read again now, still names the generation this
    /// reader was opened on.
    ///
    /// A short file, an unreadable header or other salts answer `false`: the WAL
    /// was truncated or restarted since `open`. SQLite writes a restart's header
    /// before any new frame, so after a failed read below the committed end a
    /// header that still matches means the bytes are wrong inside one generation.
    ///
    /// # Errors
    /// An I/O failure reading the header.
    pub fn generation_unchanged(&mut self) -> io::Result<bool> {
        let mut bytes = [0; HEADER_LEN];
        if read_at(&mut self.reader, 0, &mut bytes)? < HEADER_LEN {
            return Ok(false);
        }
        Ok(WalHeader::decode(&bytes).is_ok_and(|now| now.salts() == self.header.salts()))
    }

    /// The cursor before the first frame.
    #[must_use]
    pub const fn cursor_at_start(&self) -> FrameCursor {
        FrameCursor::at_start(&self.header)
    }

    /// A cursor at `at`, the position and generation (`expected`) a caller
    /// remembered from an earlier sync.
    ///
    /// The WAL header's salts must be `expected`, also when `at` is the start:
    /// a restarted WAL may have regrown past `at`, and only the remembered
    /// generation can tell. Unless `at` is the start, the frame before it is
    /// read whole and verified (salts, checksum chained from the frame before
    /// that, and a non-zero commit field), so `at` is a transaction boundary
    /// of this generation and not a byte position that merely looks like one.
    /// The cost is two frame-header reads and one page read per call.
    /// `cursor_at_start` is for a caller with no position (first sync, snapshot).
    ///
    /// # Errors
    /// [`ResumeError`]: a wrong page size, a WAL too short, a restarted WAL,
    /// a frame before `at` that does not verify, or an `at` inside a transaction.
    pub fn resume(&mut self, at: WalOffset, expected: Salts) -> Result<FrameCursor, ResumeError> {
        Ok(match self.verified_before(at, expected)? {
            None => self.cursor_at_start(),
            Some((_, frame)) => FrameCursor {
                next: at,
                checksum: frame.checksum,
                salts: expected,
            },
        })
    }

    /// Whether a whole committed transaction chains on from `cursor`: frames that
    /// verify with the cursor's salts and checksum, whatever generation this WAL's
    /// header names, up to a commit frame.
    ///
    /// A restarted WAL overwrites its old frames in place, so frames of the old
    /// generation can outlive the restart past the new one's end. A caller that
    /// remembers a cursor asks this to learn whether a commit did. Frames with no
    /// commit after them are a rolled-back spill, never part of the database. A
    /// file that ends first, or a frame that does not verify, answers `false`.
    ///
    /// # Errors
    /// An I/O failure reading a frame.
    pub fn commit_follows(&mut self, cursor: &FrameCursor) -> io::Result<bool> {
        let page_size = cursor.next().page_size();
        let mut buf = frame_buf(page_size);
        let (mut at, mut checksum) = (cursor.next(), cursor.checksum());
        loop {
            if read_at(&mut self.reader, at.get(), &mut buf)? < buf.len() {
                return Ok(false);
            }
            let Ok(frame) = verify_frame(
                &buf,
                self.header.byte_order(),
                page_size,
                cursor.salts(),
                checksum,
            ) else {
                return Ok(false);
            };
            if frame.commit != 0 {
                return Ok(true);
            }
            let Some(next) = at.next_frame() else {
                return Ok(false);
            };
            (at, checksum) = (next, frame.checksum);
        }
    }

    /// The commit frame right before `at`, verified exactly as [`Self::resume`] does.
    ///
    /// It carries the page number and digest the frame was scanned with, for a
    /// caller that remembers the last page it covered. `at` at the start has no
    /// frame before it and answers [`ResumeError::NotAtCommit`].
    ///
    /// # Errors
    /// [`ResumeError`], as for `resume`.
    pub fn frame_before(
        &mut self,
        at: WalOffset,
        expected: Salts,
    ) -> Result<FrameRef, ResumeError> {
        match self.verified_before(at, expected)? {
            Some((offset, frame)) => Ok(FrameRef {
                pgno: frame.pgno,
                offset,
                digest: frame.digest,
            }),
            None => Err(ResumeError::NotAtCommit { at }),
        }
    }

    /// The verified commit frame before `at` and where it starts; `None` at the start.
    fn verified_before(
        &mut self,
        at: WalOffset,
        expected: Salts,
    ) -> Result<Option<(WalOffset, VerifiedFrame)>, ResumeError> {
        let page_size = self.header.page_size();
        if at.page_size() != page_size {
            return Err(ResumeError::PageSizeMismatch {
                expected: page_size,
                found: at.page_size(),
            });
        }
        let found = self.header.salts();
        if found != expected {
            return Err(ResumeError::Restarted { expected, found });
        }
        let Some(prev) = at.prev_frame() else {
            return Ok(None);
        };
        let running = self.checksum_before(prev, at)?;
        let mut buf = frame_buf(page_size);
        if read_at(&mut self.reader, prev.get(), &mut buf)? < buf.len() {
            return Err(ResumeError::Truncated { at });
        }
        let frame = verify_frame(&buf, self.header.byte_order(), page_size, expected, running)
            .map_err(|fault| ResumeError::PrevFrame { at, fault })?;
        if frame.commit == 0 {
            return Err(ResumeError::NotAtCommit { at });
        }
        Ok(Some((prev, frame)))
    }

    /// The stored checksum of the frame before `frame`, or the header's for frame 0.
    /// It is not trusted on its own: the frame after it must chain from it.
    fn checksum_before(
        &mut self,
        frame: WalOffset,
        at: WalOffset,
    ) -> Result<WalChecksum, ResumeError> {
        let Some(before) = frame.prev_frame() else {
            return Ok(self.header.checksum());
        };
        let mut bytes = [0; FRAME_HEADER_LEN];
        if read_at(&mut self.reader, before.get(), &mut bytes)? < FRAME_HEADER_LEN {
            return Err(ResumeError::Truncated { at });
        }
        let mut fields = Fields::new(&bytes);
        let (_pgno, _commit, _salt1, _salt2) =
            (fields.u32(), fields.u32(), fields.u32(), fields.u32());
        Ok(WalChecksum::new(fields.u32(), fields.u32()))
    }

    /// Scans forward from `cursor` to the end of the next committed transaction.
    ///
    /// # Errors
    /// [`WalError`]: an I/O failure, a cursor or limit that does not belong to this WAL,
    /// or, under [`ScanLimit::Committed`], damage below the committed end.
    pub fn next_tx(
        &mut self,
        cursor: &FrameCursor,
        limit: ScanLimit,
    ) -> Result<TxOutcome, WalError> {
        self.check_cursor(cursor, limit)?;
        let page_size = self.header.page_size();
        let mut buf = frame_buf(page_size);
        let mut at = *cursor;
        let mut frames = Vec::new();
        loop {
            if let ScanLimit::Committed(end) = limit
                && at.next.frame_index() == end.frame_count()
            {
                return if frames.is_empty() {
                    Ok(stop(cursor, Tail::Clean))
                } else {
                    Err(WalError::EndNotAtCommit { end: end.offset() })
                };
            }
            let found = read_at(&mut self.reader, at.next.get(), &mut buf)?;
            if found < buf.len() {
                return partial_frame(cursor, at.next, limit, found, frames.len());
            }
            let frame = match verify_frame(
                &buf,
                self.header.byte_order(),
                page_size,
                at.salts,
                at.checksum,
            ) {
                Ok(frame) => frame,
                Err(fault) => return bad_frame(cursor, at.next, limit, fault),
            };
            frames.push(FrameRef {
                pgno: frame.pgno,
                offset: at.next,
                digest: frame.digest,
            });
            at = FrameCursor {
                next: at.next.next_frame().ok_or(WalError::TooManyFrames)?,
                checksum: frame.checksum,
                salts: at.salts,
            };
            if let Some(commit) = NonZeroU32::new(frame.commit) {
                return Ok(TxOutcome::Tx(CommittedTx {
                    frames,
                    commit,
                    end: at,
                }));
            }
        }
    }

    fn check_cursor(&self, cursor: &FrameCursor, limit: ScanLimit) -> Result<(), WalError> {
        let expected = self.header.page_size();
        let mut offsets = vec![cursor.next];
        if let ScanLimit::Committed(end) = limit {
            offsets.push(end.offset());
        }
        if let Some(wrong) = offsets.iter().find(|o| o.page_size() != expected) {
            return Err(WalError::PageSizeMismatch {
                expected,
                found: wrong.page_size(),
            });
        }
        if cursor.salts != self.header.salts() {
            return Err(WalError::Restarted {
                cursor: cursor.salts,
                header: self.header.salts(),
            });
        }
        if let ScanLimit::Committed(end) = limit
            && end.salts() != self.header.salts()
        {
            return Err(WalError::Restarted {
                cursor: end.salts(),
                header: self.header.salts(),
            });
        }
        match limit {
            ScanLimit::Committed(end) if cursor.next.frame_index() > end.frame_count() => {
                Err(WalError::CursorPastEnd {
                    cursor: cursor.next,
                    end: end.offset(),
                })
            }
            ScanLimit::Committed(_) | ScanLimit::FirstInvalidFrame => Ok(()),
        }
    }

    /// Reads a frame's page again into `buf` and checks it against the digest
    /// the scan computed, so no page reaches an LTX file unverified.
    ///
    /// On any error `buf` is zeroed.
    ///
    /// # Errors
    /// [`ReadPageError`]; [`ReadPageError::ChangedSinceScan`] when the bytes differ from the scan's.
    pub fn read_page(&mut self, frame: &FrameRef, buf: &mut [u8]) -> Result<(), ReadPageError> {
        let result = self.read_page_inner(frame, buf);
        if result.is_err() {
            buf.fill(0);
        }
        result
    }

    fn read_page_inner(&mut self, frame: &FrameRef, buf: &mut [u8]) -> Result<(), ReadPageError> {
        let page_size = self.header.page_size();
        if frame.offset.page_size() != page_size {
            return Err(ReadPageError::PageSizeMismatch {
                expected: page_size,
                found: frame.offset.page_size(),
            });
        }
        if buf.len() != page_size.bytes() {
            return Err(ReadPageError::BufferSize {
                expected: page_size.bytes(),
                found: buf.len(),
            });
        }
        let at = frame
            .offset
            .get()
            .saturating_add(u64::try_from(FRAME_HEADER_LEN).unwrap_or(0));
        let (pgno, offset) = (frame.pgno, frame.offset);
        if read_at(&mut self.reader, at, buf)? < buf.len() {
            return Err(ReadPageError::Truncated { pgno, offset });
        }
        if PageChecksum::of(pgno, buf) != frame.digest {
            return Err(ReadPageError::ChangedSinceScan { pgno, offset });
        }
        Ok(())
    }
}

fn stop(cursor: &FrameCursor, tail: Tail) -> TxOutcome {
    TxOutcome::End { at: *cursor, tail }
}

/// The file ended inside the frame at `offset` after `found` of its bytes.
fn partial_frame(
    cursor: &FrameCursor,
    offset: WalOffset,
    limit: ScanLimit,
    found: usize,
    pending: usize,
) -> Result<TxOutcome, WalError> {
    let tail = match (limit, found, pending) {
        (ScanLimit::Committed(_), _, _) => {
            return Err(WalError::TruncatedBelowCommitted { offset });
        }
        (ScanLimit::FirstInvalidFrame, 0, 0) => Tail::Clean,
        (ScanLimit::FirstInvalidFrame, 0, pending) => Tail::Uncommitted {
            frames: u32::try_from(pending).unwrap_or(u32::MAX),
        },
        (ScanLimit::FirstInvalidFrame, _, _) => Tail::PartialFrame,
    };
    Ok(stop(cursor, tail))
}

fn bad_frame(
    cursor: &FrameCursor,
    offset: WalOffset,
    limit: ScanLimit,
    fault: FrameFault,
) -> Result<TxOutcome, WalError> {
    match limit {
        ScanLimit::Committed(_) => Err(WalError::CorruptBelowCommitted { offset, fault }),
        ScanLimit::FirstInvalidFrame => Ok(stop(cursor, Tail::InvalidFrame(fault))),
    }
}
