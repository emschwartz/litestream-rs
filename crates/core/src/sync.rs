//! One sync step: scan the WAL from a position and stage what it finds as one L0 file.
//!
//! The step is the I/O half that [`recover`](crate::recover) decides for. It
//! reads the WAL through a fresh [`WalReader`] under the committed end of one
//! wal-index read, applies the transactions to a [`PageMap`], lists the pages
//! with [`plan_pages`], and streams the file into the staging store. Nothing
//! moves unless the staging commit returned `Ok`: a failure hands back the same
//! [`Step`] in [`StepFailed`], and the retry encodes the file again over the
//! same key.
//!
//! A wal-index reset ahead of the WAL header
//! ([`CommittedRead::reset_ahead_of`]) is not the WAL moving: the index counts
//! no frame. A step from a position then has nothing to stage and answers
//! [`Synced::IndexReset`], with no proof; a snapshot takes every page from the
//! database file and ends [`StepEnd::Unproven`]. Neither pins, since the
//! position is in the header's generation and not the index's, so the header
//! the next commit writes goes through `recover` as any restart does.
//!
//! The step sees one epoch, so what it returns at the committed end is a
//! [`ReachedEnd`], not a proof. The second half of the condition (the epoch
//! unchanged after the staging commit) is `ReachedEnd::confirm`, which reads
//! `WalSource::epoch` again and hands back the proof, or the position
//! unpinned when the epoch moved.

use std::cmp::Ordering;
use std::num::{NonZeroU32, NonZeroU64};

use lsr_ltx::{
    DbChecksum, EncodeError, Encoder, FileChecksum, Header, HeaderFields, PageChecksum, PageNo,
    PageSize, Timestamp, Trailer, Txid, TxidRange, WalOrigin,
};
use lsr_wal::{
    FrameCursor, FrameRef, PageMap, ReadPageError, ScanLimit, TxOutcome, WalError, WalReader,
};
use std::io::{Read, Seek};

use crate::fault::role::{Role, Staging};
use crate::fault::{BugSite, Clamped, Fault, SnapshotCause};
use crate::layout::{FileInfo, FileKey};
use crate::pages::{PageChecksumTable, snapshot_clears};
use crate::position::{Continuity, LastPage, Position, ReachedEnd, ReadLockHold};
use crate::publish::Writer;
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::store::{ObjectStore, Seeing, StoreError, Upload, WriteMode};
use crate::witness::Witnessed;

/// Go's default cap on the WAL bytes one file covers (`db.go:33-40`).
const DEFAULT_CAP: u64 = 64 << 20;

/// How many encoded bytes are held before they go to the store.
pub(crate) const FLUSH_AT: usize = 256 * 1024;

/// What a step may do beyond the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    /// Stop after the first transaction that brings the file to this many WAL
    /// bytes; `None` is no cap. Zero cannot be expressed, which avoids Go's
    /// "zero means default" (#1348). A snapshot ignores it.
    pub cap: Option<NonZeroU64>,
    /// The writing node; Go never sets it, so it writes 0.
    pub node_id: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            cap: NonZeroU64::new(DEFAULT_CAP),
            node_id: 0,
        }
    }
}

/// What to scan from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// From the position's cursor, in its generation.
    Incremental(Position),
    /// From the start of the WAL's new generation; the position keeps its txid.
    NewGeneration(Position),
    /// Every page of the database, with the WAL's pages where it has them.
    Snapshot {
        /// The transaction id it follows.
        after: Option<Txid>,
        /// The last timestamp used, so the clock cannot step back.
        prev_timestamp: Option<Timestamp>,
        /// Why.
        cause: SnapshotCause,
    },
}

/// Where a step ended.
#[derive(Debug, PartialEq, Eq)]
pub enum StepEnd {
    /// Everything committed is staged; [`ReachedEnd::confirm`] makes it the
    /// proof.
    Through(ReachedEnd),
    /// The cap stopped the scan on a commit boundary.
    Limited(Position),
    /// A snapshot staged while the wal-index was reset ahead of the WAL header
    /// ([`CommittedRead::reset_ahead_of`]): it holds every committed page, from
    /// the database file, but its position is in the header's generation and
    /// not the one the index counts, so it is unpinned and proves nothing.
    Unproven(Position),
}

/// What a step did.
#[derive(Debug, PartialEq, Eq)]
pub enum Synced {
    /// Nothing new: the position is the committed end.
    Idle(ReachedEnd),
    /// Nothing to stage, and no proof: the wal-index was reset ahead of the WAL
    /// header ([`CommittedRead::reset_ahead_of`]) and no commit follows the
    /// position, so everything committed is already staged. The position is
    /// the step's own, unchanged; the next commit writes a header and goes
    /// through `recover` as a restart.
    IndexReset,
    /// One file was staged.
    Wrote {
        /// The staged file.
        file: FileInfo,
        /// Its token for the uploader: this process wrote those bytes.
        witness: Witnessed,
        /// Where the step stopped.
        end: StepEnd,
        /// How many WAL transactions the file covers (0 for a snapshot of a
        /// database the WAL holds nothing of): what the checkpoint policy's
        /// "was that only our own bump frame" question needs.
        txs: u32,
        /// Whether the file may change the database: false only when every page it holds
        /// came from the WAL with the digest the page checksum table already held, at the same
        /// size. The driver's and the checkpoint's own writes rewrite page 1 unchanged; such a
        /// file holds no commit of the application's (`crate::failstop`).
        changes: bool,
    },
}

/// A step that did not finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepFailed {
    /// What went wrong.
    pub fault: Fault,
    /// The step to run again. The position never moves on a failure.
    pub retry: Step,
}

/// Where one page of a file comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageFrom {
    /// A WAL frame, read back and checked against the scan's digest.
    Wal(FrameRef),
    /// The database file.
    Db(PageNo),
}

impl PageFrom {
    /// The page number.
    #[must_use]
    pub const fn pgno(&self) -> PageNo {
        match self {
            Self::Wal(frame) => frame.pgno(),
            Self::Db(pgno) => *pgno,
        }
    }
}

/// The pages of one file, ascending, listed on demand: no list of them is held.
///
/// A snapshot (`prev_commit` is `None`) lists every page up to `commit` except
/// the lock page. An incremental lists the map's pages and the pages
/// the database grew by that no scanned frame holds ("grown pages": those above
/// `prev_commit` that the map lacks), which come from the
/// database file (Go `db.go:2504-2561`).
///
/// Every page of a database used to take a list entry of its own (32 bytes, 48 while it was
/// sorted); the plan is a walk of the map and a counter instead, so it holds nothing that
/// grows with the database (issue #64).
#[derive(Clone, Copy)]
pub(crate) struct Plan<'a> {
    map: &'a PageMap,
    prev_commit: Option<u32>,
    commit: NonZeroU32,
    page_size: PageSize,
    /// The first page listed: 1, or where a snapshot part-way through it reads on.
    from: u32,
}

impl<'a> Plan<'a> {
    /// The plan of a file that ends at `commit` pages, after `prev_commit` for an incremental.
    #[must_use]
    pub(crate) const fn new(
        map: &'a PageMap,
        prev_commit: Option<u32>,
        commit: NonZeroU32,
        page_size: PageSize,
    ) -> Self {
        Self {
            map,
            prev_commit,
            commit,
            page_size,
            from: 1,
        }
    }

    /// The same plan, listing only its pages from `from` on.
    #[must_use]
    pub(crate) const fn starting_at(self, from: u32) -> Self {
        Self { from, ..self }
    }

    /// At least as many pages as the plan lists, without listing them: a buffer for the file is
    /// sized from it.
    #[must_use]
    pub(crate) fn at_most(&self) -> usize {
        let commit = self.commit.get();
        match self.prev_commit {
            None => usize::try_from(commit).unwrap_or(usize::MAX),
            Some(prev) => {
                let held = self
                    .map
                    .pages()
                    .take_while(|frame| frame.pgno().get() <= commit)
                    .count();
                let grown = usize::try_from(commit.saturating_sub(prev)).unwrap_or(usize::MAX);
                held.saturating_add(grown)
            }
        }
    }

    /// The pages, in ascending page order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = PageFrom> + '_ {
        let commit = self.commit.get();
        let from = self.from;
        let mut raw = self
            .prev_commit
            .map_or(1, |prev| prev.saturating_add(1))
            .max(from);
        let mut held = self
            .map
            .pages()
            .skip_while(move |frame| frame.pgno().get() < from)
            .peekable();
        std::iter::from_fn(move || {
            loop {
                let from_map = held.peek().map(|frame| frame.pgno().get());
                let grown = (raw <= commit).then_some(raw);
                let (take_map, take_grown) = match (from_map, grown) {
                    (None, None) => return None,
                    (None, Some(_)) => (false, true),
                    (Some(held_pgno), None) => {
                        if held_pgno > commit {
                            return None;
                        }
                        (true, false)
                    }
                    (Some(held_pgno), Some(next)) => match held_pgno.cmp(&next) {
                        Ordering::Less => (true, false),
                        Ordering::Equal => (true, true),
                        Ordering::Greater => (false, true),
                    },
                };
                // A frame the map holds is a page of the file, whether the counter is at it or
                // has not reached it (an incremental lists the map's pages below the first
                // grown one). In a snapshot the counter starts at page 1 and reaches every
                // page, so a map entry is always met when the counter is on it.
                let frame = if take_map { held.next() } else { None };
                let pgno = grown.filter(|_| take_grown);
                if take_grown {
                    raw = raw.saturating_add(1);
                }
                match (frame, pgno) {
                    (Some(frame), _) => return Some(PageFrom::Wal(*frame)),
                    (None, None) => {}
                    (None, Some(pgno)) => {
                        if let Some(pgno) = PageNo::new(pgno, self.page_size) {
                            return Some(PageFrom::Db(pgno));
                        }
                    }
                }
            }
        })
    }
}

/// The header Go writes (`db.go:2294-2311`): untracked, with no pre-apply checksum.
#[must_use]
pub(crate) const fn header_fields(
    txid: Txid,
    commit: NonZeroU32,
    page_size: PageSize,
    origin: WalOrigin,
    timestamp: Timestamp,
    node_id: u64,
) -> HeaderFields {
    HeaderFields {
        tracked: false,
        page_size,
        commit: commit.get(),
        txids: TxidRange::single(txid),
        timestamp,
        pre_apply: None,
        wal: Some(origin),
        node_id,
    }
}

/// The timestamp to record as created: `now`, but never before `prev`. Go uses `time.Now()`
/// unclamped (`db.go:2294`).
#[must_use]
pub fn clamp(prev: Option<Timestamp>, now: Timestamp) -> (Timestamp, Option<Clamped>) {
    match prev {
        Some(prev) if prev > now => (prev, Some(Clamped { prev, now })),
        Some(_) | None => (now, None),
    }
}

/// What a step starts from. A snapshot holds no position.
struct Start {
    cursor: FrameCursor,
    begin: u64,
    txid: Txid,
    prev_timestamp: Option<Timestamp>,
    held: Option<Position>,
}

impl Start {
    fn of<R: Read + Seek>(step: &Step, wal: &WalReader<R>) -> Result<Self, Fault> {
        let from_start = wal.cursor_at_start();
        let held = |position: &Position, cursor: FrameCursor| {
            Ok(Self {
                cursor,
                begin: cursor.next().get(),
                txid: position
                    .txid()
                    .next()
                    .ok_or_else(|| BugSite::TxidExhausted.hit())?,
                prev_timestamp: Some(position.timestamp()),
                held: Some(*position),
            })
        };
        match step {
            Step::Incremental(position) => held(position, seeded(position.cursor(), from_start)),
            Step::NewGeneration(position) => held(position, from_start),
            Step::Snapshot {
                after,
                prev_timestamp,
                ..
            } => Self::snapshot(wal, *after, *prev_timestamp),
        }
    }

    /// A start with nothing held: from the WAL's first frame, after `after`.
    fn snapshot<R: Read + Seek>(
        wal: &WalReader<R>,
        after: Option<Txid>,
        prev_timestamp: Option<Timestamp>,
    ) -> Result<Self, Fault> {
        let cursor = wal.cursor_at_start();
        Ok(Self {
            cursor,
            begin: cursor.next().get(),
            txid: match after {
                Some(after) => after.next().ok_or_else(|| BugSite::TxidExhausted.hit())?,
                None => Txid::MIN,
            },
            prev_timestamp,
            held: None,
        })
    }

    /// The commit size the file chains on from: none for a snapshot.
    fn prev_commit(&self) -> Option<u32> {
        self.held.map(|held| held.commit().get())
    }

    /// Whether the file is a snapshot of every page.
    fn is_snapshot(&self) -> bool {
        self.held.is_none()
    }
}

/// The cursor a step resumes from: `held`, unless it is the start of the header's generation,
/// where the header as it is now seeds the chain. SQLite writes a restarted WAL's header again,
/// with the same salts and the writing connection's own checkpoint count, whenever the
/// wal-index holds no frame (the connection that restarted rolled back, or died, before its
/// commit), so the seed a position took at the start can be stale; nothing of the generation
/// was staged from it, and [`WalReader::resume`] reseeds the same way.
fn seeded(held: FrameCursor, start: FrameCursor) -> FrameCursor {
    if held.next() == start.next() && held.salts() == start.salts() {
        start
    } else {
        held
    }
}

/// The page map of every committed transaction of the current generation up to `upto`.
///
/// A snapshot's scan: from the start of the WAL, no cap. `upto` need not be the
/// wal-index's committed end; the snapshot writer passes the synced end.
pub(crate) fn scan_whole<R: Read + Seek>(
    wal: &mut WalReader<R>,
    upto: CommittedRead,
) -> Result<PageMap, Fault> {
    let start = Start::snapshot(wal, None, None)?;
    scan(wal, &start, upto, None).map(|scanned| scanned.map)
}

/// The fault for a failed WAL read, `stop` when the failure means damage below
/// the committed end: a WAL that changes under a scan is moved, not corrupt.
///
/// A read on read mark 0 holds no lock against an application TRUNCATE checkpoint or
/// a restart in place, so a short or non-verifying frame can be the WAL moving
/// under the read. The header is read again, after the failed read: other salts,
/// a short file or no header is `WalMoved` (`recover` classifies what happened),
/// and the stop stands only when the header still names the scanned generation.
/// Every read below the committed end (the scan, the page fill, a snapshot's
/// whole scan, each step of the checkpoint protocol) goes through here.
fn below_committed<R: Read + Seek>(wal: &mut WalReader<R>, stop: Fault) -> Fault {
    match wal.generation_unchanged() {
        Ok(true) => stop,
        Ok(false) => Fault::WalMoved,
        Err(_err) => Fault::LocalIo,
    }
}

/// [`Fault::from_wal`], with damage below the committed end sorted by [`below_committed`].
fn scan_fault<R: Read + Seek>(wal: &mut WalReader<R>, err: WalError) -> Fault {
    match err {
        WalError::CorruptBelowCommitted { .. } => {
            below_committed(wal, Fault::WalCorruptBelowCommitted)
        }
        WalError::TruncatedBelowCommitted { .. } => {
            below_committed(wal, Fault::WalTruncatedBelowCommitted)
        }
        WalError::Io(_)
        | WalError::PageSizeMismatch { .. }
        | WalError::Restarted { .. }
        | WalError::CursorPastEnd { .. }
        | WalError::EndNotAtCommit { .. }
        | WalError::TooManyFrames => Fault::from_wal(err),
    }
}

/// [`Fault::from_read_page`], with a truncated read sorted by [`below_committed`].
fn page_fault<R: Read + Seek>(wal: &mut WalReader<R>, err: ReadPageError) -> Fault {
    match err {
        ReadPageError::Truncated { .. } => below_committed(wal, Fault::WalTruncatedBelowCommitted),
        ReadPageError::Io(_)
        | ReadPageError::BufferSize { .. }
        | ReadPageError::PageSizeMismatch { .. }
        | ReadPageError::ChangedSinceScan { .. } => Fault::from_read_page(err),
    }
}

/// What a scan found.
struct Scanned {
    map: PageMap,
    cursor: FrameCursor,
    last_frame: Option<FrameRef>,
    limited: bool,
    txs: u32,
}

fn scan<R: Read + Seek>(
    wal: &mut WalReader<R>,
    start: &Start,
    committed: CommittedRead,
    cap: Option<NonZeroU64>,
) -> Result<Scanned, Fault> {
    let mut map = PageMap::new(wal.header().page_size());
    let mut cursor = start.cursor;
    let mut last_frame = None;
    let mut limited = false;
    let mut txs = 0_u32;
    loop {
        match wal
            .next_tx(&cursor, ScanLimit::Committed(committed.end()))
            .map_err(|err| scan_fault(wal, err))?
        {
            TxOutcome::End { .. } => break,
            TxOutcome::Tx(tx) => {
                map.apply(&tx).map_err(Fault::from_page_map)?;
                cursor = *tx.end();
                txs = txs.saturating_add(1);
                last_frame = tx.frames().last().copied();
                let bytes = cursor.next().get().saturating_sub(start.begin);
                if !start.is_snapshot() && cap.is_some_and(|cap| bytes >= cap.get()) {
                    limited = true;
                    break;
                }
            }
        }
    }
    Ok(Scanned {
        map,
        cursor,
        last_frame,
        limited,
        txs,
    })
}

/// What a step under a reset wal-index scans: nothing, since the index counts no
/// frame. A commit that chains on from a held position was backfilled before the
/// reset and is in no staged file; `recover`'s rule 9a snapshots for it, and a
/// step that meets one anyway retries as the WAL having moved rather than call
/// itself done.
fn unscanned<R: Read + Seek>(wal: &mut WalReader<R>, start: &Start) -> Result<Scanned, Fault> {
    if start.held.is_some()
        && wal
            .commit_follows(&start.cursor)
            .map_err(|_err| Fault::LocalIo)?
    {
        return Err(Fault::WalMoved);
    }
    Ok(Scanned {
        map: PageMap::new(wal.header().page_size()),
        cursor: start.cursor,
        last_frame: None,
        limited: false,
        txs: 0,
    })
}

/// What [`fill`] wrote: the last page, the bytes and the trailer's checksum.
struct Filled {
    last: Option<LastPage>,
    bytes: Seeing,
    checksum: FileChecksum,
}

/// Where [`fill`] writes a file's bytes: a [`Writer`], which alone can publish
/// them, or a test's recorder.
trait FileSink {
    /// Appends a chunk.
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send;
}

impl<U: Upload> FileSink for Writer<U> {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        Writer::write(self, chunk)
    }
}

/// Encodes `plan` into `upload` in bounded chunks.
async fn fill<R: Read + Seek, P: PageSource, U: FileSink>(
    upload: &mut U,
    header: Header,
    plan: &Plan<'_>,
    wal: &mut WalReader<R>,
    db: &mut P,
    pages: &mut PageChecksumTable,
    map_store: fn(&StoreError) -> Fault,
) -> Result<Filled, Fault> {
    let mut filling = Filling::new(header, plan)?;
    let mut bytes = Seeing::new();
    while !filling.whole() {
        filling.encode(plan, wal, db, pages, false)?;
        let out = filling.out();
        // A file not yet whole has bytes waiting after every step, and the buffer handed over is
        // the one `whole` reads: either failing would spin this loop forever.
        crate::invariant!(!out.is_empty(), BugSite::Encode);
        upload.write(out).await.map_err(|err| map_store(&err))?;
        bytes.add(out);
        out.clear();
        crate::invariant!(filling.out().is_empty(), BugSite::Encode);
    }
    Ok(Filled {
        last: filling.last,
        bytes,
        checksum: filling.checksum.ok_or_else(|| BugSite::Encode.hit())?,
    })
}

/// An LTX file's bytes waiting to be handed over: the encoder's buffer while it encodes, then
/// the tail it finished with. A staged or snapshot file ([`Filling`]) and a compaction's
/// output (`crate::compact::Compacting`) both hold one.
#[derive(Debug)]
pub(crate) struct Outbound {
    /// `None` once the trailer is written: what is left is in `tail`.
    encoder: Option<Encoder<Vec<u8>>>,
    tail: Vec<u8>,
}

impl Outbound {
    pub(crate) const fn new(encoder: Encoder<Vec<u8>>) -> Self {
        Self {
            encoder: Some(encoder),
            tail: Vec::new(),
        }
    }

    /// The encoder, until the trailer is written.
    pub(crate) const fn encoder(&mut self) -> Option<&mut Encoder<Vec<u8>>> {
        self.encoder.as_mut()
    }

    /// Whether the file is encoded and every byte of it taken from [`Self::out`].
    pub(crate) fn whole(&self) -> bool {
        self.encoder.is_none() && self.tail.is_empty()
    }

    /// The encoded bytes not yet taken: the caller drains what it hands over.
    pub(crate) fn out(&mut self) -> &mut Vec<u8> {
        match self.encoder.as_mut() {
            Some(encoder) => encoder.writer_mut(),
            None => &mut self.tail,
        }
    }

    /// Writes the trailer, which leaves the rest of the file in the tail. `None` when it
    /// was written already.
    pub(crate) fn finish(
        &mut self,
        post_apply: Option<DbChecksum>,
    ) -> Result<Option<Trailer>, EncodeError> {
        let Some(encoder) = self.encoder.take() else {
            return Ok(None);
        };
        let (tail, trailer) = encoder.finish(post_apply)?;
        self.tail = tail;
        Ok(Some(trailer))
    }
}

/// An LTX file being encoded from a [`Plan`], up to [`FLUSH_AT`] bytes a step, so its bytes
/// can be handed over in as many steps as the caller likes: a staged file goes in one call, a
/// level-9 snapshot a slice a tick (`crate::snapshot::Snapshotting`).
#[derive(Debug)]
pub(crate) struct Filling {
    buf: Outbound,
    /// The next page to encode; `u32::MAX` once every page of the plan is.
    next: u32,
    last: Option<LastPage>,
    checksum: Option<FileChecksum>,
}

impl Filling {
    /// A file of `plan`'s pages under `header`.
    pub(crate) fn new(header: Header, plan: &Plan<'_>) -> Result<Self, Fault> {
        // Room for the file or, past the flush size, for it and the page that crosses it: the
        // buffer is emptied, never replaced, and a small file allocates only what it holds.
        let page = header.page_size().bytes();
        let room = FLUSH_AT.saturating_add(page.saturating_mul(2));
        let out = Vec::with_capacity(
            plan.at_most()
                .saturating_mul(page)
                .saturating_add(page)
                .min(room),
        );
        Ok(Self {
            buf: Outbound::new(Encoder::new(out, header).map_err(|_err| BugSite::Encode.hit())?),
            next: 1,
            last: None,
            checksum: None,
        })
    }

    /// The next page to encode: every page below it is.
    pub(crate) const fn next(&self) -> u32 {
        self.next
    }

    /// Whether the file is encoded and every byte of it taken from [`Self::out`].
    pub(crate) fn whole(&self) -> bool {
        self.buf.whole()
    }

    /// The encoded bytes not yet taken: the caller drains what it hands over.
    pub(crate) fn out(&mut self) -> &mut Vec<u8> {
        self.buf.out()
    }

    /// Encodes `plan`'s pages from where the last step stopped, then the index and the
    /// trailer, until [`FLUSH_AT`] bytes wait in [`Self::out`] or the file is encoded. Every
    /// database-file page is checked against `pages` first, as the table stood at its pin
    /// when `pinned` (`PageChecksumTable::check_pinned`).
    pub(crate) fn encode<R: Read + Seek, P: PageSource>(
        &mut self,
        plan: &Plan<'_>,
        wal: &mut WalReader<R>,
        db: &mut P,
        pages: &mut PageChecksumTable,
        pinned: bool,
    ) -> Result<(), Fault> {
        let Some(encoder) = self.buf.encoder() else {
            return Ok(());
        };
        if self.next != u32::MAX {
            let mut buf = vec![0_u8; plan.page_size.bytes()];
            let plan = plan.starting_at(self.next);
            let mut listed = plan.iter().peekable();
            while let Some(page) = listed.next() {
                match page {
                    PageFrom::Wal(frame) => wal
                        .read_page(&frame, &mut buf)
                        .map_err(|err| page_fault(wal, err))?,
                    PageFrom::Db(pgno) => {
                        db.read_page(pgno, &mut buf).map_err(Fault::from_source)?;
                        let checked = if pinned {
                            pages.check_pinned(pgno, &buf)
                        } else {
                            pages.check(pgno, &buf)
                        };
                        checked.map_err(|mismatch| Fault::DatabasePageMismatch {
                            pgno: mismatch.pgno,
                        })?;
                    }
                }
                encoder
                    .write_page(page.pgno(), &buf)
                    .map_err(|_err| BugSite::Encode.hit())?;
                if listed.peek().is_none() {
                    self.last = Some(LastPage::new(
                        page.pgno(),
                        PageChecksum::of(page.pgno(), &buf),
                    ));
                }
                self.next = page.pgno().get().saturating_add(1);
                if encoder.writer_mut().len() >= FLUSH_AT {
                    return Ok(());
                }
            }
            self.next = u32::MAX;
        }
        // The index leaves in pieces, as the pages did, so it is never copied whole into the
        // buffer.
        while encoder
            .write_index_chunk()
            .map_err(|_err| BugSite::Encode.hit())?
        {
            if encoder.writer_mut().len() >= FLUSH_AT {
                return Ok(());
            }
        }
        let trailer = (self
            .buf
            .finish(None)
            .map_err(|_err| BugSite::Encode.hit())?)
        .ok_or_else(|| BugSite::Encode.hit())?;
        self.checksum = Some(trailer.file_checksum());
        Ok(())
    }
}

/// Writes one file into `store`; the upload is aborted when anything before the
/// commit fails. Every database-file page is checked against `pages` before it
/// is encoded. The store's role says which store it is: the staging directory
/// and the replica read the same error differently ([`Role`]). The
/// [`Witnessed`] token is minted from the bytes written and the encoder's own
/// trailer: this process made the file.
pub(crate) async fn stage<R: Read + Seek, P: PageSource, L: Role>(
    store: &L,
    key: &FileKey,
    header: Header,
    plan: &Plan<'_>,
    wal: &mut WalReader<R>,
    db: &mut P,
    pages: &mut PageChecksumTable,
) -> Result<(FileInfo, Option<LastPage>, Witnessed), Fault> {
    let map_store: fn(&StoreError) -> Fault = L::fault;
    // A staged file replaces a torn one under its key, and a snapshot (level 9)
    // shares its key with Go's snapshot of the same transaction: both replace.
    let mut upload = Writer::begin(
        store,
        key,
        header.fields().timestamp,
        WriteMode::Replace,
        None,
    )
    .await
    .map_err(|err| map_store(&err))?;
    match fill(&mut upload, header, plan, wal, db, pages, map_store).await {
        Ok(filled) => {
            let file = upload
                .commit(store)
                .await
                .map_err(|err| map_store(err.error()))?
                .info();
            let token = Witnessed::made_here(
                *key,
                filled.bytes,
                filled.checksum,
                header.fields().timestamp,
            );
            Ok((file, filled.last, token))
        }
        Err(fault) => {
            // The write already failed; a failed abort changes nothing the retry relies on.
            drop(upload.abort().await);
            Err(fault)
        }
    }
}

/// The durability barrier: syncs the WAL after the committed read was taken
/// and before any frame it counts is staged.
///
/// SQLite under `synchronous = NORMAL` leaves committed frames unsynced until
/// its next checkpoint. Staging them unsynced would make the replica more
/// durable than the database: a power loss rewinds the database below what
/// the bucket holds, and the application's next commits fork history under
/// the same salts. A frame is written before the commit that counts it lands
/// in the wal-index, so one sync after the read covers every frame the read
/// counts, so callers take the read first and call this before staging.
///
/// # Errors
/// [`Fault::WalSyncFailed`] when the sync failed; the step stages nothing and the next step
/// syncs again, remembering nothing (`docs/decisions/D173.md`).
pub(crate) fn wal_barrier<W: WalSource>(wal: &mut W) -> Result<(), Fault> {
    wal.sync().map_err(Fault::from_source)
}

/// Scans the WAL from `step`'s start and stages one file, or none when nothing is new.
///
/// `committed` carries the read-lock hold it was taken under. A position pinned
/// to another epoch fails with `ReadTxLost`: the checkpoint
/// protocol retook the read transaction, and `recover` must prove the new one.
/// `pages` is the page checksum table (`crate::pages`): every database-file page
/// is checked against it, every staged WAL page recorded in it. `now` is the
/// file's created-at time: a clock read taken after `committed` was read, so
/// that no transaction the file carries committed after it (`Env::clock`).
///
/// # Errors
/// [`StepFailed`], carrying the same step to run again.
#[expect(
    clippy::too_many_arguments,
    reason = "the plan fixes this signature; each argument is a distinct source of the step"
)]
pub async fn sync_step<R: Read + Seek, P: PageSource, L: ObjectStore>(
    step: Step,
    wal: &mut WalReader<R>,
    committed: CommittedRead,
    db: &mut P,
    pages: &mut PageChecksumTable,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Synced, StepFailed> {
    match run(
        &step,
        wal,
        committed,
        db,
        pages,
        &Staging(staging),
        cfg,
        now,
    )
    .await
    {
        Ok(synced) => Ok(synced),
        Err(fault) => Err(StepFailed { fault, retry: step }),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the plan fixes this signature; each argument is a distinct source of the step"
)]
async fn run<R: Read + Seek, P: PageSource, L: Role>(
    step: &Step,
    wal: &mut WalReader<R>,
    committed: CommittedRead,
    db: &mut P,
    pages: &mut PageChecksumTable,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Synced, Fault> {
    let epoch = committed.epoch();
    let start = Start::of(step, wal)?;
    forget(pages, step, wal.header().page_size());
    if let Some(Continuity::Held(held)) = start.held.map(|held| held.continuity())
        && held != epoch
    {
        return Err(Fault::ReadTxLost);
    }
    let reset = committed.end().salts() != wal.header().salts();
    if reset && !committed.reset_ahead_of(wal.header().salts()) {
        return Err(Fault::WalMoved);
    }
    let scanned = if reset {
        unscanned(wal, &start)?
    } else {
        scan(wal, &start, committed, cfg.cap)?
    };
    let nothing = scanned.last_frame.is_none();
    if reset && nothing && start.held.is_some() {
        return Ok(Synced::IndexReset);
    }
    if nothing && let Some(held) = start.held {
        return Ok(Synced::Idle(ReachedEnd::new(
            held.with_cursor(scanned.cursor),
            epoch,
        )));
    }
    let page_size = wal.header().page_size();
    let commit = commit_of(&scanned.map, db)?;
    let plan = Plan::new(&scanned.map, start.prev_commit(), commit, page_size);
    let header = created_at(&start, &scanned, wal, commit, cfg.node_id, now)?;
    let key = FileKey::staged(start.txid);
    let (file, last_written, witness, changes) = stage_recorded(
        staging,
        &key,
        header,
        &plan,
        wal,
        db,
        pages,
        start.prev_commit(),
    )
    .await?;
    let last_page = match (scanned.last_frame, last_written) {
        (Some(frame), _) => LastPage::of_frame(&frame),
        (None, Some(page)) => page,
        (None, None) => return Err(BugSite::StepWithoutPages.hit()),
    };
    let position = Position::new(
        start.txid,
        scanned.cursor,
        commit,
        last_page,
        header.fields().timestamp,
        Continuity::Broken,
    );
    let stopped_short =
        scanned.limited && scanned.cursor.next().get() != committed.end().offset().get();
    let end = ended(position, reset, stopped_short, epoch);
    Ok(Synced::Wrote {
        file,
        witness,
        end,
        txs: scanned.txs,
        changes,
    })
}

/// How far a staged position can be trusted: not at all under a reset
/// wal-index, unpinned when the cap stopped it short, otherwise the proof.
fn ended(position: Position, reset: bool, stopped_short: bool, epoch: ReadLockHold) -> StepEnd {
    if reset {
        StepEnd::Unproven(position)
    } else if stopped_short {
        StepEnd::Limited(position)
    } else {
        StepEnd::Through(ReachedEnd::new(position, epoch))
    }
}

/// Empties the table on a page-size change and before a snapshot whose cause
/// says the WAL may hold frames the database never got.
fn forget(pages: &mut PageChecksumTable, step: &Step, page_size: PageSize) {
    pages.fit(page_size);
    if let Step::Snapshot { cause, .. } = step
        && snapshot_clears(cause)
    {
        pages.clear();
    }
}

/// [`stage`] from the staging store, then [`record`]: also whether the file may change the
/// database, judged against the table before it records the file's pages.
#[expect(
    clippy::too_many_arguments,
    reason = "stage's arguments, and the size the file commits"
)]
async fn stage_recorded<R: Read + Seek, P: PageSource, L: Role>(
    staging: &L,
    key: &FileKey,
    header: Header,
    plan: &Plan<'_>,
    wal: &mut WalReader<R>,
    db: &mut P,
    pages: &mut PageChecksumTable,
    prev_commit: Option<u32>,
) -> Result<(FileInfo, Option<LastPage>, Witnessed, bool), Fault> {
    let commit = NonZeroU32::new(header.fields().commit).ok_or_else(|| BugSite::Encode.hit())?;
    let changes = changes_pages(pages, plan, prev_commit, commit);
    let (file, last, witness) = stage(staging, key, header, plan, wal, db, pages).await?;
    record(pages, plan, commit);
    Ok((file, last, witness, changes))
}

/// Whether a file of `plan` may change the database whose page checksum table is `pages`: a
/// page the table does not hold, holds with another digest, or read from the database file,
/// or a size other than `prev_commit`, all may.
fn changes_pages(
    pages: &PageChecksumTable,
    plan: &Plan<'_>,
    prev_commit: Option<u32>,
    commit: NonZeroU32,
) -> bool {
    prev_commit != Some(commit.get())
        || plan.iter().any(|page| match page {
            PageFrom::Wal(frame) => pages.get(frame.pgno()) != Some(frame.digest()),
            PageFrom::Db(_) => true,
        })
}

/// Records the staged WAL pages, the newest copy of each, and forgets pages the
/// database no longer has.
fn record(pages: &mut PageChecksumTable, plan: &Plan<'_>, commit: NonZeroU32) {
    for page in plan.iter() {
        match page {
            PageFrom::Wal(frame) => pages.record(frame.pgno(), frame.digest()),
            PageFrom::Db(_) => {}
        }
    }
    pages.truncate(commit);
}

/// The database size after the scanned transactions, or the database file's
/// when the scan found none (a snapshot of a WAL that holds nothing).
fn commit_of<P: PageSource>(map: &PageMap, db: &mut P) -> Result<NonZeroU32, Fault> {
    match map.commit() {
        Some(commit) => Ok(commit),
        None => NonZeroU32::new(db.page_count().map_err(Fault::from_source)?.get())
            .ok_or(Fault::NothingToSnapshot),
    }
}

/// The header of the file a scan calls for, with its clamped timestamp.
fn created_at<R: Read + Seek>(
    start: &Start,
    scanned: &Scanned,
    wal: &WalReader<R>,
    commit: NonZeroU32,
    node_id: u64,
    now: Timestamp,
) -> Result<Header, Fault> {
    let salts = wal.header().salts();
    let size = scanned.cursor.next().get().saturating_sub(start.begin);
    let origin = WalOrigin::new(start.begin, size, salts.salt1, salts.salt2)
        .ok_or_else(|| BugSite::Encode.hit())?;
    let (timestamp, _clamped) = clamp(start.prev_timestamp, now);
    let fields = header_fields(
        start.txid,
        commit,
        wal.header().page_size(),
        origin,
        timestamp,
        node_id,
    );
    Header::new(fields).map_err(|_err| BugSite::Encode.hit())
}

#[cfg(test)]
mod tests;
