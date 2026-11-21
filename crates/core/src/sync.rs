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

use std::num::{NonZeroU32, NonZeroU64};

use lsr_ltx::{
    Encoder, Header, HeaderFields, PageChecksum, PageNo, PageSize, Timestamp, Txid, TxidRange,
    WalOrigin,
};
use lsr_wal::{
    FrameCursor, FrameRef, PageMap, ReadPageError, ScanLimit, TxOutcome, WalError, WalReader,
};
use std::io::{Read, Seek};

use crate::fault::{BugSite, Clamped, Fault, SnapshotCause, StopCause};
use crate::layout::{FileInfo, FileKey, Level};
use crate::pages::{PageChecksumTable, snapshot_clears};
use crate::position::{Continuity, LastPage, Position, ReachedEnd, ReadLockHold};
use crate::recover::Recovered;
use crate::source::{CommittedRead, PageSource};
use crate::store::{ObjectStore, StoreError, Upload};

/// Go's default cap on the WAL bytes one file covers (`db.go:33-40`).
const DEFAULT_CAP: u64 = 64 << 20;

/// How many encoded bytes are held before they go to the store.
const FLUSH_AT: usize = 1 << 20;

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

impl TryFrom<Recovered> for Step {
    type Error = StopCause;

    fn try_from(recovered: Recovered) -> Result<Self, StopCause> {
        match recovered {
            Recovered::Continue(position) => Ok(Self::Incremental(position)),
            Recovered::WalRestarted(position) => Ok(Self::NewGeneration(position)),
            Recovered::Snapshot { after, cause } => Ok(Self::Snapshot {
                after,
                prev_timestamp: None,
                cause,
            }),
            Recovered::Stop(cause) => Err(cause),
        }
    }
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
        /// Where the step stopped.
        end: StepEnd,
        /// How many WAL transactions the file covers (0 for a snapshot of a
        /// database the WAL holds nothing of): what the checkpoint policy's
        /// "was that only our own bump frame" question needs.
        txs: u32,
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

/// The pages of one file, ascending.
///
/// A snapshot (`prev_commit` is `None`) lists every page up to `commit` except
/// the lock page. An incremental lists the map's pages and the pages
/// the database grew by that no scanned frame holds ("grown pages": those above
/// `prev_commit` that the map lacks), which come from the
/// database file (Go `db.go:2504-2561`).
#[must_use]
pub fn plan_pages(
    map: &PageMap,
    prev_commit: Option<u32>,
    commit: NonZeroU32,
    page_size: PageSize,
) -> Vec<PageFrom> {
    let mut pages: Vec<PageFrom> = match prev_commit {
        Some(_) => map.pages().map(|frame| PageFrom::Wal(*frame)).collect(),
        None => Vec::new(),
    };
    let first = prev_commit.map_or(1, |prev| prev.saturating_add(1));
    for raw in first..=commit.get() {
        let Some(pgno) = PageNo::new(raw, page_size) else {
            continue;
        };
        match map.get(pgno) {
            Some(frame) => {
                if prev_commit.is_none() {
                    pages.push(PageFrom::Wal(*frame));
                }
            }
            None => pages.push(PageFrom::Db(pgno)),
        }
    }
    pages.retain(|page| page.pgno().get() <= commit.get());
    pages.sort_by_key(|page| page.pgno());
    pages
}

/// The header Go writes (`db.go:2294-2311`): untracked, with no pre-apply checksum.
#[must_use]
pub const fn header_fields(
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

/// What a step starts from.
struct Start {
    cursor: FrameCursor,
    begin: u64,
    txid: Txid,
    prev_commit: Option<u32>,
    prev_timestamp: Option<Timestamp>,
    held: Option<Position>,
    snapshot: bool,
}

impl Start {
    fn of<R: Read + Seek>(step: &Step, wal: &WalReader<R>) -> Result<Self, Fault> {
        let from_start = wal.cursor_at_start();
        let next = |txid: Option<Txid>| {
            txid.and_then(Txid::next)
                .ok_or(Fault::Bug(BugSite::TxidExhausted))
        };
        match step {
            Step::Incremental(position) => Ok(Self {
                cursor: position.cursor(),
                begin: position.cursor().next().get(),
                txid: next(Some(position.txid()))?,
                prev_commit: Some(position.commit().get()),
                prev_timestamp: Some(position.timestamp()),
                held: Some(*position),
                snapshot: false,
            }),
            Step::NewGeneration(position) => Ok(Self {
                cursor: from_start,
                begin: from_start.next().get(),
                txid: next(Some(position.txid()))?,
                prev_commit: Some(position.commit().get()),
                prev_timestamp: Some(position.timestamp()),
                held: Some(*position),
                snapshot: false,
            }),
            Step::Snapshot {
                after,
                prev_timestamp,
                ..
            } => Ok(Self {
                cursor: from_start,
                begin: from_start.next().get(),
                txid: match after {
                    Some(after) => next(Some(*after))?,
                    None => Txid::MIN,
                },
                prev_commit: None,
                prev_timestamp: *prev_timestamp,
                held: None,
                snapshot: true,
            }),
        }
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
    let cursor = wal.cursor_at_start();
    let start = Start {
        cursor,
        begin: cursor.next().get(),
        txid: Txid::MIN,
        prev_commit: None,
        prev_timestamp: None,
        held: None,
        snapshot: true,
    };
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
                if !start.snapshot && cap.is_some_and(|cap| bytes >= cap.get()) {
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

/// Encodes `plan` into `upload` in bounded chunks; returns the last page written.
async fn fill<R: Read + Seek, P: PageSource, U: Upload>(
    upload: &mut U,
    header: Header,
    plan: &[PageFrom],
    wal: &mut WalReader<R>,
    db: &mut P,
    pages: &mut PageChecksumTable,
    map_store: fn(&StoreError) -> Fault,
) -> Result<Option<LastPage>, Fault> {
    let mut encoder =
        Encoder::new(Vec::new(), header).map_err(|_err| Fault::Bug(BugSite::Encode))?;
    let mut buf = vec![0_u8; header.page_size().bytes()];
    let mut last = None;
    for (index, page) in plan.iter().enumerate() {
        match page {
            PageFrom::Wal(frame) => wal
                .read_page(frame, &mut buf)
                .map_err(|err| page_fault(wal, err))?,
            PageFrom::Db(pgno) => {
                db.read_page(*pgno, &mut buf).map_err(Fault::from_source)?;
                pages
                    .check(*pgno, &buf)
                    .map_err(|mismatch| Fault::DatabasePageMismatch {
                        pgno: mismatch.pgno,
                    })?;
            }
        }
        encoder
            .write_page(page.pgno(), &buf)
            .map_err(|_err| Fault::Bug(BugSite::Encode))?;
        if index.saturating_add(1) == plan.len() {
            last = Some(LastPage::new(
                page.pgno(),
                PageChecksum::of(page.pgno(), &buf),
            ));
        }
        if encoder.writer_mut().len() >= FLUSH_AT {
            let chunk = std::mem::take(encoder.writer_mut());
            upload.write(&chunk).await.map_err(|err| map_store(&err))?;
        }
    }
    let (rest, _trailer) = encoder
        .finish(None)
        .map_err(|_err| Fault::Bug(BugSite::Encode))?;
    upload.write(&rest).await.map_err(|err| map_store(&err))?;
    Ok(last)
}

/// Writes one file into `store`; the upload is aborted when anything before the
/// commit fails. Every database-file page is checked against `pages` before it
/// is encoded. `map_store` says which store it is: the staging directory and
/// the replica read the same error differently (`Fault::from_staging`,
/// `Fault::from_remote`).
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct source or sink of the file"
)]
pub(crate) async fn stage<R: Read + Seek, P: PageSource, L: ObjectStore>(
    store: &L,
    key: &FileKey,
    header: Header,
    plan: &[PageFrom],
    wal: &mut WalReader<R>,
    db: &mut P,
    pages: &mut PageChecksumTable,
    map_store: fn(&StoreError) -> Fault,
) -> Result<(FileInfo, Option<LastPage>), Fault> {
    let mut upload = store
        .begin_upload(key, header.fields().timestamp)
        .await
        .map_err(|err| map_store(&err))?;
    match fill(&mut upload, header, plan, wal, db, pages, map_store).await {
        Ok(last) => {
            let file = upload.commit().await.map_err(|err| map_store(&err))?;
            Ok((file, last))
        }
        Err(fault) => {
            // The write already failed; a failed abort changes nothing the retry relies on.
            drop(upload.abort().await);
            Err(fault)
        }
    }
}

/// Scans the WAL from `step`'s start and stages one file, or none when nothing is new.
///
/// `committed` carries the read-lock hold it was taken under. A position pinned
/// to another epoch fails with `ReadTxLost`: the checkpoint
/// protocol retook the read transaction, and `recover` must prove the new one.
/// `pages` is the page checksum table (`crate::pages`): every database-file page
/// is checked against it, every staged WAL page recorded in it.
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
    let source = Source { db, pages };
    match run(&step, wal, committed, source, staging, cfg, now).await {
        Ok(synced) => Ok(synced),
        Err(fault) => Err(StepFailed { fault, retry: step }),
    }
}

async fn run<R: Read + Seek, P: PageSource, L: ObjectStore>(
    step: &Step,
    wal: &mut WalReader<R>,
    committed: CommittedRead,
    source: Source<'_, P>,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Synced, Fault> {
    let Source { db, pages } = source;
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
    let plan = plan_pages(&scanned.map, start.prev_commit, commit, page_size);
    let header = created_at(&start, &scanned, wal, commit, cfg.node_id, now)?;
    let key = FileKey::new(Level::ZERO, TxidRange::single(start.txid));
    let (file, last_written) = stage(
        staging,
        &key,
        header,
        &plan,
        wal,
        db,
        pages,
        Fault::from_staging,
    )
    .await?;
    record(pages, &plan, commit);
    let last_page = match (scanned.last_frame, last_written) {
        (Some(frame), _) => LastPage::of_frame(&frame),
        (None, Some(page)) => page,
        (None, None) => return Err(Fault::Bug(BugSite::StepWithoutPages)),
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
        end,
        txs: scanned.txs,
    })
}

/// The database file and its page checksum table, as one argument.
struct Source<'a, P> {
    db: &'a mut P,
    pages: &'a mut PageChecksumTable,
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

/// Records the staged WAL pages, the newest copy of each, and forgets pages the
/// database no longer has.
fn record(pages: &mut PageChecksumTable, plan: &[PageFrom], commit: NonZeroU32) {
    for page in plan {
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
        .ok_or(Fault::Bug(BugSite::Encode))?;
    let (timestamp, _clamped) = clamp(start.prev_timestamp, now);
    let fields = header_fields(
        start.txid,
        commit,
        wal.header().page_size(),
        origin,
        timestamp,
        node_id,
    );
    Header::new(fields).map_err(|_err| Fault::Bug(BugSite::Encode))
}

#[cfg(test)]
mod tests;
