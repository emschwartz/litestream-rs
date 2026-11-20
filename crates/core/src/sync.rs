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
//! The step sees one epoch. A driver that wants the second half of the F1
//! condition (the epoch unchanged after the staging commit) compares
//! `WalSource::epoch` again and demotes the returned position with
//! `Position::with_continuity(Continuity::Unpinned)` when it moved.

use std::num::{NonZeroU32, NonZeroU64};

use lsr_ltx::{
    Checksum, Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange,
    WalOrigin,
};
use lsr_wal::{FrameCursor, FrameRef, PageMap, ScanLimit, TxOutcome, WalReader};
use std::io::{Read, Seek};

use crate::fault::{Clamped, Fault, HaltCause, SnapshotCause};
use crate::layout::{FileInfo, FileKey, Level};
use crate::position::{Continuity, LastPage, Position, ReadEpoch, SyncedThrough};
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

impl Step {
    /// The same step with the clamp's last timestamp set (a snapshot has no position to carry it).
    #[must_use]
    pub fn with_prev_timestamp(self, prev: Option<Timestamp>) -> Self {
        match self {
            Self::Snapshot { after, cause, .. } => Self::Snapshot {
                after,
                prev_timestamp: prev,
                cause,
            },
            Self::Incremental(_) | Self::NewGeneration(_) => self,
        }
    }
}

impl TryFrom<Recovered> for Step {
    type Error = HaltCause;

    fn try_from(recovered: Recovered) -> Result<Self, HaltCause> {
        match recovered {
            Recovered::Continue(position) => Ok(Self::Incremental(position)),
            Recovered::WalRestarted(position) => Ok(Self::NewGeneration(position)),
            Recovered::Snapshot { after, cause } => Ok(Self::Snapshot {
                after,
                prev_timestamp: None,
                cause,
            }),
            Recovered::Halt(cause) => Err(cause),
        }
    }
}

/// Where a step ended.
#[derive(Debug, PartialEq, Eq)]
pub enum StepEnd {
    /// Everything committed is staged.
    Through(SyncedThrough),
    /// The cap stopped the scan on a commit boundary.
    Limited(Position),
}

/// What a step did.
#[derive(Debug, PartialEq, Eq)]
pub enum Synced {
    /// Nothing new: the position is the committed end.
    Idle(SyncedThrough),
    /// One file was staged.
    Wrote {
        /// The staged file.
        file: FileInfo,
        /// Where the step stopped.
        end: StepEnd,
        /// Set when the clock read before the last timestamp used.
        clamped: Option<Clamped>,
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
/// the lock page. An incremental lists the map's pages and the "walgrowth"
/// pages: those above `prev_commit` that the map lacks, which come from the
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

/// The timestamp to stamp: `now`, but never before `prev`. Go uses `time.Now()`
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
        let next = |txid: Option<Txid>| txid.and_then(Txid::next).ok_or(Fault::Bug);
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

/// What a scan found.
struct Scanned {
    map: PageMap,
    cursor: FrameCursor,
    last_frame: Option<FrameRef>,
    limited: bool,
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
    loop {
        match wal
            .next_tx(&cursor, ScanLimit::Committed(committed.end()))
            .map_err(Fault::from_wal)?
        {
            TxOutcome::End { .. } => break,
            TxOutcome::Tx(tx) => {
                map.apply(&tx).map_err(Fault::from_page_map)?;
                cursor = *tx.end();
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
    })
}

/// Encodes `plan` into `upload` in bounded chunks; returns the last page written.
async fn fill<R: Read + Seek, P: PageSource, U: Upload>(
    upload: &mut U,
    header: Header,
    plan: &[PageFrom],
    wal: &mut WalReader<R>,
    db: &mut P,
    map_store: fn(&StoreError) -> Fault,
) -> Result<Option<LastPage>, Fault> {
    let mut encoder = Encoder::new(Vec::new(), header).map_err(|_err| Fault::Bug)?;
    let mut buf = vec![0_u8; header.page_size().bytes()];
    let mut last = None;
    for (index, page) in plan.iter().enumerate() {
        match page {
            PageFrom::Wal(frame) => wal
                .read_page(frame, &mut buf)
                .map_err(Fault::from_read_page)?,
            PageFrom::Db(pgno) => db.read_page(*pgno, &mut buf).map_err(Fault::from_source)?,
        }
        encoder
            .write_page(page.pgno(), &buf)
            .map_err(|_err| Fault::Bug)?;
        if index.saturating_add(1) == plan.len() {
            last = Some(LastPage::new(
                page.pgno(),
                Checksum::of_page(page.pgno(), &buf),
            ));
        }
        if encoder.writer_mut().len() >= FLUSH_AT {
            let chunk = std::mem::take(encoder.writer_mut());
            upload.write(&chunk).await.map_err(|err| map_store(&err))?;
        }
    }
    let (rest, _trailer) = encoder.finish(None).map_err(|_err| Fault::Bug)?;
    upload.write(&rest).await.map_err(|err| map_store(&err))?;
    Ok(last)
}

/// Writes one file into `store`; the upload is aborted when anything before the
/// commit fails. `map_store` says which store it is: the staging directory and
/// the replica read the same error differently (`Fault::from_staging`,
/// `Fault::from_remote`).
pub(crate) async fn stage<R: Read + Seek, P: PageSource, L: ObjectStore>(
    store: &L,
    key: &FileKey,
    header: Header,
    plan: &[PageFrom],
    wal: &mut WalReader<R>,
    db: &mut P,
    map_store: fn(&StoreError) -> Fault,
) -> Result<(FileInfo, Option<LastPage>), Fault> {
    let mut upload = store
        .begin_upload(key, header.fields().timestamp)
        .await
        .map_err(|err| map_store(&err))?;
    match fill(&mut upload, header, plan, wal, db, map_store).await {
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
/// `committed` and `epoch` come from one read of the same WAL source. A
/// position pinned to another epoch fails with `ReadTxLost`: the checkpoint
/// protocol retook the read transaction, and `recover` must prove the new one.
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
    epoch: ReadEpoch,
    db: &mut P,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Synced, StepFailed> {
    match run(&step, wal, committed, epoch, db, staging, cfg, now).await {
        Ok(synced) => Ok(synced),
        Err(fault) => Err(StepFailed { fault, retry: step }),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "sync_step's arguments, passed through"
)]
async fn run<R: Read + Seek, P: PageSource, L: ObjectStore>(
    step: &Step,
    wal: &mut WalReader<R>,
    committed: CommittedRead,
    epoch: ReadEpoch,
    db: &mut P,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Synced, Fault> {
    let start = Start::of(step, wal)?;
    if let Some(Continuity::Pinned(pin)) = start.held.map(|held| held.continuity())
        && pin != epoch
    {
        return Err(Fault::ReadTxLost);
    }
    if committed.end().salts() != wal.header().salts() {
        return Err(Fault::WalMoved);
    }
    let scanned = scan(wal, &start, committed, cfg.cap)?;
    let nothing = scanned.last_frame.is_none();
    if nothing && let Some(held) = start.held {
        let cursor = scanned.cursor;
        return Ok(Synced::Idle(SyncedThrough::new(Position::new(
            held.txid(),
            cursor,
            held.commit(),
            held.last_page(),
            held.timestamp(),
            Continuity::Pinned(epoch),
        ))));
    }
    let page_size = wal.header().page_size();
    let commit = match NonZeroU32::new(scanned.map.commit()) {
        Some(commit) => commit,
        None => NonZeroU32::new(db.page_count().map_err(Fault::from_source)?)
            .ok_or(Fault::NothingToSnapshot)?,
    };
    let plan = plan_pages(&scanned.map, start.prev_commit, commit, page_size);
    let (header, clamped) = stamp(&start, &scanned, wal, commit, cfg.node_id, now)?;
    let key = FileKey::new(Level::ZERO, TxidRange::single(start.txid));
    let (file, last_written) =
        stage(staging, &key, header, &plan, wal, db, Fault::from_staging).await?;
    let last_page = match (scanned.last_frame, last_written) {
        (Some(frame), _) => LastPage::of_frame(&frame),
        (None, Some(page)) => page,
        (None, None) => return Err(Fault::Bug),
    };
    let stopped_short =
        scanned.limited && scanned.cursor.next().get() != committed.end().offset().get();
    let position = Position::new(
        start.txid,
        scanned.cursor,
        commit,
        last_page,
        header.fields().timestamp,
        if stopped_short {
            Continuity::Unpinned
        } else {
            Continuity::Pinned(epoch)
        },
    );
    let end = if stopped_short {
        StepEnd::Limited(position)
    } else {
        StepEnd::Through(SyncedThrough::new(position))
    };
    Ok(Synced::Wrote { file, end, clamped })
}

/// The header of the file a scan calls for, with its clamped timestamp.
fn stamp<R: Read + Seek>(
    start: &Start,
    scanned: &Scanned,
    wal: &WalReader<R>,
    commit: NonZeroU32,
    node_id: u64,
    now: Timestamp,
) -> Result<(Header, Option<Clamped>), Fault> {
    let salts = wal.header().salts();
    let size = scanned.cursor.next().get().saturating_sub(start.begin);
    let origin = WalOrigin::new(start.begin, size, salts.salt1, salts.salt2).ok_or(Fault::Bug)?;
    let (timestamp, clamped) = clamp(start.prev_timestamp, now);
    let fields = header_fields(
        start.txid,
        commit,
        wal.header().page_size(),
        origin,
        timestamp,
        node_id,
    );
    let header = Header::new(fields).map_err(|_err| Fault::Bug)?;
    Ok((header, clamped))
}

#[cfg(test)]
mod tests;
