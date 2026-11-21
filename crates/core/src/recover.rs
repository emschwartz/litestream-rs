//! Deciding where to resume: the pure half of a sync step.
//!
//! [`recover`] looks at the newest staged file, at the WAL as one read
//! of the wal-index and the WAL header saw it, and at the replica's newest
//! transaction, and says whether to continue where the newest staged file ended, continue in a
//! restarted WAL, snapshot, or stop. It reads nothing: the reads are done by
//! [`NewestStagedFile::load`] (or, with nothing staged, [`NewestStagedFile::load_remote`], which
//! reads the same newest staged file back from the replica) and [`Probes::read`], so every rule
//! below can be driven from a table of inputs. Go answers the same question in
//! `verifyWithExecutor` (`db.go:1782-1925`) with a heuristic per branch; the rules here replace two
//! of those heuristics with the read-transaction proof of [`Continuity`].
//!
//! Rules, first match wins: 1 no newest staged file, 2 replica ahead, 3 newest staged file unreadable,
//! 4 no WAL header, 5 page size changed, 6 salts changed, 7 same salts but the
//! WAL is shorter than the newest staged file's end, 8 the frame before the end is not a
//! commit frame, 9 the last page differs, 9a the wal-index was reset ahead of
//! the header past a commit the position never covered, 10 continue.
//!
//! Rule 9a is a running position's (#26). SQLite can reset the wal-index to a
//! generation with no frame while the WAL file keeps the old header (see
//! [`CommittedRead::reset_ahead_of`]). The reset needed every frame backfilled,
//! so a commit that chains on from the position is in the database file and in
//! no file this engine staged, and no WAL read will show it again: a snapshot.
//! When none follows, nothing is lost and the position continues; the sync step
//! then has nothing to stage and no proof to give.
//!
//! Rule 6 proves one restart with the read lock and `salt1 + 1`; it
//! does not check `checkpoint_seq`, which real SQLite takes from the restarting
//! connection's own count. At boot, rule 9 compares the whole newest staged file with
//! the WAL range it covers (page bytes and commit size), not only the last page,
//! so a tail dropped by power loss and regrown differently is a rewind.
//!
//! Rule 5 is a stop, not a snapshot: a replica prefix holds one page size. At
//! boot the actor also stops when the newest staged file is not readable but the replica's
//! newest file names another size than the WAL, before `recover` runs.

use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::num::NonZeroU32;

use lsr_ltx::{
    Event, HEADER_LEN, Header, PageChecksum, PageNo, PageSize, Timestamp, Txid, WalOrigin,
};
use lsr_wal::{
    FrameCursor, FrameRef, PageMap, ResumeError, Salts, ScanLimit, TxOutcome, WalError, WalHeader,
    WalOffset, WalReader,
};

use crate::fault::{Fault, SnapshotCause, StopCause};
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::{LtxRead, ReadFault};
use crate::position::{Continuity, LastPage, Position, ReadLockHold, WalFileLen};
use crate::source::CommittedRead;
use crate::store::{ListAt, ObjectStore, PageLimit, ReadLen, StoreError};

/// How much of the newest staged file one store read asks for while it is verified.
const NEWEST_STAGED_READ: ReadLen = ReadLen::CHUNK;

/// A read of exactly one LTX header ([`HEADER_LEN`], pinned by a test).
const HEADER_READ: ReadLen = ReadLen::or_chunk(100);

/// Why a frame could not be had at a resume point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Miss {
    /// The WAL ends before the frame.
    Truncated,
    /// The frame does not verify, or is not a commit frame.
    NotACommit,
    /// The WAL is not the generation or page size asked for.
    Moved,
}

/// The commit frame before a resume point, or why there is none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameCheck {
    /// The frame, verified.
    Frame(FrameRef),
    /// There is none.
    Miss(Miss),
}

impl FrameCheck {
    /// Sorts a [`WalReader::frame_before`] answer.
    ///
    /// # Errors
    /// [`Fault::LocalIo`] when the read itself failed, which says nothing about the WAL.
    pub fn of(answer: Result<FrameRef, ResumeError>) -> Result<Self, Fault> {
        match answer {
            Ok(frame) => Ok(Self::Frame(frame)),
            Err(ResumeError::Truncated { .. }) => Ok(Self::Miss(Miss::Truncated)),
            Err(ResumeError::PrevFrame { .. } | ResumeError::NotAtCommit { .. }) => {
                Ok(Self::Miss(Miss::NotACommit))
            }
            Err(ResumeError::Restarted { .. } | ResumeError::PageSizeMismatch { .. }) => {
                Ok(Self::Miss(Miss::Moved))
            }
            Err(ResumeError::Io(_)) => Err(Fault::LocalIo),
        }
    }
}

/// Go's `lastPageMatch` for the newest staged file read at boot (`db.go:1963-2007`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastPageCheck {
    /// The WAL's frame before the end has the page and bytes the newest staged file holds.
    Match {
        /// That page.
        last_page: LastPage,
        /// The cursor the next scan continues from.
        cursor: FrameCursor,
    },
    /// The newest staged file's copy of that page has other bytes.
    Mismatch,
    /// The newest staged file does not hold that page at all.
    NotInNewestStagedFile,
    /// There is no such frame.
    NoFrame(Miss),
    /// The WAL holds a last page like the newest staged file's, but what it holds in the newest staged file's range
    /// is not what the newest staged file staged: another page set, other bytes or another commit
    /// size (a rewound tail regrown with a different transaction).
    Rewritten,
}

/// The newest staged file, as a boot reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NewestStagedFile {
    /// A file that decodes and says where in the WAL it came from.
    Readable {
        /// Its transaction id.
        txid: Txid,
        /// The WAL bytes it covers.
        origin: WalOrigin,
        /// The database size in pages after it.
        commit: NonZeroU32,
        /// The database page size.
        page_size: PageSize,
        /// When it was written.
        timestamp: Timestamp,
        /// Whether the WAL still holds the page it ended on.
        last_page: LastPageCheck,
    },
    /// A file that is not usable as a position: it fails verification, has no
    /// WAL origin, or ends off a frame boundary.
    Unreadable {
        /// Its transaction id.
        txid: Txid,
    },
}

impl NewestStagedFile {
    /// The newest staged file's transaction id.
    #[must_use]
    pub const fn txid(&self) -> Txid {
        match self {
            Self::Readable { txid, .. } | Self::Unreadable { txid } => *txid,
        }
    }

    /// Reads the newest level-0 file in `staging` and checks it against `wal`.
    ///
    /// The whole file is decoded and verified, as Go's `Decoder.Verify` does
    /// (`db.go:715`), in bounded reads. `None` when the directory holds no file.
    ///
    /// # Errors
    /// A [`Fault`] for a staging store error (`Fault::from_staging`) or a failed WAL read.
    pub async fn load<L: ObjectStore, R: Read + Seek>(
        staging: &L,
        wal: Option<&mut WalReader<R>>,
    ) -> Result<Option<Self>, Fault> {
        let Some(newest) = newest_level_zero(staging).await? else {
            return Ok(None);
        };
        let txid = newest.key.range().max();
        let header = read_header(staging, &newest.key, Fault::from_staging).await?;
        let Some(header) = header.filter(|_| newest.key.range().min() == txid) else {
            return Ok(Some(Self::Unreadable { txid }));
        };
        let Some(origin) = Origin::of(&header) else {
            return Ok(Some(Self::Unreadable { txid }));
        };
        let file = NewestStagedFileSource {
            store: staging,
            key: newest.key,
            fault: Fault::from_staging,
        };
        let view = Self::checked(&file, None, &header, origin, wal).await?;
        Ok(Some(view.unwrap_or(Self::Unreadable { txid })))
    }

    /// Checks the newest staged file whose header names a WAL origin against `wal`: the frame
    /// before its end and the whole range it covers. `first`, when
    /// given, is the file's first [`NEWEST_STAGED_READ`] bytes, already read. `None` when
    /// the file does not decode and verify whole.
    async fn checked<S: ObjectStore, R: Read + Seek>(
        file: &NewestStagedFileSource<'_, S>,
        first: Option<Vec<u8>>,
        header: &Header,
        at: Origin,
        wal: Option<&mut WalReader<R>>,
    ) -> Result<Option<Self>, Fault> {
        let Origin {
            origin,
            end,
            commit,
        } = at;
        let (salt1, salt2) = origin.salts();
        let salts = Salts { salt1, salt2 };
        let frame = match wal {
            Some(wal) => {
                let check = if end.frame_index() > 0 {
                    FrameCheck::of(wal.frame_before(end, salts))?
                } else {
                    FrameCheck::Miss(Miss::NotACommit)
                };
                let range = newest_staged_file_range(wal, &origin, end, salts)?;
                Some((wal, check, range))
            }
            None => None,
        };
        let wanted = match frame {
            Some((_, FrameCheck::Frame(frame), _)) => Some(frame.pgno()),
            Some((_, FrameCheck::Miss(_), _)) | None => None,
        };
        let none = BTreeMap::new();
        let expected = match &frame {
            Some((_, _, Range::Pages { pages, .. })) => pages,
            Some((_, _, Range::Rewritten | Range::NotInThisWal)) | None => &none,
        };
        let Some(pages) = scan_pages(file, first, wanted, expected).await? else {
            return Ok(None);
        };
        let last_page = match frame {
            None => LastPageCheck::NoFrame(Miss::Truncated),
            Some((wal, check, range)) => {
                let checked = match_last_page(wal, check, end, salts, &pages)?;
                let matched = match checked {
                    LastPageCheck::Match { .. } => true,
                    LastPageCheck::Mismatch
                    | LastPageCheck::NotInNewestStagedFile
                    | LastPageCheck::NoFrame(_)
                    | LastPageCheck::Rewritten => false,
                };
                if matched && !range.agrees(&pages, commit) {
                    LastPageCheck::Rewritten
                } else {
                    checked
                }
            }
        };
        let fields = header.fields();
        Ok(Some(Self::Readable {
            txid: header.txids().max(),
            origin,
            commit,
            page_size: fields.page_size,
            timestamp: fields.timestamp,
            last_page,
        }))
    }
}

/// Where in the WAL the newest staged file's header says it came from, when it says so usably.
#[derive(Clone, Copy)]
struct Origin {
    origin: WalOrigin,
    end: WalOffset,
    commit: NonZeroU32,
}

impl Origin {
    /// `None` with no WAL origin, an end off a frame boundary, or a commit of 0.
    fn of(header: &Header) -> Option<Self> {
        let fields = header.fields();
        let origin = fields.wal?;
        let end = WalOffset::new(
            origin.offset().saturating_add(origin.size()),
            fields.page_size,
        )?;
        let commit = NonZeroU32::new(fields.commit)?;
        Some(Self {
            origin,
            end,
            commit,
        })
    }
}

/// The newest staged file and the store it is read from. `fault` names a store error as
/// staging's or the replica's.
struct NewestStagedFileSource<'a, S> {
    store: &'a S,
    key: FileKey,
    fault: fn(&StoreError) -> Fault,
}

/// What a decode of the newest staged file saw of the pages it was asked about.
struct NewestStagedPages {
    wanted: Option<PageChecksum>,
    last: Option<LastPage>,
    /// Every page the WAL holds in the newest staged file's range is in the newest staged file with the same bytes.
    /// The newest staged file may hold more: pages the database file supplied (grown pages, a snapshot's).
    holds_wal_pages: bool,
}

/// What the WAL holds in the newest staged file's range of frames.
enum Range {
    /// The newest version of each page, and the commit size of the range's last
    /// transaction (`None` for an empty range).
    Pages {
        pages: BTreeMap<PageNo, PageChecksum>,
        commit: Option<NonZeroU32>,
    },
    /// The range does not start or end on a transaction boundary of this WAL.
    Rewritten,
    /// The range is not in this WAL (the newest staged file from an earlier generation): nothing to
    /// compare, so nothing to veto.
    NotInThisWal,
}

impl Range {
    fn agrees(
        &self,
        newest_staged: &NewestStagedPages,
        newest_staged_file_commit: NonZeroU32,
    ) -> bool {
        match self {
            Self::Pages { commit, .. } => {
                newest_staged.holds_wal_pages
                    && commit.is_none_or(|commit| commit == newest_staged_file_commit)
            }
            Self::Rewritten => false,
            Self::NotInThisWal => true,
        }
    }
}

/// Re-derives the newest staged file from the WAL frames in `[origin.offset, end)`.
///
/// Rule 9's last-page match cannot see an earlier page of the newest staged file that differs: a
/// power loss with `synchronous=NORMAL` drops the tail, and the application commits
/// another transaction over the same frames. Comparing the newest version of every
/// page the range holds, and the commit size, closes that. It reads the range once,
/// at boot, in the bounded memory of one file's page map.
fn newest_staged_file_range<R: Read + Seek>(
    wal: &mut WalReader<R>,
    origin: &WalOrigin,
    end: WalOffset,
    salts: Salts,
) -> Result<Range, Fault> {
    let page_size = end.page_size();
    let Some(start) = WalOffset::new(origin.offset(), page_size) else {
        return Ok(Range::Rewritten);
    };
    let mut cursor = match wal.resume(start, salts) {
        Ok(cursor) => cursor,
        Err(ResumeError::PrevFrame { .. } | ResumeError::NotAtCommit { .. }) => {
            return Ok(Range::Rewritten);
        }
        Err(ResumeError::Io(_)) => return Err(Fault::LocalIo),
        Err(
            ResumeError::Truncated { .. }
            | ResumeError::Restarted { .. }
            | ResumeError::PageSizeMismatch { .. },
        ) => return Ok(Range::NotInThisWal),
    };
    let mut map = PageMap::new(page_size);
    while cursor.next().get() < end.get() {
        match wal.next_tx(&cursor, ScanLimit::FirstInvalidFrame) {
            Ok(TxOutcome::Tx(tx)) => {
                map.apply(&tx).map_err(Fault::from_page_map)?;
                cursor = *tx.end();
            }
            Err(WalError::Io(_)) => return Err(Fault::LocalIo),
            // The scan started at the origin and did not reach the end on one verified
            // chain: frames past it may be residue of a dropped tail.
            Ok(TxOutcome::End { .. })
            | Err(
                WalError::PageSizeMismatch { .. }
                | WalError::Restarted { .. }
                | WalError::CursorPastEnd { .. }
                | WalError::CorruptBelowCommitted { .. }
                | WalError::TruncatedBelowCommitted { .. }
                | WalError::EndNotAtCommit { .. }
                | WalError::TooManyFrames,
            ) => return unreached(wal),
        }
    }
    if cursor.next() != end {
        return Ok(Range::Rewritten);
    }
    Ok(Range::Pages {
        pages: map
            .pages()
            .map(|frame| (frame.pgno(), frame.digest()))
            .collect(),
        commit: map.commit(),
    })
}

/// The scan from the origin stopped short of the newest staged file's end. If the header still names
/// the scanned generation the range was rewritten; if not, the WAL moved under the
/// read and the caller retries (`generation_unchanged` tells the two apart).
fn unreached<R: Read + Seek>(wal: &mut WalReader<R>) -> Result<Range, Fault> {
    match wal.generation_unchanged() {
        Ok(true) => Ok(Range::Rewritten),
        Ok(false) => Err(Fault::WalMoved),
        Err(_err) => Err(Fault::LocalIo),
    }
}

fn match_last_page<R: Read + Seek>(
    wal: &mut WalReader<R>,
    check: FrameCheck,
    end: WalOffset,
    salts: Salts,
    pages: &NewestStagedPages,
) -> Result<LastPageCheck, Fault> {
    let last_page = match (check, pages.last) {
        (FrameCheck::Frame(frame), _) => match pages.wanted {
            None => return Ok(LastPageCheck::NotInNewestStagedFile),
            Some(digest) if digest != frame.digest() => return Ok(LastPageCheck::Mismatch),
            Some(_) => LastPage::of_frame(&frame),
        },
        (FrameCheck::Miss(Miss::NotACommit), Some(last)) if end.frame_index() == 0 => last,
        (FrameCheck::Miss(miss), _) => return Ok(LastPageCheck::NoFrame(miss)),
    };
    match wal.resume(end, salts) {
        Ok(cursor) => Ok(LastPageCheck::Match { last_page, cursor }),
        Err(err) => Ok(match FrameCheck::of(Err(err))? {
            FrameCheck::Miss(miss) => LastPageCheck::NoFrame(miss),
            FrameCheck::Frame(_) => LastPageCheck::NoFrame(Miss::Moved),
        }),
    }
}

async fn newest_level_zero<L: ObjectStore>(staging: &L) -> Result<Option<FileInfo>, Fault> {
    let (mut at, mut newest) = (ListAt::first(Level::ZERO), None);
    loop {
        let page = staging
            .list(at.level, at.after, PageLimit::MAX)
            .await
            .map_err(|err| Fault::from_staging(&err))?;
        newest = page.files.last().copied().or(newest);
        match at
            .following(page.next)
            .map_err(Fault::from_staging_listing)?
        {
            Some(next) => at = next,
            None => return Ok(newest),
        }
    }
}

/// A file's header; `None` when it is short or malformed. `fault` names a
/// store error as staging's or the replica's.
pub(crate) async fn read_header<S: ObjectStore>(
    store: &S,
    key: &FileKey,
    fault: fn(&StoreError) -> Fault,
) -> Result<Option<Header>, Fault> {
    let bytes = store
        .read(key, 0, HEADER_READ)
        .await
        .map_err(|err| fault(&err))?;
    Ok(<[u8; HEADER_LEN]>::try_from(bytes)
        .ok()
        .and_then(|raw| Header::decode(&raw).ok()))
}

/// Decodes and verifies the whole file; `None` when it fails to. A read
/// shorter than asked is the file's end (the store's contract), so a file under
/// [`NEWEST_STAGED_READ`] bytes takes one read, or none when `first` holds it.
async fn scan_pages<S: ObjectStore>(
    file: &NewestStagedFileSource<'_, S>,
    first: Option<Vec<u8>>,
    wanted: Option<PageNo>,
    expected: &BTreeMap<PageNo, PageChecksum>,
) -> Result<Option<NewestStagedPages>, Fault> {
    let chunk = NEWEST_STAGED_READ;
    let mut seen = NewestStagedPages {
        wanted: None,
        last: None,
        holds_wal_pages: false,
    };
    let (mut matching, mut differing) = (0_usize, false);
    let mut read = match first {
        Some(bytes) => LtxRead::with_first(file.key, chunk, bytes),
        None => LtxRead::new(file.key, chunk),
    };
    let verified: Result<_, ReadFault> = read
        .read_verified(file.store, |event| {
            if let Event::Page(page) = event {
                let (pgno, digest) = (page.pgno(), page.checksum());
                seen.last = Some(LastPage::new(pgno, digest));
                if wanted == Some(pgno) {
                    seen.wanted = Some(digest);
                }
                match expected.get(&pgno) {
                    Some(held) if *held == digest => matching = matching.saturating_add(1),
                    Some(_) => differing = true,
                    None => {}
                }
            }
            Ok(())
        })
        .await;
    match verified {
        Ok(_) => {
            seen.holds_wal_pages = !differing && matching == expected.len();
            Ok(Some(seen))
        }
        Err(ReadFault::Store(err)) => Err((file.fault)(&err)),
        Err(ReadFault::Decode(_)) => Ok(None),
        Err(ReadFault::Rejected(never)) => match never {},
    }
}

/// What a step knows of the WAL before it recovers: one read of the WAL header
/// and one read of the wal-index, taken together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalView {
    /// The WAL header; `None` with no file, under 32 bytes, or with a bad checksum.
    pub header: Option<WalHeader>,
    /// The file's length on disk.
    pub len: Option<WalFileLen>,
    /// The wal-index read; `None` when none was taken.
    pub committed: Option<CommittedRead>,
    /// The read-lock hold the answers above belong to.
    pub epoch: ReadLockHold,
}

/// The two reads of the WAL that only a running position needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probes {
    /// The commit frame before the position's end.
    pub prev: FrameCheck,
    /// Whether a whole committed transaction of the position's own generation still
    /// chains from its end, asked only when the WAL's header names another generation,
    /// or (through [`Self::read_against`]) when the wal-index was reset ahead of a header
    /// that names the position's. Frames with no commit after them are a rolled-back
    /// spill and do not count.
    pub stale_next: bool,
}

impl Probes {
    /// Reads both from a fresh reader.
    ///
    /// # Errors
    /// [`Fault::LocalIo`] when a read fails.
    pub fn read<R: Read + Seek>(wal: &mut WalReader<R>, at: &Position) -> Result<Self, Fault> {
        Self::probe(wal, at, false)
    }

    /// As [`Self::read`], and also asks `stale_next` when `committed` shows the
    /// wal-index reset ahead of the header ([`CommittedRead::reset_ahead_of`]): the
    /// commits it finds were backfilled and are now only in the database file.
    ///
    /// # Errors
    /// [`Fault::LocalIo`] when a read fails.
    pub fn read_against<R: Read + Seek>(
        wal: &mut WalReader<R>,
        at: &Position,
        committed: &CommittedRead,
    ) -> Result<Self, Fault> {
        let reset = committed.reset_ahead_of(wal.header().salts());
        Self::probe(wal, at, reset)
    }

    fn probe<R: Read + Seek>(
        wal: &mut WalReader<R>,
        at: &Position,
        reset: bool,
    ) -> Result<Self, Fault> {
        let cursor = at.cursor();
        let prev = if cursor.next().frame_index() == 0 {
            FrameCheck::Miss(Miss::NotACommit)
        } else {
            FrameCheck::of(wal.frame_before(cursor.next(), cursor.salts()))?
        };
        let stale_next = (reset || wal.header().salts() != cursor.salts())
            && wal.commit_follows(&cursor).map_err(|_err| Fault::LocalIo)?;
        Ok(Self { prev, stale_next })
    }
}

/// What the position comes from.
#[derive(Clone, Copy, Debug)]
pub enum PositionSource<'a> {
    /// The newest staged file, read at boot.
    Boot(&'a NewestStagedFile),
    /// The position the last step left, and the reads of the WAL it needs.
    Running(&'a Position, Probes),
}

/// What to do next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovered {
    /// Scan on from the newest staged file's end.
    Continue(Position),
    /// The WAL restarted and nothing was lost: scan the new generation from its start.
    WalRestarted(Position),
    /// Write a full snapshot.
    Snapshot {
        /// The transaction id it follows.
        after: Option<Txid>,
        /// Why.
        cause: SnapshotCause,
    },
    /// Stop.
    Stop(StopCause),
}

/// Something a person should hear about, beside the decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverEvent {
    /// Frames already replicated are gone from the local WAL: a power loss
    /// under `synchronous=NORMAL`. Loud, because the replica keeps transactions
    /// the database lost.
    LocalRewound {
        /// The newest staged file's end.
        synced: WalOffset,
        /// The WAL's committed end now.
        committed: WalOffset,
    },
    /// The continuity proof failed: frames were appended after the position was
    /// pinned and then abandoned by a restart. A bug in the proof, or a broken lock.
    ReadLockViolated {
        /// The position's end.
        synced: WalOffset,
    },
}

/// A decision and the event that goes with it, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Verdict {
    /// What to do.
    pub recovered: Recovered,
    /// What to tell people.
    pub event: Option<RecoverEvent>,
}

/// [`recover_traced] without the event.
#[must_use]
pub fn recover(
    source: Option<PositionSource<'_>>,
    wal: &WalView,
    remote: Option<Txid>,
) -> Recovered {
    recover_traced(source, wal, remote).recovered
}

/// Decides, and names the event the decision calls for.
#[must_use]
pub fn recover_traced(
    source: Option<PositionSource<'_>>,
    wal: &WalView,
    remote: Option<Txid>,
) -> Verdict {
    let Some(source) = source else {
        return plain(Recovered::Snapshot {
            after: remote,
            cause: SnapshotCause::FirstSync,
        });
    };
    let txid = source_txid(&source);
    if let Some(ahead) = remote.filter(|remote| *remote > txid) {
        return plain(match source {
            PositionSource::Boot(_) => Recovered::Snapshot {
                after: Some(ahead),
                cause: SnapshotCause::BehindRemote { local: txid },
            },
            PositionSource::Running(..) => Recovered::Stop(StopCause::OtherWriter),
        });
    }
    let after = Some(remote.map_or(txid, |remote| remote.max(txid)));
    let snapshot = |cause| plain(Recovered::Snapshot { after, cause });
    let (Some(header), Some(seen)) = (wal.header, Seen::of(&source)) else {
        return snapshot(match (&source, wal.header) {
            (PositionSource::Boot(NewestStagedFile::Unreadable { .. }), _)
            | (PositionSource::Boot(_), Some(_)) => SnapshotCause::NewestStagedFileUnreadable,
            (PositionSource::Boot(_) | PositionSource::Running(..), _) => SnapshotCause::WalMissing,
        });
    };
    if seen.page_size != header.page_size() {
        return plain(Recovered::Stop(StopCause::PageSizeChanged {
            was: seen.page_size,
            now: header.page_size(),
        }));
    }
    if seen.salts != header.salts() {
        return restarted(&source, &seen, &header, wal, after);
    }
    match same_generation(&source, &seen, &header, wal) {
        Ok(position) => plain(Recovered::Continue(position)),
        Err(Refusal { cause, event }) => Verdict {
            recovered: Recovered::Snapshot { after, cause },
            event,
        },
    }
}

const fn plain(recovered: Recovered) -> Verdict {
    Verdict {
        recovered,
        event: None,
    }
}

const fn source_txid(source: &PositionSource<'_>) -> Txid {
    match source {
        PositionSource::Boot(view) => view.txid(),
        PositionSource::Running(position, _) => position.txid(),
    }
}

/// What both kinds of newest staged file agree on.
struct Seen {
    page_size: PageSize,
    salts: Salts,
    end: WalOffset,
}

impl Seen {
    fn of(source: &PositionSource<'_>) -> Option<Self> {
        match source {
            PositionSource::Boot(NewestStagedFile::Readable {
                origin, page_size, ..
            }) => {
                let (salt1, salt2) = origin.salts();
                Some(Self {
                    page_size: *page_size,
                    salts: Salts { salt1, salt2 },
                    end: WalOffset::new(origin.offset().saturating_add(origin.size()), *page_size)?,
                })
            }
            PositionSource::Boot(NewestStagedFile::Unreadable { .. }) => None,
            PositionSource::Running(position, _) => Some(Self {
                page_size: position.page_size(),
                salts: position.cursor().salts(),
                end: position.synced().offset(),
            }),
        }
    }
}

/// Rule 6: the generation changed.
fn restarted(
    source: &PositionSource<'_>,
    seen: &Seen,
    header: &WalHeader,
    wal: &WalView,
    after: Option<Txid>,
) -> Verdict {
    let changed = Recovered::Snapshot {
        after,
        cause: SnapshotCause::GenerationChanged,
    };
    let PositionSource::Running(position, probes) = source else {
        return plain(changed);
    };
    let pinned = match position.continuity() {
        Continuity::Held(epoch) => epoch == wal.epoch,
        Continuity::Broken => false,
    };
    let proven = pinned && header.salts().salt1 == seen.salts.salt1.wrapping_add(1);
    if !proven {
        return plain(changed);
    }
    let new_end = committed_offset(header, wal).unwrap_or(WalOffset::start(seen.page_size));
    if new_end.get() < seen.end.get() && probes.stale_next {
        return Verdict {
            recovered: changed,
            event: Some(RecoverEvent::ReadLockViolated { synced: seen.end }),
        };
    }
    plain(Recovered::WalRestarted(Position::new(
        position.txid(),
        FrameCursor::at_start(header),
        position.commit(),
        position.last_page(),
        position.timestamp(),
        position.continuity(),
    )))
}

/// The wal-index's committed end, when it belongs to the generation the header names.
fn committed_offset(header: &WalHeader, wal: &WalView) -> Option<WalOffset> {
    wal.committed
        .map(|read| read.end())
        .filter(|end| end.salts() == header.salts())
        .map(|end| end.offset())
}

/// Why the newest staged file cannot be continued in the generation it was made in.
struct Refusal {
    cause: SnapshotCause,
    event: Option<RecoverEvent>,
}

const fn refuse(cause: SnapshotCause) -> Refusal {
    Refusal { cause, event: None }
}

/// Rules 7 to 10.
fn same_generation(
    source: &PositionSource<'_>,
    seen: &Seen,
    header: &WalHeader,
    wal: &WalView,
) -> Result<Position, Refusal> {
    let committed = committed_offset(header, wal);
    let short_file = wal
        .len
        .is_some_and(|len| !len.covers(crate::SyncedOffset::new(seen.end)));
    let frame = frame_state(source, seen);
    let frame_gone = match frame {
        FrameState::Miss(Miss::Truncated) | FrameState::Rewritten => true,
        FrameState::Fresh
        | FrameState::Mismatch
        | FrameState::Miss(Miss::NotACommit | Miss::Moved) => false,
    };
    if committed.is_some_and(|end| end.get() < seen.end.get()) || short_file || frame_gone {
        let committed = committed.unwrap_or(WalOffset::start(seen.page_size));
        return Err(Refusal {
            cause: SnapshotCause::LocalRewound {
                synced: seen.end,
                committed,
            },
            event: Some(RecoverEvent::LocalRewound {
                synced: seen.end,
                committed,
            }),
        });
    }
    match frame {
        FrameState::Miss(Miss::NotACommit) => Err(refuse(SnapshotCause::PrevFrameMismatch)),
        FrameState::Miss(Miss::Moved) => Err(refuse(SnapshotCause::GenerationChanged)),
        FrameState::Mismatch => Err(refuse(SnapshotCause::LastPageMismatch)),
        FrameState::Miss(Miss::Truncated) | FrameState::Rewritten | FrameState::Fresh => {
            if let Some(refusal) = reset_past(source, seen, header, wal) {
                return Err(refusal);
            }
            let cursor_at_end = committed.is_some_and(|end| end.get() == seen.end.get());
            continued(source, wal, cursor_at_end)
        }
    }
}

/// Rule 9a: the wal-index was reset ahead of the header and a commit follows the
/// position. A position pinned at this epoch cannot have one after it (the reset
/// needed a backfill its read lock blocks), so finding one is what the check that catches frames abandoned after the read lock was taken exists for.
fn reset_past(
    source: &PositionSource<'_>,
    seen: &Seen,
    header: &WalHeader,
    wal: &WalView,
) -> Option<Refusal> {
    let PositionSource::Running(position, probes) = source else {
        return None;
    };
    let reset = wal
        .committed
        .is_some_and(|read| read.reset_ahead_of(header.salts()));
    if !reset || !probes.stale_next {
        return None;
    }
    let pinned = position.continuity() == Continuity::Held(wal.epoch);
    Some(Refusal {
        cause: SnapshotCause::GenerationChanged,
        event: pinned.then_some(RecoverEvent::ReadLockViolated { synced: seen.end }),
    })
}

/// What the frame before the end says, whichever way the newest staged file carries it.
#[derive(Clone, Copy)]
enum FrameState {
    /// The page matches, or there is nothing to match (the end is the start).
    Fresh,
    /// The page differs or the newest staged file lacks it.
    Mismatch,
    /// No frame to compare.
    Miss(Miss),
    /// The last page matches but the range before it is not what the newest staged file staged.
    Rewritten,
}

fn frame_state(source: &PositionSource<'_>, seen: &Seen) -> FrameState {
    let at_start = seen.end.frame_index() == 0;
    match source {
        PositionSource::Boot(NewestStagedFile::Readable { last_page, .. }) => match last_page {
            LastPageCheck::Match { .. } => FrameState::Fresh,
            LastPageCheck::Mismatch | LastPageCheck::NotInNewestStagedFile => FrameState::Mismatch,
            LastPageCheck::NoFrame(_) if at_start => FrameState::Fresh,
            LastPageCheck::NoFrame(miss) => FrameState::Miss(*miss),
            LastPageCheck::Rewritten => FrameState::Rewritten,
        },
        PositionSource::Boot(NewestStagedFile::Unreadable { .. }) => FrameState::Fresh,
        PositionSource::Running(_, _) if at_start => FrameState::Fresh,
        PositionSource::Running(position, probes) => match probes.prev {
            FrameCheck::Miss(miss) => FrameState::Miss(miss),
            FrameCheck::Frame(frame)
                if frame.pgno() == position.last_page().pgno()
                    && frame.digest() == position.last_page().digest() =>
            {
                FrameState::Fresh
            }
            FrameCheck::Frame(_) => FrameState::Mismatch,
        },
    }
}

/// Rule 10: the position to scan on from.
fn continued(
    source: &PositionSource<'_>,
    wal: &WalView,
    at_committed_end: bool,
) -> Result<Position, Refusal> {
    let prior = match source {
        PositionSource::Running(position, _) => Some(position.continuity()),
        PositionSource::Boot(_) => None,
    };
    let continuity = match prior {
        Some(Continuity::Held(epoch)) if epoch == wal.epoch => Continuity::Held(epoch),
        Some(_) | None if at_committed_end => Continuity::Held(wal.epoch),
        Some(_) | None => Continuity::Broken,
    };
    match source {
        PositionSource::Running(position, _) => Ok(position.with_continuity(continuity)),
        PositionSource::Boot(NewestStagedFile::Readable {
            txid,
            commit,
            timestamp,
            last_page: LastPageCheck::Match { last_page, cursor },
            ..
        }) => Ok(Position::new(
            *txid, *cursor, *commit, *last_page, *timestamp, continuity,
        )),
        // Rules 3 and 9 already refused every newest staged file read at boot that is not a match.
        PositionSource::Boot(_) => Err(refuse(SnapshotCause::LastPageMismatch)),
    }
}

mod remote;

#[cfg(test)]
mod tests;
