//! Deciding where to resume: the pure half of a sync step.
//!
//! [`recover`] looks at the newest staged file (the tip), at the WAL as one read
//! of the wal-index and the WAL header saw it, and at the replica's newest
//! transaction, and says whether to continue where the tip ended, continue in a
//! restarted WAL, snapshot, or halt. It reads nothing: the reads are done by
//! [`TipView::load`] and [`Probes::read`], so every rule below can be driven from
//! a table of inputs. Go answers the same question in `verifyWithExecutor`
//! (`db.go:1782-1925`) with a heuristic per branch; the rules here replace two
//! of those heuristics with the read-transaction proof of [`Continuity`].
//!
//! Rules, first match wins: 1 no tip, 2 replica ahead, 3 tip unreadable,
//! 4 no WAL header, 5 page size changed, 6 salts changed, 7 same salts but the
//! WAL is shorter than the tip's end, 8 the frame before the end is not a
//! commit frame, 9 the last page differs, 10 continue.
//!
//! Rule 6 proves one restart with the read-transaction pin and `salt1 + 1`; it
//! does not check `checkpoint_seq`, which real SQLite takes from the restarting
//! connection's own count (ruling V4). At boot, rule 9 compares the whole tip with
//! the WAL range it covers (page bytes and commit size), not only the last page,
//! so a tail dropped by power loss and regrown differently is a rewind (ruling V3).

use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::num::NonZeroU32;

use lsr_ltx::{
    Checksum, Decoder, Event, HEADER_LEN, Header, PageNo, PageSize, Timestamp, Txid, WalOrigin,
};
use lsr_wal::{
    FrameCursor, FrameRef, PageMap, ResumeError, Salts, ScanLimit, TxOutcome, WalError, WalHeader,
    WalOffset, WalReader,
};

use crate::fault::{Fault, HaltCause, SnapshotCause};
use crate::layout::{FileInfo, FileKey, Level};
use crate::position::{Continuity, LastPage, Position, ReadEpoch, WalFileLen};
use crate::source::CommittedRead;
use crate::store::{ObjectStore, PageLimit, ReadLen};

/// How much of the tip one store read asks for while it is verified.
const TIP_READ: u32 = 1 << 20;

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

/// Go's `lastPageMatch` for a tip read at boot (`db.go:1963-2007`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastPageCheck {
    /// The WAL's frame before the end has the page and bytes the tip holds.
    Match {
        /// That page.
        last_page: LastPage,
        /// The cursor the next scan continues from.
        cursor: FrameCursor,
    },
    /// The tip's copy of that page has other bytes.
    Mismatch,
    /// The tip does not hold that page at all.
    NotInTip,
    /// There is no such frame.
    NoFrame(Miss),
    /// The WAL holds a last page like the tip's, but what it holds in the tip's range
    /// is not what the tip staged: another page set, other bytes or another commit
    /// size (a rewound tail regrown with a different transaction).
    Rewritten,
}

/// The newest staged file, as a boot reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TipView {
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

impl TipView {
    /// The tip's transaction id.
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
        let single = newest.key.range().min() == txid;
        let Some(header) = read_header(staging, &newest.key).await?.filter(|_| single) else {
            return Ok(Some(Self::Unreadable { txid }));
        };
        let fields = header.fields();
        let end = fields.wal.and_then(|origin| {
            WalOffset::new(
                origin.offset().saturating_add(origin.size()),
                fields.page_size,
            )
        });
        let (Some(origin), Some(end), Some(commit)) =
            (fields.wal, end, NonZeroU32::new(fields.commit))
        else {
            return Ok(Some(Self::Unreadable { txid }));
        };
        let (salt1, salt2) = origin.salts();
        let salts = Salts { salt1, salt2 };
        let frame = match wal {
            Some(wal) => {
                let check = if end.frame_index() > 0 {
                    FrameCheck::of(wal.frame_before(end, salts))?
                } else {
                    FrameCheck::Miss(Miss::NotACommit)
                };
                let range = tip_range(wal, &origin, end, salts)?;
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
        let Some(pages) = scan_pages(staging, &newest, wanted, expected).await? else {
            return Ok(Some(Self::Unreadable { txid }));
        };
        let last_page = match frame {
            None => LastPageCheck::NoFrame(Miss::Truncated),
            Some((wal, check, range)) => {
                let checked = match_last_page(wal, check, end, salts, &pages)?;
                if matches!(checked, LastPageCheck::Match { .. })
                    && !range.agrees(&pages, fields.commit)
                {
                    LastPageCheck::Rewritten
                } else {
                    checked
                }
            }
        };
        Ok(Some(Self::Readable {
            txid,
            origin,
            commit,
            page_size: fields.page_size,
            timestamp: fields.timestamp,
            last_page,
        }))
    }
}

/// What a decode of the tip saw of the pages it was asked about.
struct TipPages {
    wanted: Option<Checksum>,
    last: Option<LastPage>,
    /// Every page the WAL holds in the tip's range is in the tip with the same bytes.
    /// The tip may hold more: pages the database file supplied (walgrowth, a snapshot's).
    holds_wal_pages: bool,
}

/// What the WAL holds in the tip's range of frames (ruling V3).
enum Range {
    /// The newest version of each page, and the commit size of the range's last
    /// transaction (0 for an empty range).
    Pages {
        pages: BTreeMap<PageNo, Checksum>,
        commit: u32,
    },
    /// The range does not start or end on a transaction boundary of this WAL.
    Rewritten,
    /// The range is not in this WAL (a tip from an earlier generation): nothing to
    /// compare, so nothing to veto.
    NotInThisWal,
}

impl Range {
    fn agrees(&self, tip: &TipPages, tip_commit: u32) -> bool {
        match self {
            Self::Pages { commit, .. } => {
                tip.holds_wal_pages && (*commit == 0 || *commit == tip_commit)
            }
            Self::Rewritten => false,
            Self::NotInThisWal => true,
        }
    }
}

/// Re-derives the tip from the WAL frames in `[origin.offset, end)`.
///
/// Rule 9's last-page match cannot see an earlier page of the tip that differs: a
/// power loss with `synchronous=NORMAL` drops the tail, and the application commits
/// another transaction over the same frames. Comparing the newest version of every
/// page the range holds, and the commit size, closes that. It reads the range once,
/// at boot, in the bounded memory of one file's page map.
fn tip_range<R: Read + Seek>(
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

/// The scan from the origin stopped short of the tip's end. If the header still names
/// the scanned generation the range was rewritten; if not, the WAL moved under the
/// read and the caller retries (ruling V1's helper, `generation_unchanged`).
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
    pages: &TipPages,
) -> Result<LastPageCheck, Fault> {
    let last_page = match (check, pages.last) {
        (FrameCheck::Frame(frame), _) => match pages.wanted {
            None => return Ok(LastPageCheck::NotInTip),
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
    let (mut after, mut newest) = (None, None);
    loop {
        let page = staging
            .list(Level::ZERO, after, PageLimit::MAX)
            .await
            .map_err(|err| Fault::from_staging(&err))?;
        newest = page.files.last().copied().or(newest);
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok(newest),
        }
    }
}

/// The tip's header; `None` when it is short or malformed.
async fn read_header<L: ObjectStore>(staging: &L, key: &FileKey) -> Result<Option<Header>, Fault> {
    let len = u32::try_from(HEADER_LEN).ok().and_then(ReadLen::new);
    let Some(len) = len else {
        return Err(Fault::Bug);
    };
    let bytes = staging
        .read(key, 0, len)
        .await
        .map_err(|err| Fault::from_staging(&err))?;
    Ok(<[u8; HEADER_LEN]>::try_from(bytes)
        .ok()
        .and_then(|raw| Header::decode(&raw).ok()))
}

/// Decodes and verifies the whole file; `None` when it fails to.
async fn scan_pages<L: ObjectStore>(
    staging: &L,
    file: &FileInfo,
    wanted: Option<PageNo>,
    expected: &BTreeMap<PageNo, Checksum>,
) -> Result<Option<TipPages>, Fault> {
    let Some(chunk) = ReadLen::new(TIP_READ) else {
        return Err(Fault::Bug);
    };
    let mut seen = TipPages {
        wanted: None,
        last: None,
        holds_wal_pages: false,
    };
    let (mut matching, mut differing) = (0_usize, false);
    let mut decoder = Decoder::new();
    let mut offset = 0u64;
    loop {
        let bytes = staging
            .read(&file.key, offset, chunk)
            .await
            .map_err(|err| Fault::from_staging(&err))?;
        if bytes.is_empty() {
            break;
        }
        offset = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let mut input = bytes.as_slice();
        while !input.is_empty() {
            let Ok((used, event)) = decoder.feed(input) else {
                return Ok(None);
            };
            if let Some(Event::Page { pgno, data }) = event {
                let digest = Checksum::of_page(pgno, data);
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
            input = input.get(used..).unwrap_or_default();
        }
    }
    seen.holds_wal_pages = !differing && matching == expected.len();
    Ok(decoder.finish().ok().map(|_| seen))
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
    /// The read epoch the answers above belong to.
    pub epoch: ReadEpoch,
}

/// The two reads of the WAL that only a running position needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probes {
    /// The commit frame before the position's end.
    pub prev: FrameCheck,
    /// Whether a whole committed transaction of the position's own generation still
    /// chains from its end, asked only when the WAL's header names another generation.
    /// Frames with no commit after them are a rolled-back spill and do not count (ruling V2).
    pub stale_next: bool,
}

impl Probes {
    /// Reads both from a fresh reader.
    ///
    /// # Errors
    /// [`Fault::LocalIo`] when a read fails.
    pub fn read<R: Read + Seek>(wal: &mut WalReader<R>, at: &Position) -> Result<Self, Fault> {
        let cursor = at.cursor();
        let prev = if cursor.next().frame_index() == 0 {
            FrameCheck::Miss(Miss::NotACommit)
        } else {
            FrameCheck::of(wal.frame_before(cursor.next(), cursor.salts()))?
        };
        let stale_next = wal.header().salts() != cursor.salts()
            && wal.commit_follows(&cursor).map_err(|_err| Fault::LocalIo)?;
        Ok(Self { prev, stale_next })
    }
}

/// What the position comes from.
#[derive(Clone, Copy, Debug)]
pub enum Tip<'a> {
    /// The newest staged file, read at boot.
    Boot(&'a TipView),
    /// The position the last step left, and the reads of the WAL it needs.
    Running(&'a Position, Probes),
}

/// What to do next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovered {
    /// Scan on from the tip's end.
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
    Halt(HaltCause),
}

/// Something a person should hear about, beside the decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverEvent {
    /// Frames already replicated are gone from the local WAL: a power loss
    /// under `synchronous=NORMAL`. Loud, because the replica keeps transactions
    /// the database lost.
    LocalRewound {
        /// The tip's end.
        synced: WalOffset,
        /// The WAL's committed end now.
        committed: WalOffset,
    },
    /// The continuity proof failed: frames were appended after the position was
    /// pinned and then abandoned by a restart. A bug in the proof, or a broken lock.
    PinViolated {
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
pub fn recover(tip: Option<Tip<'_>>, wal: &WalView, remote: Option<Txid>) -> Recovered {
    recover_traced(tip, wal, remote).recovered
}

/// Decides, and names the event the decision calls for.
#[must_use]
pub fn recover_traced(tip: Option<Tip<'_>>, wal: &WalView, remote: Option<Txid>) -> Verdict {
    let Some(tip) = tip else {
        return plain(Recovered::Snapshot {
            after: remote,
            cause: SnapshotCause::FirstSync,
        });
    };
    let txid = tip_txid(&tip);
    if let Some(ahead) = remote.filter(|remote| *remote > txid) {
        return plain(match tip {
            Tip::Boot(_) => Recovered::Snapshot {
                after: Some(ahead),
                cause: SnapshotCause::BehindRemote { local: txid },
            },
            Tip::Running(..) => Recovered::Halt(HaltCause::ForeignWriter),
        });
    }
    let after = Some(remote.map_or(txid, |remote| remote.max(txid)));
    let snapshot = |cause| plain(Recovered::Snapshot { after, cause });
    let (Some(header), Some(seen)) = (wal.header, Seen::of(&tip)) else {
        return snapshot(match (&tip, wal.header) {
            (Tip::Boot(TipView::Unreadable { .. }), _) | (Tip::Boot(_), Some(_)) => {
                SnapshotCause::LocalTipUnreadable
            }
            (Tip::Boot(_) | Tip::Running(..), _) => SnapshotCause::WalMissing,
        });
    };
    if seen.page_size != header.page_size() {
        return snapshot(SnapshotCause::PageSizeChanged);
    }
    if seen.salts != header.salts() {
        return restarted(&tip, &seen, &header, wal, after);
    }
    match same_generation(&tip, &seen, &header, wal) {
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

const fn tip_txid(tip: &Tip<'_>) -> Txid {
    match tip {
        Tip::Boot(view) => view.txid(),
        Tip::Running(position, _) => position.txid(),
    }
}

/// What both kinds of tip agree on.
struct Seen {
    page_size: PageSize,
    salts: Salts,
    end: WalOffset,
}

impl Seen {
    fn of(tip: &Tip<'_>) -> Option<Self> {
        match tip {
            Tip::Boot(TipView::Readable {
                origin, page_size, ..
            }) => {
                let (salt1, salt2) = origin.salts();
                Some(Self {
                    page_size: *page_size,
                    salts: Salts { salt1, salt2 },
                    end: WalOffset::new(origin.offset().saturating_add(origin.size()), *page_size)?,
                })
            }
            Tip::Boot(TipView::Unreadable { .. }) => None,
            Tip::Running(position, _) => Some(Self {
                page_size: position.page_size(),
                salts: position.cursor().salts(),
                end: position.synced().offset(),
            }),
        }
    }
}

/// Rule 6: the generation changed.
fn restarted(
    tip: &Tip<'_>,
    seen: &Seen,
    header: &WalHeader,
    wal: &WalView,
    after: Option<Txid>,
) -> Verdict {
    let changed = Recovered::Snapshot {
        after,
        cause: SnapshotCause::GenerationChanged,
    };
    let Tip::Running(position, probes) = tip else {
        return plain(changed);
    };
    let proven = matches!(position.continuity(), Continuity::Pinned(epoch) if epoch == wal.epoch)
        && header.salts().salt1 == seen.salts.salt1.wrapping_add(1);
    if !proven {
        return plain(changed);
    }
    let new_end = committed_offset(header, wal).unwrap_or(WalOffset::start(seen.page_size));
    if new_end.get() < seen.end.get() && probes.stale_next {
        return Verdict {
            recovered: changed,
            event: Some(RecoverEvent::PinViolated { synced: seen.end }),
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

/// Why the tip cannot be continued in the generation it was made in.
struct Refusal {
    cause: SnapshotCause,
    event: Option<RecoverEvent>,
}

const fn refuse(cause: SnapshotCause) -> Refusal {
    Refusal { cause, event: None }
}

/// Rules 7 to 10.
fn same_generation(
    tip: &Tip<'_>,
    seen: &Seen,
    header: &WalHeader,
    wal: &WalView,
) -> Result<Position, Refusal> {
    let committed = committed_offset(header, wal);
    let short_file = wal
        .len
        .is_some_and(|len| !len.covers(crate::SyncedOffset::new(seen.end)));
    let frame = frame_state(tip, seen);
    if committed.is_some_and(|end| end.get() < seen.end.get())
        || short_file
        || matches!(
            frame,
            FrameState::Miss(Miss::Truncated) | FrameState::Rewritten
        )
    {
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
            let cursor_at_end = committed.is_some_and(|end| end.get() == seen.end.get());
            continued(tip, wal, cursor_at_end)
        }
    }
}

/// What the frame before the end says, whichever way the tip carries it.
#[derive(Clone, Copy)]
enum FrameState {
    /// The page matches, or there is nothing to match (the end is the start).
    Fresh,
    /// The page differs or the tip lacks it.
    Mismatch,
    /// No frame to compare.
    Miss(Miss),
    /// The last page matches but the range before it is not what the tip staged.
    Rewritten,
}

fn frame_state(tip: &Tip<'_>, seen: &Seen) -> FrameState {
    let at_start = seen.end.frame_index() == 0;
    match tip {
        Tip::Boot(TipView::Readable { last_page, .. }) => match last_page {
            LastPageCheck::Match { .. } => FrameState::Fresh,
            LastPageCheck::Mismatch | LastPageCheck::NotInTip => FrameState::Mismatch,
            LastPageCheck::NoFrame(_) if at_start => FrameState::Fresh,
            LastPageCheck::NoFrame(miss) => FrameState::Miss(*miss),
            LastPageCheck::Rewritten => FrameState::Rewritten,
        },
        Tip::Boot(TipView::Unreadable { .. }) => FrameState::Fresh,
        Tip::Running(_, _) if at_start => FrameState::Fresh,
        Tip::Running(position, probes) => match probes.prev {
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
fn continued(tip: &Tip<'_>, wal: &WalView, at_committed_end: bool) -> Result<Position, Refusal> {
    let prior = match tip {
        Tip::Running(position, _) => Some(position.continuity()),
        Tip::Boot(_) => None,
    };
    let continuity = match prior {
        Some(Continuity::Pinned(epoch)) if epoch == wal.epoch => Continuity::Pinned(epoch),
        Some(_) | None if at_committed_end => Continuity::Pinned(wal.epoch),
        Some(_) | None => Continuity::Unpinned,
    };
    match tip {
        Tip::Running(position, _) => Ok(position.with_continuity(continuity)),
        Tip::Boot(TipView::Readable {
            txid,
            commit,
            timestamp,
            last_page: LastPageCheck::Match { last_page, cursor },
            ..
        }) => Ok(Position::new(
            *txid, *cursor, *commit, *last_page, *timestamp, continuity,
        )),
        // Rules 3 and 9 already refused every boot tip that is not a match.
        Tip::Boot(_) => Err(refuse(SnapshotCause::LastPageMismatch)),
    }
}

#[cfg(test)]
mod tests;
