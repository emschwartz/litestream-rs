//! The restore executor: a planned chain of LTX files becomes one database.
//!
//! [restore] loads the inventory, plans, then applies the chain's files oldest
//! first into a [DatabaseSink]. A fresh attempt may start past the base, at a file that holds
//! every page of the database (`start`): a file of the chain as large as the snapshot is read
//! first, kept as the start if its pages run from 1 to its commit with none missing, and
//! otherwise cut off at its first missing page and applied in its turn. A file is read in bounded chunks through one
//! decoder (the shared `LtxRead`); its header is checked before any of its pages
//! are written, and nothing counts until the decoder returns [lsr_ltx::Verified].
//! A sink takes a [PageRef], which is exactly one page of its page size, so
//! where a page lands follows from the page size and never from a slice length.
//! The sink is truncated to the file's commit after every file, so newest wins,
//! pages above the final commit are dropped, and a shrink followed by growth
//! leaves zeros, never an older file's pages. The sink is published only after
//! every file verified; any error leaves the target absent.
//!
//! The retries here are the planner's own races: a file the plan named may be
//! gone because compaction or retention ran in between ([StoreError::NotFound]),
//! or the listing planned a gap because it raced a writer (a directory read may
//! miss a file renamed into place and see a later one). The sink is reset, the
//! inventory reloaded and the target planned again, at most [MAX_ATTEMPTS] times;
//! a gap every attempt planned is refused as [RestoreError::NoPlan]. Every other store error ends the restore; retry
//! with backoff belongs to a store wrapper, and core holds no timer.
//!
//! A file gone before any of its bytes were read, after the files before it were applied, keeps
//! them: the sink holds exactly their verified database, so only the files after it are planned
//! again from a fresh listing (`plan_after`) and applied on top, linked to the last kept file as
//! any next file is. The writer's compaction replaces the files it deletes with files covering
//! the same transactions, and the snapshot is the one file of a large database that takes long
//! to read, so a restore whose small files are compacted while its snapshot downloads reads the
//! snapshot once (Go starts over from it, up to three times). The rest aims at the first plan's
//! target, an instant fixed to the transaction its plan reached; at most [MAX_TAIL_PLANS] times.
//! A file gone part-way leaves pages no verified file vouches for, so it starts over, as does a
//! rest that no longer reaches the target (retention removed it under a newer snapshot), on the
//! listing that showed it. [restore_noting] hears each [Replan] as it happens.
//!
//! The replica's store is required to list its own writes (docs/decisions/D173.md), so the
//! listing is the replica: no read is made past it. A restore of the newest state inside a
//! store's listing lag can give an earlier committed state, with success; a store whose
//! listings lag is unsupported.
//!
//! An `AsOf` target plans through [AsOfPlanner], which asks the store for the
//! exact created-at times it needs one at a time ([resolve]); a created-at time whose file is gone
//! counts as a vanished file. [Restored::created_at_read] reports how many were
//! read, over every attempt. `Latest` and `Txid` read no created-at time.
//!
//! An `AsOf` instant inside a compacted file is refused unless
//! [RestoreOptions::as_of_inside] is [AsOfInside::TakeEarlier]; then the plan's
//! chain is applied and [Restored::inside_file] names the file that straddled.
//!
//! The lock page is never written: [PageNo] cannot hold it, so the decoder
//! rejects a file that does, and a sink reads it as zeros.

use std::io;

use lsr_ltx::{
    DbChecksum, DbPages, DecodeError, Event, Header, PageNo, PageRef, PageSize, Timestamp, Txid,
    TxidRange, Verified,
};
use thiserror::Error;

use crate::created_at::{ResolveError, ResolvedCreatedAt, drive, resolve_counted};
use crate::inventory::{Inventory, InventoryError, Listing, Scope};
use crate::layout::{FileInfo, FileKey};
use crate::ltx_read::{BrokenLink, LinkEnd, LtxRead, Next, ReadFault, check_link};
use crate::plan::{
    AsOfPlanner, ContiguousChain, KeyTarget, PlanOutcome, RestoreTarget, plan, plan_after,
};
use crate::store::{ObjectStore, ReadLen, ReadStream, StoreError};

#[cfg(any(test, feature = "testing"))]
mod mem_sink;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;
mod window;

#[cfg(any(test, feature = "testing"))]
pub use mem_sink::{Database, MemPublishError, MemSink};

/// Plans attempted before the replica is called unstable.
pub const MAX_ATTEMPTS: u8 = 3;

/// Times a restore plans again only the files after the part it built
/// ([Replan::Tail]) before the replica is called unstable. Each costs a listing and the
/// small files after the kept part, never the snapshot, so the bound is wider than
/// [MAX_ATTEMPTS]; it still ends a restore whose files vanish faster than it can read them.
pub const MAX_TAIL_PLANS: u8 = 8;

/// Bytes asked of the store per read: memory per file stays one chunk plus one
/// page, whatever the file's size.
const CHUNK_BYTES: u32 = 4 * 1024 * 1024;

// `ReadLen::or_chunk` falls back to 1 MiB for a size it refuses, which would
// silently change the memory per file this constant states.
const _: () = assert!(CHUNK_BYTES >= 1 && CHUNK_BYTES <= ReadLen::MAX_RAW);

/// The fewest bytes one read of a [Stepped] restore asks for, however small a step's budget: a
/// read that asks for less only costs more requests on a store that cannot hold a stream.
const MIN_STEP_READ: u32 = 256 * 1024;

/// The most files one step of a [Stepped] restore opens: each costs a request or two whatever
/// its size, so a replica of many small files would otherwise make hundreds in one tick.
const FILES_PER_STEP: u32 = 16;

/// What a restore does when an `AsOf` instant falls inside a compacted file
/// ([PlanOutcome::AsOfInsideFile]).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum AsOfInside {
    /// Fail with [RestoreError::NoPlan]: the contract is the state at the
    /// instant or an error, never a shorter history.
    #[default]
    Refuse,
    /// Apply the plan's chain, the newest state provably before the instant,
    /// and name the straddling file in [Restored::inside_file]. Go always does
    /// this; here the caller asks for it. It covers `AsOfInsideFile` only: an
    /// as-of [PlanOutcome::Gap] stays a refusal.
    TakeEarlier,
}

/// How a restore treats the cases it can either refuse or accept.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RestoreOptions {
    /// What to do when the instant falls inside a compacted file.
    pub as_of_inside: AsOfInside,
}

/// What a restore was doing to its sink when the sink failed. A failed
/// publish is not one of these: it is [RestoreError::Publish], which says
/// what the failure left at the target.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum SinkOp {
    /// [DatabaseSink::write_page].
    WritePage,
    /// [DatabaseSink::truncate].
    Truncate,
    /// [DatabaseSink::reset].
    Reset,
    /// [DatabaseSink::checksum].
    Checksum,
}

impl std::fmt::Display for SinkOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WritePage => "write a page",
            Self::Truncate => "truncate",
            Self::Reset => "reset",
            Self::Checksum => "checksum",
        })
    }
}

/// What a failed [DatabaseSink::publish] left at the target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PublishLeft {
    /// Nothing: the target and anything beside it are as they were.
    Nothing,
    /// A complete, verified database is visible at the target, but the
    /// failed step means it may not survive a crash.
    VisibleNotDurable,
}

impl std::fmt::Display for PublishLeft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Nothing => "nothing was published",
            Self::VisibleNotDurable => "the database is visible but may not survive a crash",
        })
    }
}

/// A sink's publish error: whatever detail it carries, it says what it left
/// at the target.
pub trait PublishFailure: std::error::Error + Send + Sync + 'static {
    /// What the failed publish left at the target.
    fn left(&self) -> PublishLeft;
}

impl PublishFailure for std::convert::Infallible {
    fn left(&self) -> PublishLeft {
        match *self {}
    }
}

/// Where a restored database is built. Synchronous on purpose: one thread per
/// database drives a restore, and a sink is plain file I/O.
pub trait DatabaseSink {
    /// Writes one page at [PageRef::offset], (pgno - 1) times its page size.
    /// Writing past the end grows the database.
    ///
    /// # Errors
    /// Whatever the underlying write reports.
    fn write_page(&mut self, page: PageRef<'_>) -> io::Result<()>;

    /// Makes the length exactly pages times page size. Growth reads as zeros.
    ///
    /// # Errors
    /// Whatever the underlying resize reports.
    fn truncate(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<()>;

    /// Back to empty, for a restore that starts over.
    ///
    /// # Errors
    /// Whatever the underlying resize reports.
    fn reset(&mut self) -> io::Result<()>;

    /// The LTX rolling checksum of the database as the sink holds it now:
    /// the XOR of every page's checksum for pages 1 to [pages] except the lock
    /// page (which reads as zeros and is never counted, as in Go), flag bit
    /// set. The pages are read back from what was written, never taken from a
    /// running total, so a write that did not land is caught. A page the sink
    /// never wrote is a zero page. One page of memory at most.
    ///
    /// # Errors
    /// Whatever the underlying read reports.
    fn checksum(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<DbChecksum>;

    /// Why [Self::publish] failed, saying what it left at the target.
    type PublishError: PublishFailure;

    /// Makes the database durable and visible. A failed durability step is
    /// never retried.
    ///
    /// # Errors
    /// [Self::PublishError]. A failure does not mean nothing is visible: the
    /// last steps make the name durable after the database is in place, so a
    /// publish can fail with a complete, verified database visible at the
    /// target that may not survive a crash. The error's
    /// [PublishFailure::left] says which.
    fn publish(self) -> Result<(), Self::PublishError>;
}

/// Why a file in the chain cannot be applied.
#[derive(Debug, Error)]
pub enum FileFault {
    /// The bytes are not a valid LTX file: truncated, corrupt, or followed by
    /// more bytes.
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    /// The header's transaction range is not the one in the file's name.
    #[error("header covers {header}, not the range in its name")]
    RangeMismatch {
        /// What the header says.
        header: TxidRange,
    },
    /// The first file, or a level-9 file, is not a whole-database snapshot.
    #[error("not a snapshot")]
    NotASnapshot,
    /// A later file has a different page size from the first.
    #[error("page size {found:?} where the chain began with {expected:?}")]
    PageSizeChanged {
        /// The first file's page size.
        expected: PageSize,
        /// This file's.
        found: PageSize,
    },
    /// A file does not start from the database state the previous file ended in.
    #[error("pre-apply checksum {found} but the previous file ended at {expected}")]
    ChecksumChain {
        /// The previous file's post-apply checksum.
        expected: DbChecksum,
        /// This file's pre-apply checksum.
        found: DbChecksum,
    },
    /// The chain ends in a database of zero pages.
    #[error("the restored database has no pages")]
    EmptyDatabase,
}

/// Why a restore failed. Nothing is published, except after a
/// [Self::Publish] that says it left the database visible.
#[derive(Debug, Error)]
pub enum RestoreError {
    /// The replica could not be listed completely.
    #[error("listing the replica: {0}")]
    Inventory(#[source] InventoryError),
    /// The replica has no exact answer for the target.
    #[error("no exact restore plan: {:?}", .0.outcome())]
    NoPlan(NoPlan),
    /// A read failed in a way a restore does not retry.
    #[error("reading the replica: {0}")]
    Store(#[source] StoreError),
    /// A file the plan chose is unusable.
    #[error("{key}: {fault}")]
    File {
        /// The file.
        key: FileKey,
        /// What is wrong with it.
        fault: FileFault,
    },
    /// Files kept vanishing between listing and reading.
    #[error("{missing} vanished on each of {attempts} attempts; the replica is being compacted")]
    Unstable {
        /// Plans tried.
        attempts: u8,
        /// The file missing on the last attempt.
        missing: FileKey,
    },
    /// Every file verified, but the database they built is not the one the
    /// last file's trailer says it is: the chain does not add up (overlapping
    /// files from diverged writers, or a compactor bug).
    #[error("the restored database has checksum {found}, the last file says {expected}")]
    DatabaseChecksum {
        /// The last file's post-apply checksum.
        expected: DbChecksum,
        /// What the sink holds.
        found: DbChecksum,
    },
    /// The as-of planner asked twice for one created-at time: a planner bug, refused
    /// rather than looped on.
    #[error("the as-of planner asked twice for the created-at time of {key}")]
    CreatedAtRepeated {
        /// The file asked for twice.
        key: FileKey,
    },
    /// The destination failed before it was asked to publish.
    #[error("sink failed to {op}: {source}")]
    Sink {
        /// What the restore was doing.
        op: SinkOp,
        /// The I/O error.
        source: io::Error,
    },
    /// Every file verified and the database added up, but the sink's publish
    /// failed.
    #[error("publish failed, {left}: {source}")]
    Publish {
        /// What the failure left at the target.
        left: PublishLeft,
        /// The sink's own error ([DatabaseSink::PublishError]), for a caller
        /// that knows the sink and wants its detail.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// What a restore did when a file it planned was gone, reported as it happens
/// ([restore_noting]) so a long restore says why it is reading again.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Replan {
    /// `missing` was gone before any of it was read, with the database built through
    /// transaction `kept`: the database is kept and only the files after it are planned again,
    /// from a fresh listing.
    Tail {
        /// The file that was gone.
        missing: FileKey,
        /// The last transaction the kept database holds.
        kept: Txid,
    },
    /// `missing` was gone with nothing to keep (it was the first file, or part of it was
    /// already written): the database is emptied and the restore starts over.
    Restart {
        /// The file that was gone.
        missing: FileKey,
    },
    /// After `missing` was gone, the files past transaction `kept` no longer lead to the
    /// target (`outcome`), as when retention removed them under a newer snapshot: the database
    /// is emptied and the restore starts over on the same listing.
    NoTail {
        /// The file that was gone.
        missing: FileKey,
        /// The last transaction the database held.
        kept: Txid,
        /// Why the files after it are no plan.
        outcome: PlanOutcome,
    },
}

/// A [PlanOutcome] that is not a plan: why the replica has no exact answer.
/// Only [restore] builds one, so "no plan: here is a plan" cannot exist.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NoPlan(PlanOutcome);

impl NoPlan {
    /// The reason; never [PlanOutcome::Plan].
    #[must_use]
    pub const fn outcome(&self) -> &PlanOutcome {
        &self.0
    }
}

/// What a successful restore did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Restored {
    reached: Txid,
    page_size: PageSize,
    commit: DbPages,
    files: usize,
    bytes_read: u64,
    attempts: u8,
    created_at_read: u64,
    post_apply: Option<DbChecksum>,
    inside_file: Option<FileKey>,
    written: Timestamp,
}

impl Restored {
    /// The last transaction the database holds.
    #[must_use]
    pub const fn reached(&self) -> Txid {
        self.reached
    }

    /// The database's page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// When the last file applied was written, by its header: the newest transaction's time.
    /// A compacted file carries the time of the newest file it merged.
    #[must_use]
    pub const fn written(&self) -> Timestamp {
        self.written
    }

    /// The database's length in pages.
    #[must_use]
    pub const fn commit(&self) -> DbPages {
        self.commit
    }

    /// Files in the chain that built the database, the ones kept from an earlier attempt
    /// included; not the files before a whole-database copy the restore started from, which
    /// it never read.
    #[must_use]
    pub const fn files(&self) -> usize {
        self.files
    }

    /// Bytes read from the store, summed over every attempt.
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Plans tried, 1 when nothing vanished.
    #[must_use]
    pub const fn attempts(&self) -> u8 {
        self.attempts
    }

    /// Exact created-at times asked of the store while planning, summed over every
    /// attempt; 0 for a target other than `AsOf` and for a listing that
    /// carries exact created-at times.
    #[must_use]
    pub const fn created_at_read(&self) -> u64 {
        self.created_at_read
    }

    /// The database checksum after the last file, when the writer tracked it.
    #[must_use]
    pub const fn post_apply(&self) -> Option<DbChecksum> {
        self.post_apply
    }

    /// The file that straddled the instant when [AsOfInside::TakeEarlier]
    /// accepted it: the database then holds the state before that file, which
    /// may leave out transactions committed before the instant. `None` for
    /// every restore that met its target exactly.
    #[must_use]
    pub const fn inside_file(&self) -> Option<FileKey> {
        self.inside_file
    }
}

/// How one attempt ended without a database.
enum Abort {
    /// A planned file was gone: plan again.
    Vanished(FileKey),
    /// A planned file was gone before any of it was read, after the files before it were
    /// applied: the sink holds exactly their database, so only the rest is planned again.
    Kept(Box<Resume>),
    /// The files after a kept part no longer reach the target: start over on its listing.
    NoTail(Box<TailGone>),
    /// The listing planned a gap. A listing is not a snapshot of the store: a directory
    /// read that races a writer's rename may miss a file and see a later one. Plan again
    /// on a fresh listing; a gap every attempt shows is the replica's.
    Gap(PlanOutcome),
    Fatal(RestoreError),
}

impl From<RestoreError> for Abort {
    fn from(err: RestoreError) -> Self {
        Self::Fatal(err)
    }
}

/// The file applied last, which fixes the page size and checksum for the next.
type Previous<'a> = Option<&'a Verified>;

/// A restore cut short after its first files, with what it needs to plan the rest.
struct Resume {
    /// The files applied: the sink holds the database they built and nothing else.
    applied: ContiguousChain,
    /// The last of them, which the next file must link to.
    last: Verified,
    /// How many of them were never read ([`Applied::skipped`]).
    skipped: usize,
    /// The file that was gone.
    missing: FileKey,
    /// What the rest is planned toward: the first plan's target, an instant fixed to the
    /// transaction its plan reached, so the answer stays the state the instant was given.
    target: KeyTarget,
    /// The first plan's straddling file, when it was accepted.
    inside_file: Option<FileKey>,
}

/// Why the files after a kept part are no plan, and the listing that showed it.
struct TailGone {
    missing: FileKey,
    kept: Txid,
    outcome: PlanOutcome,
    listing: Inventory,
}

/// An attempt that built the whole database.
struct Applied {
    chain: ContiguousChain,
    last: Verified,
    inside_file: Option<FileKey>,
    /// The chain's files before the whole-database copy it started from, never read.
    skipped: usize,
}

fn sink_fault(op: SinkOp) -> impl FnOnce(io::Error) -> RestoreError {
    move |source| RestoreError::Sink { op, source }
}

const fn chunk_len() -> ReadLen {
    ReadLen::or_chunk(CHUNK_BYTES)
}

/// Rebuilds the database [target] names from [store] into [sink] and publishes it.
///
/// # Errors
/// [RestoreError]; the sink is dropped unpublished in every case but a
/// [RestoreError::Publish] that left the database visible.
pub async fn restore<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    target: RestoreTarget,
    sink: K,
) -> Result<Restored, RestoreError> {
    restore_with(store, target, RestoreOptions::default(), sink).await
}

/// [restore] with [options] for the cases it can either refuse or accept.
///
/// # Errors
/// As [restore].
pub async fn restore_with<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    target: RestoreTarget,
    options: RestoreOptions,
    sink: K,
) -> Result<Restored, RestoreError> {
    restore_noting(store, target, options, sink, |_| {}).await
}

/// [restore_with], calling `note` each time a planned file is gone and the restore plans again,
/// before it reads on, so a caller can say why a long restore is reading again.
///
/// # Errors
/// As [restore].
pub async fn restore_noting<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    target: RestoreTarget,
    options: RestoreOptions,
    sink: K,
    mut note: impl FnMut(&Replan) + Send,
) -> Result<Restored, RestoreError> {
    restore_within(store, None, target, options, sink, MAX_ATTEMPTS, &mut note).await
}

/// [restore], with each reason to start over allowed `limit` attempts and [MAX_TAIL_PLANS] plans of
/// the rest after a kept part. Its first attempt plans over the borrowed `listed` when given
/// instead of listing again. A retry after a vanished file always lists afresh. A caller that
/// hands a listing is checking those files. `note` hears each [Replan].
pub(crate) async fn restore_within<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    mut listed: Option<&Inventory>,
    target: RestoreTarget,
    options: RestoreOptions,
    mut sink: K,
    limit: u8,
    note: &mut (dyn FnMut(&Replan) + Send),
) -> Result<Restored, RestoreError> {
    let mut attempt = 1_u8;
    let mut bytes_read = 0_u64;
    let mut created_at_read = 0_u64;
    let mut retries = Retries::new(limit);
    let mut resume: Option<Box<Resume>> = None;
    let mut relisted: Option<Inventory> = None;
    loop {
        let fresh = relisted.take();
        let given = fresh.as_ref().or(listed.take());
        let applied = match resume.take() {
            Some(resume) => attempt_tail(store, *resume, &mut sink, &mut bytes_read).await,
            None => {
                attempt_once(
                    store,
                    given,
                    target,
                    options,
                    &mut sink,
                    &mut bytes_read,
                    &mut created_at_read,
                )
                .await
            }
        };
        match applied {
            Ok(Applied {
                chain,
                last,
                inside_file,
                skipped,
            }) => {
                verify_end_state(&mut sink, &last)?;
                sink.publish().map_err(|err| RestoreError::Publish {
                    left: err.left(),
                    source: Box::new(err),
                })?;
                return Ok(Restored {
                    reached: chain.reaches(),
                    page_size: last.header().page_size(),
                    commit: last.header().pages(),
                    files: chain.files().len().saturating_sub(skipped),
                    bytes_read,
                    attempts: attempt,
                    created_at_read,
                    post_apply: last.trailer().post_apply(),
                    inside_file,
                    written: last.header().fields().timestamp,
                });
            }
            Err(Abort::Fatal(err)) => return Err(err),
            Err(Abort::Kept(kept)) => {
                retries.tail(kept.missing, attempt)?;
                note(&Replan::Tail {
                    missing: kept.missing,
                    kept: kept.applied.reaches(),
                });
                resume = Some(kept);
            }
            Err(Abort::NoTail(gone)) => {
                let TailGone {
                    missing,
                    kept,
                    outcome,
                    listing,
                } = *gone;
                retries.vanished(missing, attempt)?;
                note(&Replan::NoTail {
                    missing,
                    kept,
                    outcome,
                });
                sink.reset().map_err(sink_fault(SinkOp::Reset))?;
                relisted = Some(listing);
            }
            Err(Abort::Vanished(missing)) => {
                retries.vanished(missing, attempt)?;
                note(&Replan::Restart { missing });
                sink.reset().map_err(sink_fault(SinkOp::Reset))?;
            }
            // A gap stops the plan before the first file: nothing to reset.
            Err(Abort::Gap(outcome)) => retries.gap(outcome)?,
        }
        attempt = attempt.saturating_add(1);
    }
}

/// Why a restore listed again, counted toward the limits on each reason.
struct Retries {
    /// Starts over each reason is allowed: [MAX_ATTEMPTS].
    limit: u8,
    /// Starts over after a planned file was gone.
    vanished: u8,
    /// Attempts whose listing planned a gap.
    gaps: u8,
    /// Plans of the rest after a kept part.
    tails: u8,
}

impl Retries {
    const fn new(limit: u8) -> Self {
        Self {
            limit,
            vanished: 0,
            gaps: 0,
            tails: 0,
        }
    }

    /// Counts one start over after `missing` was gone in attempt `attempt`, or fails when
    /// they are exhausted.
    fn vanished(&mut self, missing: FileKey, attempt: u8) -> Result<(), RestoreError> {
        self.vanished = self.vanished.saturating_add(1);
        exhausted(self.vanished, self.limit, missing, attempt)
    }

    /// Counts one plan of the rest after `missing` was gone in attempt `attempt`.
    fn tail(&mut self, missing: FileKey, attempt: u8) -> Result<(), RestoreError> {
        self.tails = self.tails.saturating_add(1);
        exhausted(self.tails, MAX_TAIL_PLANS, missing, attempt)
    }

    /// Counts one listing that planned a gap.
    fn gap(&mut self, outcome: PlanOutcome) -> Result<(), RestoreError> {
        self.gaps = self.gaps.saturating_add(1);
        if self.gaps >= self.limit {
            return Err(RestoreError::NoPlan(NoPlan(outcome)));
        }
        Ok(())
    }
}

/// [RestoreError::Unstable] once `count` vanished files reach `limit`.
fn exhausted(count: u8, limit: u8, missing: FileKey, attempt: u8) -> Result<(), RestoreError> {
    if count >= limit {
        return Err(RestoreError::Unstable {
            attempts: attempt,
            missing,
        });
    }
    Ok(())
}

/// One listing, one plan and the chain applied, or why not.
async fn attempt_once<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    given: Option<&Inventory>,
    target: RestoreTarget,
    options: RestoreOptions,
    sink: &mut K,
    bytes_read: &mut u64,
    created_at_read: &mut u64,
) -> Result<Applied, Abort> {
    let planned = match (given, target) {
        (Some(inventory), _) => plan_target(store, inventory, target, created_at_read).await?,
        (None, RestoreTarget::AsOf(instant)) => {
            plan_as_of_listing(store, instant, created_at_read).await?
        }
        (None, RestoreTarget::Latest | RestoreTarget::Txid(_)) => {
            let inventory = Inventory::load(store)
                .await
                .map_err(RestoreError::Inventory)?;
            plan_target(store, &inventory, target, created_at_read).await?
        }
    };
    let (chain, inside_file) = match planned.into_chain() {
        Ok(chain) => (chain, None),
        Err(PlanOutcome::AsOfInsideFile { chain, file })
            if options.as_of_inside == AsOfInside::TakeEarlier =>
        {
            (chain, Some(file))
        }
        Err(gap @ PlanOutcome::Gap { .. }) => return Err(Abort::Gap(gap)),
        Err(other) => return Err(RestoreError::NoPlan(NoPlan(other)).into()),
    };
    let rest = match target {
        RestoreTarget::Latest => KeyTarget::Latest,
        RestoreTarget::Txid(txid) => KeyTarget::Txid(txid),
        RestoreTarget::AsOf(_) => KeyTarget::Txid(chain.reaches()),
    };
    apply_chain(store, chain, None, rest, inside_file, sink, bytes_read).await
}

/// The rest of a restore cut short after a kept part: a fresh listing, a plan of the files after
/// the part, and those files applied on top of it.
async fn attempt_tail<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    resume: Resume,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Applied, Abort> {
    let listing = Inventory::load(store)
        .await
        .map_err(RestoreError::Inventory)?;
    let Resume {
        applied,
        last,
        skipped,
        missing,
        target,
        inside_file,
    } = resume;
    let kept = applied.reaches();
    let done = applied.files().len();
    let chain = match plan_after(&listing, applied, target).into_chain() {
        Ok(chain) => chain,
        Err(outcome) => {
            return Err(Abort::NoTail(Box::new(TailGone {
                missing,
                kept,
                outcome,
                listing,
            })));
        }
    };
    apply_chain(
        store,
        chain,
        Some((done, skipped, last)),
        target,
        inside_file,
        sink,
        bytes_read,
    )
    .await
}

/// The plan for [target]: an instant asks for the created-at times it needs, counted
/// into [created_at_read] whether or not the call succeeded.
async fn plan_target<S: ObjectStore>(
    store: &S,
    inventory: &Inventory,
    target: RestoreTarget,
    created_at_read: &mut u64,
) -> Result<PlanOutcome, Abort> {
    let instant = match target {
        RestoreTarget::Latest => return Ok(plan(inventory, KeyTarget::Latest)),
        RestoreTarget::Txid(txid) => return Ok(plan(inventory, KeyTarget::Txid(txid))),
        RestoreTarget::AsOf(instant) => instant,
    };
    let mut planner = AsOfPlanner::new(inventory, instant);
    resolve_counted(store, &mut planner, created_at_read)
        .await
        .map_err(resolve_abort)
}

/// How a failed created-at read ends an attempt.
fn resolve_abort(err: ResolveError) -> Abort {
    match err {
        ResolveError::Store(StoreError::NotFound { key }) => Abort::Vanished(key),
        ResolveError::Store(err) => RestoreError::Store(err).into(),
        ResolveError::Repeated { key } => RestoreError::CreatedAtRepeated { key }.into(),
    }
}

/// The plan for an instant, listing each level only as far as the chain it finds needs, as
/// Go's per-level cursors do: a replica with years of files and an instant an hour into it
/// lists a few pages a level, not every page. A plan is the same as over the whole listing,
/// because a file past a level's first file that starts beyond the chain's end cannot
/// touch it. A refusal is re-planned over the whole listing, so its words describe the
/// replica and not a prefix. Created-at times are read once across the re-plans.
async fn plan_as_of_listing<S: ObjectStore>(
    store: &S,
    instant: Timestamp,
    created_at_read: &mut u64,
) -> Result<PlanOutcome, Abort> {
    let mut listing = Listing::begin(store)
        .await
        .map_err(RestoreError::Inventory)?;
    let mut resolved = ResolvedCreatedAt::default();
    loop {
        let inventory = listing.inventory().map_err(RestoreError::Inventory)?;
        let mut planner = AsOfPlanner::new(&inventory, instant);
        let outcome = drive(store, &mut planner, &mut resolved, created_at_read)
            .await
            .map_err(resolve_abort)?;
        let scope = match outcome.reaches() {
            Some(reach) if listing.is_past(reach) => return Ok(outcome),
            Some(reach) => Scope::Past(reach),
            None if listing.is_complete() => return Ok(outcome),
            None => Scope::All,
        };
        listing
            .advance(store, scope)
            .await
            .map_err(RestoreError::Inventory)?;
    }
}

/// Per-file checks cannot see an overlapping file, or a pair with an untracked
/// side, whose pages do not reach its own post-apply checksum; the database
/// itself can. Untracked chains have nothing to compare.
fn verify_end_state<K: DatabaseSink>(sink: &mut K, last: &Verified) -> Result<(), RestoreError> {
    let Some(expected) = last.trailer().post_apply() else {
        return Ok(());
    };
    let found = sink
        .checksum(last.header().pages(), last.header().page_size())
        .map_err(sink_fault(SinkOp::Checksum))?;
    if found == expected {
        Ok(())
    } else {
        Err(RestoreError::DatabaseChecksum { expected, found })
    }
}

/// Applies `chain` into `sink`, from its start ([`start`]), or after the `kept` files already
/// settled (how many, how many of those were skipped, and the last applied). A file gone before
/// any of it was read, after the first, ends the attempt with the files before it kept
/// ([Abort::Kept]), planned on toward `rest`.
async fn apply_chain<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    chain: ContiguousChain,
    kept: Option<(usize, usize, Verified)>,
    rest: KeyTarget,
    inside_file: Option<FileKey>,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Applied, Abort> {
    let (done, skipped, mut last) = match kept {
        Some(kept) => kept,
        None => start(store, &chain, sink, bytes_read).await?,
    };
    for (index, info) in chain.files().iter().enumerate().skip(done) {
        let before = *bytes_read;
        match apply_file(store, info, Some(&last), sink, bytes_read).await {
            Ok(verified) => last = verified,
            // Nothing of the missing file reached the sink, which holds exactly the files
            // before it; a file gone part-way leaves pages no file vouches for.
            Err(Abort::Vanished(missing)) if *bytes_read == before => {
                let Some(applied) = chain.first(index) else {
                    return Err(Abort::Vanished(missing));
                };
                return Err(Abort::Kept(Box::new(Resume {
                    applied,
                    last,
                    skipped,
                    missing,
                    target: rest,
                    inside_file,
                })));
            }
            Err(abort) => return Err(abort),
        }
    }
    let key = chain.files().last().map_or(chain.snapshot().key, |f| f.key);
    not_empty(key, &last)?;
    Ok(Applied {
        chain,
        last,
        inside_file,
        skipped,
    })
}

/// The first file of a fresh attempt: the newest file that may hold the whole database
/// ([`ContiguousChain::whole_candidate`]) when it proves to, else the base. Such a file is the
/// whole database at its transaction, so the files before it, the base included, add nothing.
/// Go reads them all (its restore plan always starts from the snapshot, `replica.go`
/// `CalcRestorePlan`); here a database restored past a copy the writer took when the WAL did not
/// continue the replica reads it once, not twice. Returns how many files are settled, how many
/// of them were skipped, and the last applied.
async fn start<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    chain: &ContiguousChain,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<(usize, usize, Verified), Abort> {
    let candidate = chain
        .whole_candidate()
        .and_then(|index| Some((index, chain.files().get(index)?)));
    if let Some((index, info)) = candidate {
        if let Some(verified) = apply_whole(store, info, sink, bytes_read).await? {
            return Ok((index.saturating_add(1), index, verified));
        }
        sink.reset().map_err(sink_fault(SinkOp::Reset))?;
    }
    let base = apply_file(store, chain.snapshot(), None, sink, bytes_read).await?;
    Ok((1, 0, base))
}

/// Reads `info` as the start of the database. It is kept (`Some`) only when its pages are
/// every page from 1 to its commit, so that no earlier file can add to it. The first page out
/// of that sequence ends the read (`None`), at the cost of the bytes up to it and one read more
/// than Go makes; the size gate in [`ContiguousChain::whole_candidate`] keeps that rare.
async fn apply_whole<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    info: &FileInfo,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Option<Verified>, Abort> {
    let key = info.key;
    let mut pages = WholePages::default();
    let mut read = LtxRead::new(key, chunk_len());
    let applied = read
        .read_verified_from(&mut *window::stream(store, info), |event| match event {
            Event::Header(header) if header.txids() != key.range() => {
                Err(Some(RestoreError::File {
                    key,
                    fault: FileFault::RangeMismatch {
                        header: header.txids(),
                    },
                }))
            }
            Event::Header(_) => Ok(()),
            Event::Page(page) if pages.take(page.pgno(), page.size()) => sink
                .write_page(page)
                .map_err(|err| Some(sink_fault(SinkOp::WritePage)(err))),
            Event::Page(_) => Err(None),
        })
        .await;
    *bytes_read = bytes_read.saturating_add(read.bytes_read());
    let verified = match applied {
        Ok(verified) => verified,
        Err(ReadFault::Rejected(None)) => return Ok(None),
        Err(ReadFault::Rejected(Some(err))) => return Err(Abort::Fatal(err)),
        Err(ReadFault::Store(err)) => return Err(read_abort(key, ReadFault::Store(err))),
        Err(ReadFault::Decode(err)) => return Err(read_abort(key, ReadFault::Decode(err))),
    };
    let header = verified.header();
    if !pages.reach(header.pages(), header.page_size()) {
        return Ok(None);
    }
    end_file(sink, &verified)?;
    Ok(Some(verified))
}

/// The pages of one file read so far, while they are 1, 2, 3 and on with none missing (the
/// lock page, which no file holds, aside).
#[derive(Default, Debug)]
struct WholePages {
    last: Option<PageNo>,
}

impl WholePages {
    /// Takes the next page; false when it is not the one after the last taken.
    fn take(&mut self, pgno: PageNo, size: PageSize) -> bool {
        let after = self.last.map_or(0, PageNo::get);
        let next = PageNo::new(after.saturating_add(1), size)
            .or_else(|| PageNo::new(after.saturating_add(2), size));
        let fits = next == Some(pgno);
        if fits {
            self.last = Some(pgno);
        }
        fits
    }

    /// Whether the pages taken are every page of a database of `pages`.
    fn reach(&self, pages: DbPages, size: PageSize) -> bool {
        self.last == pages.last_page(size)
    }
}

/// A chain whose last file, `key`, leaves no database is refused.
fn not_empty(key: FileKey, last: &Verified) -> Result<(), RestoreError> {
    if last.header().commit() == 0 {
        return Err(RestoreError::File {
            key,
            fault: FileFault::EmptyDatabase,
        });
    }
    Ok(())
}

/// What a failed read of `key` means for the attempt.
fn read_abort(key: FileKey, fault: ReadFault<RestoreError>) -> Abort {
    match fault {
        ReadFault::Store(StoreError::NotFound { .. }) => Abort::Vanished(key),
        ReadFault::Store(err) => Abort::Fatal(RestoreError::Store(err)),
        ReadFault::Decode(err) => Abort::Fatal(RestoreError::File {
            key,
            fault: FileFault::Decode(err),
        }),
        ReadFault::Rejected(err) => Abort::Fatal(err),
    }
}

async fn apply_file<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    info: &FileInfo,
    previous: Previous<'_>,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Verified, Abort> {
    let key = info.key;
    let mut read = LtxRead::new(key, chunk_len());
    let applied = read
        .read_verified_from(&mut *window::stream(store, info), |event| {
            on_event(key, previous, sink, event)
        })
        .await;
    *bytes_read = bytes_read.saturating_add(read.bytes_read());
    let verified = applied.map_err(|fault| read_abort(key, fault))?;
    end_file(sink, &verified)?;
    Ok(verified)
}

/// One decoder event of file `key` into `sink`: the header checked against the file before
/// it, a page written.
fn on_event<K: DatabaseSink>(
    key: FileKey,
    previous: Previous<'_>,
    sink: &mut K,
    event: Event<'_>,
) -> Result<(), RestoreError> {
    match event {
        Event::Header(header) => {
            check_header(key, &header, previous).map_err(|fault| RestoreError::File { key, fault })
        }
        Event::Page(page) => sink.write_page(page).map_err(sink_fault(SinkOp::WritePage)),
    }
}

/// A file read to its end: the sink cut to the size the file's header gives.
fn end_file<K: DatabaseSink>(sink: &mut K, verified: &Verified) -> Result<(), RestoreError> {
    let header = verified.header();
    sink.truncate(header.pages(), header.page_size())
        .map_err(sink_fault(SinkOp::Truncate))
}

/// A restore to transaction `tip` read a budget of bytes at a time, for a caller that gives its
/// tick back between reads (the boot's identity check, `crate::identity`): [restore]'s
/// rules, files applied oldest first through the same checks, with only the place it stops
/// between reads kept. A file gone, a plan with a gap, or any other fault ends it, as one
/// attempt of [restore] ends; the caller begins again on a fresh listing. Nothing is
/// published. A file is one stream held between steps where the store can hold one
/// ([ObjectStore::hold_stream]).
pub(crate) struct Stepped<K> {
    /// The files after the one being read.
    rest: std::vec::IntoIter<FileInfo>,
    read: LtxRead,
    held: Option<Box<dyn ReadStream>>,
    previous: Option<Verified>,
    sink: K,
    /// Bytes one read asks for: a step's budget, within [MIN_STEP_READ] and [CHUNK_BYTES].
    len: ReadLen,
}

impl<K> std::fmt::Debug for Stepped<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stepped")
            .field("key", &self.read.key())
            .field("rest", &self.rest.len())
            .field("read", &self.read.bytes_read())
            .finish_non_exhaustive()
    }
}

/// Where a step of a [Stepped] restore left it.
#[derive(Debug)]
pub(crate) enum Stepping<K> {
    /// The budget is spent and files are left.
    Going(Box<Stepped<K>>),
    /// Every file verified: the sink holds the database, and this is the last file.
    Done {
        /// The database.
        sink: K,
        /// The last file applied.
        last: Verified,
    },
}

impl<K: DatabaseSink> Stepped<K> {
    /// Lists `store` and plans the restore to `tip` into `sink`; nothing is read yet.
    ///
    /// # Errors
    /// [RestoreError] as one attempt of [restore] gives it.
    pub(crate) async fn begin<S: ObjectStore>(
        store: &S,
        tip: Txid,
        sink: K,
        budget: u64,
    ) -> Result<Self, RestoreError> {
        let inventory = Inventory::load(store)
            .await
            .map_err(RestoreError::Inventory)?;
        let chain = plan(&inventory, KeyTarget::Txid(tip))
            .into_chain()
            .map_err(|outcome| RestoreError::NoPlan(NoPlan(outcome)))?;
        let len = u32::try_from(budget.clamp(u64::from(MIN_STEP_READ), u64::from(CHUNK_BYTES)))
            .ok()
            .and_then(ReadLen::new)
            .unwrap_or_else(chunk_len);
        let key = chain.snapshot().key;
        let held = store
            .hold_stream(key, 0)
            .await
            .map_err(|err| once(read_abort(key, ReadFault::Store(err))))?;
        Ok(Self {
            rest: chain
                .files()
                .iter()
                .skip(1)
                .copied()
                .collect::<Vec<_>>()
                .into_iter(),
            read: LtxRead::new(key, len),
            held,
            previous: None,
            sink,
            len,
        })
    }

    /// Reads on until `budget` bytes were read in this step, or it opened [FILES_PER_STEP] files.
    ///
    /// # Errors
    /// [RestoreError] as one attempt of [restore] gives it.
    pub(crate) async fn step<S: ObjectStore>(
        mut self: Box<Self>,
        store: &S,
        budget: u64,
    ) -> Result<Stepping<K>, RestoreError> {
        let (mut spent, mut opened) = (0_u64, 0_u32);
        let mut slot: Option<Box<dyn ReadStream + '_>> = None;
        while spent < budget && opened < FILES_PER_STEP {
            let key = self.read.key();
            let before = self.read.bytes_read();
            let stream: &mut (dyn ReadStream + '_) = match self.held.as_mut() {
                Some(held) => &mut **held,
                None => &mut **slot.get_or_insert_with(|| store.read_stream(key, before)),
            };
            let (previous, sink) = (self.previous.as_ref(), &mut self.sink);
            let next = self
                .read
                .next(stream, |event| on_event(key, previous, sink, event))
                .await;
            spent = spent.saturating_add(self.read.bytes_read().saturating_sub(before));
            let verified = match next {
                Ok(Next::Event(())) => continue,
                Ok(Next::End(verified)) => verified,
                Err(fault) => return Err(once(read_abort(key, fault))),
            };
            end_file(&mut self.sink, &verified)?;
            let Some(info) = self.rest.next() else {
                not_empty(key, &verified)?;
                verify_end_state(&mut self.sink, &verified)?;
                return Ok(Stepping::Done {
                    sink: self.sink,
                    last: verified,
                });
            };
            self.previous = Some(verified);
            self.read = LtxRead::new(info.key, self.len);
            slot = None;
            opened = opened.saturating_add(1);
            self.held = store
                .hold_stream(info.key, 0)
                .await
                .map_err(|err| once(read_abort(info.key, ReadFault::Store(err))))?;
        }
        Ok(Stepping::Going(self))
    }
}

/// What an attempt's abort is for a caller that makes one attempt: a file gone is a replica
/// that changed under it, as [restore] reports it.
fn once(abort: Abort) -> RestoreError {
    match abort {
        Abort::Fatal(err) => err,
        Abort::Vanished(missing) => RestoreError::Unstable {
            attempts: 1,
            missing,
        },
        Abort::NoTail(gone) => RestoreError::Unstable {
            attempts: 1,
            missing: gone.missing,
        },
        Abort::Kept(resume) => RestoreError::Unstable {
            attempts: 1,
            missing: resume.missing,
        },
        Abort::Gap(outcome) => RestoreError::NoPlan(NoPlan(outcome)),
    }
}

/// Every check that must pass before the first page of a file is written.
/// Snapshot-ness comes first so a level-9 file whose header covers other
/// transactions is reported as the wrong kind of file, not a wrong name.
fn check_header(key: FileKey, header: &Header, previous: Previous<'_>) -> Result<(), FileFault> {
    if (previous.is_none() || key.level().is_snapshot()) && !header.is_snapshot() {
        return Err(FileFault::NotASnapshot);
    }
    if header.txids() != key.range() {
        return Err(FileFault::RangeMismatch {
            header: header.txids(),
        });
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    let expected = previous.header().page_size();
    if expected != header.page_size() {
        return Err(FileFault::PageSizeChanged {
            expected,
            found: header.page_size(),
        });
    }
    check_link(LinkEnd::of(previous), header)
        .map_err(|BrokenLink { expected, found }| FileFault::ChecksumChain { expected, found })
}
