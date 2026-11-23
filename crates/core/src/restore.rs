//! The restore executor: a planned chain of LTX files becomes one database.
//!
//! [restore] loads the inventory, plans, then applies the chain's files oldest
//! first into a [DatabaseSink]. A file is read in bounded chunks through one
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
//! A listing is not trusted to be whole for the newest state. Before a `Latest` or `AsOf`
//! target's chain is applied, the restore reads one byte of each key a reader can name without
//! a listing for the transaction after the listing's newest (`crate::probe`, the reads the
//! engine's uploader makes past its own listings): that transaction's level-0 file, a level-0
//! file from the first transaction, and the snapshot ending at it (after planning, so an
//! `AsOf` target's created-at reads come first). A store whose listings lag its writes
//! would otherwise give an older state as the newest, with success. A key found that way is
//! listed for again. A file found that way is either hidden by a lagging listing or put by a
//! replicator after the listing, and one read cannot tell; the next listing can. A listing
//! that shows the file found before it has caught up, and that round is the writer's progress:
//! the restore follows it, up to [MAX_CHASE] listings, and then takes the next listing as it
//! stands ([Restored::writer_outpaced]): restoring the latest state while a writer runs may
//! answer any committed state, and one at least as new as every file a read found during the
//! restore is a sound answer, where failing would mean no restore ever finishes against a busy
//! writer. A listing that still ends below a file found before it began lags, and after
//! [MAX_ATTEMPTS] such listings the restore fails with [RestoreError::ListingBehind]. A `Txid` target needs no such read: its answer is fixed by
//! the files that end at or below it, and a hole the lag leaves is a gap the planner refuses.
//! An empty listing is read past for every target, at the first transaction's key: a replica
//! that exists must never look like none (the command line's `--if-replica-exists` skips a
//! restore, with success, only for an empty replica).
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
    DbChecksum, DbPages, DecodeError, Event, Header, PageRef, PageSize, Txid, TxidRange, Verified,
};
use thiserror::Error;

use crate::created_at::{ResolveError, resolve_counted};
use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey};
use crate::ltx_read::{BrokenLink, LinkEnd, LtxRead, ReadFault, check_link};
use crate::plan::{AsOfPlanner, ContiguousChain, KeyTarget, PlanOutcome, RestoreTarget, plan};
use crate::store::{ObjectStore, ReadLen, StoreError};

#[cfg(any(test, feature = "testing"))]
mod mem_sink;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;

#[cfg(any(test, feature = "testing"))]
pub use mem_sink::{Database, MemPublishError, MemSink};

/// Plans attempted before the replica is called unstable.
pub const MAX_ATTEMPTS: u8 = 3;

/// Bytes asked of the store per read: memory per file stays one chunk plus one
/// page, whatever the file's size.
const CHUNK_BYTES: u32 = 4 * 1024 * 1024;

// `ReadLen::or_chunk` falls back to 1 MiB for a size it refuses, which would
// silently change the memory per file this constant states.
const _: () = assert!(CHUNK_BYTES >= 1 && CHUNK_BYTES <= ReadLen::MAX_RAW);

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
    /// Three listings did not show a level-0 file a read had found before they began: the
    /// store's listings lag its writes, so the newest state it lists is not the newest it
    /// holds.
    #[error("{found} exists past the listed newest after {attempts} listings; the listing lags")]
    ListingBehind {
        /// Listings tried.
        attempts: u8,
        /// The file the last read found past the listing.
        found: FileKey,
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
    writer_outpaced: bool,
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

    /// The database's length in pages.
    #[must_use]
    pub const fn commit(&self) -> DbPages {
        self.commit
    }

    /// Files applied in the successful attempt.
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

    /// Whether a writer kept putting a new file after each listing for [MAX_CHASE]
    /// listings, and the restore then took the next listing as it stood: every listing
    /// had shown the file found before it, so none lagged, and the database is a committed
    /// state at least as new as every file a read found during the restore, though not
    /// necessarily the writer's newest by the time it is published.
    #[must_use]
    pub const fn writer_outpaced(&self) -> bool {
        self.writer_outpaced
    }
}

/// How one attempt ended without a database.
enum Abort {
    /// A planned file was gone: plan again.
    Vanished(FileKey),
    /// The listing planned a gap. A listing is not a snapshot of the store: a directory
    /// read that races a writer's rename may miss a file and see a later one. Plan again
    /// on a fresh listing; a gap every attempt shows is the replica's.
    Gap(PlanOutcome),
    /// A file exists past the listing's newest: list again.
    Behind {
        /// The file a read found.
        found: FileKey,
        /// The newest transaction the listing showed.
        listed: Option<Txid>,
    },
    Fatal(RestoreError),
}

impl From<RestoreError> for Abort {
    fn from(err: RestoreError) -> Self {
        Self::Fatal(err)
    }
}

/// The file applied last, which fixes the page size and checksum for the next.
type Previous<'a> = Option<&'a Verified>;

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
    restore_from(store, None, target, options, sink).await
}

/// [restore], whose first attempt plans over the borrowed `listed` when given instead of
/// listing again. A retry after a vanished file always lists afresh. A caller that hands a
/// listing is checking those files, so no read past it is made ([listed_past]).
pub(crate) async fn restore_from<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    mut listed: Option<&Inventory>,
    target: RestoreTarget,
    options: RestoreOptions,
    mut sink: K,
) -> Result<Restored, RestoreError> {
    let mut attempt = 1_u8;
    let mut bytes_read = 0_u64;
    let mut created_at_read = 0_u64;
    let probe = listed.is_none();
    let mut retries = Retries::default();
    loop {
        let probe = probe && !retries.trusting;
        let source = listed.take().map_or(Source::List { probe }, Source::Given);
        let applied = attempt_once(
            store,
            source,
            target,
            options,
            &mut sink,
            &mut bytes_read,
            &mut created_at_read,
        )
        .await;
        match applied {
            Ok((chain, last, inside_file)) => {
                verify_end_state(&mut sink, &last)?;
                sink.publish().map_err(|err| RestoreError::Publish {
                    left: err.left(),
                    source: Box::new(err),
                })?;
                return Ok(Restored {
                    reached: chain.reaches(),
                    page_size: last.header().page_size(),
                    commit: last.header().pages(),
                    files: chain.files().len(),
                    bytes_read,
                    attempts: attempt,
                    created_at_read,
                    post_apply: last.trailer().post_apply(),
                    inside_file,
                    writer_outpaced: retries.trusting,
                });
            }
            Err(Abort::Fatal(err)) => return Err(err),
            Err(abort) => {
                // A gap stops the plan before the first file: nothing to reset.
                let applied_nothing = matches!(abort, Abort::Gap(_));
                retries.after(abort, attempt)?;
                if !applied_nothing {
                    sink.reset().map_err(sink_fault(SinkOp::Reset))?;
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Listings a restore makes while it follows a replicator that keeps uploading, at most.
const MAX_CHASE: u8 = 64;

/// Why a restore listed again, counted toward the limits on each reason.
#[derive(Default)]
struct Retries {
    /// Attempts that found a planned file gone.
    vanished: u8,
    /// Attempts whose listing planned a gap.
    gaps: u8,
    /// Attempts whose listing did not show a file a read had found after an earlier
    /// listing: the store's listing lags.
    lagged: u8,
    /// The file the last read past a listing found.
    chased: Option<FileKey>,
    /// A writer outpaced [MAX_CHASE] listings that all caught up: the next listing is taken
    /// as it stands, with no read past it.
    trusting: bool,
}

impl Retries {
    /// Counts `abort`, made by attempt `attempt`, or fails when its reason is exhausted.
    ///
    /// A file found past a listing is a lagging listing or a writer that put it after the
    /// listing; one read cannot tell. The next listing can: one that shows the file found
    /// before it is not behind it, so that round is the writer's progress and is not counted
    /// as a lag. Only a listing that still ends below a file found before it began counts.
    fn after(&mut self, abort: Abort, attempt: u8) -> Result<(), RestoreError> {
        match abort {
            Abort::Fatal(err) => Err(err),
            Abort::Vanished(missing) => {
                self.vanished = self.vanished.saturating_add(1);
                if self.vanished >= MAX_ATTEMPTS {
                    return Err(RestoreError::Unstable {
                        attempts: attempt,
                        missing,
                    });
                }
                Ok(())
            }
            Abort::Gap(outcome) => {
                self.gaps = self.gaps.saturating_add(1);
                if self.gaps >= MAX_ATTEMPTS {
                    return Err(RestoreError::NoPlan(NoPlan(outcome)));
                }
                Ok(())
            }
            Abort::Behind { found, listed } => {
                let caught_up = self
                    .chased
                    .is_some_and(|before| listed >= Some(before.range().max()));
                if !caught_up {
                    self.lagged = self.lagged.saturating_add(1);
                }
                self.chased = Some(found);
                // Lagging listings are counted on their own, so past the bound only a
                // round that caught up remains, and the restore stops reading past the
                // listing; the last term only bounds the loop if that reasoning is wrong.
                let exhausted = attempt >= MAX_CHASE.saturating_add(MAX_ATTEMPTS);
                if self.lagged >= MAX_ATTEMPTS || exhausted {
                    return Err(RestoreError::ListingBehind {
                        attempts: attempt,
                        found,
                    });
                }
                if caught_up && attempt >= MAX_CHASE {
                    self.trusting = true;
                }
                Ok(())
            }
        }
    }
}

/// Where an attempt's listing comes from.
#[derive(Clone, Copy)]
enum Source<'a> {
    /// The caller's, checked as it is: no read past it.
    Given(&'a Inventory),
    /// A fresh listing, read past ([listed_past]) when `probe` is set: an empty one for
    /// every target, any other for `Latest` and `AsOf`.
    List { probe: bool },
}

/// One listing, one plan and the chain applied, or why not.
async fn attempt_once<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    source: Source<'_>,
    target: RestoreTarget,
    options: RestoreOptions,
    sink: &mut K,
    bytes_read: &mut u64,
    created_at_read: &mut u64,
) -> Result<(ContiguousChain, Verified, Option<FileKey>), Abort> {
    let loaded;
    let (inventory, probe) = match source {
        Source::Given(inventory) => (inventory, false),
        Source::List { probe } => {
            loaded = Inventory::load(store)
                .await
                .map_err(RestoreError::Inventory)?;
            (&loaded, probe)
        }
    };
    let empty = inventory.newest().is_none();
    if probe
        && empty
        && let Some(found) = listed_past(store, inventory).await?
    {
        return Err(Abort::Behind {
            found,
            listed: inventory.newest(),
        });
    }
    let (chain, inside_file) = match plan_target(store, inventory, target, created_at_read).await? {
        PlanOutcome::Plan(chain) => (chain, None),
        PlanOutcome::AsOfInsideFile { chain, file }
            if options.as_of_inside == AsOfInside::TakeEarlier =>
        {
            (chain, Some(file))
        }
        gap @ PlanOutcome::Gap { .. } => return Err(Abort::Gap(gap)),
        other @ (PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. }) => {
            return Err(RestoreError::NoPlan(NoPlan(other)).into());
        }
    };
    if probe
        && !empty
        && !matches!(target, RestoreTarget::Txid(_))
        && let Some(found) = listed_past(store, inventory).await?
    {
        return Err(Abort::Behind {
            found,
            listed: inventory.newest(),
        });
    }
    let last = apply_chain(store, &chain, sink, bytes_read).await?;
    Ok((chain, last, inside_file))
}

/// A file a read finds past the newest transaction `inventory` lists, if any: the keys a reader
/// can name without a listing (`crate::probe`), looked for from the next transaction (the
/// first, for an empty listing) as the engine's own probe walks them, until more than
/// `PROBE_GAP` transactions in a row are missing. A listing that lags the store's writes is
/// told from a whole one this way, as far as a reader can name keys; a hole in the replica
/// wider than that gap right after the listing's end is beyond it (`docs/BEHAVIOR.md`).
///
/// # Errors
/// [RestoreError::Store] for a read that fails other than as not found.
pub async fn listed_past<S: ObjectStore>(
    store: &S,
    inventory: &Inventory,
) -> Result<Option<FileKey>, RestoreError> {
    let first = match inventory.newest() {
        Some(newest) => newest.next(),
        None => Txid::new(1),
    };
    let starts = crate::probe::Starts::of(inventory);
    let found = crate::probe::walk(store, first, None, &starts, true)
        .await
        .map_err(RestoreError::Store)?;
    Ok(found.first().copied())
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
    match resolve_counted(store, &mut planner, created_at_read).await {
        Ok(outcome) => Ok(outcome),
        Err(ResolveError::Store(StoreError::NotFound { key })) => Err(Abort::Vanished(key)),
        Err(ResolveError::Store(err)) => Err(RestoreError::Store(err).into()),
        Err(ResolveError::Repeated { key }) => Err(RestoreError::CreatedAtRepeated { key }.into()),
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

async fn apply_chain<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    chain: &ContiguousChain,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Verified, Abort> {
    let mut last = apply_file(store, chain.snapshot(), None, sink, bytes_read).await?;
    for info in chain.files().iter().skip(1) {
        last = apply_file(store, info, Some(&last), sink, bytes_read).await?;
    }
    if last.header().commit() == 0 {
        let key = chain.files().last().map_or(chain.snapshot().key, |f| f.key);
        return Err(RestoreError::File {
            key,
            fault: FileFault::EmptyDatabase,
        }
        .into());
    }
    Ok(last)
}

async fn apply_file<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    info: &FileInfo,
    previous: Previous<'_>,
    sink: &mut K,
    bytes_read: &mut u64,
) -> Result<Verified, Abort> {
    let key = info.key;
    let mut read = LtxRead::new(key, chunk_len()).sized(info.size);
    let applied = read
        .read_verified(store, |event| match event {
            Event::Header(header) => check_header(key, &header, previous)
                .map_err(|fault| RestoreError::File { key, fault }),
            Event::Page(page) => sink.write_page(page).map_err(sink_fault(SinkOp::WritePage)),
        })
        .await;
    *bytes_read = bytes_read.saturating_add(read.bytes_read());
    let verified = applied.map_err(|fault| match fault {
        ReadFault::Store(StoreError::NotFound { .. }) => Abort::Vanished(key),
        ReadFault::Store(err) => Abort::Fatal(RestoreError::Store(err)),
        ReadFault::Decode(err) => Abort::Fatal(RestoreError::File {
            key,
            fault: FileFault::Decode(err),
        }),
        ReadFault::Rejected(err) => Abort::Fatal(err),
    })?;
    let header = verified.header();
    sink.truncate(header.pages(), header.page_size())
        .map_err(sink_fault(SinkOp::Truncate))?;
    Ok(verified)
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
