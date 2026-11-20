//! The restore executor: a planned chain of LTX files becomes one database.
//!
//! [restore] loads the inventory, plans, then applies the chain's files oldest
//! first into a [DatabaseSink]. A file is read in bounded chunks and pushed
//! through one [Decoder]; its header is checked before any of its pages are
//! written, and nothing counts until the decoder returns [lsr_ltx::Verified].
//! The sink is truncated to the file's commit after every file, so newest wins,
//! pages above the final commit are dropped, and a shrink followed by growth
//! leaves zeros, never an older file's pages. The sink is published only after
//! every file verified; any error leaves the target absent.
//!
//! The one retry here is the planner's own race: a file the plan named may be
//! gone because compaction or retention ran in between ([StoreError::NotFound]).
//! The sink is reset, the inventory reloaded and the target planned again, at
//! most [MAX_ATTEMPTS] times. Every other store error ends the restore; retry
//! with backoff belongs to a store wrapper, and core holds no timer.
//!
//! The lock page is never written: [PageNo] cannot hold it, so the decoder
//! rejects a file that does, and a sink reads it as zeros.

use std::io;

use lsr_ltx::{
    Checksum, DecodeError, Decoder, Event, Header, PageNo, PageSize, Txid, TxidRange, Verified,
};
use thiserror::Error;

use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey};
use crate::plan::{ContiguousChain, PlanOutcome, RestoreTarget, plan};
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
pub use mem_sink::{Database, MemSink};

/// Plans attempted before the replica is called unstable.
pub const MAX_ATTEMPTS: u8 = 3;

/// Bytes asked of the store per read: memory per file stays one chunk plus one
/// page, whatever the file's size.
const CHUNK_BYTES: u32 = 4 * 1024 * 1024;

/// What a restore was doing to its sink when the sink failed.
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
    /// [DatabaseSink::publish].
    Publish,
}

impl std::fmt::Display for SinkOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WritePage => "write a page",
            Self::Truncate => "truncate",
            Self::Reset => "reset",
            Self::Checksum => "checksum",
            Self::Publish => "publish",
        })
    }
}

/// Where a restored database is built. Synchronous on purpose: one thread per
/// database drives a restore, and a sink is plain file I/O.
pub trait DatabaseSink {
    /// Writes one page at byte offset (pgno - 1) times the page size. Writing
    /// past the end grows the database.
    ///
    /// # Errors
    /// Whatever the underlying write reports.
    fn write_page(&mut self, pgno: PageNo, data: &[u8]) -> io::Result<()>;

    /// Makes the length exactly pages times page size. Growth reads as zeros.
    ///
    /// # Errors
    /// Whatever the underlying resize reports.
    fn truncate(&mut self, pages: u32, page_size: PageSize) -> io::Result<()>;

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
    fn checksum(&mut self, pages: u32, page_size: PageSize) -> io::Result<Checksum>;

    /// Makes the database durable and visible, or fails and leaves it not
    /// visible. A failed durability step is never retried.
    ///
    /// # Errors
    /// The step that failed.
    fn publish(self) -> io::Result<()>;
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
        expected: Checksum,
        /// This file's pre-apply checksum.
        found: Checksum,
    },
    /// The chain ends in a database of zero pages.
    #[error("the restored database has no pages")]
    EmptyDatabase,
}

/// Why a restore failed. Nothing is published in any case.
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
        expected: Checksum,
        /// What the sink holds.
        found: Checksum,
    },
    /// The destination failed.
    #[error("sink failed to {op}: {source}")]
    Sink {
        /// What the restore was doing.
        op: SinkOp,
        /// The I/O error.
        source: io::Error,
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
    commit: u32,
    files: usize,
    bytes_read: u64,
    attempts: u8,
    post_apply: Option<Checksum>,
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
    pub const fn commit(&self) -> u32 {
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

    /// The database checksum after the last file, when the writer tracked it.
    #[must_use]
    pub const fn post_apply(&self) -> Option<Checksum> {
        self.post_apply
    }
}

/// How one attempt ended without a database.
enum Abort {
    /// A planned file was gone: plan again.
    Vanished(FileKey),
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

#[expect(
    clippy::expect_used,
    reason = "4 MiB is inside ReadLen's bound; a test pins it"
)]
fn chunk_len() -> ReadLen {
    ReadLen::new(CHUNK_BYTES).expect("CHUNK_BYTES is within ReadLen::MAX_RAW")
}

/// Rebuilds the database [target] names from [store] into [sink] and publishes it.
///
/// # Errors
/// [RestoreError]; the sink is dropped unpublished in every case.
pub async fn restore<S: ObjectStore, K: DatabaseSink>(
    store: &S,
    target: RestoreTarget,
    mut sink: K,
) -> Result<Restored, RestoreError> {
    let mut attempt = 1_u8;
    let mut bytes_read = 0_u64;
    loop {
        let inventory = Inventory::load(store)
            .await
            .map_err(RestoreError::Inventory)?;
        let outcome = plan(&inventory, target);
        let chain = match outcome {
            PlanOutcome::Plan(chain) => chain,
            other @ (PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }
            | PlanOutcome::Gap { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. }) => {
                return Err(RestoreError::NoPlan(NoPlan(other)));
            }
        };
        let applied = apply_chain(store, &chain, &mut sink, &mut bytes_read).await;
        match applied {
            Ok(last) => {
                verify_end_state(&mut sink, &last)?;
                sink.publish().map_err(sink_fault(SinkOp::Publish))?;
                return Ok(Restored {
                    reached: chain.reaches(),
                    page_size: last.header().page_size(),
                    commit: last.header().commit(),
                    files: chain.files().len(),
                    bytes_read,
                    attempts: attempt,
                    post_apply: last.trailer().post_apply(),
                });
            }
            Err(Abort::Fatal(err)) => return Err(err),
            Err(Abort::Vanished(missing)) if attempt >= MAX_ATTEMPTS => {
                return Err(RestoreError::Unstable {
                    attempts: attempt,
                    missing,
                });
            }
            Err(Abort::Vanished(_)) => {
                sink.reset().map_err(sink_fault(SinkOp::Reset))?;
                attempt = attempt.saturating_add(1);
            }
        }
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
        .checksum(last.header().commit(), last.header().page_size())
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
    let mut decoder = Decoder::new();
    let mut offset = 0_u64;
    loop {
        let chunk = match store.read(&key, offset, chunk_len()).await {
            Ok(chunk) => chunk,
            Err(StoreError::NotFound { .. }) => return Err(Abort::Vanished(key)),
            Err(err) => return Err(RestoreError::Store(err).into()),
        };
        // The listed size is advisory: only the decoder knows where a file ends.
        if chunk.is_empty() {
            break;
        }
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        offset = offset.saturating_add(len);
        *bytes_read = bytes_read.saturating_add(len);
        feed_chunk(&mut decoder, &chunk, key, previous, sink)?;
    }
    let verified = decoder.finish().map_err(|err| RestoreError::File {
        key,
        fault: FileFault::Decode(err),
    })?;
    let header = verified.header();
    sink.truncate(header.commit(), header.page_size())
        .map_err(sink_fault(SinkOp::Truncate))?;
    Ok(verified)
}

fn feed_chunk<K: DatabaseSink>(
    decoder: &mut Decoder,
    chunk: &[u8],
    key: FileKey,
    previous: Previous<'_>,
    sink: &mut K,
) -> Result<(), RestoreError> {
    let file_fault = |fault| RestoreError::File { key, fault };
    let mut rest = chunk;
    while !rest.is_empty() {
        let (used, event) = decoder
            .feed(rest)
            .map_err(|err| file_fault(FileFault::Decode(err)))?;
        match event {
            Some(Event::Header(header)) => {
                check_header(key, &header, previous).map_err(file_fault)?;
            }
            Some(Event::Page { pgno, data }) => sink
                .write_page(pgno, data)
                .map_err(sink_fault(SinkOp::WritePage))?,
            None => {}
        }
        rest = rest.get(used..).unwrap_or_default();
    }
    Ok(())
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
    // An overlapping file's pre-apply checksum is the state before its own
    // first transaction, which the previous file has already moved past; only
    // a file that starts right after the previous one can be compared.
    let contiguous = previous.header().txids().max().next() == Some(header.txids().min());
    if let (Some(expected), Some(found)) =
        (previous.trailer().post_apply(), header.fields().pre_apply)
        && contiguous
        && expected != found
    {
        return Err(FileFault::ChecksumChain { expected, found });
    }
    Ok(())
}
