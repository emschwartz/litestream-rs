//! The judge: what the replica must be able to restore, against what the
//! database really was.
//!
//! Ground truth comes from two places, never from the code under test: the
//! [`RecorderLog`](crate::store::RecorderLog), which records every file the store really
//! took with its WAL origin, and the [`Model`], which holds every commit's page
//! image. A file's WAL origin names the commit that ended it, so the oracle maps
//! a transaction id to a commit without reading a single page of a file.
//!
//! What is checked, over the unfaulted store handle:
//!
//! - every transaction from 1 to the newest gets the answer the planner's
//!   contract gives for the listing ([`crate::contract`], derived from the
//!   contract's text, not the planner's code): a plan restores to the image of
//!   the commit it covers, and a refusal is accepted only where no chain of
//!   kept files reaches the transaction exactly. From the oldest snapshot on,
//!   only a refusal inside a kept compacted file is history the replica may
//!   lack;
//! - `Latest` restores to the image of its commit, and is not older than the
//!   newest transaction the engine acknowledged, except across a rewind;
//! - with the staging recorder, every created-at time the replica took is a
//!   stamp the simulator's clock allows, and `AsOf` restores at instants
//!   around each created-at time get the contract's answer and are never
//!   newer than the instant (`as_of`);
//! - with retention windows, no delete removed a file younger than its
//!   window (`retention`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use lsr_core::conformance::block_on;
use lsr_core::{Did, FileKey, Inventory, MemStore, PlanOutcome, RestoreTarget, is_snapshot_start};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use lsr_wal::Salts;
use thiserror::Error;

use crate::contract::{self, Answer, Expected};
use crate::model::{CommitId, Model, PageImage};
use crate::store::{RecorderEvent, RecorderLog};

/// A resource the engine must keep bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    /// Files kept on disk while nothing happens.
    IdleFiles,
    /// Checkpoints run while nothing happens.
    IdleCheckpoints,
    /// Listings made while nothing happens.
    IdleLists,
    /// Passive checkpoints in one tick.
    PassivePerTick,
    /// Bytes staged in one tick.
    StagedBytesPerTick,
    /// The WAL's length in frames.
    WalLength,
    /// Listing calls in one tick.
    ListCalls,
    /// The most bytes one upload buffered.
    UploadBuffer,
    /// Ticks the engine took to drain its queue.
    DrainTicks,
    /// Milliseconds a retry waits.
    RetryWait,
}

impl fmt::Display for Bound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::IdleFiles => "idle files",
            Self::IdleCheckpoints => "idle checkpoints",
            Self::IdleLists => "idle listings",
            Self::PassivePerTick => "passive checkpoints per tick",
            Self::StagedBytesPerTick => "staged bytes per tick",
            Self::WalLength => "WAL length",
            Self::ListCalls => "listing calls",
            Self::UploadBuffer => "upload buffer",
            Self::DrainTicks => "ticks to drain",
            Self::RetryWait => "retry wait in milliseconds",
        };
        f.write_str(name)
    }
}

/// A property the run broke.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Violation {
    /// A level-0 file's WAL range does not end on a commit.
    #[error("{key} covers part of a transaction")]
    CoversMidTx {
        /// The file.
        key: String,
    },
    /// One transaction id covers two different commits.
    #[error("transaction {txid} covers {first} and then {second}")]
    TxidReused {
        /// The id.
        txid: u64,
        /// The commit it covered first.
        first: CommitId,
        /// The commit it covered later.
        second: CommitId,
    },
    /// A later transaction id covers an earlier commit. The same commit is
    /// allowed: a snapshot on request is a new id with nothing new in the WAL.
    #[error("transaction {lower} covers {low_commit} but {higher} covers {high_commit}")]
    CoversNotIncreasing {
        /// The smaller id.
        lower: u64,
        /// The commit it covers.
        low_commit: CommitId,
        /// The larger id.
        higher: u64,
        /// The commit it covers.
        high_commit: CommitId,
    },
    /// A restore built a different database.
    #[error("restoring {target:?} differs from {commit} first at page {first_diff}")]
    RestoreMismatch {
        /// What was restored.
        target: RestoreTarget,
        /// The commit it should equal.
        commit: CommitId,
        /// The first page that differs, or one past the shorter database.
        first_diff: u32,
    },
    /// A restore the replica should answer was refused.
    #[error("restoring {target:?} was refused: {outcome:?}")]
    RestoreRefused {
        /// What was asked.
        target: RestoreTarget,
        /// The planner's reason.
        outcome: PlanOutcome,
    },
    /// `Latest` is older than something the engine acknowledged.
    #[error("latest is {latest:?} but {acked} was acknowledged")]
    LatestLostAck {
        /// The newest acknowledged transaction.
        acked: u64,
        /// What `Latest` reached, if anything.
        latest: Option<u64>,
    },
    /// A resource grew past its limit.
    #[error("{what}: {seen} is over the limit {limit}")]
    ResourceBound {
        /// The resource.
        what: Bound,
        /// What was seen.
        seen: u64,
        /// The most allowed.
        limit: u64,
    },
    /// The planner answered something its contract does not allow.
    #[error("restoring {target:?} gave {got} where the planner's contract says {expected}")]
    WrongAnswer {
        /// What was asked.
        target: RestoreTarget,
        /// What the contract allows, as the oracle derived it.
        expected: Expected,
        /// What the restore answered.
        got: Box<Answer>,
    },
    /// A replica file's created-at time is not the time its newest
    /// transaction was staged.
    #[error(
        "{key:?} was put as created at {created_at:?}, a time no staged file that ends at its last transaction was stamped with"
    )]
    WrongCreatedAt {
        /// The file.
        key: FileKey,
        /// The created-at time it was put with.
        created_at: Timestamp,
    },
    /// An `AsOf` restore reached a transaction that was first staged at or
    /// after the instant: a later state than the one asked for.
    #[error("restoring as of {instant:?} reached {reached}, first staged at {staged:?}")]
    AsOfTooNew {
        /// The instant asked for.
        instant: Timestamp,
        /// The transaction reached.
        reached: u64,
        /// When the first file ending at it was staged.
        staged: Timestamp,
    },
    /// An `AsOf` instant later than every file's created-at time restored
    /// something other than `Latest`.
    #[error("restoring {target:?}, after every file, gave {got} where latest reached {latest}")]
    NotLatest {
        /// What was asked.
        target: RestoreTarget,
        /// What it gave.
        got: Box<Answer>,
        /// What `Latest` reached.
        latest: u64,
    },
    /// Retention deleted a file younger than its window.
    #[error(
        "{key:?}, created at {created_at:?}, was deleted when the engine had been told {latest:?}"
    )]
    RetentionTooEager {
        /// The file.
        key: FileKey,
        /// Its created-at time.
        created_at: Timestamp,
        /// The latest time the engine had been told when it deleted it.
        latest: Timestamp,
    },
    /// The engine reported its own bug, or the oracle could not decide.
    #[error("bug fault: {context}")]
    BugFault {
        /// Where.
        context: String,
    },
}

/// `Err(ResourceBound)` when `seen` is over `limit`.
///
/// # Errors
/// [`Violation::ResourceBound`].
pub fn within(what: Bound, seen: u64, limit: u64) -> Result<(), Violation> {
    if seen > limit {
        return Err(Violation::ResourceBound { what, seen, limit });
    }
    Ok(())
}

/// A `BugFault` naming `context`.
#[must_use]
pub fn bug_fault(context: impl Into<String>) -> Violation {
    Violation::BugFault {
        context: context.into(),
    }
}

/// The newest transaction the engine has told us is safe on the replica.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Acked {
    highest: Option<Txid>,
}

impl Acked {
    /// Nothing acknowledged.
    #[must_use]
    pub const fn new() -> Self {
        Self { highest: None }
    }

    /// The newest acknowledged transaction.
    #[must_use]
    pub const fn highest(&self) -> Option<Txid> {
        self.highest
    }

    /// Notes an acknowledgement.
    pub fn record(&mut self, txid: Txid) {
        self.highest = self.highest.max(Some(txid));
    }

    /// The local database was rewound: what was acknowledged before may
    /// legitimately be gone from `Latest`.
    pub fn note_rewound(&mut self) {
        self.highest = None;
    }

    /// Reads an acknowledgement off one step's result: a commit or a listing
    /// that confirms a transaction.
    pub fn on_did(&mut self, did: &Did) {
        match did {
            Did::Put(info) => self.record(info.key.range().max()),
            Did::Listed {
                newest: Some(newest),
            } => self.record(*newest),
            Did::Listed { newest: None } | Did::Nothing => {}
        }
    }
}

/// What a passing check looked at.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Checked {
    /// Transactions restored and compared.
    pub restored: u32,
    /// Transactions from the oldest snapshot on refused for lying inside a
    /// compacted file.
    pub inside_file: u32,
    /// Transactions below the oldest snapshot's end, refused as the
    /// contract says.
    pub below_base: u32,
    /// What `Latest` reached.
    pub latest: Option<Txid>,
    /// `AsOf` restores that met their instant exactly.
    pub as_of_planned: u32,
    /// `AsOf` restores whose instant fell inside a file.
    pub as_of_inside: u32,
    /// `AsOf` restores the contract refuses.
    pub as_of_refused: u32,
    /// Replica files whose created-at time was judged against the staging
    /// recorder.
    pub stamps: u32,
    /// Deleted keys judged against their retention window.
    pub deletes: u32,
    /// Restores refused over a gap that a bucket rollback left.
    pub lost: u32,
    /// Restores that stopped on a file the store damaged.
    pub damaged: u32,
}

impl Checked {
    /// Counts what the check judged into `reach`.
    pub fn tally(&self, reach: &mut crate::reach::Reach) {
        reach.bump("check.runs");
        reach.add("check.txid.restored", u64::from(self.restored));
        reach.add("check.txid.inside", u64::from(self.inside_file));
        reach.add("check.txid.below_base", u64::from(self.below_base));
        if self.latest.is_some() {
            reach.bump("check.latest");
        }
        reach.add("check.asof.planned", u64::from(self.as_of_planned));
        reach.add("check.asof.inside", u64::from(self.as_of_inside));
        reach.add("check.asof.refused", u64::from(self.as_of_refused));
        reach.add("check.stamps", u64::from(self.stamps));
        reach.add("check.deletes", u64::from(self.deletes));
        reach.add("check.lost", u64::from(self.lost));
        reach.add("check.damaged", u64::from(self.damaged));
    }
}

fn range_str(range: TxidRange) -> String {
    format!("{}-{}", range.min().get(), range.max().get())
}

/// Which commit each transaction id covers, from the recorder and the model.
fn covers(
    model: &Model,
    recorder: &RecorderLog,
    ground: Ground<'_>,
) -> Result<BTreeMap<Txid, CommitId>, Violation> {
    // A rewind, or a bucket rollback, lets the engine take an id again.
    let rewound = model.snapshot_notes().iter().any(|n| n.rewound)
        || model.rewound()
        || !ground.lost.is_empty();
    let mut cover: BTreeMap<Txid, CommitId> = BTreeMap::new();
    for (at, event) in recorder.events().iter().enumerate() {
        let RecorderEvent::Put {
            key,
            header: Some(header),
            origin,
            ..
        } = event
        else {
            continue;
        };
        let max = key.range().max();
        // A snapshot taken against an empty WAL names the header and no frames.
        let commit = match origin.filter(|origin| origin.size() > 0) {
            Some(origin) => {
                let (salt1, salt2) = origin.salts();
                let end = origin.offset().saturating_add(origin.size());
                let salts = Salts { salt1, salt2 };
                let found = model
                    .staged_from(max, salts, end, at)
                    .or_else(|| model.find(salts, end, at));
                Some(found.ok_or_else(|| Violation::CoversMidTx {
                    key: format!("{} {}", key.level().get(), range_str(key.range())),
                })?)
            }
            // A staged snapshot lost before upload (torn by a power loss, or
            // its staging directory wiped) leaves its note behind, and the
            // engine takes the same id again from a later commit. Staging holds
            // one file per id, so a level-0 file put is the newest noted by
            // then; so is one put again after a bucket rollback took the
            // first. A compacted file names no origin and carries the commit
            // of the level-0 file that ended it.
            None => {
                let _ = header;
                let noted = model.staged_as(max, at);
                let known = cover.get(&max).copied();
                if key.level().get() == 0 {
                    noted.or(known)
                } else {
                    known.or(noted)
                }
            }
        };
        let Some(commit) = commit else { continue };
        match cover.get(&max) {
            // A put the store reported failed or wrong may be put again from
            // another commit under the same id.
            Some(first) if *first != commit && !rewound && !ground.exempt.contains(key) => {
                return Err(Violation::TxidReused {
                    txid: max.get(),
                    first: *first,
                    second: commit,
                });
            }
            Some(_) | None => {
                cover.insert(max, commit);
            }
        }
    }
    // Another writer's file holds the pages of the file it copied, so its
    // transaction is that file's commit wherever it was compacted into.
    for (theirs, copied) in ground.foreign {
        if let Some(commit) = cover.get(copied).copied() {
            cover.entry(*theirs).or_insert(commit);
        }
    }
    if !rewound {
        increasing(&cover)?;
    }
    Ok(cover)
}

/// A later transaction id never covers an earlier commit.
fn increasing(cover: &BTreeMap<Txid, CommitId>) -> Result<(), Violation> {
    let mut previous: Option<(Txid, CommitId)> = None;
    for (txid, commit) in cover {
        if let Some((lower, low_commit)) = previous.filter(|(_, c)| c > commit) {
            return Err(Violation::CoversNotIncreasing {
                lower: lower.get(),
                low_commit,
                higher: txid.get(),
                high_commit: *commit,
            });
        }
        previous = Some((*txid, *commit));
    }
    Ok(())
}

/// The first page where the restored database differs from `image`.
fn first_diff(image: &PageImage, got: &lsr_core::Database) -> Option<u32> {
    let want = image.db_size();
    let have = got.page_count();
    if want != have {
        return Some(want.min(have).saturating_add(1));
    }
    (1..=want).find(|pgno| {
        let expected = image.page(*pgno);
        let actual = got.page(*pgno);
        match (expected, actual) {
            (Some(e), Some(a)) => e.as_slice() != a.as_slice(),
            (None, None) => false,
            (Some(_), None) | (None, Some(_)) => true,
        }
    })
}

/// What the oracle knows besides the model and the replica's recorder.
#[derive(Clone, Copy, Debug)]
pub struct Ground<'a> {
    /// What the staging store really did, read with the simulator's clock:
    /// the created-at time of every replica file is judged against it.
    pub staging: Option<&'a RecorderLog>,
    /// How long retention keeps files, when the run judges its deletes.
    pub retention: Option<Windows>,
    /// Keys the engine may delete whatever their age: commits the store
    /// reported failed or wrong.
    pub exempt: &'a [FileKey],
    /// Transactions taken from the replica for good from outside (a bucket
    /// rollback): a gap over one of them is the loss, not a bug.
    pub lost: &'a BTreeSet<Txid>,
    /// Files another writer put: its transaction, and the transaction whose
    /// file's pages it holds.
    pub foreign: &'a [(Txid, Txid)],
    /// Objects the store damaged as it published them: a restore may stop on
    /// one with a typed error.
    pub damaged: &'a [FileKey],
}

/// Nothing lost.
static NONE_LOST: BTreeSet<Txid> = BTreeSet::new();

impl Default for Ground<'_> {
    /// Nothing besides the model and the recorder.
    fn default() -> Self {
        Self {
            staging: None,
            retention: None,
            exempt: &[],
            lost: &NONE_LOST,
            foreign: &[],
            damaged: &[],
        }
    }
}

/// Judges the replica. `remote` is the raw store handle: no fault or yield
/// stands between the oracle and the bytes.
///
/// # Errors
/// The first [`Violation`] found.
pub fn check(
    model: &Model,
    recorder: &RecorderLog,
    remote: &MemStore,
    acked: &Acked,
) -> Result<Checked, Violation> {
    check_with(model, recorder, remote, acked, Ground::default())
}

/// [`check`] with what else the oracle knows: with the staging recorder it
/// also judges every created-at time and how new an `AsOf` restore may be.
///
/// # Errors
/// The first [`Violation`] found.
pub fn check_with(
    model: &Model,
    recorder: &RecorderLog,
    remote: &MemStore,
    acked: &Acked,
    ground: Ground<'_>,
) -> Result<Checked, Violation> {
    let cover = covers(model, recorder, ground)?;
    let inventory = block_on(Inventory::load(remote))
        .map_err(|err| bug_fault(format!("listing the replica: {err}")))?;
    let files = inventory.files();
    let mut out = Checked::default();
    let judge = as_of::Judge {
        remote,
        files,
        model,
        cover: &cover,
        staging: ground.staging,
        lost: ground.lost,
        damaged: ground.damaged,
        foreign: ground.foreign,
    };
    let oldest = files
        .iter()
        .filter(|f| is_snapshot_start(f))
        .map(|f| f.key.range().max())
        .min();
    let reach = contract::Reach::over(files, |_| true);
    let newest = inventory.newest().map_or(0, Txid::get);
    for txid in (1..=newest).filter_map(Txid::new) {
        judge.txid(txid, &reach, oldest, &mut out)?;
    }
    if files.is_empty() {
        return match acked.highest() {
            Some(acked) => Err(Violation::LatestLostAck {
                acked: acked.get(),
                latest: None,
            }),
            None => Ok(out),
        };
    }
    let Some(latest) = judge.latest(&reach, inventory.newest(), &mut out)? else {
        return Ok(out);
    };
    if let Some(acked) = acked.highest().filter(|a| *a > latest) {
        return Err(Violation::LatestLostAck {
            acked: acked.get(),
            latest: Some(latest.get()),
        });
    }
    out.latest = Some(latest);
    if let Some(staging) = ground.staging {
        out.stamps = as_of::stamps(recorder, staging, ground.foreign)?;
    }
    if let Some(windows) = ground.retention {
        out.deletes = retention::deletes(recorder, windows, ground.exempt)?;
    }
    judge.all(latest, &mut out)?;
    Ok(out)
}

mod as_of;
mod retention;

pub use retention::Windows;

#[cfg(test)]
mod tests;
