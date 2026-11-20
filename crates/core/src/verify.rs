//! The replica checker: what a restore would trip over, found without restoring.
//!
//! [verify] loads one [Inventory] and reports everything wrong with it as
//! [Finding]s, each with a [Severity]. [Depth::Names] makes list calls only:
//! foreign names, level-9 files that cannot start a chain, and holes in the
//! history. A chain may start at any file that begins at transaction 1, so
//! [Finding::NoSnapshot] means no such file exists. For every valid level-9 snapshot in turn the planner is asked for the next
//! snapshot's last transaction (with the newer snapshots hidden, so the older
//! one is the base), and for the newest snapshot it is asked for the latest
//! state; each [PlanOutcome::Gap] is one finding, so a hole in older history
//! is reported, not only the one at the tip.
//!
//! [Depth::Names] never reads contents, so a clean report there cannot vouch
//! that a restore would succeed; only [Depth::Contents] can.
//!
//! [Depth::Contents] reads every file through one [Decoder], so a file's
//! checksums, index and framing are checked, and compares what the header says
//! with what the name and the listing say. It then runs the real
//! [restore] of the latest state into a sink that keeps one checksum per page
//! and publishes nothing. Whether the latest state restores is that run's
//! answer, never a re-derivation: every cross-file, cross-level, end-state and
//! empty-database rule is restore's own, and a failure it reports is one
//! [Finding::LatestRestoreFails]. So the report's worst severity is
//! [Severity::Unrestorable] if and only if `restore(Latest)` fails (or there is
//! no plan to restore).
//!
//! The per-file findings are graded by use: a damaged file the latest plan
//! reads is [Severity::Unrestorable] (and restore fails on it, so it is not
//! reported a second time), any other is [Severity::Degraded], older history.
//! The files of the latest plan are read twice, once by the walk and once by
//! the dry run; skipping the second read would need a second code path that
//! feeds restore from the walk's decoder, which is the drift this design
//! removes. [Report::files_read] and [Report::bytes_read] count both. Memory is
//! one decoder and one chunk, plus 8 bytes a page for the dry run's sink.
//!
//! Only what stops the walk is an error ([VerifyError]): a failed listing or a
//! failed read. A damaged file is a finding, never an error, and a file that
//! vanished between the listing and the read (compaction or retention ran) is
//! a note. Findings are capped at [MAX_FINDINGS]; the last one then says how
//! many were dropped, and [Report::worst] still counts them.

use std::time::Duration;

use lsr_ltx::{Checksum, DecodeError, Decoder, Header, PageSize, Txid, TxidRange};
use thiserror::Error;

use self::dry_run::latest_restore;
use crate::inventory::{Inventory, InventoryError, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::{PlanOutcome, RestoreTarget, plan};
use crate::restore::RestoreError;
use crate::store::{ObjectStore, ReadLen, StoreError};

mod dry_run;

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;

/// Findings kept in a [Report]; the rest are counted in a final
/// [Finding::Truncated].
pub const MAX_FINDINGS: usize = 1000;

/// Bytes asked of the store per read.
const CHUNK_BYTES: u32 = 4 * 1024 * 1024;

/// How much of the replica to look at.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Depth {
    /// List calls only: no object is read.
    Names,
    /// Read and decode every file, then dry-run the restore of the latest
    /// state.
    Contents,
}

/// How bad a finding is, least first.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Severity {
    /// Worth knowing; no state is affected.
    Note,
    /// The newest state restores, but some older state or a detail is off.
    Degraded,
    /// The replica is damaged or incomplete in a way that can fail a restore
    /// or leave the newest state out of reach.
    Unrestorable,
}

/// One thing wrong with a replica. Closed, so a new kind breaks every match.
#[derive(Debug)]
pub enum Finding {
    /// Files exist but none can start a restore chain.
    NoSnapshot,
    /// The chain from a snapshot stops short of where the history goes on.
    Gap {
        /// The snapshot the chain starts from.
        snapshot: FileKey,
        /// The last transaction reachable from it.
        have_through: Txid,
        /// The first transaction of the nearest file beyond the hole.
        next_starts: Txid,
        /// Whether this is the newest snapshot, so the latest state is cut
        /// off, rather than a hole in older history.
        at_tip: bool,
    },
    /// A level-9 file that does not start at transaction 1, so it is never a
    /// base.
    MalformedSnapshot {
        /// The file.
        key: FileKey,
    },
    /// Names in a level that are not LTX files.
    ForeignNames {
        /// The level.
        level: Level,
        /// How many names.
        count: usize,
    },
    /// The file does not decode (truncated, corrupt, trailing bytes).
    Unreadable {
        /// The file.
        key: FileKey,
        /// Why.
        error: DecodeError,
        /// Whether the latest restore reads this file; if so this is
        /// [Severity::Unrestorable], otherwise [Severity::Degraded].
        in_latest_plan: bool,
    },
    /// The header covers other transactions than the name says.
    RangeMismatch {
        /// The file.
        key: FileKey,
        /// What the header covers.
        header: TxidRange,
        /// Whether the latest restore reads this file.
        in_latest_plan: bool,
    },
    /// A level-9 file whose header is not a whole-database snapshot.
    NotASnapshot {
        /// The file.
        key: FileKey,
        /// Whether the latest restore reads this file.
        in_latest_plan: bool,
    },
    /// The page size differs from the one the latest plan's snapshot has (or,
    /// with no plan, from the first file read).
    PageSizeDiffers {
        /// The file.
        key: FileKey,
        /// The reference page size.
        expected: PageSize,
        /// This file's.
        found: PageSize,
        /// Whether the latest restore reads this file.
        in_latest_plan: bool,
    },
    /// The listed size differs from the bytes the file really has.
    SizeDiffers {
        /// The file.
        key: FileKey,
        /// The size in the listing.
        listed: u64,
        /// The bytes read.
        read: u64,
    },
    /// Some files' header timestamps differ from the store's, as they do on
    /// a copied file replica (whose modification times are new). One fact
    /// about the replica, not one per file: restores to a point in time follow
    /// the store's times, the latest state is unaffected.
    TimestampsDiffer {
        /// How many files differ.
        files: u32,
        /// The first, in walk order.
        first: FileKey,
        /// The largest gap between a header's time and the store's.
        largest: Duration,
    },
    /// A file does not start from the state the previous file in its level
    /// ended in.
    ChecksumChain {
        /// The later file.
        key: FileKey,
        /// The earlier file's post-apply checksum.
        expected: Checksum,
        /// The later file's pre-apply checksum.
        found: Checksum,
        /// Whether the latest restore applies these two files in a row.
        in_latest_plan: bool,
    },
    /// The dry run of `restore(Latest)` failed. Not reported when a per-file
    /// finding on the same file already says why.
    LatestRestoreFails {
        /// The file the error names, when it names one.
        key: Option<FileKey>,
        /// What restore said.
        error: RestoreError,
    },
    /// The file was listed and then gone, as compaction or retention does.
    Vanished {
        /// The file.
        key: FileKey,
    },
    /// Findings past [MAX_FINDINGS] were dropped.
    Truncated {
        /// How many.
        more: usize,
    },
}

impl Finding {
    /// How bad this finding is.
    #[must_use]
    pub const fn severity(&self) -> Severity {
        match self {
            Self::NoSnapshot
            | Self::LatestRestoreFails { .. }
            | Self::Gap { at_tip: true, .. }
            | Self::Unreadable {
                in_latest_plan: true,
                ..
            }
            | Self::RangeMismatch {
                in_latest_plan: true,
                ..
            }
            | Self::NotASnapshot {
                in_latest_plan: true,
                ..
            }
            | Self::PageSizeDiffers {
                in_latest_plan: true,
                ..
            }
            | Self::ChecksumChain {
                in_latest_plan: true,
                ..
            } => Severity::Unrestorable,
            Self::Gap { at_tip: false, .. }
            | Self::MalformedSnapshot { .. }
            | Self::SizeDiffers { .. }
            | Self::Unreadable { .. }
            | Self::RangeMismatch { .. }
            | Self::NotASnapshot { .. }
            | Self::PageSizeDiffers { .. }
            | Self::ChecksumChain { .. } => Severity::Degraded,
            Self::ForeignNames { .. }
            | Self::TimestampsDiffer { .. }
            | Self::Vanished { .. }
            | Self::Truncated { .. } => Severity::Note,
        }
    }

    /// The one file a finding is about, when it is about one.
    const fn file(&self) -> Option<FileKey> {
        match self {
            Self::Unreadable { key, .. }
            | Self::RangeMismatch { key, .. }
            | Self::NotASnapshot { key, .. }
            | Self::PageSizeDiffers { key, .. }
            | Self::ChecksumChain { key, .. } => Some(*key),
            Self::LatestRestoreFails { key, .. } => *key,
            Self::NoSnapshot
            | Self::Gap { .. }
            | Self::MalformedSnapshot { .. }
            | Self::ForeignNames { .. }
            | Self::SizeDiffers { .. }
            | Self::TimestampsDiffer { .. }
            | Self::Vanished { .. }
            | Self::Truncated { .. } => None,
        }
    }
}

/// Why the walk stopped. Damage in the replica is a [Finding], not this.
#[derive(Debug, Error)]
pub enum VerifyError {
    /// The replica could not be listed completely.
    #[error("listing the replica: {0}")]
    Inventory(#[source] InventoryError),
    /// A read failed in a way that says nothing about the file.
    #[error("reading the replica: {0}")]
    Store(#[source] StoreError),
}

/// What one level holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LevelSummary {
    files: usize,
    bytes: u64,
    range: Option<TxidRange>,
    skipped: usize,
}

impl LevelSummary {
    /// Number of LTX files.
    #[must_use]
    pub const fn files(&self) -> usize {
        self.files
    }

    /// Their listed sizes added up.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// From the first transaction of any file to the last of any file.
    #[must_use]
    pub const fn range(&self) -> Option<TxidRange> {
        self.range
    }

    /// Foreign names.
    #[must_use]
    pub const fn skipped(&self) -> usize {
        self.skipped
    }

    fn of(inventory: &Inventory, level: Level) -> Self {
        let files = || {
            inventory
                .files()
                .iter()
                .filter(move |info| info.key.level() == level)
        };
        let min = files().map(|info| info.key.range().min()).min();
        let max = files().map(|info| info.key.range().max()).max();
        Self {
            files: files().count(),
            bytes: files().fold(0, |sum, info| sum.saturating_add(info.size)),
            range: min.zip(max).and_then(|(min, max)| TxidRange::new(min, max)),
            skipped: inventory.skipped(level),
        }
    }
}

/// What [verify] found.
#[derive(Debug)]
pub struct Report {
    levels: [LevelSummary; 10],
    findings: Vec<Finding>,
    worst: Option<Severity>,
    latest: PlanOutcome,
    files_read: usize,
    bytes_read: u64,
}

impl Report {
    /// One summary per level, indexed by level number.
    #[must_use]
    pub const fn levels(&self) -> &[LevelSummary; 10] {
        &self.levels
    }

    /// Everything found, in the order found, at most [MAX_FINDINGS] and a
    /// final [Finding::Truncated] when more were dropped.
    #[must_use]
    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// What a restore of the latest state would do.
    #[must_use]
    pub const fn latest(&self) -> &PlanOutcome {
        &self.latest
    }

    /// Files opened for reading; always 0 at [Depth::Names].
    #[must_use]
    pub const fn files_read(&self) -> usize {
        self.files_read
    }

    /// Bytes read from the store.
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// The most severe finding, dropped ones included. A latest state that
    /// does not plan is [Severity::Unrestorable] whatever the findings say.
    #[must_use]
    pub fn worst(&self) -> Option<Severity> {
        let planless = !matches!(self.latest, PlanOutcome::Plan(_));
        planless
            .then_some(Severity::Unrestorable)
            .into_iter()
            .chain(self.worst)
            .max()
    }
}

/// The findings so far, capped.
#[derive(Default)]
struct Findings {
    kept: Vec<Finding>,
    dropped: usize,
    worst: Option<Severity>,
    /// Files with an [Severity::Unrestorable] finding of their own, kept past
    /// the cap so the dry run's duplicate check never depends on it.
    blamed: Vec<FileKey>,
}

impl Findings {
    fn push(&mut self, finding: Finding) {
        self.worst = self.worst.max(Some(finding.severity()));
        if finding.severity() == Severity::Unrestorable
            && let Some(key) = finding.file()
        {
            self.blamed.push(key);
        }
        if self.kept.len() < MAX_FINDINGS {
            self.kept.push(finding);
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }

    fn finish(mut self) -> (Vec<Finding>, Option<Severity>) {
        if self.dropped > 0 {
            self.kept.push(Finding::Truncated { more: self.dropped });
        }
        (self.kept, self.worst)
    }
}

/// Checks the replica [store] holds.
///
/// # Errors
/// [VerifyError] when the walk cannot go on; damage is in the [Report].
pub async fn verify<S: ObjectStore>(store: &S, depth: Depth) -> Result<Report, VerifyError> {
    let inventory = Inventory::load(store)
        .await
        .map_err(VerifyError::Inventory)?;
    let mut findings = Findings::default();
    let latest = name_findings(&inventory, &mut findings);
    let (mut files_read, mut bytes_read) = (0, 0);
    if depth == Depth::Contents {
        let plan = match &latest {
            PlanOutcome::Plan(chain) => chain.files(),
            PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }
            | PlanOutcome::Gap { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. } => &[],
        };
        let reads = contents_pass(store, &inventory, plan, &mut findings).await?;
        (files_read, bytes_read) = reads;
    }
    let (findings, worst) = findings.finish();
    Ok(Report {
        levels: Level::ALL.map(|level| LevelSummary::of(&inventory, level)),
        findings,
        worst,
        latest,
        files_read,
        bytes_read,
    })
}

/// Reads every file, then dry-runs the latest restore when there is a plan
/// (without one the report is already unrestorable). Returns the files and
/// bytes really read.
async fn contents_pass<S: ObjectStore>(
    store: &S,
    inventory: &Inventory,
    plan: &[FileInfo],
    findings: &mut Findings,
) -> Result<(usize, u64), VerifyError> {
    let mut walk = Walk::new(plan);
    // Snapshots first, so the page size every other file is held to is the one
    // a restore starts from.
    let (snapshots, rest): (Vec<_>, Vec<_>) = inventory
        .files()
        .iter()
        .partition(|info| info.key.level().is_snapshot());
    for info in snapshots.into_iter().chain(rest) {
        walk.file(store, info, findings).await?;
    }
    walk.finish(findings);
    let (mut files_read, mut bytes_read) = (walk.files_read, walk.bytes_read);
    if let Some(last) = plan.last() {
        let dry = latest_restore(store, last.key).await?;
        files_read = files_read.saturating_add(dry.files_read);
        bytes_read = bytes_read.saturating_add(dry.bytes_read);
        if let Some((key, error)) = dry.fault
            && !key.is_some_and(|key| findings.blamed.contains(&key))
        {
            findings.push(Finding::LatestRestoreFails { key, error });
        }
    }
    Ok((files_read, bytes_read))
}

/// Everything the listing alone shows. Returns the plan for the latest state.
fn name_findings(inventory: &Inventory, findings: &mut Findings) -> PlanOutcome {
    for level in Level::ALL {
        let count = inventory.skipped(level);
        if count > 0 {
            findings.push(Finding::ForeignNames { level, count });
        }
    }
    for key in inventory.malformed_snapshots() {
        findings.push(Finding::MalformedSnapshot { key });
    }
    // Every level-9 file that starts a chain is a restore point of its own, so
    // each gets a window. A lower-level file from transaction 1 can also be the
    // base, but it is no restore point: a stray one beside a newer snapshot
    // would only report history nobody needs.
    let snapshots: Vec<FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| info.key.level().is_snapshot() && is_snapshot_start(info))
        .copied()
        .collect();
    let newest_start = inventory
        .files()
        .iter()
        .filter(|info| is_snapshot_start(info))
        .max_by_key(|info| (info.key.range().max(), info.key.level()));
    for pair in snapshots.windows(2) {
        if let [base, next] = pair {
            older_history(inventory, base, next, findings);
        }
    }
    let latest = plan(inventory, RestoreTarget::Latest);
    match &latest {
        PlanOutcome::NoSnapshot { .. } => findings.push(Finding::NoSnapshot),
        PlanOutcome::Gap {
            have_through,
            next_starts,
        } => {
            if let Some(newest) = newest_start {
                findings.push(Finding::Gap {
                    snapshot: newest.key,
                    have_through: *have_through,
                    next_starts: *next_starts,
                    at_tip: true,
                });
            }
        }
        // None of these is a damage finding of its own: the first two are not
        // possible for the latest state, the rest are already findings or a
        // healthy plan.
        PlanOutcome::Plan(_)
        | PlanOutcome::EmptyReplica
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. } => {}
    }
    latest
}

/// The history from [base] up to where [next] takes over, planned with every
/// other snapshot hidden so [base] is what the chain starts from.
fn older_history(inventory: &Inventory, base: &FileInfo, next: &FileInfo, findings: &mut Findings) {
    let only_base = inventory
        .files()
        .iter()
        .filter(|info| !info.key.level().is_snapshot() || info.key == base.key)
        .copied();
    // A subset of a conflict-free inventory has no conflict.
    let Ok(hidden) = Inventory::from_files(only_base) else {
        return;
    };
    let target = RestoreTarget::Txid(next.key.range().max());
    if let PlanOutcome::Gap {
        have_through,
        next_starts,
    } = plan(&hidden, target)
    {
        findings.push(Finding::Gap {
            snapshot: base.key,
            have_through,
            next_starts,
            at_tip: false,
        });
    }
}

/// The neighbour a file in the same level is compared with.
#[derive(Clone, Copy)]
struct Before {
    key: FileKey,
    level: Level,
    through: Txid,
    post_apply: Option<Checksum>,
}

/// How the headers' timestamps differ from the store's.
#[derive(Default)]
struct Skew {
    files: u32,
    first: Option<FileKey>,
    largest_millis: u64,
}

/// State carried from one file to the next, and the read totals.
struct Walk<'a> {
    /// The files the latest restore reads, oldest first; empty without a plan.
    plan: &'a [FileInfo],
    page_sizes: Vec<(FileKey, PageSize)>,
    skew: Skew,
    before: Option<Before>,
    files_read: usize,
    bytes_read: u64,
}

/// What reading one file produced.
struct Read {
    header: Option<Header>,
    decoded: Result<lsr_ltx::Verified, DecodeError>,
    bytes: u64,
}

impl<'a> Walk<'a> {
    fn new(plan: &'a [FileInfo]) -> Self {
        Self {
            plan,
            page_sizes: Vec::new(),
            skew: Skew::default(),
            before: None,
            files_read: 0,
            bytes_read: 0,
        }
    }

    fn in_plan(&self, key: FileKey) -> bool {
        self.plan.iter().any(|info| info.key == key)
    }

    /// The file the latest restore applies right before `key`.
    fn plan_previous(&self, key: FileKey) -> Option<FileKey> {
        let at = self.plan.iter().position(|info| info.key == key)?;
        self.plan.get(at.checked_sub(1)?).map(|info| info.key)
    }

    /// What only the whole walk can say: page sizes against the plan's
    /// snapshot, and the one timestamp note.
    fn finish(&self, findings: &mut Findings) {
        let base = self.plan.first().map(|info| info.key);
        let reference = self
            .page_sizes
            .iter()
            .find(|(key, _)| Some(*key) == base)
            .or_else(|| self.page_sizes.first())
            .map(|(_, size)| *size);
        for (key, found) in &self.page_sizes {
            if let Some(expected) = reference.filter(|expected| expected != found) {
                findings.push(Finding::PageSizeDiffers {
                    key: *key,
                    expected,
                    found: *found,
                    in_latest_plan: self.in_plan(*key),
                });
            }
        }
        if let Some(first) = self.skew.first {
            findings.push(Finding::TimestampsDiffer {
                files: self.skew.files,
                first,
                largest: Duration::from_millis(self.skew.largest_millis),
            });
        }
    }

    async fn file<S: ObjectStore>(
        &mut self,
        store: &S,
        info: &FileInfo,
        findings: &mut Findings,
    ) -> Result<(), VerifyError> {
        let key = info.key;
        let Some(read) = self.read(store, &key).await? else {
            findings.push(Finding::Vanished { key });
            self.before = None;
            return Ok(());
        };
        if let Some(header) = &read.header {
            self.check_header(info, header, findings);
        }
        let in_latest_plan = self.in_plan(key);
        match &read.decoded {
            Ok(verified) => {
                if read.bytes != info.size {
                    findings.push(Finding::SizeDiffers {
                        key,
                        listed: info.size,
                        read: read.bytes,
                    });
                }
                self.check_chain(&key, verified, findings);
            }
            Err(_) => self.before = None,
        }
        if let Err(error) = read.decoded {
            findings.push(Finding::Unreadable {
                key,
                error,
                in_latest_plan,
            });
        }
        Ok(())
    }

    /// Reads the whole file through one decoder. [None] when it is gone.
    async fn read<S: ObjectStore>(
        &mut self,
        store: &S,
        key: &FileKey,
    ) -> Result<Option<Read>, VerifyError> {
        let mut decoder = Decoder::new();
        let mut offset = 0_u64;
        let mut opened = false;
        let mut failure = None;
        while failure.is_none() {
            let chunk = match store.read(key, offset, chunk_len()).await {
                Ok(chunk) => chunk,
                Err(StoreError::NotFound { .. }) => return Ok(None),
                Err(err) => return Err(VerifyError::Store(err)),
            };
            if !opened {
                opened = true;
                self.files_read = self.files_read.saturating_add(1);
            }
            // The listed size is advisory: only the decoder knows where a file ends.
            if chunk.is_empty() {
                break;
            }
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            offset = offset.saturating_add(len);
            self.bytes_read = self.bytes_read.saturating_add(len);
            failure = feed_all(&mut decoder, &chunk).err();
        }
        let header = decoder.header().copied();
        let decoded = match failure {
            Some(error) => Err(error),
            None => decoder.finish(),
        };
        Ok(Some(Read {
            header,
            decoded,
            bytes: offset,
        }))
    }

    fn check_header(&mut self, info: &FileInfo, header: &Header, findings: &mut Findings) {
        let key = info.key;
        let in_latest_plan = self.in_plan(key);
        if key.level().is_snapshot() && !header.is_snapshot() {
            findings.push(Finding::NotASnapshot {
                key,
                in_latest_plan,
            });
        }
        if header.txids() != key.range() {
            findings.push(Finding::RangeMismatch {
                key,
                header: header.txids(),
                in_latest_plan,
            });
        }
        self.page_sizes.push((key, header.page_size()));
        let stamped = header.fields().timestamp.unix_millis();
        let stored = info.created_at.unix_millis();
        if stamped != stored {
            self.skew.files = self.skew.files.saturating_add(1);
            self.skew.first.get_or_insert(key);
            self.skew.largest_millis = self.skew.largest_millis.max(stamped.abs_diff(stored));
        }
    }

    /// Compares a file with the one before it in its level, then becomes the
    /// one before. An overlapping file's pre-apply checksum is the state
    /// before its own first transaction, so only a file that starts right
    /// after its neighbour can be compared, as the restore does.
    fn check_chain(
        &mut self,
        key: &FileKey,
        verified: &lsr_ltx::Verified,
        findings: &mut Findings,
    ) {
        let header = verified.header();
        let level = key.level();
        let contiguous = |before: &Before| {
            before.level == level && before.through.next() == Some(header.txids().min())
        };
        if let Some(before) = self.before.as_ref().filter(|before| contiguous(before))
            && let (Some(expected), Some(found)) = (before.post_apply, header.fields().pre_apply)
            && expected != found
        {
            findings.push(Finding::ChecksumChain {
                key: *key,
                expected,
                found,
                in_latest_plan: self.plan_previous(*key) == Some(before.key),
            });
        }
        self.before = Some(Before {
            key: *key,
            level,
            through: header.txids().max(),
            post_apply: verified.trailer().post_apply(),
        });
    }
}

fn feed_all(decoder: &mut Decoder, chunk: &[u8]) -> Result<(), DecodeError> {
    let mut rest = chunk;
    while !rest.is_empty() {
        let (used, _event) = decoder.feed(rest)?;
        rest = rest.get(used..).unwrap_or_default();
    }
    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "4 MiB is inside ReadLen's bound; a test pins it"
)]
fn chunk_len() -> ReadLen {
    ReadLen::new(CHUNK_BYTES).expect("CHUNK_BYTES is within ReadLen::MAX_RAW")
}
