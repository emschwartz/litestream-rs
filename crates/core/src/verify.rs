//! The replica checker: what a restore would trip over, found without restoring.
//!
//! [verify] loads one [Inventory] and reports everything wrong with it as
//! [Finding]s, each with a [Severity]. [Depth::Names] makes list calls only:
//! foreign names, level-9 files that cannot start a chain, and holes in the
//! history. For every valid snapshot in turn the planner is asked for the next
//! snapshot's last transaction (with the newer snapshots hidden, so the older
//! one is the base), and for the newest snapshot it is asked for the latest
//! state; each [PlanOutcome::Gap] is one finding, so a hole in older history
//! is reported, not only the one at the tip.
//!
//! [Depth::Contents] also reads every file through one [Decoder], so a file's
//! checksums, index and framing are checked, and compares what the header says
//! with what the name and the listing say. Memory is one decoder and one chunk
//! whatever the replica's size.
//!
//! Only what stops the walk is an error ([VerifyError]): a failed listing or a
//! failed read. A damaged file is a finding, never an error, and a file that
//! vanished between the listing and the read (compaction or retention ran) is
//! a note. Findings are capped at [MAX_FINDINGS]; the last one then says how
//! many were dropped, and [Report::worst] still counts them.

use lsr_ltx::{Checksum, DecodeError, Decoder, Header, PageSize, Timestamp, Txid, TxidRange};
use thiserror::Error;

use crate::inventory::{Inventory, InventoryError, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::{PlanOutcome, RestoreTarget, plan};
use crate::store::{ObjectStore, ReadLen, StoreError};

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
    /// Read and decode every file.
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
    },
    /// The header covers other transactions than the name says.
    RangeMismatch {
        /// The file.
        key: FileKey,
        /// What the header covers.
        header: TxidRange,
    },
    /// A level-9 file whose header is not a whole-database snapshot.
    NotASnapshot {
        /// The file.
        key: FileKey,
    },
    /// The page size differs from the first file read.
    PageSizeDiffers {
        /// The file.
        key: FileKey,
        /// The first file's page size.
        expected: PageSize,
        /// This file's.
        found: PageSize,
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
    /// The header's timestamp differs from the store's; as-of restores go by
    /// the stored one.
    TimestampDiffers {
        /// The file.
        key: FileKey,
        /// The header's.
        header: Timestamp,
        /// The store's.
        stored: Timestamp,
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
            | Self::Unreadable { .. }
            | Self::RangeMismatch { .. }
            | Self::NotASnapshot { .. }
            | Self::PageSizeDiffers { .. }
            | Self::ChecksumChain { .. }
            | Self::Gap { at_tip: true, .. } => Severity::Unrestorable,
            Self::Gap { at_tip: false, .. }
            | Self::MalformedSnapshot { .. }
            | Self::SizeDiffers { .. }
            | Self::TimestampDiffers { .. } => Severity::Degraded,
            Self::ForeignNames { .. } | Self::Vanished { .. } | Self::Truncated { .. } => {
                Severity::Note
            }
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
}

impl Findings {
    fn push(&mut self, finding: Finding) {
        self.worst = self.worst.max(Some(finding.severity()));
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
    let mut walk = Walk::default();
    if depth == Depth::Contents {
        // Snapshots first, so the page size every other file is held to is
        // the one a restore starts from.
        let (snapshots, rest): (Vec<_>, Vec<_>) = inventory
            .files()
            .iter()
            .partition(|info| info.key.level().is_snapshot());
        for info in snapshots.into_iter().chain(rest) {
            walk.file(store, info, &mut findings).await?;
        }
    }
    let (findings, worst) = findings.finish();
    Ok(Report {
        levels: Level::ALL.map(|level| LevelSummary::of(&inventory, level)),
        findings,
        worst,
        latest,
        files_read: walk.files_read,
        bytes_read: walk.bytes_read,
    })
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
    let snapshots: Vec<FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| is_snapshot_start(info))
        .copied()
        .collect();
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
            if let Some(newest) = snapshots.last() {
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
    level: Level,
    through: Txid,
    post_apply: Option<Checksum>,
}

/// State carried from one file to the next, and the read totals.
#[derive(Default)]
struct Walk {
    page_size: Option<PageSize>,
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

impl Walk {
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
            findings.push(Finding::Unreadable { key, error });
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
        if key.level().is_snapshot() && !header.is_snapshot() {
            findings.push(Finding::NotASnapshot { key });
        }
        if header.txids() != key.range() {
            findings.push(Finding::RangeMismatch {
                key,
                header: header.txids(),
            });
        }
        let expected = *self.page_size.get_or_insert(header.page_size());
        if expected != header.page_size() {
            findings.push(Finding::PageSizeDiffers {
                key,
                expected,
                found: header.page_size(),
            });
        }
        let stamped = header.fields().timestamp;
        if stamped != info.created_at {
            findings.push(Finding::TimestampDiffers {
                key,
                header: stamped,
                stored: info.created_at,
            });
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
            });
        }
        self.before = Some(Before {
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
