//! The snapshot liveness oracle: a snapshot needs a fault that justifies it.
//!
//! A snapshot is never wrong about content, so the other oracles pass a
//! run whatever it snapshots. It is a cost: the whole database uploaded and
//! kept for the retention window. An engine that snapshots on every boot, or
//! on a rule that misfires, loses nothing a restore can see and pays on every
//! restart. So each snapshot cause the engine announces (`Booted` with a
//! cause, `Resnapshot`) or stages (an operator's request) is judged here
//! against what happened to the world since the evidence it overturns:
//! [`justifies`] maps every [`SnapshotCauseKind`] to the faults and steps
//! that can make it necessary, and a cause with none of them in the window
//! fails the run with [`Violation::UnjustifiedSnapshot`].
//!
//! The window. Each justifier is forgotten once the engine has moved past it
//! ([`Justifier::forgotten_at`]): one about the WAL or a power loss once a
//! level-0 file is staged after it, since the next boot or step compares the
//! WAL with that file; one about the replica or the staged files themselves
//! once an upload brings the replica up to the newest staged file, since an
//! upload's read or a listing is what finds a damaged staged file or a
//! divergence, and an upload that only repairs an older file proves nothing
//! about the rest; a request once its snapshot is staged. What a tick noted itself
//! (a fault that hit in it, staged files it dropped) outlives that tick's
//! own staging and uploads, whose order inside the tick is not known.
//!
//! Restore points given up: staged level-0 files the engine deleted that the
//! replica never took, or that lie past the newest transaction it holds now
//! (it rolled back), so no restore can reach them. A prune of files the
//! replica took, and since compacted or let go by retention, is not one. Nor
//! is the delete of a file at or below the replica's newest transaction when
//! a replica file ends at that transaction (another writer's file there was
//! compacted, so a restore reaches it), or when the same key was counted
//! before (a staging backup put back a file already given up: deleting it
//! again loses nothing new). Seeds 0xf4a87, 0xf47e6, 0xf4bab and 0xf44c6
//! failed on the double count and the other writer's transaction.
//! Each is counted (`liveness.given_up`) and judged like a snapshot: one with
//! no fault in the window that could force it fails the run with
//! [`Violation::UnjustifiedGiveUp`]. A give-up is then a
//! [`Justifier::StagedDropped`] for what follows, since the newest staged
//! file moved back.
//!
//! The engine's own per-cause counter ([`lsr_core::Actor::snapshots`]) is
//! checked too: on a tick that ends with no staging fault, it grows by at
//! least the staged files with no WAL range and at most the staged files that
//! hold every page.

use std::collections::BTreeSet;

use lsr_core::{FileKey, SnapshotCauseKind, SnapshotCounts};

use crate::generate::Op;
use crate::oracle::Violation;
use crate::reach::{Reach, variant};
use crate::store::RecorderEvent;

/// Something that happened to the world that can make a snapshot necessary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Justifier {
    /// The run began: nothing is staged and nothing replicated.
    RunStart,
    /// The machine lost power: unsynced WAL frames and staged files may be gone.
    PowerLoss,
    /// A power loss took or tore staged files.
    StagedLost,
    /// The staging directory was changed outside the engine.
    StagingEdited,
    /// A scheduled staging fault hit a call.
    StagingFault,
    /// A fault failed a step, or a boot ran under a wal-index reset ahead of
    /// the WAL header, so the position proves no continuity until the next
    /// step stages a file.
    PositionUnproven,
    /// The engine deleted staged files the replica never took.
    StagedDropped,
    /// The replica was changed outside the engine.
    RemoteEdited,
    /// A scheduled replica fault hit a call.
    RemoteFault,
    /// The WAL, the wal-index, the database file or our SQLite handles were
    /// damaged or failed outside SQLite's own rules.
    WalDamaged,
    /// The WAL's header names a new generation.
    WalRestarted,
    /// The WAL file is gone or holds no whole header.
    WalEmptied,
    /// The WAL changed generation while our read transaction was not held.
    /// A change under a hold on read mark zero is no reason: the hold blocks
    /// every backfill, so no commit can go with the old generation and the
    /// engine follows it (`lsr_core::recover`, rule 6; seeds 0xe and
    /// 0xf4846 snapshotted there before).
    ReadLockLapsed,
    /// The process stopped with a staged file the replica does not hold
    /// that the boot's witness cannot prove from the live WAL: a snapshot
    /// with a page taken from the database file that has a WAL frame since,
    /// or a hop into a new WAL generation, which only the running position
    /// that held the read lock across it could prove. The boot drops it and
    /// snapshots (`StagedUnverified`), giving up its restore points. A known
    /// cost of the witness, not a fault: the local engine journal is to record
    /// these proofs at staging time and remove it. Counted on its own
    /// (`liveness.pending_unprovable.*`, `liveness.pending_unprovable_*`) so
    /// the number stays visible.
    PendingUnprovable,
    /// The operator asked for a snapshot.
    SnapshotRequested,
    /// The disk lies about the engine's own fsync of the WAL: a power loss
    /// can then take what was staged from it. The lie lasts the run.
    LyingDisk,
}

/// When a justifier stops counting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Forgotten {
    /// Once a level-0 file is staged after it.
    AtStaging,
    /// Once the replica takes a level-0 file that reaches the newest staged
    /// one: the replica holds everything staging does.
    AtUpload,
    /// Once an operator's snapshot is staged.
    AtRequestedSnapshot,
    /// Never: the condition lasts the run.
    Never,
}

impl Justifier {
    /// When this justifier stops counting; see the module docs.
    #[must_use]
    pub const fn forgotten_at(self) -> Forgotten {
        match self {
            Self::RemoteEdited
            | Self::RemoteFault
            | Self::StagedLost
            | Self::StagingEdited
            | Self::StagingFault
            | Self::PendingUnprovable => Forgotten::AtUpload,
            Self::SnapshotRequested => Forgotten::AtRequestedSnapshot,
            Self::LyingDisk => Forgotten::Never,
            Self::RunStart
            | Self::PowerLoss
            | Self::StagedDropped
            | Self::WalDamaged
            | Self::WalRestarted
            | Self::WalEmptied
            | Self::ReadLockLapsed
            | Self::PositionUnproven => Forgotten::AtStaging,
        }
    }
}

/// What can force the engine to delete staged files the replica never took.
/// The WAL's own changes are here because the boot's witness deletes every
/// pending staged file the live WAL cannot vouch for (`StagedUnverified`): one
/// staged before a restart, an emptied WAL or damage can no longer be proved.
const GIVE_UP: &[Justifier] = &[
    Justifier::StagingEdited,
    Justifier::StagingFault,
    Justifier::StagedLost,
    Justifier::PowerLoss,
    Justifier::LyingDisk,
    Justifier::RemoteEdited,
    Justifier::RemoteFault,
    Justifier::WalRestarted,
    Justifier::WalEmptied,
    Justifier::WalDamaged,
];

/// What can make the boot's witness delete staged files the live WAL cannot
/// vouch for (`StagedUnverified`): staging edited or torn, the WAL they came
/// from changed or lost what they hold, or the replica was rolled back so files
/// it had taken are pending again against a WAL that has moved on (0x83).
const STAGED_UNVERIFIED: &[Justifier] = &[
    Justifier::RemoteEdited,
    Justifier::StagingEdited,
    Justifier::StagedLost,
    Justifier::StagingFault,
    Justifier::PowerLoss,
    Justifier::LyingDisk,
    Justifier::WalDamaged,
    Justifier::WalRestarted,
    Justifier::WalEmptied,
];

/// What can make a snapshot for `cause` necessary, announced at a boot
/// (`at_boot`) or by a running engine; `None` for a cause that is the engine's
/// own design rather than a response to the world (a TRUNCATE checkpoint
/// always snapshots), which is counted but never judged.
#[must_use]
pub const fn justifies(cause: SnapshotCauseKind, at_boot: bool) -> Option<&'static [Justifier]> {
    use Justifier::{
        LyingDisk, PositionUnproven, PowerLoss, ReadLockLapsed, RemoteEdited, RemoteFault,
        RunStart, SnapshotRequested, StagedDropped, StagedLost, StagingEdited, StagingFault,
        WalDamaged, WalEmptied, WalRestarted,
    };
    Some(match cause {
        // Nothing staged and nothing on the replica to continue from.
        SnapshotCauseKind::FirstSync => &[
            RunStart,
            StagingEdited,
            StagedLost,
            StagingFault,
            StagedDropped,
        ],
        // The replica holds more than staging: staging lost files, or the
        // replica took files staging never had.
        SnapshotCauseKind::BehindRemote => &[
            StagingEdited,
            StagedLost,
            StagingFault,
            StagedDropped,
            RemoteEdited,
            RemoteFault,
        ],
        // The newest staged file does not read, or staged files were
        // dropped because the replica diverged from them.
        SnapshotCauseKind::NewestStagedFileUnreadable => &[
            StagingEdited,
            StagedLost,
            StagingFault,
            RemoteEdited,
            RemoteFault,
        ],
        SnapshotCauseKind::WalMissing => &[WalDamaged, WalEmptied],
        // The WAL restarted while no proof of continuity held: across a
        // boot (a boot never holds one), after a fault left the position
        // unproven, or against a newest staged file from an older generation
        // (a backup put back, staged files dropped).
        SnapshotCauseKind::GenerationChanged if at_boot => &[
            WalRestarted,
            WalEmptied,
            WalDamaged,
            StagingEdited,
            StagingFault,
            PositionUnproven,
            StagedDropped,
        ],
        // A running engine holds its read lock and pins the restarts it sees
        // follow it: a restart alone is no reason, one after the lock lapsed,
        // a fault or damage is.
        SnapshotCauseKind::GenerationChanged => &[
            ReadLockLapsed,
            WalEmptied,
            WalDamaged,
            StagingEdited,
            StagingFault,
            PositionUnproven,
            StagedDropped,
        ],
        // The WAL at the position is not what the newest staged file says.
        SnapshotCauseKind::PrevFrameMismatch | SnapshotCauseKind::LastPageMismatch => &[
            WalDamaged,
            WalRestarted,
            PowerLoss,
            StagingEdited,
            StagedLost,
            StagedDropped,
        ],
        // The engine syncs the WAL before it stages from it, so an honest
        // power loss cannot take what was staged: only a disk that lies about
        // that sync, damage, or a staging backup put back can.
        SnapshotCauseKind::LocalRewound => &[LyingDisk, WalDamaged, StagingEdited],
        SnapshotCauseKind::StagedUnverified => STAGED_UNVERIFIED,
        // The engine's fsync of the WAL failed (a fault failed the step), and
        // a TRUNCATE checkpoint then moved the unstaged frames into the
        // database file.
        SnapshotCauseKind::WalSyncFailed => &[PositionUnproven, WalDamaged, LyingDisk],
        SnapshotCauseKind::CheckpointBoundary => return None,
        SnapshotCauseKind::OperatorRequested => &[SnapshotRequested],
    })
}

/// What an operation, applied, says by itself. Crashes, store faults that
/// hit and changes to the WAL's header are noted where they happen instead.
#[must_use]
pub const fn of_op(op: &Op) -> &'static [Justifier] {
    match op {
        Op::Meta(_) | Op::StagingDamage(_) | Op::EnvFault(crate::EnvFault::WipeStaging) => {
            &[Justifier::StagingEdited]
        }
        Op::EnvFault(_) | Op::Wal(_) | Op::WalDamage { .. } => &[Justifier::WalDamaged],
        Op::Remote(_) | Op::SecondWriter { .. } => &[Justifier::RemoteEdited],
        // A lasting fault, noted as it begins and again as it is lifted.
        Op::Hold { hold, .. } => match hold {
            crate::generate::Hold::StoreDown(_)
            | crate::generate::Hold::CredentialsRevoked
            | crate::generate::Hold::LaggingListing
            | crate::generate::Hold::LyingReads => &[Justifier::RemoteFault],
            crate::generate::Hold::DiskFull => &[Justifier::StagingFault],
            crate::generate::Hold::Wal(_) => &[Justifier::WalDamaged],
            crate::generate::Hold::LyingDisk => &[Justifier::LyingDisk],
            crate::generate::Hold::ReadersHeld => &[],
        },
        Op::OurReader { begin: false } => &[Justifier::ReadLockLapsed],
        Op::RequestSnapshot => &[Justifier::SnapshotRequested],
        Op::LyingWalSync => &[Justifier::LyingDisk],
        Op::Commit { .. }
        | Op::CommitInTick { .. }
        | Op::Spill
        | Op::Rollback
        | Op::Vacuum { .. }
        | Op::OtherProcessReader { .. }
        | Op::OtherProcessCheckpoint(_)
        | Op::Crash(_)
        | Op::StoreFault { .. }
        | Op::ClockJump(_)
        | Op::Tick
        | Op::Shutdown
        | Op::Restore
        | Op::Restart
        | Op::PowerLoss
        | Op::OurReader { begin: true }
        | Op::AppAutocheckpoint(_)
        | Op::Shrink { .. }
        | Op::Heal => &[],
    }
}

/// The oracle's state over one run.
#[derive(Clone, Debug)]
pub struct Liveness {
    window: BTreeSet<Justifier>,
    /// What the tick under way noted itself: kept past its own staging.
    in_tick: BTreeSet<Justifier>,
    /// The engine's counter as the last tick of this incarnation left it.
    counted: SnapshotCounts,
    /// Level-0 keys the replica took.
    uploaded: BTreeSet<FileKey>,
    /// Level-0 keys already counted as given up.
    lost: BTreeSet<FileKey>,
    staging_seen: usize,
    remote_seen: usize,
    /// The WAL header's salts when last looked at.
    wal: Option<(u32, u32)>,
    /// Whether our read transaction pinned the WAL when last looked at.
    pinned: bool,
    /// A justified snapshot was announced and is not staged yet: a crash
    /// before it is staged makes the next boot announce it again.
    owed: bool,
}

impl Default for Liveness {
    fn default() -> Self {
        Self::new()
    }
}

impl Liveness {
    /// The state at the start of a run.
    #[must_use]
    pub fn new() -> Self {
        Self {
            window: BTreeSet::from([Justifier::RunStart]),
            in_tick: BTreeSet::new(),
            counted: SnapshotCounts::default(),
            uploaded: BTreeSet::new(),
            lost: BTreeSet::new(),
            staging_seen: 0,
            remote_seen: 0,
            wal: None,
            pinned: false,
            owed: false,
        }
    }

    /// Notes that `why` happened.
    pub fn note(&mut self, why: Justifier) {
        self.window.insert(why);
    }

    /// Notes that `why` happened inside the tick under way.
    pub fn note_in_tick(&mut self, why: Justifier) {
        self.window.insert(why);
        self.in_tick.insert(why);
    }

    /// Notes what `op` says by itself.
    pub fn note_op(&mut self, op: &Op) {
        for why in of_op(op) {
            self.note(*why);
        }
    }

    /// Looks at the WAL's header (`crate::SimSqlite::wal_header_salts`) and
    /// whether our read transaction pins it
    /// (`crate::SimSqlite::our_reader_pins_wal`), and notes how the header
    /// changed since the last look: a restart while nothing pinned the WAL
    /// is one the engine could not follow.
    pub fn see_wal(&mut self, now: Option<(u32, u32)>, pinned: bool) {
        if !self.pinned && now.is_some() && now != self.wal {
            self.note(Justifier::ReadLockLapsed);
        }
        self.note_wal(self.wal, now);
        self.wal = now;
        self.pinned = pinned;
    }

    /// As [`Self::see_wal`], after a tick: its order against what the tick
    /// staged is not known, so the change outlives that staging.
    pub fn see_wal_after_tick(&mut self, now: Option<(u32, u32)>, pinned: bool) {
        let before = self.window.clone();
        self.see_wal(now, pinned);
        for why in self.window.difference(&before).copied().collect::<Vec<_>>() {
            self.in_tick.insert(why);
        }
    }

    /// Notes a change of the WAL's header across a step.
    pub fn note_wal(&mut self, before: Option<(u32, u32)>, after: Option<(u32, u32)>) {
        match (before, after) {
            (Some(_), None) => self.note(Justifier::WalEmptied),
            (was, Some(now)) if was != Some(now) => self.note(Justifier::WalRestarted),
            (None | Some(_), Some(_)) | (None, None) => {}
        }
    }

    /// A new engine: its counter starts again from zero.
    pub fn rebooted(&mut self) {
        self.counted = SnapshotCounts::default();
    }

    /// Judges a snapshot cause the engine announced.
    ///
    /// # Errors
    /// [`Violation::UnjustifiedSnapshot`] when nothing in the window justifies it.
    pub fn judge(
        &mut self,
        cause: SnapshotCauseKind,
        at_boot: bool,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let Some(needs) = justifies(cause, at_boot) else {
            return Ok(());
        };
        if needs.iter().any(|why| self.window.contains(why)) {
            reach.bump(format!("liveness.justified.{}", variant(&cause)));
            self.owed = true;
            return Ok(());
        }
        if self.owed {
            // The same debt, announced again after a restart, perhaps
            // under the name of what the boot found first.
            reach.bump("liveness.owed");
            return Ok(());
        }
        if cause == SnapshotCauseKind::StagedUnverified
            && at_boot
            && self.window.contains(&Justifier::PendingUnprovable)
        {
            reach.bump("liveness.pending_unprovable_snapshots");
            self.owed = true;
            return Ok(());
        }
        Err(Violation::UnjustifiedSnapshot {
            cause,
            window: self.window.iter().copied().collect(),
        })
    }

    /// Takes in what a tick did to the stores and the engine's counter:
    /// counts restore points given up, checks the counter (when the tick
    /// ended rather than crashed), judges an operator's snapshot when it is
    /// staged, and moves the window past what was staged and uploaded.
    /// `world` answers what the stores hold now.
    ///
    /// # Errors
    /// [`Violation::UnjustifiedSnapshot`], or a `BugFault` when the counter
    /// disagrees with the staging store.
    pub fn after_tick(
        &mut self,
        stores: (&[RecorderEvent], &[RecorderEvent]),
        world: &dyn TickWorld,
        counts: SnapshotCounts,
        ended: bool,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let (staging, remote) = stores;
        let newest_staged = world.newest_staged();
        let caught_up = self
            .take_remote(remote)
            .is_some_and(|took| newest_staged.is_none_or(|newest| took >= newest));
        let staged = self.take_staging(staging, world, reach)?;
        let grew = self.take_counts(counts, reach)?;
        // A file with no WAL range is a snapshot for certain; one with a range
        // that holds every page may be a snapshot or a transaction that wrote
        // every page, so the counter is bounded, not matched, by the two.
        // A staging fault in the tick can fail a step after its file was
        // written: the engine counts only what it believes it staged.
        let checkable = ended && !self.in_tick.contains(&Justifier::StagingFault);
        if checkable && (grew < staged.bare || grew > staged.full.max(staged.bare)) {
            return Err(crate::bug_fault(format!(
                "the engine counted {grew} snapshots staged in a tick that staged {} without a \
                 WAL range and {} holding every page",
                staged.bare, staged.full
            )));
        }
        let requested = counts
            .get(SnapshotCauseKind::OperatorRequested)
            .saturating_sub(self.counted.get(SnapshotCauseKind::OperatorRequested));
        self.counted = counts;
        let snapshots = if ended {
            grew
        } else {
            staged.full.max(staged.bare)
        };
        if snapshots > 0 {
            self.owed = false;
        }
        self.window.retain(|why| match why.forgotten_at() {
            Forgotten::AtStaging => !staged.any,
            Forgotten::AtUpload => !caught_up,
            Forgotten::AtRequestedSnapshot => requested == 0,
            Forgotten::Never => true,
        });
        self.window.append(&mut self.in_tick);
        Ok(())
    }

    /// The newest transaction of the level-0 files the replica took, if any.
    fn take_remote(&mut self, remote: &[RecorderEvent]) -> Option<u64> {
        let mut took = None;
        for event in remote.iter().skip(self.remote_seen) {
            if let RecorderEvent::Put { key, .. } = event
                && key.level().get() == 0
            {
                self.uploaded.insert(*key);
                took = took.max(Some(key.range().max().get()));
            }
        }
        self.remote_seen = remote.len();
        took
    }

    /// How much the engine's counter grew; judges an operator's snapshot.
    fn take_counts(&mut self, counts: SnapshotCounts, reach: &mut Reach) -> Result<u64, Violation> {
        let mut grew = 0_u64;
        let counted = self.counted;
        for ((kind, now), (_, was)) in counts.iter().zip(counted.iter()) {
            let more = now.saturating_sub(was);
            grew = grew.saturating_add(more);
            if more > 0 {
                reach.add(format!("snapshot.staged.{}", variant(&kind)), more);
                if kind == SnapshotCauseKind::OperatorRequested {
                    self.judge(kind, false, reach)?;
                }
            }
        }
        Ok(grew)
    }

    /// What the tick staged at level 0; counts and notes the staged files
    /// deleted before the replica took them.
    fn take_staging(
        &mut self,
        staging: &[RecorderEvent],
        world: &dyn TickWorld,
        reach: &mut Reach,
    ) -> Result<Staged, Violation> {
        let mut staged = Staged::default();
        for event in staging.iter().skip(self.staging_seen) {
            match event {
                RecorderEvent::Put {
                    key,
                    header: Some(_),
                    origin,
                    ..
                } if key.level().get() == 0 => {
                    staged.any = true;
                    if origin.is_none_or(|origin| origin.size() == 0) {
                        staged.bare = staged.bare.saturating_add(1);
                    } else if world.full_image(key) {
                        staged.full = staged.full.saturating_add(1);
                    }
                }
                RecorderEvent::Put { .. } => {}
                RecorderEvent::Delete { keys, .. } => {
                    let newest = world.replica_newest();
                    let lost: Vec<FileKey> = keys
                        .iter()
                        .filter(|key| {
                            key.level().get() == 0
                                && (newest.is_none_or(|newest| key.range().max().get() > newest)
                                    || (!self.uploaded.contains(key)
                                        && !self.lost.contains(key)
                                        && !world.replica_ends_at(key.range().max().get())))
                        })
                        .copied()
                        .collect();
                    self.lost.extend(lost.iter().copied());
                    let lost = lost.len();
                    self.given_up(u64::try_from(lost).unwrap_or(u64::MAX), reach)?;
                }
            }
        }
        self.staging_seen = staging.len();
        Ok(staged)
    }

    /// A boot found the replica past the newest staged file and snapshots
    /// above it (`BehindRemote`): every staged file at or below the replica's
    /// newest transaction that it never took, and that no replica file ends
    /// at, can never be uploaded, so its restore point is given up now, with
    /// the window that justified the snapshot, not later when a prune deletes
    /// it (seed 0xf4cf9).
    ///
    /// # Errors
    /// [`Violation::UnjustifiedGiveUp`] when nothing in the window forces it.
    pub fn passed_by_replica(
        &mut self,
        staged: &[FileKey],
        world: &dyn TickWorld,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let Some(newest) = world.replica_newest() else {
            return Ok(());
        };
        let lost: Vec<FileKey> = staged
            .iter()
            .filter(|key| {
                key.level().get() == 0
                    && key.range().max().get() <= newest
                    && !self.uploaded.contains(key)
                    && !self.lost.contains(key)
                    && !world.replica_ends_at(key.range().max().get())
            })
            .copied()
            .collect();
        self.lost.extend(lost.iter().copied());
        self.given_up(u64::try_from(lost.len()).unwrap_or(u64::MAX), reach)
    }

    /// Counts `lost` restore points given up; fails unless a fault in the
    /// window could force it.
    fn given_up(&mut self, lost: u64, reach: &mut Reach) -> Result<(), Violation> {
        if lost == 0 {
            return Ok(());
        }
        reach.add("liveness.given_up", lost);
        let witness_cost = self.window.contains(&Justifier::PendingUnprovable);
        if witness_cost && !GIVE_UP.iter().any(|why| self.window.contains(why)) {
            reach.add("liveness.pending_unprovable_given_up", lost);
        } else if !GIVE_UP.iter().any(|why| self.window.contains(why)) {
            return Err(Violation::UnjustifiedGiveUp {
                lost,
                window: self.window.iter().copied().collect(),
            });
        }
        self.note_in_tick(Justifier::StagedDropped);
        Ok(())
    }
}

/// What the oracle asks of the stores after a tick.
pub trait TickWorld {
    /// Whether the staged level-0 file `key` holds every page of its database.
    fn full_image(&self, key: &FileKey) -> bool;
    /// The newest staged transaction.
    fn newest_staged(&self) -> Option<u64>;
    /// The newest transaction the replica holds, at any level.
    fn replica_newest(&self) -> Option<u64>;
    /// Whether a file on the replica, at any level, ends at `txid`.
    fn replica_ends_at(&self, txid: u64) -> bool;
}

impl TickWorld for crate::World {
    fn full_image(&self, key: &FileKey) -> bool {
        self.staged_is_full_image(key)
    }

    fn newest_staged(&self) -> Option<u64> {
        crate::World::newest_staged(self)
            .ok()
            .flatten()
            .map(|info| info.key.range().max().get())
    }

    fn replica_newest(&self) -> Option<u64> {
        self.remote_newest()
    }

    fn replica_ends_at(&self, txid: u64) -> bool {
        self.remote_ends_at(txid)
    }
}

/// The stores as the engine sees them: its newest staged transaction may be
/// one an outside edit removed from staging (a backup put back), and an
/// upload has caught up only when it reaches that one too (seed 0xf43fb: the
/// replica took the restored files, the engine then missed its own newest
/// and dropped from it, with the staging edit forgotten).
#[derive(Debug)]
pub struct WithEngine<'a, W> {
    /// The stores.
    pub world: &'a W,
    /// The engine's newest staged transaction.
    pub staged: Option<u64>,
}

impl<W: TickWorld> TickWorld for WithEngine<'_, W> {
    fn full_image(&self, key: &FileKey) -> bool {
        self.world.full_image(key)
    }

    fn newest_staged(&self) -> Option<u64> {
        self.world.newest_staged().max(self.staged)
    }

    fn replica_newest(&self) -> Option<u64> {
        self.world.replica_newest()
    }

    fn replica_ends_at(&self, txid: u64) -> bool {
        self.world.replica_ends_at(txid)
    }
}

/// What one tick staged at level 0.
#[derive(Clone, Copy, Debug, Default)]
struct Staged {
    /// Any file.
    any: bool,
    /// Files with no WAL range: snapshots of a WAL that held nothing.
    bare: u64,
    /// Files with a WAL range that hold every page of their database.
    full: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stores after a staging backup was put back: staging's newest is
    /// 25, the engine's own newest, 26, is gone from it.
    struct Restored;

    impl TickWorld for Restored {
        fn full_image(&self, _: &FileKey) -> bool {
            false
        }
        fn newest_staged(&self) -> Option<u64> {
            Some(25)
        }
        fn replica_newest(&self) -> Option<u64> {
            Some(25)
        }
        fn replica_ends_at(&self, _: u64) -> bool {
            false
        }
    }

    fn remote_put(txid: u64) -> RecorderEvent {
        let zero = lsr_ltx::Timestamp::from_unix_millis(0);
        RecorderEvent::Put {
            key: FileKey::new(
                lsr_core::Level::ZERO,
                lsr_ltx::TxidRange::single(lsr_ltx::Txid::new(txid).unwrap()),
            ),
            header: None,
            origin: None,
            created_at: zero,
            size: 0,
            digest: 0,
            at: zero,
            high: zero,
        }
    }

    /// An upload that reaches staging's newest file has not caught up while
    /// the engine's own newest staged file is past it: the staging edit that
    /// removed that file stays in the window (seed 0xf43fb).
    #[test]
    fn an_upload_short_of_the_engines_newest_staged_file_keeps_a_staging_edit() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        live.window.clear();
        live.note(Justifier::StagingEdited);
        let remote = [remote_put(24), remote_put(25)];
        let engine = WithEngine {
            world: &Restored,
            staged: Some(26),
        };
        live.after_tick(
            (&[], &remote),
            &engine,
            SnapshotCounts::default(),
            false,
            &mut reach,
        )
        .unwrap();
        assert!(live.window.contains(&Justifier::StagingEdited));
        live.remote_seen = 0;
        let caught_up = WithEngine {
            world: &Restored,
            staged: Some(25),
        };
        live.after_tick(
            (&[], &remote),
            &caught_up,
            SnapshotCounts::default(),
            false,
            &mut reach,
        )
        .unwrap();
        assert!(!live.window.contains(&Justifier::StagingEdited));
    }

    #[test]
    fn every_judged_cause_has_a_justifier() {
        for kind in SnapshotCauseKind::ALL {
            for at_boot in [true, false] {
                if let Some(needs) = justifies(kind, at_boot) {
                    assert!(!needs.is_empty(), "{kind:?} can never be justified");
                }
            }
        }
    }

    /// A replica holding up to `newest`, with files ending at `ends`.
    struct Replica {
        newest: u64,
        ends: &'static [u64],
    }

    impl TickWorld for Replica {
        fn full_image(&self, _: &FileKey) -> bool {
            false
        }
        fn newest_staged(&self) -> Option<u64> {
            None
        }
        fn replica_newest(&self) -> Option<u64> {
            Some(self.newest)
        }
        fn replica_ends_at(&self, txid: u64) -> bool {
            self.ends.contains(&txid)
        }
    }

    fn staged_key(txid: u64) -> FileKey {
        let txid = lsr_ltx::Txid::new(txid).unwrap();
        FileKey::new(lsr_core::Level::ZERO, lsr_ltx::TxidRange::single(txid))
    }

    fn deleted(txid: u64) -> RecorderEvent {
        RecorderEvent::Delete {
            keys: vec![staged_key(txid)],
            at: lsr_ltx::Timestamp::from_unix_millis(0),
            high: lsr_ltx::Timestamp::from_unix_millis(0),
        }
    }

    /// Deleting a staged file the replica never took counts once, and not
    /// at all when a replica file ends at its transaction.
    #[test]
    fn a_restore_point_is_given_up_once_and_only_when_no_restore_reaches_it() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        let world = Replica {
            newest: 9,
            ends: &[4, 9],
        };
        live.window.clear();
        live.note(Justifier::StagingEdited);
        let events = [deleted(5)];
        live.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("liveness.given_up"), 1);
        live.window.clear();
        live.staging_seen = 0;
        let events = [deleted(5), deleted(5), deleted(4)];
        live.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("liveness.given_up"), 1);
        live.staging_seen = 0;
        live.take_staging(&[deleted(10)], &world, &mut reach)
            .unwrap_err();
    }

    /// A boot that snapshots above a replica that passed staging gives up
    /// the staged files the replica never took then, once, with its window;
    /// the prune that deletes them later loses nothing new (seed 0xf4cf9).
    #[test]
    fn a_replica_that_passed_staging_gives_up_its_files_at_the_boot() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        let world = Replica {
            newest: 8,
            ends: &[4, 8],
        };
        live.window.clear();
        live.note(Justifier::RemoteEdited);
        let staged = [staged_key(4), staged_key(5), staged_key(6), staged_key(9)];
        live.passed_by_replica(&staged, &world, &mut reach).unwrap();
        assert_eq!(reach.get("liveness.given_up"), 2);
        live.window.clear();
        let events = [deleted(5), deleted(6)];
        live.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("liveness.given_up"), 2);
    }

    #[test]
    fn a_running_engine_needs_more_than_a_restart() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        live.window.clear();
        live.note(Justifier::WalRestarted);
        live.judge(SnapshotCauseKind::GenerationChanged, true, &mut reach)
            .unwrap();
        live.owed = false;
        assert!(
            live.judge(SnapshotCauseKind::GenerationChanged, false, &mut reach)
                .is_err()
        );
        live.note(Justifier::ReadLockLapsed);
        live.judge(SnapshotCauseKind::GenerationChanged, false, &mut reach)
            .unwrap();
    }

    #[test]
    fn a_cause_with_nothing_in_the_window_fails() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        live.judge(SnapshotCauseKind::FirstSync, true, &mut reach)
            .unwrap();
        live.window.clear();
        live.note(Justifier::WalRestarted);
        live.judge(SnapshotCauseKind::GenerationChanged, true, &mut reach)
            .unwrap();
        live.owed = false;
        assert!(matches!(
            live.judge(SnapshotCauseKind::LocalRewound, true, &mut reach),
            Err(Violation::UnjustifiedSnapshot { .. })
        ));
        live.judge(SnapshotCauseKind::CheckpointBoundary, true, &mut reach)
            .unwrap();
    }

    #[test]
    fn a_justified_snapshot_announced_again_before_it_is_staged_passes() {
        let mut live = Liveness::new();
        let mut reach = Reach::default();
        live.window.clear();
        live.note(Justifier::LyingDisk);
        live.judge(SnapshotCauseKind::LocalRewound, true, &mut reach)
            .unwrap();
        live.window.clear();
        live.judge(SnapshotCauseKind::LastPageMismatch, true, &mut reach)
            .unwrap();
        assert_eq!(reach.get("liveness.owed"), 1);
    }

    #[test]
    fn a_wal_header_change_is_noted() {
        let mut live = Liveness::new();
        live.window.clear();
        live.note_wal(Some((1, 2)), Some((1, 2)));
        assert!(live.window.is_empty());
        live.note_wal(Some((1, 2)), Some((2, 7)));
        live.note_wal(Some((2, 7)), None);
        assert_eq!(
            live.window,
            BTreeSet::from([Justifier::WalRestarted, Justifier::WalEmptied])
        );
    }
}
