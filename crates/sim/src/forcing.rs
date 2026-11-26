//! Forcings: what happened to the world that can force the engine's hand.
//!
//! Two checks judge the engine's costly or lossy choices against what is in
//! effect here: [`crate::needless_snapshot`], an efficiency check (a snapshot
//! nothing forced is a needless full upload), and
//! [`crate::oracle::lost_restore_points`], a safety check (staged history given
//! up that nothing forced is history lost). Each [`Forcing`] is noted where it
//! happens: by an operation ([`noted_by_op`]), by a drawn run's own settings
//! ([`noted_by_opts`]), by a fault that hit, or by a change of the WAL's
//! header.
//!
//! The window. Each forcing is forgotten once the engine has moved past it
//! ([`Forcing::forgotten_at`]): one about the WAL or a power loss once a
//! level-0 file is staged after it, since the next boot or step compares the
//! WAL with that file; an unproven position once a staged file reaches the
//! WAL's committed end, since a step that stops short at the size cap proves
//! nothing (`docs/decisions/D283.md`); one about the replica or the staged files themselves
//! once an upload brings the replica up to the newest staged file, since an
//! upload's read or a listing is what finds a damaged staged file or a
//! divergence, and an upload that only repairs an older file proves nothing
//! about the rest, nor does one whose bytes were read before the damage (a
//! file read on one tick and published on a later one, damaged in between):
//! while a staged file is one the replica never took, or has other bytes than
//! the replica's copy, the fault that left it stays; a request once its
//! snapshot is staged. What a tick noted itself
//! (a fault that hit in it, staged files it gave up) outlives that tick's
//! own staging and uploads, whose order inside the tick is not known.
//!
//! A give-up the safety check counted is itself a forcing
//! ([`Forcing::StagedGivenUp`]): the newest staged file moved back, which can
//! force a snapshot after it.

use std::collections::BTreeSet;

use lsr_core::{FileKey, SnapshotCauseKind, SnapshotCounts};

use crate::generate::Op;
use crate::needless_snapshot::NeedlessSnapshot;
use crate::oracle::Violation;
use crate::oracle::lost_restore_points::LostRestorePoints;
use crate::reach::Reach;
use crate::store::RecorderEvent;

/// Something that happened to the world that can make a snapshot or a give-up
/// necessary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Forcing {
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
    /// A fault failed a step, a boot ran under a wal-index reset ahead of
    /// the WAL header, or a boot continued short of a commit it never staged,
    /// so the position proves no continuity until a step stages a file that
    /// reaches the WAL's committed end. A step stopped short by the size cap
    /// proves nothing: the boot's read transaction may sit on mark 0, which
    /// blocks no restart, and a restart then takes the frames past it.
    PositionUnproven,
    /// The engine deleted staged files the replica never took
    /// ([`crate::oracle::lost_restore_points`] counted the give-up).
    StagedGivenUp,
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
    /// engine follows it (`lsr_core::recover_traced`, rule 6; seeds 0xe and
    /// 0xf4846 snapshotted there before). That holds for a position that reached the
    /// committed end in the hold; one still behind it is [`Self::PositionUnproven`].
    ReadLockLapsed,
    /// The operator asked for a snapshot.
    SnapshotRequested,
    /// The disk lies about the engine's own fsync of the WAL: a power loss
    /// can then take what was staged from it. The lie lasts the run.
    LyingDisk,
}

/// When a forcing stops counting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForgottenAt {
    /// Once a level-0 file is staged after it.
    Staging,
    /// Once a level-0 file staged after it reaches the WAL's committed end.
    StagedThrough,
    /// Once the replica takes a level-0 file that reaches the newest staged
    /// one: the replica holds everything staging does.
    Upload,
    /// Once an operator's snapshot is staged.
    RequestedSnapshot,
    /// Never: the condition lasts the run.
    Never,
}

impl Forcing {
    /// When this forcing stops counting; see the module docs.
    #[must_use]
    pub const fn forgotten_at(self) -> ForgottenAt {
        match self {
            Self::RemoteEdited
            | Self::RemoteFault
            | Self::StagedLost
            | Self::StagingEdited
            | Self::StagingFault => ForgottenAt::Upload,
            Self::SnapshotRequested => ForgottenAt::RequestedSnapshot,
            Self::LyingDisk => ForgottenAt::Never,
            Self::RunStart
            | Self::PowerLoss
            | Self::StagedGivenUp
            | Self::WalDamaged
            | Self::WalRestarted
            | Self::WalEmptied
            | Self::ReadLockLapsed => ForgottenAt::Staging,
            Self::PositionUnproven => ForgottenAt::StagedThrough,
        }
    }
}

/// What an operation, applied, says by itself. Crashes, store faults that
/// hit and changes to the WAL's header are noted where they happen instead.
#[must_use]
pub const fn noted_by_op(op: &Op) -> &'static [Forcing] {
    match op {
        Op::Meta(_) | Op::StagingDamage(_) | Op::EnvFault(crate::EnvFault::WipeStaging) => {
            &[Forcing::StagingEdited]
        }
        Op::EnvFault(_) | Op::Wal(_) | Op::WalDamage { .. } => &[Forcing::WalDamaged],
        Op::Remote(_) | Op::SecondWriter { .. } => &[Forcing::RemoteEdited],
        // A provider fault on one request, noted as a store fault that hit is.
        Op::HttpFault(_) => &[Forcing::RemoteFault],
        // A lasting fault, noted as it begins and again as it is lifted.
        Op::Hold { hold, .. } => match hold {
            crate::generate::Hold::StoreDown(_)
            | crate::generate::Hold::StoreDownAll
            | crate::generate::Hold::CredentialsRevoked
            | crate::generate::Hold::LyingReads => &[Forcing::RemoteFault],
            crate::generate::Hold::DiskFull => &[Forcing::StagingFault],
            crate::generate::Hold::Wal(_) => &[Forcing::WalDamaged],
            crate::generate::Hold::LyingDisk => &[Forcing::LyingDisk],
            crate::generate::Hold::ReadersHeld => &[],
            crate::generate::Hold::SecondWriter => &[Forcing::RemoteEdited],
        },
        Op::OurReader { begin: false } => &[Forcing::ReadLockLapsed],
        // The database goes back to an older image: nothing the engine staged vouches for it.
        Op::PutBackDatabase { .. } => &[Forcing::StagingEdited],
        Op::RequestSnapshot => &[Forcing::SnapshotRequested],
        Op::LyingWalSync => &[Forcing::LyingDisk],
        Op::Commit { .. }
        | Op::CommitInTick { .. }
        | Op::OtherConnectionCommit { .. }
        | Op::AppInTick { .. }
        | Op::Spill
        | Op::Structured { .. }
        | Op::SaveDatabase
        | Op::Rollback
        | Op::Vacuum { .. }
        | Op::OtherProcessReader { .. }
        | Op::OtherProcessCheckpoint(_)
        | Op::Crash(_)
        | Op::StoreFault { .. }
        | Op::ClockJump(_)
        // When a part lands is the store's own traffic: it forces nothing.
        | Op::Parts(_)
        | Op::Tick
        | Op::Settle
        | Op::UntilPublished
        | Op::Shutdown
        | Op::ShutdownWithSnapshot
        | Op::ShutdownWithRetention
        | Op::Restore
        | Op::Restart
        | Op::PowerLoss
        | Op::OurReader { begin: true }
        | Op::AppAutocheckpoint(_)
        | Op::Shrink { .. }
        | Op::Heal
        | Op::Verify
        // The operator's word lets a boot build above the replica; the
        // snapshot that follows is forced by what disturbed staging.
        | Op::TrustLocal => &[],
    }
}

/// What a drawn run's own settings make true from its first step, as [`noted_by_op`] says for an
/// operation. Every field is named, with no rest pattern, so a new setting does not compile
/// until it says here whether it is a fault the engine may have to answer (a disk lying from
/// the start once went unnoted and failed runs).
#[must_use]
pub const fn noted_by_opts(opts: &crate::run::Opts) -> &'static [Forcing] {
    let crate::run::Opts {
        // What the harness records, raises, judges or expects: no change to the world.
        plant: _,
        trace: _,
        record: _,
        allow_stop: _,
        judges: _,
        drawn: _,
        // The engine's own configuration.
        responses: _,
        retention: _,
        switch_retention: _,
        // A law for what a power loss keeps acts only at a power loss, which notes
        // `PowerLoss`, and `StagedLost` for staged files it takes or brings back.
        block_crash_law: _,
        // The app's behaviour after a restart, and how the clock reads inside a tick.
        after_restart: _,
        clock_law: _,
        // An overlapping hold's crashes, power losses and store faults are noted as they
        // happen, as the same operations are outside a hold.
        overlap: _,
        error_domain: _,
        // Staging on the directory store: what its power losses take is noted with
        // `StagedLost`, as on `MemStore`; a file it took that it reported durable fails the run.
        staging_fs: _,
        // The engine's own configuration.
        truncate_pages: _,
        slice_bytes: _,
        // The database's size and the cost of I/O force nothing.
        first_db_pages: _,
        cost: _,
        // The law's process crashes are noted as they happen, as a crash operation's are.
        kill_after_fault: _,
        // The replica on S3, in parts or not: its provider faults are noted as they are drawn.
        remote_s3: _,
        remote_parts: _,
        // Which SQLite the engine runs against: each backend's faults are drawn and noted
        // as the model's are.
        sqlite: _,
        // A store without conditional writes refuses the first create-only upload of each
        // process, which publishes nothing and is retried as a replace: no snapshot follows.
        no_create_only: _,
        // The write gate refuses the application's commits, which never reach the engine, and
        // keeps the engine's checkpoints waiting: neither is a fault that forces a snapshot.
        write_gate: _,
        lying_wal_sync,
    } = opts;
    if *lying_wal_sync {
        &[Forcing::LyingDisk]
    } else {
        &[]
    }
}

/// The forcings in effect over one run, with the two checks judged against
/// them: [`NeedlessSnapshot`] and [`LostRestorePoints`].
#[derive(Clone, Debug)]
pub struct Forcings {
    window: BTreeSet<Forcing>,
    /// What the tick under way noted itself: kept past its own staging.
    in_tick: BTreeSet<Forcing>,
    /// Level-0 keys the replica took.
    uploaded: BTreeSet<FileKey>,
    staging_seen: usize,
    remote_seen: usize,
    /// The WAL header's salts when last looked at.
    wal: Option<(u32, u32)>,
    /// Whether our read transaction pinned the WAL when last looked at.
    pinned: bool,
    /// The efficiency check.
    snapshots: NeedlessSnapshot,
    /// The safety check.
    restore_points: LostRestorePoints,
}

impl Default for Forcings {
    fn default() -> Self {
        Self::new()
    }
}

impl Forcings {
    /// The state at the start of a run.
    #[must_use]
    pub fn new() -> Self {
        Self {
            window: BTreeSet::from([Forcing::RunStart]),
            in_tick: BTreeSet::new(),
            uploaded: BTreeSet::new(),
            staging_seen: 0,
            remote_seen: 0,
            wal: None,
            pinned: false,
            snapshots: NeedlessSnapshot::default(),
            restore_points: LostRestorePoints::default(),
        }
    }

    /// Notes that `why` happened.
    pub fn note(&mut self, why: Forcing) {
        self.window.insert(why);
    }

    /// Notes that `why` happened inside the tick under way.
    pub fn note_in_tick(&mut self, why: Forcing) {
        self.window.insert(why);
        self.in_tick.insert(why);
    }

    /// Notes what `op` says by itself.
    pub fn note_op(&mut self, op: &Op) {
        for why in noted_by_op(op) {
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
            self.note(Forcing::ReadLockLapsed);
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
            (Some(_), None) => self.note(Forcing::WalEmptied),
            (was, Some(now)) if was != Some(now) => self.note(Forcing::WalRestarted),
            (None | Some(_), Some(_)) | (None, None) => {}
        }
    }

    /// A new engine: its counter starts again from zero.
    pub fn rebooted(&mut self) {
        self.snapshots.rebooted();
    }

    /// Judges a snapshot cause the engine announced
    /// ([`NeedlessSnapshot::judge`]).
    ///
    /// # Errors
    /// [`Violation::NeedlessSnapshot`] when nothing in effect forces it.
    pub fn judge(
        &mut self,
        cause: SnapshotCauseKind,
        at_boot: bool,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        self.snapshots.judge(cause, at_boot, &self.window, reach)
    }

    /// Takes in what a tick did to the stores and the engine's counter:
    /// counts restore points given up, checks the counter (when the tick
    /// ended rather than crashed), judges an operator's snapshot when it is
    /// staged, and moves the window past what was staged and uploaded.
    /// `world` answers what the stores hold now.
    ///
    /// # Errors
    /// [`Violation::NeedlessSnapshot`], [`Violation::UnforcedGiveUp`], or a
    /// `BugFault` when the counter disagrees with the staging store.
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
        let took = self.take_remote(remote);
        // A key the bucket refused because it already held it (a commit whose answer was
        // lost, confirmed by a read-back) leaves no put in the recorder: the replica holds it.
        self.uploaded.extend(world.replica_level_zero());
        // The replica holds everything staging does only once no staged file is one it never
        // took: a file left behind it (a backup put back, a file damaged in place) is still to be
        // dropped, and the fault that left it must be in the window when it is (seed 0xf42e9).
        let staged_keys = world.staged_level_zero();
        let left_behind = staged_keys.iter().any(|key| !self.uploaded.contains(key));
        // Nor while the newest staged file is gone from staging: an upload that reaches the
        // engine's newest file proves nothing about a staging directory wiped under its send
        // (its bytes were read before the wipe), and a boot over what is left finds no file to
        // continue from (seed 0x126d69380a38: the wipe was forgotten and the boot's snapshot
        // above the replica was called needless).
        let newest_kept = newest_staged.is_some_and(|newest| {
            staged_keys
                .iter()
                .any(|key| key.range().max().get() == newest)
        });
        // Nor while a staged file has other bytes than the replica's copy of it: a file read on
        // one tick and published on a later one, damaged in between, is published as it was
        // read, and no read of that upload met the damage. The boot that finds it has the edit
        // for its cause. Asked last, and only of an upload that would otherwise have caught
        // up: it reads every staged file and its copy.
        let caught_up = !left_behind
            && newest_kept
            && took
                .zip(newest_staged)
                .is_some_and(|(took, newest)| took >= newest)
            && !world.staged_differs_from_replica();
        let staged = self.take_staging(staging, world, reach)?;
        let grew = self.snapshots.take_counts(counts, &self.window, reach)?;
        let checkable = ended && !self.in_tick.contains(&Forcing::StagingFault);
        NeedlessSnapshot::check_counter(checkable, grew, staged.bare, staged.full)?;
        let staged_snapshots = staged.full.max(staged.bare);
        let requested = self.snapshots.settle(counts, ended, grew, staged_snapshots);
        let through = staged.any
            && self
                .window
                .iter()
                .any(|why| why.forgotten_at() == ForgottenAt::StagedThrough)
            && !world.staging_behind_commit();
        self.window.retain(|why| match why.forgotten_at() {
            ForgottenAt::Staging => !staged.any,
            ForgottenAt::StagedThrough => !through,
            ForgottenAt::Upload => !caught_up,
            ForgottenAt::RequestedSnapshot => requested == 0,
            ForgottenAt::Never => true,
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

    /// What the tick staged at level 0; judges the staged files deleted
    /// before the replica took them.
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
                    let lost = self.restore_points.deleted(keys, &self.uploaded, world);
                    self.given_up(lost, reach)?;
                }
            }
        }
        self.staging_seen = staging.len();
        Ok(staged)
    }

    /// A boot found the replica past the newest staged file and snapshots
    /// above it ([`LostRestorePoints::passed_by_replica`]).
    ///
    /// # Errors
    /// [`Violation::UnforcedGiveUp`] when nothing in effect forces it.
    pub fn passed_by_replica(
        &mut self,
        staged: &[FileKey],
        world: &dyn TickWorld,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let lost = self
            .restore_points
            .passed_by_replica(staged, &self.uploaded, world);
        self.given_up(lost, reach)
    }

    /// Judges `lost` restore points given up ([`LostRestorePoints::judge`]);
    /// a give-up is a forcing for what follows.
    fn given_up(&mut self, lost: u64, reach: &mut Reach) -> Result<(), Violation> {
        if LostRestorePoints::judge(lost, &self.window, reach)? {
            self.note_in_tick(Forcing::StagedGivenUp);
        }
        Ok(())
    }
}

/// What the checks ask of the stores after a tick.
pub trait TickWorld {
    /// Whether the staged level-0 file `key` holds every page of its database.
    fn full_image(&self, key: &FileKey) -> bool;
    /// The newest staged transaction.
    fn newest_staged(&self) -> Option<u64>;
    /// The newest transaction the replica holds, at any level.
    fn replica_newest(&self) -> Option<u64>;
    /// The level-0 keys the replica holds now.
    fn replica_level_zero(&self) -> Vec<FileKey> {
        Vec::new()
    }
    /// Whether a file on the replica, at any level, ends at `txid`.
    fn replica_ends_at(&self, txid: u64) -> bool;
    /// The level-0 files in staging.
    fn staged_level_zero(&self) -> Vec<FileKey>;
    /// Whether a staged level-0 file has other bytes than the replica's object under its key.
    fn staged_differs_from_replica(&self) -> bool;
    /// Whether the local database holds a commit past the newest staged file's.
    fn staging_behind_commit(&self) -> bool;
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

    fn replica_level_zero(&self) -> Vec<FileKey> {
        self.remote_level_zero()
            .map(|files| files.into_iter().map(|info| info.key).collect())
            .unwrap_or_default()
    }

    fn replica_ends_at(&self, txid: u64) -> bool {
        self.remote_ends_at(txid)
    }

    fn staged_level_zero(&self) -> Vec<FileKey> {
        crate::World::staged_level_zero(self)
    }

    fn staged_differs_from_replica(&self) -> bool {
        crate::World::staged_level_zero(self)
            .into_iter()
            .any(|key| {
                self.staging_departs_from_remote(key)
                    .is_ok_and(|departs| departs)
            })
    }

    fn staging_behind_commit(&self) -> bool {
        self.staging_behind_newest_commit()
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

    fn replica_level_zero(&self) -> Vec<FileKey> {
        self.world.replica_level_zero()
    }

    fn replica_ends_at(&self, txid: u64) -> bool {
        self.world.replica_ends_at(txid)
    }

    fn staged_level_zero(&self) -> Vec<FileKey> {
        self.world.staged_level_zero()
    }

    fn staged_differs_from_replica(&self) -> bool {
        self.world.staged_differs_from_replica()
    }

    fn staging_behind_commit(&self) -> bool {
        self.world.staging_behind_commit()
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

        fn staged_level_zero(&self) -> Vec<FileKey> {
            vec![key(24), key(25)]
        }
        fn staged_differs_from_replica(&self) -> bool {
            false
        }
        fn staging_behind_commit(&self) -> bool {
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
            mode: lsr_core::WriteMode::New,
            header: None,
            origin: None,
            created_at: zero,
            size: 0,
            digest: 0,
            begun: zero,
            at: zero,
            high: zero,
        }
    }

    /// An upload that reaches staging's newest file has not caught up while
    /// the engine's own newest staged file is past it: the staging edit that
    /// removed that file stays in the window (seed 0xf43fb).
    #[test]
    fn an_upload_short_of_the_engines_newest_staged_file_keeps_a_staging_edit() {
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        forcings.window.clear();
        forcings.note(Forcing::StagingEdited);
        let remote = [remote_put(24), remote_put(25)];
        let engine = WithEngine {
            world: &Restored,
            staged: Some(26),
        };
        forcings
            .after_tick(
                (&[], &remote),
                &engine,
                SnapshotCounts::default(),
                false,
                &mut reach,
            )
            .unwrap();
        assert!(forcings.window.contains(&Forcing::StagingEdited));
        forcings.remote_seen = 0;
        let caught_up = WithEngine {
            world: &Restored,
            staged: Some(25),
        };
        forcings
            .after_tick(
                (&[], &remote),
                &caught_up,
                SnapshotCounts::default(),
                false,
                &mut reach,
            )
            .unwrap();
        assert!(!forcings.window.contains(&Forcing::StagingEdited));
    }

    /// Staging wiped under the send of the engine's newest file, 26: the replica takes it, and
    /// staging holds nothing. The upload reaches the engine's newest file but vouches for no
    /// staging left to continue from, so the wipe stays in the window for the boot that finds
    /// the empty directory and snapshots above the replica (seed 0x126d69380a38).
    #[test]
    fn an_upload_that_leaves_staging_empty_keeps_the_staging_edit() {
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        forcings.window.clear();
        forcings.note(Forcing::StagingEdited);
        let wiped = WithEngine {
            world: &Replica {
                newest: 26,
                ends: &[26],
            },
            staged: Some(26),
        };
        forcings
            .after_tick(
                (&[], &[remote_put(26)]),
                &wiped,
                SnapshotCounts::default(),
                false,
                &mut reach,
            )
            .unwrap();
        assert!(forcings.window.contains(&Forcing::StagingEdited));
        let judged = forcings.judge(SnapshotCauseKind::AheadOfReplica, true, &mut reach);
        assert!(judged.is_ok(), "{judged:?}");
    }

    fn key(txid: u64) -> FileKey {
        FileKey::new(
            lsr_core::Level::ZERO,
            lsr_ltx::TxidRange::single(lsr_ltx::Txid::new(txid).unwrap()),
        )
    }

    /// Staging after an edit left an older file behind the replica: 37, which the replica
    /// never took, beside 43, the newest, which it did.
    struct LeftBehind {
        staged: Vec<FileKey>,
        /// Whether a staged file has other bytes than the replica's copy.
        differs: bool,
    }

    impl TickWorld for LeftBehind {
        fn full_image(&self, _: &FileKey) -> bool {
            false
        }
        fn newest_staged(&self) -> Option<u64> {
            self.staged.iter().map(|key| key.range().max().get()).max()
        }
        fn replica_newest(&self) -> Option<u64> {
            Some(43)
        }
        fn replica_ends_at(&self, _: u64) -> bool {
            false
        }
        fn staged_level_zero(&self) -> Vec<FileKey> {
            self.staged.clone()
        }
        fn staged_differs_from_replica(&self) -> bool {
            self.differs
        }
        fn staging_behind_commit(&self) -> bool {
            false
        }
    }

    /// An upload that reaches the newest staged file has not caught up while staging still
    /// holds a file the replica never took: the edit that left it there stays in the window,
    /// so the prune that later drops it is justified (seed 0xf42e9).
    #[test]
    fn an_upload_past_a_file_left_behind_keeps_the_staging_edit() {
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        forcings.window.clear();
        forcings.note(Forcing::StagingEdited);
        let world = LeftBehind {
            staged: vec![key(37), key(43)],
            differs: false,
        };
        let none = SnapshotCounts::default();
        forcings
            .after_tick((&[], &[remote_put(43)]), &world, none, false, &mut reach)
            .unwrap();
        assert!(forcings.window.contains(&Forcing::StagingEdited));
        let zero = lsr_ltx::Timestamp::from_unix_millis(0);
        let prune = [RecorderEvent::Delete {
            keys: vec![key(37)],
            at: zero,
            high: zero,
        }];
        let world = LeftBehind {
            staged: vec![key(43)],
            differs: false,
        };
        forcings
            .after_tick((&prune, &[remote_put(43)]), &world, none, false, &mut reach)
            .unwrap();
        assert_eq!(reach.get("lost_restore_points.counted"), 1);
    }

    /// An upload that reaches the newest staged file has not caught up while a staged file
    /// has other bytes than the replica's copy of it: the file was damaged after its bytes
    /// were read for a send that a later tick published, so no read of the upload met the damage.
    /// The edit stays in the window until the damaged file is gone, so the boot that drops
    /// it and snapshots has its cause.
    #[test]
    fn an_upload_of_a_file_damaged_under_its_send_keeps_the_staging_edit() {
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        forcings.window.clear();
        forcings.note(Forcing::StagingEdited);
        let none = SnapshotCounts::default();
        let damaged = LeftBehind {
            staged: vec![key(43)],
            differs: true,
        };
        forcings
            .after_tick((&[], &[remote_put(43)]), &damaged, none, false, &mut reach)
            .unwrap();
        assert!(forcings.window.contains(&Forcing::StagingEdited));
        // The damaged file is dropped: the next upload that reaches the newest staged file
        // has caught up.
        let whole = LeftBehind {
            staged: vec![key(44)],
            differs: false,
        };
        let remote = [remote_put(43), remote_put(44)];
        forcings
            .after_tick((&[], &remote), &whole, none, false, &mut reach)
            .unwrap();
        assert!(!forcings.window.contains(&Forcing::StagingEdited));
    }

    #[test]
    fn a_disk_lying_from_the_start_is_noted_and_nothing_else_is() {
        let lying = crate::run::Opts {
            lying_wal_sync: true,
            ..crate::run::Opts::default()
        };
        assert_eq!(noted_by_opts(&lying), [Forcing::LyingDisk]);
        let laws = crate::run::Opts {
            drawn: true,
            block_crash_law: true,
            ..crate::run::Opts::default()
        };
        assert!(noted_by_opts(&laws).is_empty());
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
        fn staged_level_zero(&self) -> Vec<FileKey> {
            Vec::new()
        }
        fn staged_differs_from_replica(&self) -> bool {
            false
        }
        fn staging_behind_commit(&self) -> bool {
            false
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
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        let world = Replica {
            newest: 9,
            ends: &[4, 9],
        };
        forcings.window.clear();
        forcings.note(Forcing::StagingEdited);
        let events = [deleted(5)];
        forcings.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("lost_restore_points.counted"), 1);
        forcings.window.clear();
        forcings.staging_seen = 0;
        let events = [deleted(5), deleted(5), deleted(4)];
        forcings.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("lost_restore_points.counted"), 1);
        forcings.staging_seen = 0;
        forcings
            .take_staging(&[deleted(10)], &world, &mut reach)
            .unwrap_err();
    }

    /// A boot that snapshots above a replica that passed staging gives up
    /// the staged files the replica never took then, once, with its window;
    /// the prune that deletes them later loses nothing new (seed 0xf4cf9).
    #[test]
    fn a_replica_that_passed_staging_gives_up_its_files_at_the_boot() {
        let mut forcings = Forcings::new();
        let mut reach = Reach::default();
        let world = Replica {
            newest: 8,
            ends: &[4, 8],
        };
        forcings.window.clear();
        forcings.note(Forcing::RemoteEdited);
        let staged = [staged_key(4), staged_key(5), staged_key(6), staged_key(9)];
        forcings
            .passed_by_replica(&staged, &world, &mut reach)
            .unwrap();
        assert_eq!(reach.get("lost_restore_points.counted"), 2);
        forcings.window.clear();
        let events = [deleted(5), deleted(6)];
        forcings.take_staging(&events, &world, &mut reach).unwrap();
        assert_eq!(reach.get("lost_restore_points.counted"), 2);
    }

    #[test]
    fn a_wal_header_change_is_noted() {
        let mut forcings = Forcings::new();
        forcings.window.clear();
        forcings.note_wal(Some((1, 2)), Some((1, 2)));
        assert!(forcings.window.is_empty());
        forcings.note_wal(Some((1, 2)), Some((2, 7)));
        forcings.note_wal(Some((2, 7)), None);
        assert_eq!(
            forcings.window,
            BTreeSet::from([Forcing::WalRestarted, Forcing::WalEmptied])
        );
    }
}
