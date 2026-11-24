//! The needless-snapshot check: a snapshot needs something in the world that forces it.
//!
//! A snapshot is never wrong about content, so the safety oracles pass a run
//! whatever it snapshots. It is a cost: the whole database uploaded and kept
//! for the retention window. An engine that snapshots on every boot, or on a
//! rule that misfires, loses nothing a restore can see and pays on every
//! restart. So each snapshot cause the engine announces (`Booted` with a
//! cause, `Resnapshot`) or stages (an operator's request) is judged here
//! against the forcings in effect ([`crate::forcing`]): [`forced_by`] maps
//! every [`SnapshotCauseKind`] to the forcings that can make it necessary,
//! and a cause with none of them in effect fails the run with
//! [`Violation::NeedlessSnapshot`].
//!
//! The engine's own per-cause counter ([`lsr_core::Actor::snapshots`]) is
//! checked too: on a tick that ends with no staging fault, it grows by at
//! least the staged files with no WAL range and at most the staged files that
//! hold every page.

use std::collections::BTreeSet;

use lsr_core::{SnapshotCauseKind, SnapshotCounts};

use crate::forcing::Forcing;
use crate::oracle::Violation;
use crate::reach::{Reach, variant};

/// What can make the boot drop pending staged files that do not read whole or chain
/// (`StagedUnverified`): staging edited or torn, the WAL they came from changed or
/// lost what they hold, or the replica was rolled back so files it had taken are
/// pending again against a WAL that has moved on (0x83).
const FORCES_STAGED_UNVERIFIED: &[Forcing] = &[
    Forcing::RemoteEdited,
    Forcing::StagingEdited,
    Forcing::StagedLost,
    Forcing::StagingFault,
    Forcing::PowerLoss,
    Forcing::LyingDisk,
    Forcing::WalDamaged,
    Forcing::WalRestarted,
    Forcing::WalEmptied,
];

/// What can make a snapshot for `cause` necessary, announced at a boot
/// (`at_boot`) or by a running engine; `None` for a cause that is the engine's
/// own design rather than a response to the world (a TRUNCATE checkpoint
/// always snapshots), which is counted but never judged.
#[must_use]
pub const fn forced_by(cause: SnapshotCauseKind, at_boot: bool) -> Option<&'static [Forcing]> {
    use Forcing::{
        LyingDisk, PositionUnproven, PowerLoss, ReadLockLapsed, RemoteEdited, RemoteFault,
        RunStart, SnapshotRequested, StagedGivenUp, StagedLost, StagingEdited, StagingFault,
        WalDamaged, WalEmptied, WalRestarted,
    };
    Some(match cause {
        // Nothing staged and nothing on the replica to continue from. Staging may hold nothing
        // while the replica holds its newest file, so a bucket rolled back past its only
        // transaction leaves nothing on either side.
        SnapshotCauseKind::FirstSync => &[
            RunStart,
            StagingEdited,
            StagedLost,
            StagingFault,
            StagedGivenUp,
            RemoteEdited,
        ],
        // The replica holds more than staging: staging lost files, or the
        // replica took files staging never had.
        SnapshotCauseKind::BehindRemote => &[
            StagingEdited,
            StagedLost,
            StagingFault,
            StagedGivenUp,
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
        // (a backup put back, staged files given up).
        SnapshotCauseKind::GenerationChanged if at_boot => &[
            WalRestarted,
            WalEmptied,
            WalDamaged,
            StagingEdited,
            StagingFault,
            PositionUnproven,
            StagedGivenUp,
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
            StagedGivenUp,
        ],
        // The WAL at the position is not what the newest staged file says.
        SnapshotCauseKind::PrevFrameMismatch | SnapshotCauseKind::LastPageMismatch => &[
            WalDamaged,
            WalRestarted,
            PowerLoss,
            StagingEdited,
            StagedLost,
            StagedGivenUp,
        ],
        // The engine syncs the WAL before it stages from it, so an honest
        // power loss cannot take what was staged: only a disk that lies about
        // that sync, damage, or a staging backup put back can.
        SnapshotCauseKind::LocalRewound => &[LyingDisk, WalDamaged, StagingEdited],
        SnapshotCauseKind::StagedUnverified => FORCES_STAGED_UNVERIFIED,
        // A boot over a brand-new meta directory found the database ahead of the replica: only a
        // staging directory emptied outside the engine (wiped, lost to a power loss, or given
        // up) leaves a boot with nothing staged and no takeover record over a replica that
        // holds files.
        SnapshotCauseKind::AheadOfReplica => &[StagingEdited, StagedLost, StagedGivenUp],
        SnapshotCauseKind::CheckpointBoundary => return None,
        SnapshotCauseKind::OperatorRequested => &[SnapshotRequested],
    })
}

/// The check's state over one run.
#[derive(Clone, Debug, Default)]
pub struct NeedlessSnapshot {
    /// The engine's counter as the last tick of this incarnation left it.
    counted: SnapshotCounts,
    /// A forced snapshot was announced and is not staged yet: a crash
    /// before it is staged makes the next boot announce it again.
    owed: bool,
}

impl NeedlessSnapshot {
    /// A new engine: its counter starts again from zero.
    pub fn rebooted(&mut self) {
        self.counted = SnapshotCounts::default();
    }

    /// Judges a snapshot cause the engine announced against the forcings in
    /// effect.
    ///
    /// # Errors
    /// [`Violation::NeedlessSnapshot`] when nothing in effect forces it.
    pub fn judge(
        &mut self,
        cause: SnapshotCauseKind,
        at_boot: bool,
        window: &BTreeSet<Forcing>,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let Some(needs) = forced_by(cause, at_boot) else {
            return Ok(());
        };
        if needs.iter().any(|why| window.contains(why)) {
            reach.bump(format!("needless_snapshot.forced.{}", variant(&cause)));
            self.owed = true;
            return Ok(());
        }
        if self.owed {
            // The same debt, announced again after a restart, perhaps
            // under the name of what the boot found first.
            reach.bump("needless_snapshot.owed");
            return Ok(());
        }
        Err(Violation::NeedlessSnapshot {
            cause,
            window: window.iter().copied().collect(),
        })
    }

    /// How much the engine's counter grew past what this check last saw;
    /// judges an operator's snapshot.
    ///
    /// # Errors
    /// [`Violation::NeedlessSnapshot`] for an operator's snapshot nothing forced.
    pub fn take_counts(
        &mut self,
        counts: SnapshotCounts,
        window: &BTreeSet<Forcing>,
        reach: &mut Reach,
    ) -> Result<u64, Violation> {
        let mut grew = 0_u64;
        let counted = self.counted;
        for ((kind, now), (_, was)) in counts.iter().zip(counted.iter()) {
            let more = now.saturating_sub(was);
            grew = grew.saturating_add(more);
            if more > 0 {
                reach.add(format!("snapshot.staged.{}", variant(&kind)), more);
                if kind == SnapshotCauseKind::OperatorRequested {
                    self.judge(kind, false, window, reach)?;
                }
            }
        }
        Ok(grew)
    }

    /// Checks the counter's growth (`grew`) against the files a tick staged:
    /// a file with no WAL range (`bare`) is a snapshot for certain; one with a
    /// range that holds every page (`full`) may be a snapshot or a transaction
    /// that wrote every page, so the counter is bounded, not matched, by the
    /// two. Only `checkable` ticks count: a staging fault can fail a step after
    /// its file was written, and the engine counts only what it believes it
    /// staged.
    ///
    /// # Errors
    /// A `BugFault` when the counter disagrees with the staging store.
    pub fn check_counter(
        checkable: bool,
        grew: u64,
        bare: u64,
        full: u64,
    ) -> Result<(), Violation> {
        if checkable && (grew < bare || grew > full.max(bare)) {
            return Err(crate::bug_fault(format!(
                "the engine counted {grew} snapshots staged in a tick that staged {bare} without a \
                 WAL range and {full} holding every page"
            )));
        }
        Ok(())
    }

    /// Takes the counter in after a tick and returns how many operator's
    /// snapshots it staged. A tick that staged a snapshot (by the counter when
    /// it `ended`, else by what was staged) pays the debt of one announced.
    pub fn settle(&mut self, counts: SnapshotCounts, ended: bool, grew: u64, staged: u64) -> u64 {
        let requested = counts
            .get(SnapshotCauseKind::OperatorRequested)
            .saturating_sub(self.counted.get(SnapshotCauseKind::OperatorRequested));
        self.counted = counts;
        let snapshots = if ended { grew } else { staged };
        if snapshots > 0 {
            self.owed = false;
        }
        requested
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(of: &[Forcing]) -> BTreeSet<Forcing> {
        of.iter().copied().collect()
    }

    #[test]
    fn every_judged_cause_can_be_forced() {
        for kind in SnapshotCauseKind::ALL {
            for at_boot in [true, false] {
                if let Some(needs) = forced_by(kind, at_boot) {
                    assert!(!needs.is_empty(), "{kind:?} can never be forced");
                }
            }
        }
    }

    #[test]
    fn a_running_engine_needs_more_than_a_restart() {
        let mut check = NeedlessSnapshot::default();
        let mut reach = Reach::default();
        let restarted = window(&[Forcing::WalRestarted]);
        check
            .judge(
                SnapshotCauseKind::GenerationChanged,
                true,
                &restarted,
                &mut reach,
            )
            .unwrap();
        check.owed = false;
        assert!(
            check
                .judge(
                    SnapshotCauseKind::GenerationChanged,
                    false,
                    &restarted,
                    &mut reach
                )
                .is_err()
        );
        let lapsed = window(&[Forcing::WalRestarted, Forcing::ReadLockLapsed]);
        check
            .judge(
                SnapshotCauseKind::GenerationChanged,
                false,
                &lapsed,
                &mut reach,
            )
            .unwrap();
    }

    #[test]
    fn a_cause_with_nothing_in_effect_fails() {
        let mut check = NeedlessSnapshot::default();
        let mut reach = Reach::default();
        check
            .judge(
                SnapshotCauseKind::FirstSync,
                true,
                &window(&[Forcing::RunStart]),
                &mut reach,
            )
            .unwrap();
        let restarted = window(&[Forcing::WalRestarted]);
        check
            .judge(
                SnapshotCauseKind::GenerationChanged,
                true,
                &restarted,
                &mut reach,
            )
            .unwrap();
        check.owed = false;
        assert!(matches!(
            check.judge(
                SnapshotCauseKind::LocalRewound,
                true,
                &restarted,
                &mut reach
            ),
            Err(Violation::NeedlessSnapshot { .. })
        ));
        check
            .judge(
                SnapshotCauseKind::CheckpointBoundary,
                true,
                &restarted,
                &mut reach,
            )
            .unwrap();
    }

    #[test]
    fn a_forced_snapshot_announced_again_before_it_is_staged_passes() {
        let mut check = NeedlessSnapshot::default();
        let mut reach = Reach::default();
        check
            .judge(
                SnapshotCauseKind::LocalRewound,
                true,
                &window(&[Forcing::LyingDisk]),
                &mut reach,
            )
            .unwrap();
        check
            .judge(
                SnapshotCauseKind::LastPageMismatch,
                true,
                &window(&[]),
                &mut reach,
            )
            .unwrap();
        assert_eq!(reach.get("needless_snapshot.owed"), 1);
    }
}
