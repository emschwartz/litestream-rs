//! Lost restore points: staged history the engine gave up must be forced.
//!
//! A restore point is given up when the engine deletes a staged level-0 file
//! the replica never took, or one that lies past the newest transaction the
//! replica holds now (it rolled back), so no restore can reach it. A prune of
//! files the replica took, and since compacted or let go by retention, is not
//! one. Nor is the delete of a file at or below the replica's newest
//! transaction when a replica file ends at that transaction (another writer's
//! file there was compacted, so a restore reaches it), or when the same key was
//! counted before (a staging backup put back a file already given up: deleting
//! it again loses nothing new). Seeds 0xf4a87, 0xf47e6, 0xf4bab and 0xf44c6
//! failed on the double count and the other writer's transaction.
//!
//! Each is counted (`lost_restore_points.counted`) and judged against the
//! forcings in effect ([`crate::forcing`]): one with nothing in effect that
//! could force it fails the run with [`Violation::UnforcedGiveUp`]. A give-up
//! is then [`Forcing::StagedGivenUp`] for what follows, since the newest staged
//! file moved back; the window takes this check's result as a forcing.

use std::collections::BTreeSet;

use lsr_core::FileKey;

use crate::forcing::{Forcing, TickWorld};
use crate::oracle::Violation;
use crate::reach::Reach;

/// What can force the engine to delete staged files the replica never took.
/// The WAL's own changes are here because the boot's witness deletes every
/// pending staged file the live WAL cannot vouch for (`StagedUnverified`): one
/// staged before a restart, an emptied WAL or damage can no longer be proved.
const FORCES_GIVE_UP: &[Forcing] = &[
    Forcing::StagingEdited,
    Forcing::StagingFault,
    Forcing::StagedLost,
    Forcing::PowerLoss,
    Forcing::LyingDisk,
    Forcing::RemoteEdited,
    Forcing::RemoteFault,
    Forcing::WalRestarted,
    Forcing::WalEmptied,
    Forcing::WalDamaged,
];

/// The check's state over one run.
#[derive(Clone, Debug, Default)]
pub struct LostRestorePoints {
    /// Level-0 keys already counted as given up.
    lost: BTreeSet<FileKey>,
}

impl LostRestorePoints {
    /// How many restore points a delete of staged `keys` gives up; `uploaded`
    /// holds the level-0 keys the replica took.
    pub fn deleted(
        &mut self,
        keys: &[FileKey],
        uploaded: &BTreeSet<FileKey>,
        world: &dyn TickWorld,
    ) -> u64 {
        let newest = world.replica_newest();
        let lost: Vec<FileKey> = keys
            .iter()
            .filter(|key| {
                key.level().get() == 0
                    && (newest.is_none_or(|newest| key.range().max().get() > newest)
                        || (!uploaded.contains(key)
                            && !self.lost.contains(key)
                            && !world.replica_ends_at(key.range().max().get())))
            })
            .copied()
            .collect();
        self.lost.extend(lost.iter().copied());
        u64::try_from(lost.len()).unwrap_or(u64::MAX)
    }

    /// How many restore points a boot gives up when it found the replica past
    /// the newest staged file and snapshots above it (`BehindRemote`): every
    /// staged file at or below the replica's newest transaction that it never
    /// took, and that no replica file ends at, can never be uploaded, so its
    /// restore point is given up now, with the window that forced the
    /// snapshot, not later when a prune deletes it (seed 0xf4cf9).
    pub fn passed_by_replica(
        &mut self,
        staged: &[FileKey],
        uploaded: &BTreeSet<FileKey>,
        world: &dyn TickWorld,
    ) -> u64 {
        let Some(newest) = world.replica_newest() else {
            return 0;
        };
        let lost: Vec<FileKey> = staged
            .iter()
            .filter(|key| {
                key.level().get() == 0
                    && key.range().max().get() <= newest
                    && !uploaded.contains(key)
                    && !self.lost.contains(key)
                    && !world.replica_ends_at(key.range().max().get())
            })
            .copied()
            .collect();
        self.lost.extend(lost.iter().copied());
        u64::try_from(lost.len()).unwrap_or(u64::MAX)
    }

    /// Counts `lost` restore points given up and judges them against the
    /// forcings in effect; true when some were, so the window notes
    /// [`Forcing::StagedGivenUp`].
    ///
    /// # Errors
    /// [`Violation::UnforcedGiveUp`] when nothing in effect could force it.
    pub fn judge(
        lost: u64,
        window: &BTreeSet<Forcing>,
        reach: &mut Reach,
    ) -> Result<bool, Violation> {
        if lost == 0 {
            return Ok(false);
        }
        reach.add("lost_restore_points.counted", lost);
        let forced = FORCES_GIVE_UP.iter().any(|why| window.contains(why));
        if !forced && window.contains(&Forcing::PendingUnprovable) {
            reach.add("pending_unprovable.given_up", lost);
        } else if !forced {
            return Err(Violation::UnforcedGiveUp {
                lost,
                window: window.iter().copied().collect(),
            });
        }
        Ok(true)
    }
}
