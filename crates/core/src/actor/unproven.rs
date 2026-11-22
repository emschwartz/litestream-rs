//! What the engine does after its sync of the WAL failed.
//!
//! The kernel reports a failed writeback once and then calls the pages clean (the "fsyncgate"
//! behaviour of Linux): a later sync that succeeds says nothing about the frames the failed sync
//! covered, though every read still returns them from the page cache. The WAL's checksums
//! chain, so each later frame of the same generation depends on them too. No frame of that WAL
//! generation past the last sync that succeeded is staged, then.
//!
//! What is left is in the page cache, and the one way to put it on disk is to write it again.
//! The engine does that through SQLite: a TRUNCATE checkpoint reads every committed frame and
//! writes its page into the database file, syncs the database file once the backfill reaches
//! the end of the WAL, advances `nBackfill` only when that sync succeeded, and then empties the
//! WAL (`walCheckpoint` in the bundled `sqlite3.c`). The WAL sync that opens the checkpoint
//! proves nothing; the rewrite and the database sync do. The next sync is then a snapshot
//! ([`SnapshotCause::WalSyncFailed`]), taken from the database file and the new WAL generation,
//! whose every byte was written after the failure, so the barrier's sync covers it.
//!
//! The steps, each in its own tick:
//! 1. The barrier's sync fails: [`Fault::WalSyncFailed`] is routed as any persistent fault
//!    (degraded at `wal_sync_failed` and retried, or a stop), and [`Actor::fault`] notes it,
//!    wherever it was met (a sync step or a checkpoint's seal).
//! 2. The retry runs the TRUNCATE instead of a sync step. Busy, or refused because the
//!    application's readers hold the WAL, is the same fault again.
//! 3. The snapshot. The debt is paid once the TRUNCATE completes: the WAL it emptied holds
//!    nothing the failed sync covered, and the snapshot is an ordinary pending one.
//!
//! The debt lives in this process only (in `Kept`, so a rebuild keeps it). A restart between
//! steps 1 and 2 forgets it, and the next process trusts the WAL again: that needs a durable
//! engine-owned record, which waits on the engine journal decision (the ignored test
//! `a_restart_after_a_failed_wal_sync_ships_no_unproven_frame` in the SQLite suite).
//!
//! What this cannot protect against:
//! - The page cache losing the pages before the TRUNCATE copies them (a reboot, a power loss,
//!   or eviction under memory pressure). SQLite then backfills whatever the disk holds. Nothing
//!   was staged from the lost frames, so the replica is not ahead of the database, but the
//!   database may be damaged.
//! - A failed writeback this process's sync never reports: the kernel reports it to the
//!   descriptors open when it happened, and to a new one only if no one has seen it yet.
//! - A database page an earlier partial checkpoint wrote (it advances `nBackfill` without
//!   syncing the database file) whose writeback failed: the TRUNCATE's database sync reports the
//!   failure once, the retry starts after those frames and never writes them again, and the
//!   TRUNCATE then empties the WAL that held them. That is SQLite's own exposure, not this
//!   engine's to repair.
//! - A power loss after the TRUNCATE before its truncate of the WAL file is on disk: SQLite may
//!   replay the old generation's durable frames over pages the checkpoint wrote.

use lsr_ltx::Timestamp;

use super::{Actor, Env, Stepped, Task, Tick};
use crate::fault::{Fault, SnapshotCause};
use crate::source::{CheckpointMode, Checkpointer, PageSource, SourceError, WalSource};
use crate::store::ObjectStore;

/// How far the engine is past a failed sync of the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unproven {
    /// No sync failed since the last TRUNCATE that completed.
    No,
    /// A sync failed: the next sync step is a TRUNCATE checkpoint.
    Truncate,
}

impl Actor {
    /// Notes a failed sync of the WAL, wherever it was met. Called from [`Actor::fault`], the
    /// one place every fault passes, so no path can miss it.
    pub(super) fn sync_failed(&mut self, fault: Fault) {
        if fault == Fault::WalSyncFailed {
            mark!(Run3);
            self.kept.unproven = Unproven::Truncate;
        }
    }

    /// Step 2: the TRUNCATE checkpoint, in place of a sync step.
    pub(super) fn unprove<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
    ) -> Result<Stepped, Fault>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        let run = env.ck.checkpoint(CheckpointMode::Truncate);
        self.kept.app.ours(env.wal.committed().ok(), run.ok());
        match run {
            Ok(run) if !run.busy => {}
            Ok(_) | Err(SourceError::Busy | SourceError::ReadersHeld) => {
                return Err(Fault::WalSyncFailed);
            }
            Err(err) => return Err(Fault::from_source(err)),
        }
        self.kept.unproven = Unproven::No;
        Ok(Stepped::Resnapshot(SnapshotCause::WalSyncFailed))
    }

    /// Whether the engine owes a TRUNCATE before it may read the WAL for staging.
    pub(super) const fn owes_truncate(&self) -> bool {
        matches!(self.kept.unproven, Unproven::Truncate)
    }

    /// The tick of a drain that owes a TRUNCATE: a drain runs no checkpoint, so the sync task
    /// stays degraded until the drain ends.
    pub(super) fn owed_in_drain(&mut self, now: Timestamp) -> Tick {
        self.fault(Task::Sync, Fault::WalSyncFailed, now)
    }
}
