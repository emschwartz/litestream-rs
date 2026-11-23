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
//! The debt is kept in `Kept` (so a rebuild keeps it) and, when a journal is lent, in the
//! engine journal (`docs/journal.md`): at the end of every tick the journal's debt is written
//! when it differs from the debt here, and boot restores a recorded debt. A restart between
//! steps 1 and 2 therefore still owes the TRUNCATE. The record lags the fault by the rest of
//! one tick (a kill in that window forgets it, as without a journal), and a journal that cannot
//! be written leaves the debt in this process only, retried every tick and reported as
//! `JournalUnwritable`.
//!
//! After the TRUNCATE the debt is [`Unproven::Snapshot`] until the paying snapshot is staged:
//! a restart then snapshots anyway (the WAL the TRUNCATE emptied is a new generation), and the
//! journal records that the TRUNCATE ran, so a restart never owes a second one.
//!
//! Before its TRUNCATE the cure reads the WAL with checked reads (`crate::cure`, D108): a page
//! the kernel evicted reads as the disk's older bytes and fails its frame's checksum, and the
//! step fails `WalCorruptBelowCommitted` instead of copying those bytes into the database file.
//!
//! What this cannot protect against:
//! - The page cache losing the pages before a checkpoint copies them, where the engine cannot
//!   see it: a reboot or power loss, an eviction inside the checkpoint call after the cure's read,
//!   or an eviction followed by the application's own checkpoint. SQLite then backfills whatever
//!   the disk holds. Nothing
//!   was staged from the lost frames, so the replica is not ahead of the database, but the
//!   database may be damaged. A checkpoint that copied some of those frames first (the
//!   application's, another process's, or a TRUNCATE that went busy behind a reader after
//!   copying, which SQLite does before it reports busy) leaves the database file ahead of its
//!   WAL; the next boot finds it before its own TRUNCATE (`super::ahead`, `docs/decisions.md`
//!   D103), and holding the read mark back cannot prevent it (D104).
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
use crate::fault::{Evidence, Fault, Obligation, SnapshotCause, evidence};
use crate::source::{CheckpointMode, Checkpointer, PageSource, SourceError, WalSource};
use crate::store::ObjectStore;

/// How far the engine is past a failed sync of the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unproven {
    /// No sync failed since the last TRUNCATE that completed.
    No,
    /// A sync failed: the next sync step is a TRUNCATE checkpoint.
    Truncate,
    /// The TRUNCATE ran: the next file staged, the snapshot, pays the debt.
    Snapshot,
}

impl Unproven {
    /// The debt as the journal records it.
    pub(super) const fn recorded(self) -> lsr_journal::Unproven {
        match self {
            Self::No => lsr_journal::Unproven::No,
            Self::Truncate => lsr_journal::Unproven::Truncate,
            Self::Snapshot => lsr_journal::Unproven::Snapshot,
        }
    }

    /// The larger of this debt and one a journal recorded: a restart never forgives a debt.
    pub(super) const fn with_recorded(self, recorded: lsr_journal::Unproven) -> Self {
        match (self, recorded) {
            (Self::Truncate, _) | (_, lsr_journal::Unproven::Truncate) => Self::Truncate,
            (Self::Snapshot, _) | (_, lsr_journal::Unproven::Snapshot) => Self::Snapshot,
            (Self::No, lsr_journal::Unproven::No) => Self::No,
        }
    }
}

impl Actor {
    /// Notes a failed sync of the WAL, wherever it was met. Called from [`Actor::fault`], the
    /// one place every fault passes, so no path can miss it.
    pub(super) fn sync_failed(&mut self, fault: Fault) {
        match evidence(&fault) {
            Evidence::Once(Obligation::UnprovenWal) => {
                self.kept.unproven = Unproven::Truncate;
            }
            Evidence::Repeats => {}
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
        // A frame the kernel evicted reads as the disk's older bytes and fails its checksum: no
        // checkpoint of ours copies it into the database file (`crate::cure`).
        crate::cure::scan(env.wal)?;
        let run = env.ck.checkpoint(CheckpointMode::Truncate);
        self.kept.app.ours(env.wal.committed().ok(), run.ok());
        match run {
            Ok(run) if !run.busy => {}
            Ok(_) | Err(SourceError::Busy | SourceError::ReadersHeld) => {
                return Err(Fault::WalSyncFailed);
            }
            Err(err) => return Err(Fault::from_source(err)),
        }
        self.kept.unproven = Unproven::Snapshot;
        Ok(Stepped::Resnapshot(SnapshotCause::WalSyncFailed))
    }

    /// Step 3 is done: a file was staged after the TRUNCATE, and the first one is the snapshot.
    pub(super) fn staged_after_truncate(&mut self) {
        if matches!(self.kept.unproven, Unproven::Snapshot) {
            self.kept.unproven = Unproven::No;
        }
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
