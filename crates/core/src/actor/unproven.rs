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
//! 2. The retry runs the verified cure instead of a sync step (`crate::cure`, D108): a checked
//!    read of the WAL, a FULL checkpoint that must copy exactly the frames read, a read of every
//!    copied page back against its frame's digest, then the TRUNCATE. Busy, refused because the
//!    application's readers hold the WAL, or a commit between the read and the FULL, is the same
//!    fault again. Pages unlike their frames are a verdict (below). A TRUNCATE that goes busy
//!    after a proven FULL is retried alone while the WAL's end is unchanged.
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
//! The checked read finds a page the kernel evicted before the cure: it reads as the disk's
//! older bytes and fails its frame's checksum, and the step fails `WalCorruptBelowCommitted`
//! instead of copying those bytes into the database file. A page evicted after the read and
//! before the FULL checkpoint copies it is found by the read-back: commits are lost, and the
//! verdict ([`StopCause::WalLostCommits`]) keeps the newest staged transaction restorable for
//! the window D103's verdict has, raised before the TRUNCATE so the journal records it first.
//! The proof that the FULL copied the WAL as it stands lives in this process only: a restart
//! before the TRUNCATE reads the WAL again and finds the evicted frames (`wal_corrupt`).
//!
//! What this cannot protect against:
//! - The page cache losing the pages before a checkpoint copies them, where the engine cannot
//!   see it: a reboot or power loss, or an eviction followed by the application's own
//!   checkpoint. SQLite then backfills whatever the disk holds. Nothing was staged from the lost
//!   frames, so the replica is not ahead of the database, but the database may be damaged. A checkpoint that copied some of those frames first (the
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

use lsr_journal::AheadKind;
use lsr_ltx::Timestamp;

use super::{Actor, Env, Stepped, Task, Tick};
#[cfg(doc)]
use crate::fault::StopCause;
use crate::fault::{Evidence, Fault, Obligation, SnapshotCause, evidence};
use crate::source::{
    CheckpointMode, CheckpointRun, Checkpointer, PageSource, SourceError, WalSource,
};
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

/// A cure's checkpoint that ran to the end; busy, or refused because the application's readers
/// hold the WAL, is the failed sync's fault again.
fn cured(run: Result<CheckpointRun, SourceError>) -> Result<CheckpointRun, Fault> {
    match run {
        Ok(run) if !run.busy => Ok(run),
        Ok(_) | Err(SourceError::Busy | SourceError::ReadersHeld) => Err(Fault::WalSyncFailed),
        Err(err) => Err(Fault::from_source(err)),
    }
}

impl Actor {
    /// Notes a failed sync of the WAL, wherever it was met. Called from [`Actor::fault`], the
    /// one place every fault passes, so no path can miss it.
    pub(super) fn sync_failed(&mut self, fault: Fault) {
        match evidence(&fault) {
            Evidence::Once(Obligation::UnprovenWal) => {
                mark!(Run3);
                self.kept.unproven = Unproven::Truncate;
            }
            Evidence::Repeats => {}
        }
    }

    /// Step 2: the verified cure (`crate::cure`), then the TRUNCATE checkpoint, in place of a
    /// sync step. A read-back that finds pages unlike their frames is a verdict, raised before
    /// the TRUNCATE so the journal holds it first; the next attempt runs the TRUNCATE alone.
    pub(super) fn unprove<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        if !self.proven(env.wal)?
            && let Some(scanned) = crate::cure::scan(env.wal)?
        {
            self.copy(env, &scanned)?;
            let found = crate::cure::verify(env.wal, env.db, &scanned)?;
            self.kept.verified = Some(scanned.end());
            if let Some(found) = found {
                let restore_point = self.engine.course.staged();
                return Ok(self.found(AheadKind::WalLostCommits, restore_point, found, now));
            }
        }
        let run = env.ck.checkpoint(CheckpointMode::Truncate);
        self.kept.app.ours(env.wal.committed().ok(), run.ok());
        cured(run)?;
        self.kept.verified = None;
        self.kept.unproven = Unproven::Snapshot;
        Ok(Stepped::Resnapshot(SnapshotCause::WalSyncFailed))
    }

    /// Whether the cure already proved the WAL as it stands: its committed end is the one the
    /// last read-back verified. An end names its generation and frame count, and frames are only
    /// appended within a generation, so a WAL that moved never matches a stale proof again.
    fn proven<W: WalSource>(&self, wal: &mut W) -> Result<bool, Fault> {
        let Some(verified) = self.kept.verified else {
            return Ok(false);
        };
        let end = wal.committed().map_err(Fault::from_source)?.end();
        Ok(end == verified)
    }

    /// The cure's FULL checkpoint, which must copy exactly the scanned frames.
    fn copy<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        scanned: &crate::cure::Scanned,
    ) -> Result<(), Fault>
    where
        W: WalSource,
        C: Checkpointer,
    {
        let run = env.ck.checkpoint(CheckpointMode::Full);
        let after = env.wal.committed();
        self.kept.app.ours(after.as_ref().ok().copied(), run.ok());
        let run = cured(run)?;
        crate::cure::copied(scanned, run, after.map_err(Fault::from_source)?)
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
