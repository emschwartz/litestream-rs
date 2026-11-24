//! The application's long readers, and the checkpointer that respects them.
//!
//! A TRUNCATE checkpoint waits for every reader to finish, and SQLite cannot tell the
//! replicator whether one is coming. So TRUNCATE runs only on the application's word:
//! under [`ReaderTracking::Untracked`] it never runs, and under [`ReaderTracking::Tracked`] it runs only while no [`ReaderGuard`]
//! lives. A TRUNCATE asked for while one does is refused without calling SQLite, as
//! `SourceError::ReadersHeld` and not as busy (SQLite was never asked), so the engine waits its
//! backoff and asks again. A FULL checkpoint (the cure after a failed WAL sync runs one)
//! waits for readers on older snapshots the same way and is guarded alike. PASSIVE checkpoints
//! never wait on readers and are unaffected.
//!
//! The same checkpointer stands down while fail-stop holds the application's writes
//! ([`WritesHeld`]): every step that writes or checkpoints (the barrier, a checkpoint of any
//! mode, the sequence bump, the write lock) is refused before SQLite is asked, as
//! `SourceError::WritesHeld`, which the engine waits out without counting a failure. No
//! application commit can grow the WAL meanwhile, so nothing is lost by not checkpointing, and
//! the engine's own write would only meet the gate's lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError, WriteLock};
use lsr_sqlite::{Sqlite, WritesHeld};

use crate::config::ReaderTracking;

/// Held for the length of one long read. While any guard lives, the replicator runs no
/// TRUNCATE checkpoint. Counted, so overlapping readers are safe; dropping it ends the read.
#[derive(Debug)]
#[must_use = "a reader guard protects the read only while it is held"]
pub struct ReaderGuard {
    count: Arc<AtomicUsize>,
}

impl ReaderGuard {
    pub(crate) fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self {
            count: Arc::clone(count),
        }
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The backend's checkpointer, refusing TRUNCATE unless the application said no reader runs,
/// and every write while the application's writes are held.
#[derive(Clone, Debug)]
pub(crate) struct Guarded {
    sqlite: Sqlite,
    tracking: ReaderTracking,
    readers: Arc<AtomicUsize>,
    held: WritesHeld,
}

impl Guarded {
    pub(crate) const fn new(
        sqlite: Sqlite,
        tracking: ReaderTracking,
        readers: Arc<AtomicUsize>,
        held: WritesHeld,
    ) -> Self {
        Self {
            sqlite,
            tracking,
            readers,
            held,
        }
    }

    /// `WritesHeld` while the gate holds the application's writes.
    fn writes_allowed(&self) -> Result<(), SourceError> {
        if self.held.is_held() {
            return Err(SourceError::WritesHeld);
        }
        Ok(())
    }

    fn may_wait(&self) -> bool {
        match self.tracking {
            ReaderTracking::Untracked => false,
            ReaderTracking::Tracked => self.readers.load(Ordering::SeqCst) == 0,
        }
    }
}

impl Checkpointer for Guarded {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.writes_allowed()?;
        self.sqlite.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.sqlite.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        self.writes_allowed()?;
        match mode {
            CheckpointMode::Truncate | CheckpointMode::Full if !self.may_wait() => {
                Err(SourceError::ReadersHeld)
            }
            CheckpointMode::Truncate | CheckpointMode::Full | CheckpointMode::Passive => {
                self.sqlite.checkpoint(mode)
            }
        }
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.writes_allowed()?;
        self.sqlite.bump_seq()
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.writes_allowed()?;
        self.sqlite.begin_write_lock()
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.sqlite.end_write_lock(lock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guards_count_overlapping_readers() {
        let count = Arc::new(AtomicUsize::new(0));
        let first = ReaderGuard::new(&count);
        let second = ReaderGuard::new(&count);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        drop(first);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(second);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
