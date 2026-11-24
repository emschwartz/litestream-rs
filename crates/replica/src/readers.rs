//! The application's long readers, and the checkpointer that respects them.
//!
//! A TRUNCATE checkpoint cannot finish while a reader holds an older snapshot of the database,
//! and SQLite cannot tell the replicator whether one is coming. The engine's connections never
//! wait on a lock (a busy timeout of zero), so a TRUNCATE a reader blocks does not wait for it:
//! it copies what the reader allows into the database file, reports busy, leaves the WAL as it
//! was and takes no snapshot. So TRUNCATE runs only on the application's word:
//! under [`ReaderTracking::Untracked`] it never runs, and under [`ReaderTracking::Tracked`] it runs only while no [`ReaderGuard`]
//! lives. A TRUNCATE asked for while one does is refused without calling SQLite, as
//! `SourceError::ReadersHeld` and not as busy (SQLite was never asked), so the engine waits its
//! backoff and asks again. A FULL checkpoint (the cure after a failed WAL sync runs one) is
//! blocked by readers on older snapshots the same way, also reports busy without waiting, and
//! is guarded alike. PASSIVE checkpoints never wait on readers and are unaffected.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError, WriteLock};
use lsr_sqlite::Sqlite;

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

/// The backend's checkpointer, refusing TRUNCATE unless the application said no reader runs.
#[derive(Clone, Debug)]
pub(crate) struct Guarded {
    sqlite: Sqlite,
    tracking: ReaderTracking,
    readers: Arc<AtomicUsize>,
}

impl Guarded {
    pub(crate) const fn new(
        sqlite: Sqlite,
        tracking: ReaderTracking,
        readers: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            sqlite,
            tracking,
            readers,
        }
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
        self.sqlite.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.sqlite.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
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
        self.sqlite.bump_seq()
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
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
