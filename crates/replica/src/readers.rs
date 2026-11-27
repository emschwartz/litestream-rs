//! The application's long readers, and the checkpointer that respects them.
//!
//! A TRUNCATE checkpoint cannot finish while a reader holds an older snapshot of the database,
//! and SQLite cannot tell the replicator whether one is coming. The engine's connections never
//! wait on a lock (a busy timeout of zero), so a TRUNCATE a reader blocks does not wait for it:
//! it copies what the reader allows into the database file, reports busy, leaves the WAL as it
//! was and takes no snapshot. An application that says when it reads holds a [`ReaderGuard`] for
//! each long read: a TRUNCATE asked for while one lives is refused without calling SQLite, as
//! `SourceError::ReadersHeld` and not as busy (SQLite was never asked), so the engine waits its
//! backoff and asks again. A FULL checkpoint (the cure after a failed WAL sync runs one) is
//! blocked by readers on older snapshots the same way, also reports busy without waiting, and
//! is guarded alike. PASSIVE checkpoints never wait on readers and are unaffected.
//!
//! The same checkpointer stands down while fail-stop holds the application's writes
//! ([`WritesHeld`]): every step that writes or checkpoints (the barrier, a checkpoint of any
//! mode, the sequence bump) is refused before SQLite is asked, as `SourceError::WritesHeld`,
//! which the engine waits out without counting a failure. No application commit can grow the
//! WAL meanwhile, so nothing is lost by not checkpointing, and the engine's own write would
//! only meet the gate's lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, ReadLockHold, SourceError};
use lsr_sqlite::{Sqlite, WritesHeld};

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

/// The backend's checkpointer, refusing TRUNCATE and FULL while a reader guard lives,
/// and every write while the application's writes are held.
#[derive(Clone, Debug)]
pub(crate) struct Guarded {
    sqlite: Sqlite,
    readers: Arc<AtomicUsize>,
    held: WritesHeld,
}

impl Guarded {
    pub(crate) const fn new(sqlite: Sqlite, readers: Arc<AtomicUsize>, held: WritesHeld) -> Self {
        Self {
            sqlite,
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
        self.readers.load(Ordering::SeqCst) == 0
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
            CheckpointMode::Truncate
            | CheckpointMode::Full
            | CheckpointMode::Passive
            | CheckpointMode::Backlog => self.sqlite.checkpoint(mode),
        }
    }

    fn renew_read(&mut self) -> Result<ReadLockHold, SourceError> {
        self.sqlite.renew_read()
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.writes_allowed()?;
        self.sqlite.bump_seq()
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
