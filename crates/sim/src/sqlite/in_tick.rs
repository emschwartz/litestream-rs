//! The application's operations inside a tick, at any of the engine's calls into SQLite.
//!
//! The engine's read-lock proofs are claims about what the application can do between two of
//! its own calls: that a commit, a reader, another process's checkpoint or a deleted WAL lands
//! either before a read or after it, never inside a step the engine relies on. A tick runs
//! as one future, so without this module the application acts only between ticks, and once
//! inside one: a commit right before a checkpoint takes its barrier
//! ([`SimSqlite::arm_commit_in_tick`]).
//!
//! An operation armed here ([`SimSqlite::arm_app_in_tick`]) runs right before one of the
//! tick's calls into SQLite: the one with a given number of calls before it, counting every
//! call the engine makes (opening the WAL, its length, the committed read, each read of WAL
//! bytes, the page count, each page, the sync, the barrier, the checkpoint, the sequence bump,
//! the write lock and their releases), or the first call of a given kind. A tick that makes
//! no such call ends with the operation not run. Each operation follows the
//! same rules as when the application makes it between ticks: a commit is the armed
//! [`SimSqlite::arm_commit_in_tick`] commit, made here instead of at the barrier; a reader's
//! transaction takes the mark SQLite's rules give it; a checkpoint is refused while the app
//! is in a transaction; a WAL is deleted only when no connection holds a transaction.

use lsr_core::SourceOp;
use lsr_wal::{Salts, WalOffset};

use super::{
    CheckpointNumbers, Conn, HEADER_LEN, InTickSite, OtherProcessMode, ReaderId, SimSqlite, State,
};

/// Where inside a tick the application acts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InTickAt {
    /// Right before the tick's call into SQLite that has this many calls before it.
    Call(u32),
    /// Right before the tick's first call of this kind: where the engine's protocol relies on
    /// what the application cannot do between two of its calls (a checkpoint after the seal,
    /// a page count after the committed read).
    Before(SourceOp),
}

/// What the application does inside a tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InTickOp {
    /// It makes the commit armed with [`SimSqlite::arm_commit_in_tick`].
    Commit,
    /// Another process begins a read transaction.
    ReaderBegin,
    /// That process's read transaction ends.
    ReaderEnd(ReaderId),
    /// It runs `PRAGMA wal_checkpoint(mode)`.
    Checkpoint(OtherProcessMode),
    /// The last connection closes and the WAL is deleted.
    DeleteWal,
}

/// What an operation inside the tick did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InTickDone {
    /// The commit ran (`SimSqlite::take_commit_in_tick` says whether SQLite took it).
    Commit,
    /// The reader began.
    ReaderBegan(ReaderId),
    /// The reader's transaction ended, or was not held.
    ReaderEnded {
        /// The reader.
        id: ReaderId,
        /// Whether it held one.
        held: bool,
    },
    /// The checkpoint ran (`None`: refused, the app was in a transaction), and whether the
    /// WAL was durable as it returned, as `World::other_process_checkpoint` asks.
    Checkpointed {
        /// What it returned.
        numbers: Option<CheckpointNumbers>,
        /// Whether the whole WAL was durable right after it.
        synced: bool,
    },
    /// Whether the WAL was deleted.
    WalDeleted(bool),
}

/// What an operation inside the tick did, and what it did to the WAL file: the liveness
/// oracle must know before it judges the tick's snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InTickRan {
    /// What it did.
    pub done: InTickDone,
    /// The wal-index names a new generation after it.
    pub restarted: bool,
    /// The WAL file is gone or holds no whole header after it.
    pub emptied: bool,
    /// The wal-index's salts, committed end and database size right before it: the engine
    /// may have committed a frame of its own earlier in the tick (its sequence bump).
    pub before: (Salts, WalOffset, u32),
}

/// The armed operation, the tick's calls so far, what the operation did, and a commit's
/// created-at reading still owed to the engine's next committed read.
#[derive(Default)]
pub(super) struct AppInTick {
    armed: Option<(InTickAt, InTickOp)>,
    calls: u32,
    ran: Option<InTickRan>,
    staging_len: Option<StagingLen>,
    seen_by: bool,
}

/// The staging log's length now: where the next staged file will sit in it.
pub type StagingLen = Box<dyn Fn() -> usize + Send>;

impl std::fmt::Debug for AppInTick {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppInTick")
            .field("armed", &self.armed)
            .field("calls", &self.calls)
            .field("ran", &self.ran)
            .field("staging_len", &self.staging_len.is_some())
            .field("seen_by", &self.seen_by)
            .finish()
    }
}

impl State {
    /// One call into SQLite by the engine: the armed operation runs first when its turn
    /// has come.
    pub(super) fn app_call(&mut self, kind: Option<SourceOp>) {
        let at = self.app.calls;
        self.app.calls = self.app.calls.saturating_add(1);
        let Some((when, op)) = self.app.armed else {
            return;
        };
        let now = match when {
            InTickAt::Call(call) => call == at,
            InTickAt::Before(before) => kind == Some(before),
        };
        if !now {
            return;
        }
        self.app.armed = None;
        let salts = self.index.salts;
        let before = (
            salts,
            WalOffset::from_frame_index(self.index.mx_frame, self.page_size),
            self.index.db_size,
        );
        let done = self.run_in_tick(op);
        self.app.ran = Some(InTickRan {
            done,
            restarted: self.index.salts != salts,
            emptied: !self.present || self.wal.len() < HEADER_LEN,
            before,
        });
    }

    /// Makes the armed commit as [`State::commit_in_tick`] does, but its staging position (the
    /// second created-at value) is read at the engine's next committed read, not now: a step
    /// whose committed read came before the commit stages a file without it, and one cut from
    /// the database file alone names no WAL range the model could tell it by.
    fn commit_seen_later(&mut self) {
        if self
            .in_tick
            .as_ref()
            .is_none_or(|armed| armed.site != InTickSite::AtCall)
        {
            return;
        }
        self.commit_in_tick(InTickSite::AtCall);
        self.app.seen_by = self.in_tick_done.is_some();
    }

    /// The engine reads the committed end: a commit made inside the tick is in what it
    /// stages from here on.
    pub(super) fn app_committed_read(&mut self) {
        if !std::mem::take(&mut self.app.seen_by) {
            return;
        }
        let len = self.app.staging_len.as_ref().map(|len| len());
        if let (Some(done), Some(len)) = (self.in_tick_done.as_mut(), len) {
            done.created_at.1 = len;
        }
    }

    /// The tick ended: a commit no committed read saw is in nothing the tick staged.
    pub(super) fn app_commit_unseen(&mut self) {
        if std::mem::take(&mut self.app.seen_by)
            && let Some(done) = self.in_tick_done.as_mut()
        {
            done.created_at.1 = usize::MAX;
        }
    }

    fn run_in_tick(&mut self, op: InTickOp) -> InTickDone {
        match op {
            InTickOp::Commit => {
                self.commit_seen_later();
                InTickDone::Commit
            }
            InTickOp::ReaderBegin => {
                let id = ReaderId(self.next_reader);
                self.next_reader = self.next_reader.saturating_add(1);
                self.begin(Conn::OtherProcess(id));
                InTickDone::ReaderBegan(id)
            }
            InTickOp::ReaderEnd(id) => InTickDone::ReaderEnded {
                id,
                held: self.end(Conn::OtherProcess(id)),
            },
            InTickOp::Checkpoint(mode) => {
                let in_tx = self.writer == Some(Conn::App) || self.readers.contains_key(&Conn::App);
                let numbers = (!in_tx).then(|| self.checkpoint(Conn::App, mode));
                InTickDone::Checkpointed {
                    numbers,
                    synced: self.durable == self.wal,
                }
            }
            InTickOp::DeleteWal => {
                let deleted = self.close_all();
                if deleted {
                    self.present = false;
                }
                InTickDone::WalDeleted(deleted)
            }
        }
    }
}

impl SimSqlite {
    /// Arms `op` to run in the next tick, at `when`; it replaces any armed operation.
    /// [`SimSqlite::take_app_in_tick`] disarms it.
    pub fn arm_app_in_tick(&self, when: InTickAt, op: InTickOp) {
        self.with(|state| state.app.armed = Some((when, op)));
    }

    /// Where a commit armed at a call (`InTickSite::AtCall`) learns the staging log's length
    /// at the engine's next committed read.
    pub fn app_staging_len(&self, len: StagingLen) {
        self.with(|state| state.app.staging_len = Some(len));
    }

    /// A tick begins: its calls are counted from 0.
    pub fn app_tick_begins(&self) {
        self.with(|state| {
            state.app.calls = 0;
            state.app.ran = None;
        });
    }

    /// The tick ended: an operation that did not run is disarmed, and what one did stays for
    /// [`SimSqlite::take_app_in_tick`] until the next tick begins.
    pub fn app_tick_ends(&self) -> Option<InTickRan> {
        self.with(|state| {
            state.app.armed = None;
            state.app.ran
        })
    }

    /// The calls into SQLite the tick so far made: after a tick, all of them.
    #[must_use]
    pub fn app_calls(&self) -> u32 {
        self.with(|state| state.app.calls)
    }

    /// What the armed operation did in the last tick (`None` when the tick made too few calls),
    /// and disarms it either way.
    pub fn take_app_in_tick(&self) -> Option<InTickRan> {
        self.with(|state| {
            state.app.armed = None;
            state.app.ran.take()
        })
    }
}

#[cfg(test)]
mod tests;
