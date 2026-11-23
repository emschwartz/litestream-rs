//! The application inside a tick, on real connections: the same arming, call counting and
//! sites as the model's (`lsr_sim::sqlite`'s `in_tick` module), with each operation made as
//! SQL right before the engine's call it is armed for.
//!
//! Before an operation runs, the backend reads the database back: whatever the engine wrote
//! earlier in the tick (its sequence bump) is owed to the world as its own change, ahead of the
//! operation's, so the world's model takes up the engine's commit with its real pages.

use lsr_core::SourceOp;
use lsr_ltx::PageNo;
use lsr_sim::sqlite::{
    CreatedAt, InTickAt, InTickCommit, InTickDone, InTickOp, InTickRan, InTickSite, StagingLen,
};

use super::index::Index;
use super::state::Real;

/// A commit armed for the tick.
pub(super) struct Armed {
    pub(super) pages: Vec<(PageNo, Vec<u8>)>,
    pub(super) db_size: u32,
    pub(super) site: InTickSite,
    pub(super) created_at: CreatedAt,
}

/// The tick's calls, what is armed in it and what ran.
#[derive(Default)]
pub(super) struct Tick {
    pub(super) calls: u32,
    pub(super) op: Option<(InTickAt, InTickOp)>,
    pub(super) ran: Option<InTickRan>,
    pub(super) commit: Option<Armed>,
    pub(super) done: Option<InTickCommit>,
    pub(super) staging_len: Option<StagingLen>,
    pub(super) seen_by: bool,
}

impl Real {
    /// One call into SQLite by the engine: the armed operation runs first when its turn has
    /// come, and a commit armed for this kind of call is made.
    pub(super) fn call(&mut self, kind: Option<SourceOp>) {
        let at = self.tick.calls;
        self.tick.calls = self.tick.calls.saturating_add(1);
        if let Some((when, op)) = self.tick.op {
            let now = match when {
                InTickAt::Call(call) => call == at,
                InTickAt::Before(before) => kind == Some(before),
            };
            if now {
                self.tick.op = None;
                self.run_op(op);
            }
        }
        match kind {
            Some(SourceOp::Committed) => {
                self.commit_in_tick(InTickSite::BeforeCommittedRead);
                self.committed_read();
            }
            Some(SourceOp::BeginBarrier) => self.commit_in_tick(InTickSite::BeforeBarrier),
            Some(_) | None => {}
        }
    }

    fn run_op(&mut self, op: InTickOp) {
        let owed = self.read_back();
        if !owed.pages.is_empty() {
            self.owed.push_back(owed);
        }
        let index = Index::read(self);
        let before = (index.salts, index.end(self.page_size), index.db_size);
        let done = match op {
            InTickOp::Commit => {
                self.commit_in_tick(InTickSite::AtCall);
                self.tick.seen_by = self.tick.done.is_some();
                InTickDone::Commit
            }
            InTickOp::ReaderBegin => InTickDone::ReaderBegan(self.reader_begin()),
            InTickOp::ReaderEnd(id) => InTickDone::ReaderEnded {
                id,
                held: self.reader_end(id),
            },
            InTickOp::Checkpoint(mode) => {
                let numbers = if self.app_tx {
                    None
                } else {
                    self.app
                        .as_ref()
                        .and_then(|app| Self::checkpoint_on(app, mode))
                };
                InTickDone::Checkpointed {
                    numbers,
                    synced: self.wal_synced(),
                }
            }
            InTickOp::DeleteWal => InTickDone::WalDeleted(false),
        };
        let after = Index::read(self);
        let wal = self.wal_file();
        self.tick.ran = Some(InTickRan {
            done,
            restarted: after.salts != index.salts,
            emptied: wal.is_none_or(|wal| wal.len() < 32),
            before,
        });
    }

    /// Makes the commit armed for `site`, if one is.
    pub(super) fn commit_in_tick(&mut self, site: InTickSite) {
        if self
            .tick
            .commit
            .as_ref()
            .is_none_or(|armed| armed.site != site)
        {
            return;
        }
        let Some(armed) = self.tick.commit.take() else {
            return;
        };
        if let Some(app) = self.commit(&armed.pages, armed.db_size) {
            let written = self.read_back();
            self.tick.done = Some(InTickCommit {
                pages: written.pages,
                db_size: written.db_size,
                app,
                created_at: (armed.created_at)(),
            });
        }
    }

    /// The engine reads the committed end: a commit made at a call is in what it stages from
    /// here on.
    fn committed_read(&mut self) {
        if !std::mem::take(&mut self.tick.seen_by) {
            return;
        }
        let len = self.tick.staging_len.as_ref().map(|len| len());
        if let (Some(done), Some(len)) = (self.tick.done.as_mut(), len) {
            done.created_at.1 = len;
        }
    }

    /// The tick ended: a commit no committed read saw is in nothing the tick staged.
    pub(super) fn commit_unseen(&mut self) {
        if std::mem::take(&mut self.tick.seen_by)
            && let Some(done) = self.tick.done.as_mut()
        {
            done.created_at.1 = usize::MAX;
        }
    }

    /// Whether a power loss now would keep the whole WAL.
    pub(super) fn wal_synced(&self) -> bool {
        self.vfs.durable(&self.wal_path(), 0, u64::MAX)
    }
}
