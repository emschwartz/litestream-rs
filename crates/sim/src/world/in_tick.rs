//! What the model learns from the app's operation inside a tick (`sqlite/in_tick.rs`).
//!
//! The engine may have committed a frame of its own (its sequence bump) earlier in the tick,
//! which the model takes up first, from the WAL's end as the operation found it. A commit then
//! goes through [`World::take_commit_in_tick`] as the barrier's commit does. A
//! checkpoint that left the WAL durable, and a deleted WAL (a full checkpoint first), make
//! every commit the model knows durable, as they do between ticks. The model learns of them
//! before it takes up the engine's own frames of the tick, so a frame the engine wrote after
//! the checkpoint is never called durable by it; one written before is left to the power
//! loss's own reading of the WAL, which keeps whatever SQLite recovers.

use lsr_wal::{Salts, WalOffset};

use super::World;
use lsr_ltx::PageNo;

use crate::sqlite::{AppCommit, InTickDone, InTickRan, InTickSite};

impl World {
    /// Tells the model what the armed operation did in the tick that just ran, which the
    /// SQLite model keeps for [`World::take_app_in_tick`].
    pub(super) fn record_app_in_tick(&mut self) {
        let Some(ran) = self.sqlite.app_tick_ends() else {
            return;
        };
        let (salts, end, db_size) = ran.before;
        self.model_learns_end(salts, end, db_size);
        match ran.done {
            InTickDone::Checkpointed {
                numbers: Some(_),
                synced: true,
            }
            | InTickDone::WalDeleted(true) => self.model.on_checkpoint(),
            InTickDone::Checkpointed { .. }
            | InTickDone::WalDeleted(false)
            | InTickDone::Commit
            | InTickDone::Spilled(_)
            | InTickDone::ReaderBegan(_)
            | InTickDone::ReaderEnded { .. } => {}
        }
    }

    /// The model takes up a commit ending at `end` in generation `salts` unless its newest
    /// commit ends there: the engine's own frame, as [`World::sync_model_at`] takes up the one
    /// at a tick's end.
    fn model_learns_end(&mut self, salts: Salts, end: WalOffset, db_size: u32) {
        self.retire_generation(salts);
        if end.frame_index() == 0 {
            return;
        }
        let known = self
            .model
            .newest_commit()
            .and_then(|id| self.model.rec(id))
            .is_some_and(|rec| rec.salts == salts && rec.end.get() == end.get());
        if !known {
            let at = self.remote_recorder.len();
            let (pages, db_size) = self
                .sqlite
                .take_written()
                .map_or_else(|| (Vec::new(), db_size), |real| (real.pages, real.db_size));
            self.model
                .commit(&pages, db_size, AppCommit { salts, end }, at);
            self.log_history();
        }
    }

    /// Arms a commit for an [`crate::sqlite::InTickOp::Commit`]: at the call it names, with
    /// the staging position it is in read at the engine's next committed read.
    pub fn arm_app_commit_in_tick(&self, pages: Vec<(PageNo, Vec<u8>)>, db_size: u32) {
        self.arm_commit_in_tick(pages, db_size, InTickSite::AtCall);
        let staging = self.staging_recorder.clone();
        self.sqlite.app_staging_len(Box::new(move || staging.len()));
    }

    /// What the app's operation inside the last tick did, once; `None` when none ran.
    pub fn take_app_in_tick(&mut self) -> Option<InTickRan> {
        self.sqlite.take_app_in_tick()
    }

    /// What the app's operation inside the last tick did, kept for [`World::take_app_in_tick`].
    #[must_use]
    pub fn app_in_tick(&self) -> Option<InTickRan> {
        self.sqlite.app_tick_ends()
    }
}
