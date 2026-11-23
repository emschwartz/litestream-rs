//! [`crate::Op::AppInTick`]: one tick with the app acting inside it (`sqlite/in_tick.rs`).
//!
//! The commit is chosen as [`crate::Op::Commit`] chooses one (random pages, the spilled ones with
//! them, and every page a growth adds) and armed as [`crate::Op::CommitInTick`] arms its own, so the
//! model and the snapshot notes take it up the same way. The other operations are SQLite's
//! own; the runner keeps its list of other readers in step with what ran. A WAL restart or
//! deletion the operation caused is noted for the forcing checks inside the tick, before it
//! judges the tick's snapshots, since the engine can meet it in the same tick.

use super::{Runner, small};
use crate::forcing::Forcing;
use crate::generate::{AppAt, AppOp};
use crate::oracle::Violation;
use crate::reach::variant;
use crate::sqlite::{InTickAt, InTickDone, InTickOp};

impl Runner {
    /// Arms `op` at one of the tick's calls into SQLite and runs the tick: a numbered call
    /// modulo the calls the last tick made, so that it falls inside a tick of that length,
    /// or the first call of a kind.
    pub(super) fn app_in_tick(&mut self, at: AppAt, op: AppOp) -> Result<(), Violation> {
        let when = match at {
            AppAt::Call(call) => InTickAt::Call(
                u32::from(call)
                    .checked_rem(self.w.sqlite.app_calls())
                    .unwrap_or(0),
            ),
            AppAt::Before(kind) => InTickAt::Before(kind),
        };
        let armed = match op {
            AppOp::Commit { pages, grow } => {
                self.arm_in_tick_commit(pages, grow);
                Some(InTickOp::Commit)
            }
            AppOp::ReaderBegin => Some(InTickOp::ReaderBegin),
            AppOp::ReaderEnd => self.readers.first().copied().map(InTickOp::ReaderEnd),
            AppOp::Checkpoint(mode) => Some(InTickOp::Checkpoint(mode)),
            AppOp::DeleteWal => Some(InTickOp::DeleteWal),
        };
        if let Some(armed) = armed {
            self.w.sqlite.arm_app_in_tick(when, armed);
        }
        self.tick()?;
        let ran = self.w.take_app_in_tick();
        let outcome = if ran.is_some() { "ran" } else { "missed" };
        self.report
            .reach
            .bump(format!("app_in_tick.{}.{outcome}", variant(&op)));
        if let Some(point) = crate::domain::points::point_of(at, op) {
            self.report.reach.bump(format!("{point}.{outcome}"));
        }
        match ran.map(|ran| ran.done) {
            Some(InTickDone::ReaderBegan(id)) => self.readers.push(id),
            Some(InTickDone::ReaderEnded { id, .. }) => self.readers.retain(|held| *held != id),
            Some(
                InTickDone::Commit | InTickDone::Checkpointed { .. } | InTickDone::WalDeleted(_),
            )
            | None => {}
        }
        Ok(())
    }

    /// The commit of an [`AppOp::Commit`], armed for the tick.
    fn arm_in_tick_commit(&mut self, pages: u8, grow: bool) {
        let size = self.w.sqlite.db_size();
        let new_size = if grow {
            size.saturating_add(small(&self.rng, 3).saturating_add(1))
        } else {
            size
        };
        let mut all = self.spilled.clone();
        all.extend(self.pick_pages(u32::from(pages), size));
        if grow {
            let added = self.all_pages(new_size).into_iter();
            all.extend(added.filter(|(page, _)| page.get() > size));
        }
        self.w.arm_app_commit_in_tick(all, new_size);
    }

    /// Tells the forcing checks, inside the tick, what the app's operation in it did to the
    /// WAL file.
    pub(super) fn app_in_tick_seen(&mut self) {
        let Some(ran) = self.w.app_in_tick() else {
            return;
        };
        if ran.restarted {
            self.forcings.note_in_tick(Forcing::WalRestarted);
        }
        if ran.emptied {
            self.forcings.note_in_tick(Forcing::WalEmptied);
        }
    }
}
