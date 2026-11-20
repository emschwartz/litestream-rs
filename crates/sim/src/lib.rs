//! The deterministic simulator's pieces.
//!
//! [`sqlite`] is SQLite in WAL mode as rules: the source, page source and
//! checkpointer the sync engine runs against in the simulator, checked against
//! real SQLite by `tests/sqlite_rules.rs`. The harness around it: [`store`]
//! wraps a store with faults, crash points, a ground-truth tap and power-loss
//! bookkeeping; [`exec`] crashes a future; [`model`] and [`oracle`] hold what
//! the database really was and judge the replica against it; [`world`] builds
//! all of it for one run; [`seed`] names a run.
//!
//! The actor (core's `actor`) plugs in through one seam, a future: the run
//! loop (task 6f) builds the actor from [`World`]'s handles and passes each
//! tick's future to [`World::run_tick`]. Nothing here owns loop logic.

pub mod exec;
pub mod model;
pub mod oracle;
pub mod seed;
pub mod sqlite;
pub mod store;
#[cfg(test)]
mod testkit;
pub mod world;

pub use exec::{Polls, RunEnd, count_polls, run_until};
pub use model::{CommitId, CommitRec, Model, PageImage};
pub use oracle::{Acked, Bound, Checked, Violation, bug_fault, check, within};
pub use seed::{ParseSeedError, Seed, replay_line};
pub use sqlite::{
    AppCommit, CheckpointNumbers, CrashKind, ReaderId, SimCheckpointer, SimPages, SimSource,
    SimSqlite, SimWal, StrangerMode,
};
pub use store::{
    FaultHandle, FaultShape, Faults, Lost, Stage, StageHandle, Tap, TapCounts, TapEvent, TapLog,
    TapResult, Yield,
};
pub use world::{CrashPlan, RemoteStore, StagingStore, TickEnd, World, pick_crash};
