//! The deterministic simulator's pieces.
//!
//! [`sqlite`] is SQLite in WAL mode as rules: the source, page source and
//! checkpointer the sync engine runs against in the simulator, checked against
//! real SQLite by `tests/sqlite_rules.rs`. The harness around it: [`store`]
//! wraps a store with faults, crash points, a ground-truth tap and power-loss
//! bookkeeping; [`exec`] crashes a future; [`model`] and [`oracle`] hold what
//! the database really was and judge the replica against it; [`world`] builds
//! all of it for one run; [`seed`] names a run. [`generate`] draws a history from
//! a seed, [`resources`] holds the resource bounds, and [`run`] applies a
//! history to the actor, judges it, and shrinks a failure.
//!
//! The actor (core's `actor`) plugs in through one seam, a future: [`run`]
//! builds the actor from [`World`]'s handles and passes each tick's future to
//! [`World::run_tick`].

pub mod exec;
pub mod generate;
pub mod model;
pub mod oracle;
pub mod resources;
pub mod run;
pub mod seed;
pub mod sqlite;
pub mod store;
#[cfg(test)]
mod testkit;
pub mod world;

pub use exec::{Polls, RunEnd, count_polls, run_until};
pub use generate::{EnvFault, Op, Target, Weights, history};
pub use model::{CommitId, CommitRec, Model, PageImage};
pub use oracle::{Acked, Bound, Checked, Violation, bug_fault, check, within};
pub use run::{Failure, Opts, RunReport, run, run_with, shrink};
pub use seed::{ParseSeedError, Seed, replay_line};
pub use sqlite::{
    AppCommit, CheckpointNumbers, CrashKind, ReaderId, SimCheckpointer, SimPages, SimSource,
    SimSqlite, SimWal, StrangerMode,
};
pub use store::{
    FaultHandle, FaultShape, Faults, Lost, Stage, StageHandle, Tap, TapCounts, TapEvent, TapLog,
    Yield,
};
pub use world::{CrashPlan, RemoteStore, StagingStore, TickEnd, World, pick_crash};
