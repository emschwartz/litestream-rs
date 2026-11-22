//! The deterministic simulator's pieces.
//!
//! [`sqlite`] is SQLite in WAL mode as rules: the source, page source and
//! checkpointer the sync engine runs against in the simulator, checked against
//! real SQLite by `tests/sim/sqlite_rules.rs`; its power losses follow
//! [`crash_law`], the disk law it shares with the real-SQLite fault VFS, and
//! real SQLite recovers what they leave. The harness around it: [`store`]
//! wraps a store with faults, crash points, a ground-truth recorder and power-loss
//! bookkeeping; [`exec`] crashes a future; [`model`] and [`oracle`] hold what
//! the database really was and judge the replica against it, and [`history`]
//! judges the same replica by content alone, sharing nothing with the engine; [`world`] builds
//! all of it for one run; [`seed`] names a run. [`generate`] draws a history from
//! a seed, [`resources`] holds the resource bounds, [`liveness`] judges each
//! snapshot against the faults that could justify it, and [`run`] applies a
//! history to the actor, judges it, and shrinks a failure. [`staging`] puts the real directory
//! store (`lsr-store-fs`) under the staging wrappers in a share of runs, on [`memfs`], an
//! in-memory filesystem under the same crash law, so a power loss acts on the store's own syncs.
//!
//! The actor (core's `actor`) plugs in through one seam, a future: [`run`]
//! builds the actor from [`World`]'s handles and passes each tick's future to
//! [`World::run_tick`].

pub mod budget;
pub mod clock;
pub mod contract;
pub mod crash_law;
pub mod domain;
pub mod exec;
pub mod generate;
pub mod history;
pub mod liveness;
pub mod memfs;
pub mod model;
pub mod oracle;
pub mod reach;
pub mod recovery;
pub mod resources;
pub mod run;
pub mod scenario;
pub mod seed;
pub mod sqlite;
pub mod staging;
pub mod store;
#[cfg(test)]
mod testkit;
pub mod world;

pub use clock::{ClockLaw, SimClock};
pub use exec::{Polls, RunEnd, count_polls, run_until};
pub use generate::{
    AfterRestart, AppAt, AppOp, EnvFault, Generator, Hold, Op, Target, Weights, frozen_history,
    history,
};
pub use model::{CommitId, CommitRec, Model, PageImage, Recovery};
pub use oracle::{
    Acked, Bound, Checked, Ground, Violation, Windows, bug_fault, check, check_with, within,
};
pub use reach::Reach;
pub use run::{Failure, Judges, Opts, RunReport, run, run_with, shrink};
pub use seed::{ParseDayError, ParseSeedError, SEEDS_PER_NIGHT, Seed, nightly_base, replay_line};
pub use sqlite::{
    AppCommit, AutoTally, CheckpointNumbers, CrashKind, CreatedAt, InTickAt, InTickCommit,
    InTickDone, InTickOp, InTickRan, InTickSite, OtherProcessMode, ReaderId, SimCheckpointer,
    SimPages, SimSource, SimSqlite, SimWal,
};
pub use store::{
    FaultHandle, FaultShape, Faults, HeldFault, HighWater, Lost, Recorder, RecorderCounts,
    RecorderEvent, RecorderLog, Volatile, VolatileHandle, YieldEachCall,
};
pub use world::{
    CommittedInTick, CrashPlan, RemoteStore, StagedEditError, StagingStore, TickEnd, World,
    pick_crash,
};
