//! The deterministic simulator's pieces.
//!
//! [`sqlite`] is SQLite in WAL mode as rules: the source, page source and
//! checkpointer the sync engine runs against in the simulator, checked against
//! real SQLite by `tests/sqlite_rules.rs`.

pub mod sqlite;

pub use sqlite::{
    AppCommit, CheckpointNumbers, CrashKind, ReaderId, SimCheckpointer, SimPages, SimSource,
    SimSqlite, SimWal, StrangerMode,
};
