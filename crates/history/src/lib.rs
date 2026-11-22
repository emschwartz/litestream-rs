//! An outside-the-process oracle for replication: is what I restored a state my application
//! committed?
//!
//! The engine, its simulator and the simulator's oracle can share one imagined model of what a
//! disk, SQLite and a bucket hand back. This crate shares nothing with them: no engine crate is a
//! dependency (its manifest test holds it to that), and it never sees a transaction id, a WAL
//! offset, salts or an LTX header. It reads two things:
//!
//! - **The commit log** ([`Log`]), written by the driver from what the application saw: each
//!   acknowledged commit, each commit whose outcome is unknown, and what the database held when
//!   it came back from a crash. Local rewinds fork it, so it is a tree.
//! - **A restored database**, reduced to a [`Digest`] of its content: in the simulator, of its
//!   pages; on real SQLite ([`chain`], feature `sqlite`), of the self-certifying chain table after
//!   `PRAGMA integrity_check` and a row-by-row hash check.
//!
//! The [`Judge`] then says whether the restore is a node of the tree, whether `Latest` only ever
//! moves forward on a branch (or a recorded rewind explains it), whether `AsOf(t)` is the node
//! live at `t` within a bound, whether lag stays bounded with no fault active, and it counts
//! restores that hold commits the database lost ([`AheadMode`]).

mod digest;
mod judge;
mod log;

#[cfg(feature = "sqlite")]
pub mod chain;

pub use digest::Digest;
pub use judge::{AheadMode, Anomaly, Counts, Judge, Signals, Target};
pub use log::{Log, NodeId, Rewind};
