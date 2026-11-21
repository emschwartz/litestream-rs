//! Test support for the real-SQLite fault suite (the crate's `testing` feature).
//!
//! Two pieces, both generic over what holds the read lock, so a later backend runs the same suite:
//!
//! - [`vfs`]: [`FaultVfs`], a VFS that delegates to `unix` and fails reads, writes, syncs,
//!   truncates and locks on a schedule, writes short, loses power (every write since a file's
//!   last sync undone) and hangs a process at a sync for a parent to kill; and [`seed_prng`],
//!   which pins SQLite's random numbers so WAL salts repeat.
//! - [`harness`]: [`Rig`], core's `Actor` driven tick by tick over a [`Backend`] on a fake clock,
//!   staging and replica on `FsStore`s, and the simulator's oracle judging the replica against
//!   every transaction SQLite committed; and [`Stepper`], the sync engine's steps one at a time
//!   over a backend, for the cases that need the engine's position and verdicts. Both put the
//!   application in this process or in a child at SQLite's defaults ([`Placement`]); a test binary
//!   that uses the child calls [`serve_app`] first in its `child_process_entry` test.

pub mod harness;
pub mod vfs;

pub use harness::{
    Backend, Placement, Rig, RigConfig, RigError, StepDid, Stepper, rig_actor_config, serve_app,
};
pub use vfs::{Effect, Fault, FaultVfs, FileKind, Fired, HANG_LINE, Op, VfsError, seed_prng};
