//! Test support for the real-SQLite fault suite (the crate's `testing` feature).
//!
//! Two pieces, both generic over what holds the read lock, so a later backend runs the same suite:
//!
//! - [`vfs`]: [`FaultVfs`], a VFS that delegates to `unix` and fails reads, writes, syncs,
//!   truncates and locks on a schedule, writes short, loses power (what each file keeps of its
//!   writes since its last sync follows a [`CrashLaw`]: none of them, or each dirty block, the
//!   length and each unsynced directory entry independently) and hangs a process at a sync for a
//!   parent to kill; and [`seed_prng`],
//!   which pins SQLite's random numbers so WAL salts repeat.
//! - [`harness`]: [`Rig`], core's `Actor` driven tick by tick over a [`Backend`] on a fake clock,
//!   staging and replica on `FsStore`s, and the simulator's oracle judging the replica against
//!   every transaction SQLite committed; and [`Stepper`], the sync engine's steps one at a time
//!   over a backend, for the cases that need the engine's position and verdicts. Both put the
//!   application in this process or in a child at SQLite's defaults ([`Placement`]); a test binary
//!   that uses the child calls [`serve_app`] first in its `child_process_entry` test.
//! - [`real`]: [`RealSide`], the simulator's SQLite seam over real SQLite, so the simulator's
//!   drawn histories run the engine against this crate on a real database.

pub mod harness;
mod host_fs;
pub mod real;
pub mod vfs;

pub use harness::{
    Backend, Placement, Rig, RigConfig, RigError, StepDid, Stepper, rig_actor_config, serve_app,
};
pub use real::RealSide;
pub use vfs::{
    BlockSize, CrashLaw, Effect, Fault, FaultVfs, FileKind, Fired, HANG_LINE, Op, VfsError,
    seed_prng,
};
