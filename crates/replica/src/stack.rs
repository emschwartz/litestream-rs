//! The stack every thread the library and the daemon start is given, named rather than
//! left at the standard library's 2 MiB default, so a process with thousands of databases
//! reserves a known amount of address space per thread (and commits it, under strict
//! overcommit).
//!
//! **Threads per database.** One replicator thread for the database's life
//! ([`ENGINE_STACK`]); one verify thread while a scheduled verify runs, when
//! `validation.interval` is set ([`ENGINE_STACK`]); one short-lived watcher while a final
//! sync is awaited ([`WAIT_STACK`]). The daemon adds a fixed few for the whole process: the
//! signal reader, the waiter for databases not created yet ([`ENGINE_STACK`], since it starts
//! replicators and restores), the HTTP listener and its connections, the `-exec` reaper
//! ([`WAIT_STACK`] each). So N databases take N to 2N long-lived threads; a pool is issue #17.
//!
//! **How the sizes were chosen** (2026-11-21, x86-64 Linux, a debug build of the workspace,
//! whose own code runs unoptimised and so deeper than a release build; dependencies are at
//! opt-level 3 in both). With every engine thread at a probed size, the replica, CLI and
//! sidecar suites (full syncs, compaction, snapshots, restores over the file and in-memory S3
//! stores, scheduled verifies, the daemon's waiter) overflowed the replicator thread at
//! 160 KiB and passed at 192 KiB; an HTTPS request through ureq and rustls alone needs 48 KiB
//! (it fails at 32). [`ENGINE_STACK`] is 512 KiB: about twice the two together. The waiting
//! threads passed at 16 KiB, the smallest stack the platform gives; [`WAIT_STACK`] is four
//! times that.

use std::io;
use std::thread::{Builder, JoinHandle};

/// The stack of a thread that runs the engine, SQLite, the stores or a restore.
pub const ENGINE_STACK: usize = 512 * 1024;

/// The stack of a thread that only waits and wakes another (signals, a child's exit, a
/// final sync's answer) or answers a small HTTP request.
pub const WAIT_STACK: usize = 64 * 1024;

/// Starts a thread named `name` with a `stack` of the given bytes.
///
/// # Errors
/// The OS refused the thread: the process is at its thread limit (`RLIMIT_NPROC`,
/// `kernel.threads-max`) or out of memory or address space.
pub fn spawn<F, T>(name: &str, stack: usize, f: F) -> io::Result<JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Builder::new()
        .name(name.to_owned())
        .stack_size(stack)
        .spawn(f)
}
