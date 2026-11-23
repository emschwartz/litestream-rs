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
//! **How the sizes were chosen.** Measured on 2026-11-23, x86-64 Linux, on main 079ceb3.
//! [`ENGINE_STACK`] was set to each probed size, then the replica, sidecar and CLI suites were
//! run. They cover full syncs, compaction, snapshots, scheduled verifies, the daemon's waiter,
//! and restores over the file and in-memory S3 stores, through real SQLite. An overflow aborts
//! the test.
//!
//! | build | passes at | fails at |
//! |---|---|---|
//! | debug (the workspace's own code unoptimised, dependencies at opt-level 3) | 240 KiB | 232 KiB |
//! | release | 64 KiB | 48 KiB |
//!
//! The depth is ordinary call frames, about four times shallower in release: the largest
//! future `block_on` holds on the stack is the engine's tick, 4.4 KiB. An HTTPS request through
//! ureq and rustls needed 48 KiB alone when last measured (it fails at 32); it is not in these
//! suites, which need no network. [`ENGINE_STACK`] is 512 KiB: about twice the debug peak and
//! eight times the release peak, with room for the HTTPS path on top of either.
//! [`WAIT_STACK`] is 64 KiB; the waiting threads passed at 16 KiB, the smallest stack the
//! platform gives.
//!
//! **The headroom gate.** Under the `test-half-stack` feature, which `just check` turns on,
//! [`spawn`] gives every thread half the stack it asks for, so every suite runs with half the
//! shipped size and a path that grows past it fails a test before it can overflow in the
//! field. The debug peak sat 16 KiB under that half when measured: the gate trips early as
//! the code grows, which is when to measure again (`docs/decisions.md` D87). The feature does
//! not compile without debug assertions, so it cannot reach a release build.

#[cfg(all(feature = "test-half-stack", not(debug_assertions)))]
compile_error!(
    "`test-half-stack` halves every thread's stack for tests; never build it into a release"
);

use std::io;
use std::thread::{Builder, JoinHandle};

/// The stack of a thread that runs the engine, SQLite, the stores or a restore.
pub const ENGINE_STACK: usize = 512 * 1024;

/// The stack of a thread that only waits and wakes another (signals, a child's exit, a
/// final sync's answer) or answers a small HTTP request: 64 KiB.
pub const WAIT_STACK: usize = 65_536;

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
    // The headroom gate (see the module docs): tests run with half the shipped stack.
    let stack = if cfg!(feature = "test-half-stack") {
        stack / 2
    } else {
        stack
    };
    Builder::new()
        .name(name.to_owned())
        .stack_size(stack)
        .spawn(f)
}
