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
//! **How the sizes were chosen.** [`ENGINE_STACK`] was set to each probed size, then the
//! replica, sidecar and CLI suites were run. They cover full syncs, compaction, snapshots,
//! scheduled verifies, the daemon's waiter, and restores over the file and in-memory S3 stores,
//! through real SQLite. An overflow aborts the test. Measured twice on x86-64 Linux: on
//! 2026-11-23 (main 079ceb3), and on 2026-11-25 (main bc16a71, under emulation on an arm64
//! host) after the land gate's half-stack run overflowed in CI.
//!
//! | build | 2026-11-23 passes at / fails at | 2026-11-25 passes at / fails at |
//! |---|---|---|
//! | debug (the workspace's own code unoptimised, dependencies at opt-level 3) | 240 KiB / 232 KiB | 272 KiB / 264 KiB |
//! | release | 64 KiB / 48 KiB | 48 KiB / 40 KiB |
//!
//! On arm64 macOS the debug build passed at 248 KiB and failed at 240 on the second date, which
//! is why a run there did not show what CI did: a Linux thread loses more of its stack to
//! thread-local storage before the first frame.
//!
//! The depth is ordinary call frames, about six times shallower in release. In a debug build
//! each `async fn` on the path from the engine's loop down to a compaction (the loop, the tick,
//! the maintenance step, the job, the merge) has a frame of 15 to 37 KiB; release is far
//! smaller. An HTTPS request
//! through ureq and rustls needed 48 KiB alone when last measured (it fails at 32); it is not
//! in these suites, which need no network. [`ENGINE_STACK`] is 768 KiB: 2.8 times the debug
//! peak and 16 times the release peak, with room for the HTTPS path on top of either
//! (`docs/decisions/D202.md`). The stack is reserved
//! address space; only the pages a thread touches are resident (about 48 KiB in release).
//! [`WAIT_STACK`] is 64 KiB; the waiting threads passed at 16 KiB, the smallest stack the
//! platform gives.
//!
//! **The headroom gate.** Under the `test-half-stack` feature, which `just check` turns on,
//! [`spawn`] gives every thread half the stack it asks for, so every suite runs with half the
//! shipped size and a path that grows past it fails a test before it can overflow in the
//! field. The debug peak sits 112 KiB under that half on x86-64 Linux: when the gate trips,
//! measure again (`docs/decisions/D087.md`, `docs/decisions/D202.md`). It trips on Linux
//! first, so a green run on macOS does not show the margin CI has. The feature does not
//! compile without debug assertions, so it cannot reach a release build.

#[cfg(all(feature = "test-half-stack", not(debug_assertions)))]
compile_error!(
    "`test-half-stack` halves every thread's stack for tests; never build it into a release"
);

use std::io;
use std::thread::{Builder, JoinHandle};

/// The stack of a thread that runs the engine, SQLite, the stores or a restore.
pub const ENGINE_STACK: usize = 768 * 1024;

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
