//! The alternation test: Go's `litestream replicate` and Rust's `lsr replicate DB REPLICA`
//! take turns on one database, meta directory and replica, each turn ended by SIGKILL at a
//! point chosen from a seed the test prints, and every switch must be a plain restart unless
//! the WAL restarted unseen before the kill. The runs cross checkpoints and WAL restarts: Go
//! checkpoints from its config file (every 5 s, at 1,000 pages, TRUNCATE at 1,500), Rust with
//! its own defaults (PASSIVE at 1,000 pages). The run, its kill points and its oracle are in
//! `alternate/mod.rs`, shared with `sidecar.rs`, which runs the same alternation with both
//! sides reading one config file, so Rust also checkpoints on Go's schedule and runs TRUNCATE.
//!
//! Seeds: `LSR_ALTERNATION_SEED` (decimal) replays a run; without it the seed is the clock.
//! The Go builds are those under test in the block era (`just compat-live`); a frame-era
//! build cannot read Rust's files, so it has nothing to alternate with.

#[expect(
    dead_code,
    reason = "each test binary starts Rust one way, so the other way's variant is unused in it"
)]
mod alternate;

use alternate::{Launch, alternate, seed};
use lsr_compat::{Era, builds_in, lsr_binary};

/// The evidence the exception for #38 rests on is found when a writer leaves a whole
/// transaction past the wal-index's end, in a continued WAL and as the first frame of a
/// restarted one, its state is the one SQLite recovers with that transaction, and it is gone
/// once the application's next commit overwrites it. No Go binary needed.
#[test]
fn a_tail_past_the_wal_index_is_recognised() {
    alternate::uncommitted::a_tail_past_the_wal_index_is_recognised();
}

/// Go and Rust take turns on one prefix, each turn ended by SIGKILL at a seeded point, and
/// every switch is a plain restart with no snapshot (or the snapshot the engines' rules give
/// after an unseen WAL restart, with its cause); every acknowledged transaction restores to a
/// state the database had, across checkpoints.
///
/// For a Go build without upstream bcd69ae (every release through v0.5.18), one outcome is
/// accepted and asserted exactly (#38): Go's boot transaction holds a WAL tail SQLite never
/// committed, left by Rust stopped mid-commit, and restores to precisely that tail's state
/// (`alternate/uncommitted.rs`). The run prints how often it was seen.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_rust_alternate_on_one_prefix() {
    let builds = builds_in(Era::Blocks);
    if builds.is_empty() {
        println!("no block-era build under test: a frame-era Go cannot read Rust's files");
        return;
    }
    let lsr = lsr_binary().expect("lsr builds");
    let seed = seed();
    for build in &builds {
        alternate(build, &lsr, seed, Launch::Arguments);
    }
}
