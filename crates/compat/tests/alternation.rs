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

/// Go and Rust take turns on one prefix, each turn ended by SIGKILL at a seeded point, and
/// every switch is a plain restart with no snapshot (or the snapshot the engines' rules give
/// after an unseen WAL restart, with its cause); every acknowledged transaction restores to a
/// state the database had, across checkpoints.
///
/// Under review (#32): with v0.5.18, seed 1 once restored transaction 0x42 to commit 103's
/// application pages with a `_litestream_seq` value the database never committed. Rust had
/// been stopped mid-bump (its WAL frame written, the wal-index not yet updated); Go booted,
/// read that unpublished frame as a transaction (v0.5.18 reads the WAL file to its last valid
/// frame; `main` reads the committed end from the wal-index first), and its next sync started
/// past the frame the application's next commit then overwrote. A snapshot healed it there.
#[test]
#[ignore = "review: Go v0.5.18 can stage a WAL frame SQLite never committed (#32); just compat"]
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
