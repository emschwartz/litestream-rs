//! The alternation test: Go's `litestream replicate` and Rust's `lsr replicate DB REPLICA`
//! take turns on one database, meta directory and replica, each turn ended by SIGKILL at a
//! point chosen from a seed the test prints, and every switch must be a plain restart. The
//! run, its kill points and its oracle are in `alternate/mod.rs`, shared with `sidecar.rs`,
//! which runs the same alternation with both sides reading one config file.
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
/// every switch is a plain restart with no snapshot; every acknowledged transaction restores
/// to a state the database had.
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
