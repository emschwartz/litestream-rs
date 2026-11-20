//! The Go compatibility harness: pinned Go `litestream` binaries, a seeded workload
//! generator, golden replica fixtures and the helpers later tasks read them with.
//!
//! Mechanism. `scripts/build-go-litestream.sh` builds each pinned Go version once
//! ([`GoVersion`]). [`generate`] runs a deterministic SQLite workload while a real
//! `litestream replicate` writes a file replica with short intervals, so the replica holds
//! L0, higher levels and several snapshots. It knows the replica is caught up by comparing
//! the newest L0 file's WAL position with the live WAL (never by sleeping a fixed time), then
//! stops Go, copies the replica into `fixtures/<version>/<scenario>/replica` and records, in
//! `manifest.json`, what that version's own `litestream restore` produced for each target.
//!
//! The fixtures are the contract between Go and Rust: later tasks restore them with Rust and
//! compare per-page hashes with the manifest. Equality is on pages, never on LTX bytes.
//!
//! The Rust side of the matrix is [`rsrestore`]: it restores a fixture replica with
//! `lsr-core`'s executor over `FsStore` and `FileSink`, so tests compare Rust's pages with
//! the pages Go wrote into the manifest. The live half (`just compat`) re-encodes every
//! fixture file with `lsr_ltx::Encoder` and has the pinned Go versions restore and
//! `ltx.Decoder.Verify` it (`go/verify`).

mod binaries;
mod chain;
mod error;
mod generate;
mod golive;
mod live_source;
mod manifest;
mod pages;
mod replica;
mod replicator;
mod rsrestore;
mod scenario;
mod workload;

pub use binaries::{GoVersion, go_binary, workspace_root};
pub use chain::{
    Chain, Outside, Point, STEP_MS, Shape, Site, Written, clock_ending_now, real_now_ms,
};
pub use error::CompatError;
pub use generate::generate;
pub use golive::{
    GoRestore, build_verify_helper, go_verify, restore_with_go, rfc3339_millis, write_config,
    write_config_snapshotting,
};
pub use live_source::LiveSqlite;
pub use manifest::{
    FIXTURE_FORMAT, FileEntry, MANIFEST_FILE, Manifest, Outcome, Target, TargetEntry, all_fixtures,
    fixture_dir, fixtures_root,
};
pub use pages::{PageDigest, hash_pages, integrity_check};
pub use replica::{Listing, ReplicaFile, list, newest_l0, parse_rel, rel_of};
pub use replicator::{Replicator, wait_until};
pub use rsrestore::{Staged, chain_files, copy_tree, restore_replica, stage};
pub use scenario::Scenario;
