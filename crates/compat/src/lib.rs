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
//! Three sets of Go builds. Pinned: [`GoVersion`]'s five, with committed fixtures and live
//! tests, on every push (`just compat`). Releases: every upstream tag `v0.5.N` without a
//! pre-release suffix ([`KNOWN_RELEASES`]), live tests only (`just compat-releases`). Floating:
//! upstream `main` and any tag newer than the table (`just compat-live main`), nightly. The
//! live tests take a [`GoBuild`] and an [`Era`]; a release that differs from its era is one
//! row in [`QUIRKS`], and tests assert the quirk instead of skipping. Out of scope: Litestream
//! 0.3.x and 0.4.x, a different replica format; the port reads and writes the 0.5 LTX layout only.
//!
//! The Rust side of the matrix is [`rsrestore`]: it restores a fixture replica with
//! `lsr-core`'s executor over `FsStore` and `FileSink`, so tests compare Rust's pages with
//! the pages Go wrote into the manifest. The live half (`just compat`) re-encodes every
//! fixture file with `lsr_ltx::Encoder` and has the pinned Go versions restore and
//! `ltx.Decoder.Verify` it (`go/verify`). [`read_with_vfs`] opens a replica through upstream's
//! read-replica VFS extension, from a small C host (`c/vfs-read.c`), for the direction where
//! the Go VFS reads what Rust wrote.
//!
//! The process half: [`Replicator`] runs Go's `litestream replicate` or Rust's `lsr replicate`
//! on one [`Site`], stopped by SIGINT or killed by SIGKILL, and the watchers re-exported from
//! the handover module ([`tip`], [`wait_boot_file`], [`snapshots`]) read what each process
//! staged and uploaded, so a test can tell a boot that continued from one that snapshotted.

mod binaries;
mod chain;
mod error;
mod generate;
mod golive;
mod handover;
mod manifest;
mod pages;
mod replica;
mod replicator;
mod rsrestore;
mod scenario;
mod vfs;
mod workload;

pub use binaries::{
    Era, FIRST_WAL_INDEX_END_PATCH, GoBuild, GoVersion, KNOWN_RELEASES, QUIRKS, Quirk, QuirkEntry,
    VERSIONS_ENV, WAL_INDEX_END_COMMIT, builds_in, builds_under_test, go_binary, workspace_root,
};
pub use chain::{
    Chain, Outside, Point, STEP_MS, Shape, Site, Written, clock_ending_now, real_now_ms,
};
pub use error::CompatError;
pub use generate::generate;
pub use golive::{
    GoRestore, build_verify_helper, go_verify, restore_with_go, rfc3339_millis, write_config,
    write_config_daily_snapshots, write_config_snapshotting,
};
pub use handover::{
    PROCESS_STEP, snapshots, tip, uploaded, wait_boot_file, wait_staged_past, wait_uploaded,
};
pub use manifest::{
    FIXTURE_FORMAT, FileEntry, MANIFEST_FILE, Manifest, Outcome, Target, TargetEntry, all_fixtures,
    fixture_dir, fixtures_root,
};
pub use pages::{PageDigest, hash_pages, integrity_check};
pub use replica::{
    Listing, ReplicaFile, Summary, l0_starting_at, list, newest_l0, newest_txid, parse_rel, rel_of,
    summarize,
};
pub use replicator::{Kind, Replicator, lsr_binary, wait_until};
pub use rsrestore::{Staged, chain_files, copy_tree, restore_replica, stage};
pub use scenario::{Scenario, Step};
pub use vfs::{VfsRead, VfsView, build_vfs_reader, read_plain, read_with_vfs};
pub use workload::{SplitMix, Workload};
