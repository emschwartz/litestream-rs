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
//! Temporary pieces, to be replaced when their crate lands (each is marked where it lives):
//! [`ltxkey`] and [`ltxhdr`] stand in for the `lsr-ltx` key and header types, and
//! [`model`] stands in for the restore planner of `lsr-core`.

mod binaries;
mod error;
mod generate;
mod golive;
mod ltxhdr;
mod ltxkey;
mod manifest;
mod model;
mod pages;
mod replica;
mod scenario;
mod workload;

pub use binaries::{GoVersion, go_binary, workspace_root};
pub use error::CompatError;
pub use generate::generate;
pub use golive::{GoRestore, restore_with_go, rfc3339_millis, write_config};
pub use ltxhdr::LtxHeader;
pub use ltxkey::{KeyLayout, LocalKey, SNAPSHOT_LEVEL};
pub use manifest::{
    FIXTURE_FORMAT, FileEntry, MANIFEST_FILE, Manifest, Outcome, Target, TargetEntry, all_fixtures,
    fixture_dir, fixtures_root,
};
pub use pages::{PageDigest, hash_pages, integrity_check};
pub use scenario::Scenario;
