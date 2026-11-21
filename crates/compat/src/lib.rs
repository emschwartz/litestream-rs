//! The Go compatibility harness: pinned Go `litestream` binaries, a seeded workload
//! generator, golden replica fixtures and the helpers the tests read them with.
//!
//! Mechanism. `scripts/build-go-litestream.sh` builds each pinned Go version once
//! ([`GoVersion`]). [`generate`] runs a deterministic SQLite workload while a real
//! `litestream replicate` writes a file replica with short intervals, so the replica holds
//! L0, higher levels and several snapshots. It knows the replica is caught up by comparing
//! the newest L0 file's WAL position with the live WAL (never by sleeping a fixed time), then
//! stops Go, copies the replica into `fixtures/<version>/<scenario>/replica` and records, in
//! `manifest.json`, what that version's own `litestream restore` produced for each target.
//!
//! The fixtures are the contract between Go and Rust: the compat tests restore them with Rust and
//! compare per-page hashes with the manifest. Equality is on pages, never on LTX bytes.
//!
//! Three sets of Go builds. Pinned: [`GoVersion`]'s five, with committed fixtures and live
//! tests, on every push (`just compat`). Releases: every upstream tag `v0.5.N` without a
//! pre-release suffix ([`KNOWN_RELEASES`]), live tests only (`just compat-releases`). Floating:
//! upstream `main` and any tag newer than the table (`just compat-live main`), nightly. The
//! live tests take a [`GoBuild`] and an [`PageEncoding`]; a release that differs from its page encoding is one
//! row in [`QUIRKS`], and tests assert the quirk instead of skipping. Out of scope: Litestream
//! 0.3.x and 0.4.x, a different replica format; the port reads and writes the 0.5 LTX layout only.
//!
//! The Rust side of the matrix is [`rsrestore`]: it restores a fixture replica with
//! `lsr-core`'s executor over `FsStore` and `FileSink`, so tests compare Rust's pages with
//! the pages Go wrote into the manifest. The live half (`just compat`) re-encodes every
//! fixture file with `lsr_ltx::Encoder` and has the pinned Go versions restore and
//! `ltx.Decoder.Verify` it (`go/verify`). [`read_with_vfs`] opens a replica through upstream's
//! read-replica VFS extension, from a small C application (`c/vfs-read.c`), for the direction where
//! the Go VFS reads what Rust wrote.
//!
//! The process half: [`Replicator`] runs Go's `litestream replicate` or Rust's `lsr replicate`
//! on one [`AppFixture`], stopped by SIGINT or killed by SIGKILL, and the watchers re-exported from
//! the handover module ([`position`], [`wait_boot_file`], [`snapshots`]) read what each process
//! staged and uploaded, so a test can tell a boot that continued from one that snapshotted.
//!
//! The replica of a [`AppFixture`] is a directory or a prefix of a local MinIO bucket ([`Remote`],
//! [`S3Replica`]), so the same checks run with Go's AWS SDK and Rust's S3 store on one bucket
//! (`just compat-s3`). Every child process the harness starts runs without the machine's
//! `AWS_*` and `LITESTREAM_*` variables ([`scrub_env`]), and the S3 checks refuse any endpoint
//! but loopback `http` ([`loopback_endpoint`]): they never reach a real object store.

mod binaries;
mod chain;
mod checks;
mod error;
mod generate;
mod golive;
mod handover;
mod manifest;
mod pages;
mod remote;
mod replica;
mod replicator;
mod rsrestore;
mod s3;
mod scenario;
mod vfs;
mod workload;

pub use binaries::{
    FIRST_WAL_INDEX_END_PATCH, GoBuild, GoVersion, KNOWN_RELEASES, LIVE_MODULE, PageEncoding,
    QUIRKS, Quirk, QuirkEntry, S3_MODULE, VERSIONS_ENV, WAL_INDEX_END_COMMIT, builds_in,
    builds_under_test, go_binary, is_live_test, workspace_root,
};
pub use chain::{
    AppFixture, Chain, Outside, Point, STEP_MS, Shape, Written, clock_ending_now, real_now_ms,
};
pub use checks::{fresh, go_restore, pages_of, rust_restore};
pub use error::CompatError;
pub use generate::generate;
pub use golive::{
    GoRestore, Schedule, build_verify_helper, go_verify, restore_with_go, rfc3339_millis,
    write_config, write_config_daily_snapshots, write_config_for, write_config_snapshotting,
};
pub use handover::{
    PROCESS_STEP, position, snapshots, uploaded, wait_boot_file, wait_staged_past, wait_uploaded,
};
pub use manifest::{
    FIXTURE_FORMAT, FileEntry, MANIFEST_FILE, Manifest, Outcome, Target, TargetEntry, all_fixtures,
    fixture_dir, fixtures_root,
};
pub use pages::{PageDigest, hash_pages, integrity_check};
pub use remote::Remote;
pub use replica::{
    Listing, ReplicaFile, Summary, l0_starting_at, list, newest_l0, newest_txid, parse_rel, rel_of,
    summarize, summarize_bytes,
};
pub use replicator::{Kind, Replicator, UP_LINE, lsr_binary, wait_until};
pub use rsrestore::{Staged, chain_files, copy_tree, restore_replica, stage};
pub use s3::{
    ACCESS_KEY_VAR, BUCKET_VAR, ENDPOINT_VAR, MinioEnv, S3Replica, SECRET_KEY_VAR,
    loopback_endpoint, scrub_env,
};
pub use scenario::{Scenario, Step};
pub use vfs::{VfsRead, VfsView, build_vfs_reader, read_plain, read_with_vfs};
pub use workload::{SplitMix, Workload};
