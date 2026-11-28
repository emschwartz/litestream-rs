//! Checksum-tracked LTX files against pinned Go: what Go's level compactor and Go's restore do
//! with files whose `NoChecksum` flag is clear, which litestream-rs does not write
//! (docs/decisions.md D2). Two short explicit scenarios, each a file replica Rust writes at level
//! 0 (a snapshot, then two incremental files), given to Go's own `Compactor.Compact` (through the
//! dumper's compaction hook) and to `litestream restore`:
//!
//! - every file tracked;
//! - a mixed chain: an untracked snapshot and incremental file, then a tracked one, as a replica
//!   would hold if a writer that tracks took over from one that does not.
//!
//! The results are D2's evidence for rollback to Go, recorded there.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::path::Path;
use std::process::Command;

use lsr_compat::{GoVersion, go_binary, go_dump_binary, hash_pages};
use lsr_core::file_name;
use lsr_core::needs::{Resource, must};
use lsr_ltx::{
    DbChecksum, Encoder, Header, HeaderFields, PageChecksum, PageNo, PageSize, Timestamp, Txid,
    TxidRange, WalOrigin,
};

const PAGE: u32 = 4096;

fn page_size() -> PageSize {
    PageSize::new(PAGE).unwrap()
}

fn pgno(n: u32) -> PageNo {
    PageNo::new(n, page_size()).unwrap()
}

/// The pages of a real SQLite database (one table, a few rows), so a restored file opens.
fn base_pages(dir: &Path) -> Vec<Vec<u8>> {
    let path = dir.join("model.db");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "PRAGMA page_size = 4096; CREATE TABLE t (x TEXT); INSERT INTO t VALUES ('a'), ('b');",
    )
    .unwrap();
    drop(db);
    std::fs::read(&path)
        .unwrap()
        .chunks(usize::try_from(PAGE).unwrap())
        .map(<[u8]>::to_vec)
        .collect()
}

fn checksum(pages: &[Vec<u8>]) -> DbChecksum {
    (1_u32..)
        .zip(pages)
        .fold(DbChecksum::EMPTY, |sum, (n, page)| {
            sum.toggle(PageChecksum::of(pgno(n), page))
        })
}

/// One level-0 file: `changed` pages of `after` at `txid`, tracked or not.
fn file(
    txid: u64,
    after: &[Vec<u8>],
    changed: &[u32],
    before: Option<&[Vec<u8>]>,
    tracked: bool,
) -> Vec<u8> {
    let snapshot = before.is_none();
    let header = Header::new(HeaderFields {
        tracked,
        page_size: page_size(),
        commit: u32::try_from(after.len()).unwrap(),
        txids: TxidRange::single(Txid::new(txid).unwrap()),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000 + i64::try_from(txid).unwrap()),
        pre_apply: before.filter(|_| tracked && !snapshot).map(checksum),
        wal: WalOrigin::new(0, 0, 0, 0),
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in changed {
        let page = &after[usize::try_from(*n - 1).unwrap()];
        encoder.write_page(pgno(*n), page).unwrap();
    }
    encoder.finish(tracked.then(|| checksum(after))).unwrap().0
}

/// `pages` with page `n`'s last bytes set to `byte`: a change past SQLite's own data, so the
/// database stays valid.
fn touched(pages: &[Vec<u8>], n: u32, byte: u8) -> Vec<Vec<u8>> {
    let mut out = pages.to_vec();
    let page = &mut out[usize::try_from(n - 1).unwrap()];
    let len = page.len();
    page[len - 8..].fill(byte);
    out
}

/// A replica of three level-0 files whose `tracked` flags are given in order, and the pages
/// after the last.
fn replica(dir: &Path, tracked: [bool; 3]) -> Vec<Vec<u8>> {
    let first = base_pages(dir);
    let last = u32::try_from(first.len()).unwrap();
    let second = touched(&first, last, 0xa5);
    let third = touched(&second, last, 0x5a);
    let level0 = dir.join("replica").join("ltx").join("0");
    std::fs::create_dir_all(&level0).unwrap();
    let all: Vec<u32> = (1..=last).collect();
    let files = [
        file(1, &first, &all, None, tracked[0]),
        file(2, &second, &[last], Some(&first), tracked[1]),
        file(3, &third, &[last], Some(&second), tracked[2]),
    ];
    for (txid, bytes) in (1_u64..).zip(files) {
        let range = TxidRange::single(Txid::new(txid).unwrap());
        std::fs::write(level0.join(file_name(range)), bytes).unwrap();
    }
    third
}

/// Go's error compacting level 0 into level 1, or `None` when it compacted.
fn go_compact(dir: &Path) -> Option<String> {
    let dump = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let output = Command::new(dump)
        .env("LSR_COMPACT", "1")
        .arg(dir.join("replica"))
        .arg("1")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let out: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    out["error"].as_str().map(str::to_owned)
}

/// How Go's restore of the replica's latest state ended.
#[derive(Debug, PartialEq, Eq)]
enum Restored {
    /// The restored file's pages.
    Pages(Vec<Vec<u8>>),
    /// Go's error.
    Refused(String),
}

fn go_restore(dir: &Path) -> Restored {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    let out = dir.join("restored.db");
    let output = Command::new(go)
        .arg("restore")
        .arg("-o")
        .arg(&out)
        .arg(format!("file://{}", dir.join("replica").display()))
        .env_clear()
        .env("HOME", dir)
        .output()
        .unwrap();
    if !output.status.success() {
        return Restored::Refused(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    hash_pages(&out).unwrap();
    Restored::Pages(
        std::fs::read(&out)
            .unwrap()
            .chunks(usize::try_from(PAGE).unwrap())
            .map(<[u8]>::to_vec)
            .collect(),
    )
}

/// Every file tracked: Go restores the chain, and Go's compactor refuses it, because it forces
/// `NoChecksum` on its output while copying the last input's post-apply checksum.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_restores_a_tracked_chain_and_cannot_compact_it() {
    let dir = tempfile::tempdir().unwrap();
    let pages = replica(dir.path(), [true, true, true]);
    assert_eq!(go_restore(dir.path()), Restored::Pages(pages));
    let compacted = go_compact(dir.path());
    assert!(
        compacted
            .as_ref()
            .is_some_and(|e| e.contains("post-apply checksum not allowed")),
        "{compacted:?}"
    );
    let level1 = dir.path().join("replica").join("ltx").join("1");
    let left = std::fs::read_dir(&level1).map_or(0, |d| d.count());
    assert_eq!(left, 0, "a refused compaction leaves no level-1 file");
}

/// Untracked files, then a tracked one: Go restores the chain, and Go's compactor refuses it
/// for the tracked last input, as above.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_restores_untracked_then_tracked_and_cannot_compact_it() {
    let dir = tempfile::tempdir().unwrap();
    let pages = replica(dir.path(), [false, false, true]);
    assert_eq!(go_restore(dir.path()), Restored::Pages(pages));
    let compacted = go_compact(dir.path());
    assert!(
        compacted
            .as_ref()
            .is_some_and(|e| e.contains("post-apply checksum not allowed")),
        "{compacted:?}"
    );
}

/// The control: every file untracked, as litestream-rs writes them, compacts and restores.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_compacts_and_restores_an_untracked_chain() {
    let dir = tempfile::tempdir().unwrap();
    let pages = replica(dir.path(), [false, false, false]);
    assert_eq!(go_compact(dir.path()), None);
    assert_eq!(go_restore(dir.path()), Restored::Pages(pages));
}
