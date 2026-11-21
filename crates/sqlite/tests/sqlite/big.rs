//! The large real-SQLite cases: a 16 KiB-page database past 1 GiB (the lock page), a database
//! past 4 GiB (byte offsets beyond 32 bits), and a WAL larger than the sync cap. Minutes and
//! gigabytes each, so `#[ignore]`d out of `just check`; `just sqlite-big` runs them.
//!
//! The first two keep no model (the oracle would hold every page in memory): the replica's
//! latest state is restored into a file and compared with the database byte for byte once every
//! connection is closed, then checked with `PRAGMA integrity_check`.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::io::Read;
use std::num::NonZeroU64;
use std::path::Path;
use std::process::Command;

use lsr_core::conformance::block_on;
use lsr_core::{Event, RestoreTarget, SyncConfig, Tick};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{Rig, RigConfig, rig_actor_config};
use tempfile::TempDir;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

fn big_config(page_size: u32) -> RigConfig {
    RigConfig {
        page_size,
        oracle: false,
        ..RigConfig::default()
    }
}

/// Free bytes on the filesystem holding `dir`, from `df`. POSIX output (`-P`) in 1 KiB blocks
/// (`-k`) is the form both GNU and BSD `df` print: a header, then one line per filesystem whose
/// fourth column is the available blocks.
fn free_bytes(dir: &Path) -> u64 {
    let out = Command::new("df").arg("-Pk").arg(dir).output().unwrap();
    assert!(out.status.success(), "df -Pk failed: {out:?}");
    let text = String::from_utf8(out.stdout).unwrap();
    let line = text.lines().nth(1).unwrap();
    let kib: u64 = line.split_whitespace().nth(3).unwrap().parse().unwrap();
    kib.checked_mul(1024).unwrap()
}

/// Commits `rows` rows of `bytes` zero bytes each, the actor settling after every commit.
fn grow(rig: &mut Rig<Sqlite>, rows: u64, bytes: u64) {
    for _ in 0..rows {
        rig.exec(&format!("INSERT INTO t(v) VALUES (zeroblob({bytes}))"))
            .unwrap();
        rig.settle().unwrap();
    }
    // Something that is not zeros at the end, so a page misplaced past the boundary shows.
    rig.insert(100_000).unwrap();
    rig.settle().unwrap();
}

fn same_files(a: &Path, b: &Path) {
    #[expect(
        clippy::disallowed_methods,
        reason = "every connection on the database is closed; no lock is held to drop"
    )]
    let (mut a, mut b) = (
        std::fs::File::open(a).unwrap(),
        std::fs::File::open(b).unwrap(),
    );
    assert_eq!(a.metadata().unwrap().len(), b.metadata().unwrap().len());
    let (mut x, mut y) = (vec![0_u8; 8 << 20], vec![0_u8; 8 << 20]);
    let mut at = 0_u64;
    loop {
        let n = a.read(&mut x).unwrap();
        b.read_exact(&mut y[..n]).unwrap();
        assert!(
            x[..n] == y[..n],
            "the files differ in the 8 MiB from byte {at}"
        );
        if n == 0 {
            return;
        }
        at += u64::try_from(n).unwrap();
    }
}

/// Closes everything, restores the replica's latest state into a file, and compares it with
/// the database.
fn restore_matches(rig: Rig<Sqlite>, root: &Path) {
    let (db, replica) = (rig.db().to_owned(), rig.replica_root().to_owned());
    drop(rig);
    let target = root.join("restored.db");
    let sink = lsr_local::FileSink::create(&target).unwrap();
    block_on(lsr_core::restore(
        &lsr_store_fs::FsStore::new(&replica),
        RestoreTarget::Latest,
        sink,
    ))
    .unwrap();
    same_files(&db, &target);
    let check: String = rusqlite::Connection::open(&target)
        .unwrap()
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}

/// Upstream `Test1GBBoundary`: at 16 KiB pages the lock page is page 65537, the one whose bytes
/// hold SQLite's file locks and which no database ever stores. The database grows across it
/// through the WAL; the replica restores page for page, the lock page zeros.
#[test]
#[ignore = "large; just sqlite-big"]
fn a_16k_page_database_past_1_gib_crosses_the_lock_page() {
    let dir = TempDir::new().unwrap();
    let mut rig = Rig::<Sqlite>::new(dir.path(), big_config(16384)).unwrap();
    grow(&mut rig, 132, 8 * MIB);
    let pages = lsr_core::PageSource::page_count(rig.backend().unwrap())
        .unwrap()
        .get();
    let lock_page = u32::try_from(GIB / 16384 + 1).unwrap();
    assert!(pages > lock_page + 100, "{pages} pages");
    restore_matches(rig, dir.path());
}

/// Byte offsets past 4 GiB: the database file, the WAL reads and the restore all address
/// beyond `u32::MAX` bytes. Needs about 9 GiB of disk; skipped below 20 GB free.
#[test]
#[ignore = "large; just sqlite-big"]
fn a_database_past_4_gib_replicates_and_restores() {
    let dir = TempDir::new().unwrap();
    let free = free_bytes(dir.path());
    if free < 20_000_000_000 {
        println!("skipped: {free} bytes free, the case needs 20 GB");
        return;
    }
    let mut rig = Rig::<Sqlite>::new(dir.path(), big_config(4096)).unwrap();
    grow(&mut rig, 66, 64 * MIB);
    let pages = lsr_core::PageSource::page_count(rig.backend().unwrap())
        .unwrap()
        .get();
    assert!(u64::from(pages) * 4096 > 4 * GIB, "{pages} pages");
    restore_matches(rig, dir.path());
}

/// A WAL past the sync cap (64 MiB, Go's default): commits pile up while the actor is not
/// ticking, then it catches up in steps that each stop at the cap, and the oracle restores
/// every transaction.
#[test]
#[ignore = "large; just sqlite-big"]
fn a_wal_larger_than_the_sync_cap_syncs_in_steps() {
    let dir = TempDir::new().unwrap();
    let mut actor = rig_actor_config();
    actor.sync = SyncConfig::default();
    actor.checkpoint.min_pages = None;
    actor.checkpoint.truncate_pages = None;
    let cap = actor.sync.cap.map_or(u64::MAX, NonZeroU64::get);
    let cfg = RigConfig {
        actor,
        ..RigConfig::default()
    };
    let mut rig = Rig::<Sqlite>::new(dir.path(), cfg).unwrap();
    rig.insert(100).unwrap();
    rig.settle().unwrap();
    let from = rig.ticks().len();
    for _ in 0..48 {
        rig.insert(2 * 1024 * 1024).unwrap();
    }
    let len = std::fs::metadata(rig.wal()).unwrap().len();
    assert!(len > cap + 16 * MIB, "the WAL is {len} bytes");
    rig.settle().unwrap();
    let steps = rig.ticks()[from..]
        .iter()
        .filter(|t| {
            matches!(
                t,
                Tick::Ran {
                    event: Event::Synced { files: 1 },
                    ..
                }
            )
        })
        .count();
    assert!(steps >= 2, "the backlog took {steps} steps");
    let checked = rig.check().unwrap();
    assert!(checked.restored > 2, "{checked:?}");
}
