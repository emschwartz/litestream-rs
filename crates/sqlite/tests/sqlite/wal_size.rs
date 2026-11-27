//! The WAL file's size after a restart: SQLite cuts the file back to `journal_size_limit` only on
//! the connection whose write restarts the log, which after the replicator's own checkpoint is
//! its sequence bump on the writer connection (upstream Litestream #1605: a WAL that kept its
//! 1.7 GB peak for good). The actor cases run with the application in both placements.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use lsr_core::{CheckpointMode, Checkpointer, Event, Tick};
use lsr_sqlite::testing::{Placement, Rig, RigConfig, rig_actor_config};
use lsr_sqlite::{DEFAULT_JOURNAL_SIZE_LIMIT, Sqlite, SqliteConfig, app_connection};
use rusqlite::Connection;
use tempfile::TempDir;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

fn len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn database(dir: &Path) -> (Connection, PathBuf) {
    let db = dir.join("app.db");
    let app = Connection::open(&db).unwrap();
    app.execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE t (v BLOB);")
        .unwrap();
    app_connection(&app).unwrap();
    (app, db)
}

/// One transaction of `rows` rows of `bytes` bytes: the WAL grows by more than their product.
/// One line, since the application in a child process takes one statement per line.
fn burst_sql(rows: u64, bytes: u64, blob: &str) -> String {
    format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {rows}) \
         INSERT INTO t (v) SELECT {blob}({bytes}) FROM n;"
    )
}

/// The replicator's PASSIVE checkpoint, which backfills everything, then its sequence bump,
/// which restarts the log.
fn checkpoint_and_restart(sqlite: &mut Sqlite) {
    let run = sqlite.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(!run.busy && run.log == run.checkpointed, "{run:?}");
    sqlite.bump_seq().unwrap();
}

/// The bug: a WAL grown well past 64 MiB, then a checkpoint and restart through the library,
/// left the file at its peak. At the default config it is cut back to at most 64 MiB.
#[test]
fn a_restart_by_the_replicator_cuts_the_wal_back_to_the_default_limit() {
    let dir = TempDir::new().unwrap();
    let (app, db) = database(dir.path());
    let mut sqlite = Sqlite::open(&db).unwrap();
    app.execute_batch(&burst_sql(80, MIB, "zeroblob")).unwrap();
    let wal = sqlite.wal_path();
    let peak = len(&wal);
    assert!(peak > 80 * MIB, "{peak}");
    checkpoint_and_restart(&mut sqlite);
    let after = len(&wal);
    assert!(
        after <= DEFAULT_JOURNAL_SIZE_LIMIT.get(),
        "the WAL kept {after} bytes of its {peak}"
    );
}

struct Burst {
    rig: Rig<Sqlite>,
    /// The file's length once the burst committed.
    peak: u64,
    /// Its length once the actor settled after it.
    after: u64,
}

/// A rig at `limit` that boots, then commits one burst of about 300 KiB (some 75 frames) and
/// settles: the actor syncs it, its PASSIVE backfills it all (no reader holds it), and its
/// sequence bump restarts the log. Never a TRUNCATE, which would empty the file at any limit.
fn burst_through_the_actor(
    dir: &TempDir,
    placement: Placement,
    limit: Option<NonZeroU64>,
) -> Burst {
    let cfg = RigConfig {
        journal_size_limit: limit,
        ..RigConfig::placed(placement)
    };
    let mut rig = Rig::<Sqlite>::new(dir.path(), cfg).unwrap();
    rig.insert(100).unwrap();
    rig.settle().unwrap();
    let begin = rig.ticks().len();
    rig.exec(&burst_sql(100, 3000, "randomblob")).unwrap();
    let peak = len(&rig.wal());
    assert!(peak > 300 * KIB, "{peak}");
    rig.settle().unwrap();
    assert!(rig.checkpointed_since(begin, CheckpointMode::Passive, false));
    for busy in [false, true] {
        assert!(!rig.checkpointed_since(begin, CheckpointMode::Truncate, busy));
    }
    let after = len(&rig.wal());
    Burst { rig, peak, after }
}

/// A WAL several times the limit, with the TRUNCATE threshold out of reach, shrinks by itself:
/// the engine's PASSIVE checkpoints backfill it, and its sequence bump on the writer connection
/// (the one holding `journal_size_limit`) restarts the log and cuts the file, though the
/// application, which sets no limit, wrote the burst through its own connection.
#[test]
fn a_wal_several_times_the_limit_shrinks_with_truncate_out_of_reach() {
    const BOUND: u32 = 5_000;
    for placement in Placement::ALL {
        let dir = TempDir::new().unwrap();
        let mut actor = rig_actor_config();
        actor.checkpoint.truncate_pages = None;
        let cfg = RigConfig {
            actor,
            oracle: false,
            ..RigConfig::placed(placement)
        };
        let mut rig = Rig::<Sqlite>::new(dir.path(), cfg).unwrap();
        rig.insert(100).unwrap();
        rig.settle().unwrap();
        let begin = rig.ticks().len();
        let limit = DEFAULT_JOURNAL_SIZE_LIMIT.get();
        rig.exec(&burst_sql(200, MIB, "zeroblob")).unwrap();
        let peak = len(&rig.wal());
        assert!(peak > 3 * limit, "{placement:?}: {peak}");
        let mut ticks = 0;
        while len(&rig.wal()) > limit {
            assert!(
                ticks < BOUND,
                "{placement:?}: still {} after {ticks} ticks",
                len(&rig.wal())
            );
            rig.tick().unwrap();
            ticks += 1;
        }
        assert!(rig.checkpointed_since(begin, CheckpointMode::Passive, false));
        for busy in [false, true] {
            assert!(!rig.checkpointed_since(begin, CheckpointMode::Truncate, busy));
        }
    }
}

fn no_resnapshot_and_no_fault(rig: &Rig<Sqlite>) {
    let snapshots: Vec<&Tick> = rig
        .ticks()
        .iter()
        .filter(|tick| {
            matches!(
                tick,
                Tick::Ran {
                    event: Event::Resnapshot(_),
                    ..
                }
            )
        })
        .collect();
    assert!(snapshots.is_empty(), "{snapshots:?}");
    assert!(rig.faults().is_empty(), "{:?}", rig.faults());
}

/// The WAL reader and the actor across the shrink: the file is cut to the limit after the
/// actor's own restart, and the syncs after it continue the new generation from the start of
/// the shorter file, with no snapshot and no fault; a second burst and restart on the shrunk
/// file do the same, and the replica restores every transaction to what the application committed.
#[test]
fn the_actor_continues_with_no_snapshot_after_the_wal_shrinks() {
    for placement in Placement::ALL {
        let limit = NonZeroU64::new(16 * KIB);
        let dir = TempDir::new().unwrap();
        let Burst {
            mut rig,
            peak,
            after,
        } = burst_through_the_actor(&dir, placement, limit);
        assert_eq!(after, 16 * KIB, "{placement:?}: {after} of {peak}");
        for _ in 0..3 {
            rig.insert(3000).unwrap();
            rig.settle().unwrap();
        }
        let begin = rig.ticks().len();
        rig.exec(&burst_sql(100, 3000, "randomblob")).unwrap();
        rig.settle().unwrap();
        assert!(rig.checkpointed_since(begin, CheckpointMode::Passive, false));
        assert_eq!(len(&rig.wal()), 16 * KIB, "{placement:?}");
        rig.insert(3000).unwrap();
        rig.settle().unwrap();
        no_resnapshot_and_no_fault(&rig);
        let checked = rig.check().unwrap();
        assert!(checked.restored > 5, "{placement:?}: {checked:?}");
    }
}

/// The option is honoured: with no limit the restart leaves the file at its peak (SQLite's
/// own default), and a limit below the peak cuts it to exactly that limit.
#[test]
fn the_wal_size_limit_can_be_off_or_another_size() {
    let dir = TempDir::new().unwrap();
    let off = burst_through_the_actor(&dir, Placement::InProcess, None);
    assert_eq!(off.after, off.peak);
    off.rig.check().unwrap();
    let dir = TempDir::new().unwrap();
    let other = burst_through_the_actor(&dir, Placement::InProcess, NonZeroU64::new(64 * KIB));
    assert_eq!(other.after, 64 * KIB, "of {}", other.peak);
    other.rig.check().unwrap();
}

/// The default config is the 64 MiB limit, and the closing replicator leaves the WAL whole.
///
/// The writer connection closes last of the replicator's two; when it is the database's last
/// connection, SQLite checkpoints and, with a size limit set, truncates the WAL to zero bytes,
/// which would make the next start snapshot. The replicator turns the limit off before it
/// closes, so the WAL keeps its frames (`PERSIST_WAL` keeps the file).
#[test]
fn the_default_limit_is_64_mib_and_a_last_close_keeps_the_wal() {
    assert_eq!(
        SqliteConfig::default().journal_size_limit,
        Some(DEFAULT_JOURNAL_SIZE_LIMIT)
    );
    assert_eq!(DEFAULT_JOURNAL_SIZE_LIMIT.get(), 64 * MIB);
    let dir = TempDir::new().unwrap();
    let (app, db) = database(dir.path());
    let sqlite = Sqlite::open(&db).unwrap();
    app.execute_batch(&burst_sql(4, 3000, "randomblob"))
        .unwrap();
    let wal = sqlite.wal_path();
    let before = len(&wal);
    drop(app);
    drop(sqlite);
    let kept = std::fs::metadata(&wal)
        .unwrap_or_else(|err| panic!("the WAL is gone: {err}"))
        .len();
    assert_eq!(kept, before);
}
