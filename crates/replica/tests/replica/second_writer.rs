//! A second engine on one meta directory is refused at start, from this process or another,
//! and a stop for another writer on the replica survives a restart until the other writer's
//! files are gone.

use crate::common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use common::{Site, config, fake, stopped, wait};
use lsr_core::{FileKey, Level, ObjectStore, Upload};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
use lsr_replica::{Config, Phase, Replicator, StartError, StopCause, Stopped, block_on};
use lsr_store_fs::FsStore;

/// The environment variable that names the meta directory a child holds.
const CHILD_META: &str = "LSR_TEST_SECOND_WRITER_META";

fn caught_up(health: &lsr_replica::Health) -> bool {
    health.synced.is_some()
        && health.synced.map(|(txid, _)| txid) == health.replicated.map(|(txid, _)| txid)
        && health.unreplicated_since.is_none()
}

/// The default meta directory of the site's database.
fn meta_of(site: &Site) -> PathBuf {
    site.dir.path().join(".app.db-litestream")
}

/// A second database of its own in `dir`, set up as an application sets one up.
fn other_db(dir: &Path) -> PathBuf {
    let db = dir.join("other.db");
    let app = rusqlite::Connection::open(&db).unwrap();
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&app).unwrap();
    app.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    db
}

/// The defect: two databases configured with one meta directory both started, and both
/// staged into it. The second is refused while the first runs, and goes through after.
#[test]
fn a_second_engine_on_one_meta_directory_is_refused() {
    let site = Site::new();
    let shared = Config {
        meta_dir: Some(site.dir.path().join("shared-meta")),
        ..config()
    };
    let (runtime, _clock) = fake();
    let first = Replicator::start_with(&site.db, site.store(), shared.clone(), runtime).unwrap();
    let other = other_db(site.dir.path());
    let (runtime, _clock) = fake();
    assert!(matches!(
        Replicator::start_with(&other, site.store(), shared.clone(), runtime),
        Err(StartError::MetaLocked)
    ));
    assert!(matches!(stopped(&first), Stopped::Drained { .. }));
    let (runtime, _clock) = fake();
    let second = Replicator::start_with(&other, site.store(), shared, runtime).unwrap();
    assert!(matches!(stopped(&second), Stopped::Drained { .. }));
}

/// The same across processes: a child holds a replicator on the meta directory, and a start
/// here is refused until the child has stopped and exited.
#[test]
fn a_second_process_on_one_meta_directory_is_refused() {
    let site = Site::new();
    let meta = site.dir.path().join("shared-meta");
    let exe = std::env::current_exe().unwrap();
    let mut child = Killed(
        Command::new(exe)
            .args([
                "second_writer::child_holds_the_meta_directory",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_META, &meta)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let started = lines
        .by_ref()
        .map_while(Result::ok)
        .any(|line| line.contains("lsr-child: started"));
    assert!(started, "the child never started its replicator");
    let shared = Config {
        meta_dir: Some(meta),
        ..config()
    };
    let (runtime, _clock) = fake();
    let refused = Replicator::start_with(&site.db, site.store(), shared.clone(), runtime);
    assert!(
        matches!(refused, Err(StartError::MetaLocked)),
        "{refused:?}"
    );
    child.0.stdin.take().unwrap().write_all(b"stop\n").unwrap();
    assert!(child.0.wait().unwrap().success());
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), shared, runtime).unwrap();
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
}

/// A child process killed if the test fails before it exits, so a failure never hangs.
struct Killed(std::process::Child);

impl Drop for Killed {
    fn drop(&mut self) {
        // Already exited when the test passed: the kill then fails, harmlessly.
        let _exited = self.0.kill();
        let _reaped = self.0.wait();
    }
}

/// The child's half of [`a_second_process_on_one_meta_directory_is_refused`]: started by it
/// with the meta directory in [`CHILD_META`], and nothing at all when run as a test of its own.
#[test]
fn child_holds_the_meta_directory() {
    let Some(meta) = std::env::var_os(CHILD_META) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let db = other_db(dir.path());
    let store = std::sync::Arc::new(FsStore::new(dir.path().join("replica")));
    let shared = Config {
        meta_dir: Some(PathBuf::from(meta)),
        ..config()
    };
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&db, store, shared, runtime).unwrap();
    println!("lsr-child: started");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
}

/// A valid level-0 file for `txid` that the site's engine never staged, put on its replica.
fn put_foreign(site: &Site, txid: Txid) -> FileKey {
    let page_size = PageSize::new(4096).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 1,
        txids: TxidRange::single(txid),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let page = PageNo::new(1, page_size).unwrap();
    encoder.write_page(page, &vec![0xee; 4096]).unwrap();
    let bytes = encoder.finish(None).unwrap().0;
    let key = FileKey::new(Level::ZERO, TxidRange::single(txid));
    let store = FsStore::new(&site.replica);
    block_on(async {
        let mut up = store
            .begin_upload(&key, Timestamp::from_unix_millis(1))
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap()
    });
    key
}

fn newest_l0(site: &Site) -> Option<Txid> {
    let store = FsStore::new(&site.replica);
    let listing = block_on(lsr_core::Inventory::load(&store)).unwrap();
    listing
        .level(Level::ZERO)
        .iter()
        .map(|info| info.key.range().max())
        .max()
}

fn stopped_for_other_writer(health: &lsr_replica::Health) -> bool {
    health.phase == Phase::Stopped(StopCause::OtherWriter)
}

/// The defect: a restart after a stop for another writer took the other writer's file as
/// history and snapshotted above it. The stop is recorded in the meta directory, so the
/// restart stops again and writes nothing; once the operator removes the file, the record
/// goes and replication goes on.
#[test]
fn a_stop_for_another_writer_survives_a_restart() {
    let site = Site::new();
    site.commit(2);
    let (runtime, clock) = fake();
    let first = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let ours = wait(&first, caught_up)
        .replicated
        .map(|(txid, _)| txid)
        .unwrap();
    // Well above any id this test commits. `caught_up` can hold while a commit is still
    // unsynced, so `ours` may trail the engine: an id just past it could be the engine's own
    // next upload, which replaces the file under the same key, and nothing is left to stop for.
    let foreign = put_foreign(&site, Txid::new(ours.get().saturating_add(100)).unwrap());
    // An idle engine makes no listing. A commit's upload makes one due, which shows the
    // file, unless a maintenance listing still owed from the boot shows it first.
    site.commit(1);
    clock.advance(Duration::from_secs(1));
    wait(&first, |h| {
        stopped_for_other_writer(h) || h.replicated.map(|(txid, _)| txid) > Some(ours)
    });
    clock.advance(Duration::from_secs(600));
    wait(&first, stopped_for_other_writer);
    let record = meta_of(&site).join("lsr-stop");
    assert!(record.exists(), "the stop is recorded");
    assert_eq!(stopped(&first), Stopped::Stopped(StopCause::OtherWriter));

    site.commit(2);
    let (runtime, clock) = fake();
    let second = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    clock.advance(Duration::from_secs(1));
    wait(&second, stopped_for_other_writer);
    assert_eq!(
        newest_l0(&site),
        Some(foreign.range().max()),
        "nothing went above it"
    );
    assert_eq!(stopped(&second), Stopped::Stopped(StopCause::OtherWriter));
    assert!(record.exists());

    block_on(FsStore::new(&site.replica).delete(&[foreign])).unwrap();
    let (runtime, clock) = fake();
    let third = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    clock.advance(Duration::from_secs(1));
    wait(&third, caught_up);
    assert!(
        !record.exists(),
        "the record went with the other writer's file"
    );
    assert!(matches!(stopped(&third), Stopped::Drained { .. }));
    site.assert_restores(&*site.store());
}

/// Restore then start: the restore leaves a takeover record through what it restored, so
/// the replicator started on the restored database builds on the replica it came from and
/// never refuses it.
#[test]
fn a_restored_database_replicates_without_a_refusal() {
    let site = Site::new();
    site.commit(3);
    let (runtime, _clock) = fake();
    let first = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    wait(&first, caught_up);
    assert!(matches!(stopped(&first), Stopped::Drained { .. }));
    let restored = site.path("restored");
    let reached = lsr_replica::restore(
        &*site.store(),
        lsr_replica::RestoreTarget::Latest,
        &restored,
    )
    .unwrap()
    .reached();
    let meta = restored.with_file_name(format!(
        ".{}-litestream",
        restored.file_name().unwrap().to_str().unwrap()
    ));
    let record = std::fs::read(meta.join("lsr-takeover")).unwrap();
    assert_eq!(
        lsr_replica::TakeoverRecord::decode(&record).unwrap().txid(),
        reached
    );
    let app = rusqlite::Connection::open(&restored).unwrap();
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&app).unwrap();
    let (runtime, clock) = fake();
    let next = Replicator::start_with(&restored, site.store(), config(), runtime).unwrap();
    app.execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
        .unwrap();
    clock.advance(Duration::from_secs(1));
    let health = wait(&next, |h| {
        caught_up(h) || matches!(h.phase, Phase::Stopped(_))
    });
    assert_eq!(health.phase, Phase::Running, "{health:?}");
    assert!(matches!(stopped(&next), Stopped::Drained { .. }));
}

/// Another writer's file appeared while the replicator was down: the boot refuses it,
/// naming the replica's newest transaction in `Event::Refused`, and writes nothing. A
/// take-over through that transaction lets the next start snapshot above it.
#[test]
fn a_refused_boot_names_the_tip_and_a_take_over_clears_it() {
    let site = Site::new();
    site.commit(2);
    let (runtime, _clock) = fake();
    let first = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    wait(&first, caught_up);
    // The drain's txid, not the health's: `caught_up` can hold while a commit is unsynced.
    let Stopped::Drained { txid: Some(ours) } = stopped(&first) else {
        panic!("the first replicator drains");
    };
    let foreign = put_foreign(&site, ours.next().unwrap());

    let (sink, events) = lsr_replica::ChannelSink::new(std::num::NonZeroUsize::new(64).unwrap());
    let (runtime, _clock) = fake();
    let second =
        Replicator::start_with_events(&site.db, site.store(), config(), runtime, Box::new(sink))
            .unwrap();
    wait(&second, stopped_for_other_writer);
    assert_eq!(stopped(&second), Stopped::Stopped(StopCause::OtherWriter));
    let mut refused = None;
    while let Some(event) = events.try_recv() {
        if let lsr_replica::Event::Refused { through } = event {
            refused = Some(through);
        }
    }
    assert_eq!(refused, Some(foreign.range().max()));
    assert_eq!(
        newest_l0(&site),
        Some(foreign.range().max()),
        "nothing was written"
    );

    let taken = lsr_replica::take_over(&*site.store(), &meta_of(&site)).unwrap();
    assert_eq!(taken, Some(foreign.range().max()));
    let (runtime, clock) = fake();
    let third = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    site.commit(1);
    clock.advance(Duration::from_secs(1));
    let health = wait(&third, |h| {
        caught_up(h) || matches!(h.phase, Phase::Stopped(_))
    });
    assert_eq!(health.phase, Phase::Running, "{health:?}");
    assert!(newest_l0(&site) > Some(foreign.range().max()));
    assert!(matches!(stopped(&third), Stopped::Drained { .. }));
}
