//! A database a restore just published, replicated again with no write from the application.
//!
//! The replica holds the restored newest staged file, and the WAL SQLite creates has no header: the
//! replicator's open writes nothing once `_litestream_seq` exists. The first sync is a
//! snapshot that writes the header itself, so over a simulated minute the thread runs about
//! one tick per poll, retries nothing and ends caught up. Every wait is on the health snapshot.

use crate::common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{Site, assert_same, config, fake, stopped_on_system_clock, wait};
use lsr_replica::StartOptions;
use lsr_replica::{
    Config, IfMissing, IntegrityCheck, Replicator, Restore, RestoreError, StartError, StopOutcome,
    restore, restore_if_missing,
};
use rusqlite::Connection;

const POLL: Duration = Duration::from_secs(1);

/// More than a minute of polls and the work after a boot; a thread that ticks again at once
/// while doing nothing passes it within the first polls.
const BOUND: u64 = 200;

/// The site replicated and stopped, then restored into a new file, with no level-9 snapshot on
/// the replica: one is due as soon as the next replicator lists it. A drain runs no
/// maintenance, but the replicator runs on the real clock until the stop reaches it, and a
/// maintenance tick in that window snapshots (the replica held none). Whether it got there is
/// a race with the stop, so the snapshot it may have written is removed: the replica's
/// level-0 chain starts at transaction 1 and restores on its own.
fn restored(site: &Site) -> PathBuf {
    let store = site.store();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(3);
    let stopped = stopped_on_system_clock(&replicator);
    assert!(
        matches!(stopped, StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    drop(replicator);
    let snapshots = site.replica.join("ltx").join("9");
    if snapshots.exists() {
        std::fs::remove_dir_all(&snapshots).unwrap();
    }
    let to = site.path("restored");
    restore(&*store, &to, &Restore::default()).unwrap();
    to
}

#[test]
fn a_restored_database_with_no_write_ticks_by_the_poll_and_snapshots_at_once() {
    let site = Site::new();
    let db = restored(&site);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    for _ in 0..60 {
        clock.advance(POLL);
        let now = clock.now();
        wait(&replicator, |h| h.reported_at >= now);
    }
    let health = replicator.health();
    assert!(health.counters.ticks <= BOUND, "{health:?}");
    assert_eq!(health.counters.retries, 0, "{health:?}");
    assert_eq!(health.resnapshot, None, "{health:?}");
    assert!(health.unreplicated_since.is_none(), "{health:?}");
    let stopped = replicator.stop(Duration::ZERO).wait();
    assert!(
        matches!(stopped, StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    let app = Connection::open(&db).unwrap();
    let again = site.path("again");
    restore(&*site.store(), &again, &Restore::default()).unwrap();
    assert_same(&app, &again);
}

/// A destination that names no file has no meta directory for the takeover record, so the
/// restore is refused before anything is written: a restored database whose replicator would
/// refuse its own replica is never published.
#[test]
fn a_restore_to_a_path_with_no_file_name_is_refused_before_writing() {
    let site = Site::new();
    let replica = restored(&site);
    let dir = tempfile::tempdir().unwrap();
    let to = dir.path().join("sub").join("..");
    let refused = restore(&*site.store(), &to, &Restore::default());
    assert!(
        matches!(&refused, Err(lsr_replica::RestoreError::NoMetaDir { path }) if *path == to),
        "{refused:?}"
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    drop(replica);
}
/// A first deploy against an empty bucket: restore-if-missing says so instead of failing, and
/// a replicator started with `create_if_missing` creates the database and replicates it from
/// scratch, as Go's `replicate -restore-if-db-not-exists` starts fresh. Without it the missing
/// file is refused.
#[test]
fn a_first_start_against_an_empty_replica_creates_the_database_and_replicates() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn lsr_replica::DynStore> =
        Arc::new(lsr_store_fs::FsStore::new(dir.path().join("replica")));
    let db = dir.path().join("fresh.db");
    let missing = restore_if_missing(&*store, &db, &Config::default()).unwrap();
    assert_eq!(missing, IfMissing::EmptyReplica);
    assert!(!db.exists(), "an empty replica writes nothing");
    let refused = Replicator::start(
        &db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            ..StartOptions::default()
        },
    );
    assert!(
        matches!(
            refused,
            Err(StartError::Open {
                kind: std::io::ErrorKind::NotFound
            })
        ),
        "{refused:?}"
    );
    let replicator = Replicator::start(
        &db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            create_if_missing: true,
            ..StartOptions::default()
        },
    )
    .unwrap();
    let app = Connection::open(&db).unwrap();
    app.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB); INSERT INTO t(v) VALUES (x'01')",
    )
    .unwrap();
    drop(app);
    let stopped = stopped_on_system_clock(&replicator);
    assert!(
        matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    drop(replicator);
    let to = dir.path().join("restored.db");
    let restored = restore_if_missing(&*store, &to, &Config::default()).unwrap();
    assert!(matches!(restored, IfMissing::Restored(_)), "{restored:?}");
}

/// A restore given the replicator's configuration records its trust record in the configured
/// meta directory, where the replicator's start reads it, and not beside the database.
#[test]
fn a_restore_records_its_trust_record_in_the_configured_meta_directory() {
    let site = Site::new();
    let replica = restored(&site);
    let meta = site.dir.path().join("meta-elsewhere");
    let config = Config {
        meta_dir: Some(meta.clone()),
        ..Config::default()
    };
    let to = site.path("configured");
    restore(
        &*site.store(),
        &to,
        &Restore {
            config: config.clone(),
            ..Restore::default()
        },
    )
    .unwrap();
    assert!(
        meta.join("lsr-takeover").exists(),
        "the record is in the configured directory"
    );
    let default = Config::default().meta_dir_for(&to).unwrap();
    assert!(
        !default.join("lsr-takeover").exists(),
        "and not in the default one"
    );
    drop(replica);
}

/// A restore whose trust record cannot be written still published the database: the error
/// carries the restore that succeeded, so the caller can record exactly that transaction.
#[test]
fn a_trust_record_that_cannot_be_written_carries_the_restore() {
    let site = Site::new();
    let replica = restored(&site);
    let blocker = site.dir.path().join("a-file");
    std::fs::write(&blocker, b"not a directory").unwrap();
    let config = Config {
        meta_dir: Some(blocker.join("meta")),
        ..Config::default()
    };
    let to = site.path("unrecorded");
    let failed = restore(
        &*site.store(),
        &to,
        &Restore {
            config: config.clone(),
            ..Restore::default()
        },
    );
    let Err(RestoreError::TrustRecord { restored, .. }) = failed else {
        panic!("{failed:?}");
    };
    assert_same(&site.app, &to);
    let meta = site.dir.path().join("meta-retry");
    lsr_replica::record_trust(&meta, restored.reached()).unwrap();
    assert!(meta.join("lsr-takeover").exists());
    drop(replica);
}

/// The integrity check a restore is asked for runs before the publish and passes on a sound
/// database.
#[test]
fn a_restore_with_an_integrity_check_publishes_a_sound_database() {
    let site = Site::new();
    let replica = restored(&site);
    let to = site.path("checked");
    let how = Restore {
        integrity: IntegrityCheck::Full,
        ..Restore::default()
    };
    restore(&*site.store(), &to, &how).unwrap();
    assert_same(&site.app, &to);
    drop(replica);
}
