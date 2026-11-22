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

use common::{Site, assert_same, config, fake, stopped, wait};
use lsr_replica::{Replicator, RestoreTarget, Stopped, restore};
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
    let replicator = Replicator::start(&site.db, Arc::clone(&store), config()).unwrap();
    site.commit(3);
    let stopped = stopped(&replicator);
    assert!(matches!(stopped, Stopped::Drained { .. }), "{stopped:?}");
    drop(replicator);
    let snapshots = site.replica.join("ltx").join("9");
    if snapshots.exists() {
        std::fs::remove_dir_all(&snapshots).unwrap();
    }
    let to = site.path("restored");
    restore(&*store, RestoreTarget::Latest, &to).unwrap();
    to
}

#[test]
fn a_restored_database_with_no_write_ticks_by_the_poll_and_snapshots_at_once() {
    let site = Site::new();
    let db = restored(&site);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&db, site.store(), config(), runtime).unwrap();
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
    assert!(matches!(stopped, Stopped::Drained { .. }), "{stopped:?}");
    let app = Connection::open(&db).unwrap();
    let again = site.path("again");
    restore(&*site.store(), RestoreTarget::Latest, &again).unwrap();
    assert_same(&app, &again);
}
