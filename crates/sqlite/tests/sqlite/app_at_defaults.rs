//! What an application at SQLite's defaults does to a read lock held in another process, with the actor
//! replicating and the oracle judging: the cases an application beside a sidecar has that an application set
//! up by `lsr_sqlite::app_connection` never does.
//!
//! The application runs in a child process (`Placement::SeparateProcess`): automatic checkpoints
//! every 1000 frames, busy timeout zero. The read lock, the actor, the stores and
//! the oracle are in this process, as a sidecar's are. A case whose answer differs from an application
//! set up as replication asks runs in both placements and states both answers.
//!
//! What is not here: a sidecar killed mid-upload, which needs the replicator in a process of
//! its own (the alternation tests in `lsr-compat` kill `lsr replicate` at seeded points).

#![cfg(unix)]

use std::collections::BTreeSet;

use lsr_core::{
    CheckpointConfig, CheckpointMode, Event, Recovered, SnapshotCause, Tick, WalSource,
};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{Placement, Rig, RigConfig, StepDid, Stepper, rig_actor_config};
use tempfile::TempDir;

/// SQLite's default `wal_autocheckpoint`, in frames.
const AUTOCHECKPOINT: i64 = 1000;

/// Every snapshot the ticks from the `from`th on decided, with its cause (`None` for a boot
/// that continued).
fn boots_and_snapshots(rig: &Rig<Sqlite>, from: usize) -> Vec<Option<SnapshotCause>> {
    rig.ticks()
        .iter()
        .skip(from)
        .filter_map(|tick| match tick {
            Tick::Ran {
                event: Event::Resnapshot(cause),
                ..
            } => Some(Some(*cause)),
            Tick::Ran {
                event: Event::Booted { cause },
                ..
            } => Some(*cause),
            Tick::Ran { .. }
            | Tick::Stopped(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => None,
        })
        .collect()
}

fn app_wal(rig: &mut Rig<Sqlite>) -> (i64, i64) {
    match rig.query("PRAGMA wal_checkpoint(NOOP)").unwrap()[..] {
        [_busy, log, backfilled] => (log, backfilled),
        ref other => panic!("{other:?}"),
    }
}

/// A rig whose application is at SQLite's defaults in another process, 512-byte pages so a few
/// hundred commits pass the automatic checkpoint's threshold, and the actor's own
/// checkpoints as `checkpoint` says.
fn separate(dir: &TempDir, checkpoint: CheckpointConfig) -> Rig<Sqlite> {
    let mut cfg = RigConfig::placed(Placement::SeparateProcess);
    cfg.page_size = 512;
    cfg.actor.checkpoint = checkpoint;
    let mut rig = Rig::new(dir.path(), cfg).unwrap();
    rig.settle().unwrap();
    rig
}

/// Commits 5,000-byte rows (about eleven 512-byte frames each), settling after each, until
/// the application's WAL holds more than `frames` frames.
fn write_past(rig: &mut Rig<Sqlite>, frames: i64) {
    for _ in 0..400 {
        rig.insert(5000).unwrap();
        rig.settle().unwrap();
        if app_wal(rig).0 > frames {
            return;
        }
    }
    panic!("the WAL never passed {frames} frames");
}

const NO_CHECKPOINTS: CheckpointConfig = CheckpointConfig {
    interval: None,
    min_pages: None,
    truncate_pages: None,
};

/// SQLite belief: past 1000 frames every commit of an application at the defaults runs a PASSIVE
/// checkpoint, which backfills up to the read lock's read mark and stops there; the WAL never
/// restarts under the read lock. With the actor running none of its own, the read lock stays above mark
/// 0, so the application's backfill is the only one: the actor counts it, takes no snapshot, and the
/// oracle restores every transaction.
#[test]
fn the_apps_automatic_checkpoints_against_a_read_lock_above_read_mark_0_lose_nothing() {
    let dir = TempDir::new().unwrap();
    let mut rig = separate(&dir, NO_CHECKPOINTS);
    let from = rig.ticks().len();
    let salts = rig.backend().unwrap().committed().unwrap().end().salts();
    write_past(&mut rig, AUTOCHECKPOINT + 200);
    let (log, backfilled) = app_wal(&mut rig);
    assert!(
        backfilled > 0 && backfilled < log,
        "the application's own checkpoints backfilled up to the read lock: {log} {backfilled}"
    );
    let now = rig.backend().unwrap().committed().unwrap().end().salts();
    assert_eq!(now, salts, "no restart under the read lock");
    assert_eq!(boots_and_snapshots(&rig, from), vec![], "no snapshot");
    let app = rig.actor().app_checkpoints();
    assert!(
        app.backfilled >= 1,
        "the application's backfill is counted: {app:?}"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

/// SQLite belief: once the replicator's own PASSIVE backfills the whole log, its read lock is
/// retaken on read mark 0, which does not stop a restart, so the application's next write restarts
/// the WAL (`salt1 + 1`, in the same read-lock hold). The replicator starts its checkpoints over
/// a WAL the application's automatic checkpoints already backfilled up to the old read lock, and then
/// follows each restart with no snapshot; the oracle restores every transaction.
#[test]
fn the_apps_restarts_after_the_replicators_checkpoints_are_followed_without_a_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut rig = separate(&dir, NO_CHECKPOINTS);
    write_past(&mut rig, AUTOCHECKPOINT + 50);
    rig.restart_with(rig_actor_config()).unwrap();
    let from = rig.ticks().len();
    let mut generations = BTreeSet::new();
    for _ in 0..40 {
        rig.insert(5000).unwrap();
        rig.settle().unwrap();
        let salts = rig.backend().unwrap().committed().unwrap().end().salts();
        generations.insert((salts.salt1, salts.salt2));
    }
    let passives = rig.ticks()[from..]
        .iter()
        .filter(|t| {
            matches!(
                t,
                Tick::Ran {
                    event: Event::Checkpointed {
                        mode: CheckpointMode::Passive,
                        busy: false
                    },
                    ..
                }
            )
        })
        .count();
    assert!(passives >= 2, "the actor's own checkpoints ran: {passives}");
    assert!(
        generations.len() >= 2,
        "the application's writes restarted the WAL: {generations:?}"
    );
    assert_eq!(
        boots_and_snapshots(&rig, from),
        vec![None],
        "the restart's boot continued and nothing after it snapshotted"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

/// What the application's last close does while the replicator is down, by whether the application
/// writes before the replicator boots. Set up as replication asks or at SQLite's defaults, the
/// answer is the same, since neither sets `PERSIST_WAL` (docs/decisions.md D123): SQLite
/// checkpoints and deletes the WAL, and the next boot snapshots: `WalMissing` when it finds
/// no WAL, `GenerationChanged` when the application's write already began a new one.
const fn close_while_down_answer(app_first: bool) -> SnapshotCause {
    if app_first {
        SnapshotCause::GenerationChanged
    } else {
        SnapshotCause::WalMissing
    }
}

/// The application's last connection closing while the replicator is down (see
/// `close_while_down_answer`); the oracle restores every transaction either way.
#[test]
fn the_apps_last_close_while_the_replicator_is_down_deletes_the_wal() {
    for placement in Placement::ALL {
        for app_first in [false, true] {
            close_while_down(placement, app_first);
        }
    }
}

fn close_while_down(placement: Placement, app_first: bool) {
    let at = format!("{placement:?}, application first {app_first}");
    let dir = TempDir::new().unwrap();
    let mut rig = Rig::<Sqlite>::new(dir.path(), RigConfig::placed(placement)).unwrap();
    for _ in 0..3 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
    }
    rig.stop_replicator();
    rig.close_app().unwrap();
    let cause = close_while_down_answer(app_first);
    assert!(!rig.wal().exists(), "{at}: the WAL file");
    rig.open_app().unwrap();
    if app_first {
        rig.insert(1500).unwrap();
    }
    rig.restart().unwrap();
    let from = rig.ticks().len();
    rig.tick().unwrap();
    if !app_first {
        rig.insert(1500).unwrap();
    }
    rig.settle().unwrap();
    assert_eq!(
        boots_and_snapshots(&rig, from),
        vec![Some(cause)],
        "{at}: the boot"
    );
    rig.check().unwrap();
}

/// SQLite belief: the application's last connection closing while the read lock is open neither
/// checkpoints nor deletes the WAL, with or without `PERSIST_WAL` (`sqlite3WalClose` needs
/// the exclusive lock the read lock's connection stops), so the actor continues where it was.
#[test]
fn the_apps_last_close_while_pinned_keeps_the_wal_and_replication_continues() {
    for placement in Placement::ALL {
        let dir = TempDir::new().unwrap();
        let mut rig = Rig::<Sqlite>::new(dir.path(), RigConfig::placed(placement)).unwrap();
        for _ in 0..3 {
            rig.insert(1500).unwrap();
            rig.settle().unwrap();
        }
        let len = std::fs::metadata(rig.wal()).unwrap().len();
        let from = rig.ticks().len();
        rig.close_app().unwrap();
        assert_eq!(
            std::fs::metadata(rig.wal()).unwrap().len(),
            len,
            "{placement:?}: the WAL is whole"
        );
        rig.open_app().unwrap();
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
        assert_eq!(boots_and_snapshots(&rig, from), vec![], "{placement:?}");
        rig.check().unwrap();
    }
}

/// POSIX locks belong to a process: the application's process opening and closing the database and
/// `-shm` with `std::fs` drops only its own locks, so the read lock in this process still blocks
/// the application's RESTART (busy, the WAL's generation unchanged) and the next sync is incremental.
#[test]
fn the_app_process_opening_and_closing_the_database_files_leaves_the_read_lock_held() {
    let dir = TempDir::new().unwrap();
    let mut s = Stepper::<Sqlite>::placed(dir.path(), 4096, Placement::SeparateProcess).unwrap();
    s.sync().unwrap();
    s.write(900).unwrap();
    s.sync().unwrap();
    let salts = s.salts().unwrap();
    s.app_touches_the_files().unwrap();
    let (busy, _, _) = s.app_checkpoint("RESTART").unwrap();
    assert_eq!(busy, 1, "the read lock still blocks a restart");
    s.write(300).unwrap();
    assert_eq!(s.salts().unwrap(), salts, "same generation");
    let verdict = s.verdict().unwrap();
    assert_eq!(verdict.event, None);
    assert!(
        matches!(verdict.recovered, Recovered::Continue(_)),
        "{verdict:?}"
    );
    assert!(matches!(s.sync().unwrap(), StepDid::Wrote { txs: 1, .. }));
}

/// The sidecar's deploy (the application in its own process at SQLite's defaults, the
/// replicator in another), in both orders the two processes can exit. The replicator's own
/// connections set `PERSIST_WAL`, so whichever connection closes last decides the WAL: the
/// application last deletes it and the next boot snapshots (`WalMissing`); the replicator last
/// keeps it and the next boot continues with no snapshot.
#[test]
fn a_sidecar_deploy_snapshots_only_when_the_application_closes_last() {
    for app_last in [true, false] {
        let at = format!("application last {app_last}");
        let dir = TempDir::new().unwrap();
        let config = RigConfig::placed(Placement::SeparateProcess);
        let mut rig = Rig::<Sqlite>::new(dir.path(), config).unwrap();
        for _ in 0..3 {
            rig.insert(1500).unwrap();
            rig.settle().unwrap();
        }
        if app_last {
            rig.stop_replicator();
            rig.close_app().unwrap();
        } else {
            rig.close_app().unwrap();
            rig.stop_replicator();
        }
        assert_eq!(rig.wal().exists(), !app_last, "{at}: the WAL file");
        rig.open_app().unwrap();
        rig.restart().unwrap();
        let from = rig.ticks().len();
        rig.tick().unwrap();
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
        let want = app_last.then_some(SnapshotCause::WalMissing);
        assert_eq!(
            boots_and_snapshots(&rig, from),
            vec![want],
            "{at}: the boot"
        );
        rig.check().unwrap();
    }
}
