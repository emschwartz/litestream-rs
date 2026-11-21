//! What an application at SQLite's defaults does to a pin held in another process, with the actor
//! replicating and the oracle judging: the cases an application beside a sidecar has that an application set
//! up by `lsr_sqlite::app_connection` never does (docs/plans/phase-4-sidecar.md, "The application's
//! connection settings").
//!
//! The application runs in a child process (`Placement::SeparateProcess`): automatic checkpoints
//! every 1000 frames, no `PERSIST_WAL`, busy timeout zero. The pin, the actor, the stores and
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
            | Tick::Halted(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => None,
        })
        .collect()
}

/// The application's own view of the WAL: SQLite's frame count and how many are backfilled.
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
/// checkpoint, which backfills up to the pin's read mark and stops there; the WAL never
/// restarts under the pin. With the actor running none of its own, the pin stays above mark
/// 0, so the application's backfill is the only one: the actor counts it, takes no snapshot, and the
/// oracle restores every transaction.
#[test]
fn the_apps_automatic_checkpoints_against_a_pin_above_read_mark_0_lose_nothing() {
    let dir = TempDir::new().unwrap();
    let mut rig = separate(&dir, NO_CHECKPOINTS);
    let from = rig.ticks().len();
    let salts = rig.backend().unwrap().committed().unwrap().end().salts();
    write_past(&mut rig, AUTOCHECKPOINT + 200);
    let (log, backfilled) = app_wal(&mut rig);
    assert!(
        backfilled > 0 && backfilled < log,
        "the application's own checkpoints backfilled up to the pin: {log} {backfilled}"
    );
    let now = rig.backend().unwrap().committed().unwrap().end().salts();
    assert_eq!(now, salts, "no restart under the pin");
    assert_eq!(boots_and_snapshots(&rig, from), vec![], "no snapshot");
    let app = rig.actor().app_checkpoints();
    assert!(
        app.backfilled >= 1,
        "the application's backfill is counted: {app:?}"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

/// SQLite belief: once the replicator's own PASSIVE backfills the whole log, its pin is
/// retaken on read mark 0, which does not stop a restart, so the application's next write restarts
/// the WAL (`salt1 + 1`, in the same read epoch). The replicator starts its checkpoints over
/// a WAL the application's automatic checkpoints already backfilled up to the old pin, and then
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

/// What the application's last close does while the replicator is down, by placement and by whether
/// the application writes before the replicator boots: set up as replication asks (`PERSIST_WAL`
/// on), SQLite checkpoints but keeps the WAL, and the next boot continues; at SQLite's
/// defaults it checkpoints and deletes the WAL, and the next boot snapshots: `WalMissing`
/// when it finds no WAL, `GenerationChanged` when the application's write already began a new one.
const fn close_while_down_answers(
    placement: Placement,
    app_first: bool,
) -> (bool, Option<SnapshotCause>) {
    match (placement, app_first) {
        (Placement::InProcess, _) => (true, None),
        (Placement::SeparateProcess, false) => (false, Some(SnapshotCause::WalMissing)),
        (Placement::SeparateProcess, true) => (false, Some(SnapshotCause::GenerationChanged)),
    }
}

/// The application's last connection closing while the replicator is down (see
/// `close_while_down_answers`); the oracle restores every transaction either way.
#[test]
fn the_apps_last_close_while_the_replicator_is_down_keeps_or_deletes_the_wal() {
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
    let (kept, cause) = close_while_down_answers(placement, app_first);
    assert_eq!(rig.wal().exists(), kept, "{at}: the WAL file");
    rig.open_app().unwrap();
    if app_first {
        rig.insert(1500).unwrap();
    }
    rig.restart().unwrap();
    let from = rig.ticks().len();
    // The boot's first tick, before or after the application's write.
    rig.tick().unwrap();
    if !app_first {
        rig.insert(1500).unwrap();
    }
    rig.settle().unwrap();
    assert_eq!(
        boots_and_snapshots(&rig, from),
        vec![cause],
        "{at}: the boot"
    );
    rig.check().unwrap();
}

/// SQLite belief: the application's last connection closing while the pin is open neither
/// checkpoints nor deletes the WAL, with or without `PERSIST_WAL` (`sqlite3WalClose` needs
/// the exclusive lock the pin's connection stops), so the actor continues where it was.
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
/// `-shm` with `std::fs` drops only its own locks, so the pin in this process still blocks
/// the application's RESTART (busy, the WAL's generation unchanged) and the next sync is incremental.
#[test]
fn the_app_process_opening_and_closing_the_database_files_leaves_the_pin_held() {
    let dir = TempDir::new().unwrap();
    let mut s = Stepper::<Sqlite>::placed(dir.path(), 4096, Placement::SeparateProcess).unwrap();
    s.sync().unwrap();
    s.write(900).unwrap();
    s.sync().unwrap();
    let salts = s.salts().unwrap();
    s.app_touches_the_files().unwrap();
    let (busy, _, _) = s.app_checkpoint("RESTART").unwrap();
    assert_eq!(busy, 1, "the pin still blocks a restart");
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
