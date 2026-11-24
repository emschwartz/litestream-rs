//! Fail-stop for the library: while the
//! gate is held, the commit hook `Replicator::app_connection` installs refuses every commit
//! with `SQLITE_CONSTRAINT_COMMITHOOK`, reads go on, and a trip that clears by itself
//! releases the application without anyone acting.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lsr_core::Denied;
use lsr_replica::{
    Config, DynStore, Gate, IfReplicationStops, Phase, Replicator, Responses, StopCause, Trip,
};

use crate::common::{self, Site, config, fake, wait};

const POLL: Duration = Duration::from_secs(1);

/// The code SQLite gives a COMMIT the hook refused.
fn refused(err: &rusqlite::Error) -> bool {
    err.sqlite_error().map(|e| e.extended_code)
        == Some(lsr_sqlite::ffi::SQLITE_CONSTRAINT_COMMITHOOK)
}

fn insert(site: &Site) -> rusqlite::Result<usize> {
    site.app
        .execute("INSERT INTO t(v) VALUES (randomblob(100))", [])
}

fn rows(site: &Site) -> usize {
    Site::rows(&site.app).len()
}

/// Under `hold-start` (the default), a store that refuses before the first listing holds the
/// application: its commits fail and are rolled back. The first listing releases it.
#[test]
fn a_refusal_at_boot_refuses_commits_until_the_first_listing() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    replicator.app_connection(&site.app).unwrap();
    let held = wait(&replicator, |h| matches!(h.held_by.gate, Gate::Held(_)));
    assert_eq!(
        held.held_by.trip,
        Some(Trip::RefusedAtStart(Denied::Credentials))
    );
    let before = rows(&site);
    let err = insert(&site).unwrap_err();
    assert!(refused(&err), "{err}");
    assert_eq!(rows(&site), before, "rolled back; reads go on");

    faulty.refuse.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    wait(&replicator, |h| h.held_by.gate == Gate::Allowed);
    insert(&site).unwrap();
    assert_eq!(rows(&site), before + 1);
}

/// Under `report` nothing is ever refused, though health says why it tripped.
#[test]
fn report_never_refuses_a_commit() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let report = Config {
        if_replication_stops: IfReplicationStops::Report,
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, report, runtime).unwrap();
    replicator.app_connection(&site.app).unwrap();
    let tripped = wait(&replicator, |h| h.held_by.trip.is_some());
    assert_eq!(tripped.held_by.gate, Gate::Allowed);
    insert(&site).unwrap();
}

/// A replicator that started cleanly and then stops for an unsafe cause: `hold-start` has
/// released the application and only reports; `stop-app` refuses its commits at once.
#[test]
fn an_unsafe_stop_while_running_refuses_commits_only_under_stop_app() {
    for (mode, refuses) in [
        (IfReplicationStops::HoldStart, false),
        (IfReplicationStops::StopApp, true),
    ] {
        let site = Site::new();
        let faulty = site.faulty();
        let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
        let (runtime, clock) = fake();
        let strict = Config {
            responses: Responses::STRICT,
            if_replication_stops: mode,
            ..config()
        };
        let replicator = Replicator::start_with(&site.db, store, strict, runtime).unwrap();
        replicator.app_connection(&site.app).unwrap();
        wait(&replicator, |h| h.held_by.gate == Gate::Allowed);
        // A commit to upload, made before the store refuses: once it does, a stop can hold
        // the gate at any moment.
        insert(&site).unwrap();
        faulty.refuse.store(true, Ordering::SeqCst);
        clock.advance(POLL);
        let stopped = wait(&replicator, |h| matches!(h.phase, Phase::Stopped(_)));
        assert_eq!(
            stopped.held_by.trip,
            Some(Trip::Stopped(StopCause::StoreDenied(Denied::Credentials)))
        );
        let held = matches!(replicator.health().held_by.gate, Gate::Held(_));
        assert_eq!(held, refuses, "{mode:?}");
        let result = insert(&site);
        assert_eq!(
            result.as_ref().is_err_and(refused),
            refuses,
            "{mode:?}: {result:?}"
        );
    }
}

/// Health's lag survives a restart: a commit staged and never uploaded keeps its age when the
/// replicator starts again, where it used to count from the new start (issue #115).
///
/// The age it keeps is the staged file's: the clock's reading when the engine staged it. No
/// time health reports before the restart is that reading. `synced` carries the time the
/// tick began, which is earlier when the clock moves while the tick runs, and the first
/// sighting of the commit's WAL frames is earlier still and is this process's alone
/// (`docs/decisions.md` D80, D85). Comparing with either failed under load, so the bound is
/// the clock itself: the file was staged before the first replicator went away.
#[test]
fn a_restart_keeps_the_age_of_a_commit_off_the_replica() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime.clone()).unwrap();
    wait(&replicator, |h| h.phase == Phase::Running);
    site.commit(1);
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.unreplicated_since.is_some() && h.synced > h.replicated
    });
    drop(replicator);
    let stopped_at = clock.now();
    clock.advance(Duration::from_secs(120));
    let again = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    let after = wait(&again, |h| {
        h.phase == Phase::Running && h.unreplicated_since.is_some()
    });
    assert!(
        after.unreplicated_since.unwrap() <= stopped_at,
        "stopped at {stopped_at:?}, dated {:?} after the restart",
        after.unreplicated_since
    );
}
