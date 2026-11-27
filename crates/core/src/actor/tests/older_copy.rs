//! A boot over a brand-new meta directory whose database is an older copy of the replica's:
//! the replicator's counter in it is below the replica's, so it is not the replica plus more and
//! the boot stops (`crate::identity`, `docs/decisions/D214.md`). The ordinary histories still
//! carry on: the database ahead of the replica, and one whose counter cannot be compared.

use super::*;
use crate::identity::counter::build::{counter_page, empty_counter_page, schema_page};
use crate::{SnapshotCauseKind, TakeoverRecord};

fn tx(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

const TIP_SEQ: u32 = 5;

/// When the replica's newest file was written: the tick that staged it.
const TIP_WRITTEN: i64 = T0 + 1;

/// A replica through transaction 4 holding a six-page database in the shape SQLite's: page 1 the
/// schema naming the counter's root, page 2, which holds [`TIP_SEQ`]; pages 3 to 6 data. Then a
/// redeploy that loses the meta directory: staging empty, no takeover record, the WAL
/// restarted. The database file is the replica's state at 4, `written` after its newest file.
fn redeployed() -> Rig {
    redeployed_with(counter_page(PS, TIP_SEQ))
}

/// [`redeployed`] with `two` as page 2 of the replica's state (and of the file, to start with).
fn redeployed_with(two: Vec<u8>) -> Rig {
    let (mut rig, mut actor) = caught_up();
    let fill = |n: u8| vec![n; PS.bytes()];
    rig.wal.append_tx(
        &[
            (pg(1), schema_page(PS, 2)),
            (pg(4), fill(4)),
            (pg(5), fill(5)),
            (pg(6), fill(6)),
        ],
        nz(6),
    );
    rig.settle(&mut actor, TIP_WRITTEN);
    rig.wal.append_tx(&[(pg(2), two.clone())], nz(6));
    rig.settle(&mut actor, TIP_WRITTEN);
    rig.wal.append_tx(&[(pg(3), fill(0x33))], nz(6));
    rig.settle(&mut actor, TIP_WRITTEN);
    rig.staging = Counting::default();
    rig.wal.force_restart();
    rig.db.set_page_count(6);
    rig.db.write_page(pg(1), &schema_page(PS, 2));
    rig.db.write_page(pg(2), &two);
    rig.db.write_page(pg(3), &fill(0x33));
    for n in 4..=6 {
        rig.db.write_page(pg(n), &fill(u8::try_from(n).unwrap()));
    }
    rig.wal.set_modified(Some(ts(T0 + 2)));
    rig
}

fn stops(rig: &mut Rig) {
    let mut actor = Actor::new(go_like());
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), Some(tx(4)));
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::AheadOfReplica), 0);
}

fn carries_on(rig: &mut Rig) {
    let mut actor = Actor::new(go_like());
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), None);
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::AheadOfReplica), 1);
}

/// The older copy: one data page and the counter are as the replica had them some commits ago,
/// the rest are its pages, and the file was copied after the replica's newest file (a copy tool
/// that sets the time anew). It reads as the replica minus commits, so it stops.
#[test]
fn an_older_copy_with_a_lower_counter_stops() {
    let mut rig = redeployed();
    rig.db.write_page(pg(2), &counter_page(PS, TIP_SEQ - 1));
    rig.db.write_page(pg(3), &vec![3; PS.bytes()]);
    stops(&mut rig);
}

/// A copy of a database that was never bumped has an empty counter table, which reads 0.
#[test]
fn an_older_copy_with_an_empty_counter_table_stops() {
    let mut rig = redeployed();
    rig.db.write_page(pg(2), &empty_counter_page(PS));
    stops(&mut rig);
}

/// The ordinary history (issue 329): the application wrote on after the replica's last upload,
/// so its counter is the replica's or higher. Both carry on with a snapshot above the replica.
#[test]
fn a_database_with_a_counter_at_or_above_the_replicas_carries_on() {
    for local in [TIP_SEQ, TIP_SEQ + 1] {
        let mut rig = redeployed();
        rig.db.write_page(pg(2), &counter_page(PS, local));
        rig.db.write_page(pg(3), &vec![0x44; PS.bytes()]);
        carries_on(&mut rig);
    }
}

/// The window the rule leaves: a backup taken after the replica's last counter bump, before
/// the commits that followed it, holds the replica's counter and older data, and carries on.
/// This test pins that: what a copy that late costs is in `docs/decisions/D214.md`.
#[test]
fn an_older_copy_taken_after_the_last_bump_carries_on() {
    let mut rig = redeployed();
    rig.db.write_page(pg(3), &vec![3; PS.bytes()]);
    carries_on(&mut rig);
}

/// No counter to read locally (the schema names none): the rule has no evidence and the two
/// older tests decide, as they did before.
#[test]
fn a_database_whose_schema_names_no_counter_is_judged_as_before() {
    let mut rig = redeployed();
    rig.db.write_page(pg(1), &vec![1; PS.bytes()]);
    rig.db.write_page(pg(3), &vec![3; PS.bytes()]);
    carries_on(&mut rig);
}

/// The counter's page is not a counter in the replica (its root moved): no evidence either.
#[test]
fn a_replica_whose_page_is_not_the_counter_gives_no_evidence() {
    let mut rig = redeployed_with(vec![9; PS.bytes()]);
    rig.db.write_page(pg(2), &counter_page(PS, TIP_SEQ - 1));
    rig.db.write_page(pg(3), &vec![3; PS.bytes()]);
    carries_on(&mut rig);
}

/// A takeover record short of the replica still stops whatever the counter says.
#[test]
fn a_takeover_record_short_of_the_replica_still_stops() {
    let mut rig = redeployed();
    let mut actor = Actor::new(go_like());
    actor.remember_takeover(TakeoverRecord::through(tx(2)));
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}
