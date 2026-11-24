//! A boot over a brand-new meta directory whose database is the replica's newest state plus
//! commits the replica never got (issue 329): written after the replica's newest file and
//! holding at least half of its pages, it snapshots above the replica instead of stopping
//! (`crate::identity`, `docs/decisions/D192.md`). Everything that stopped for a real reason
//! still stops.

use super::vouch::redeployed_with_a_new_meta_directory;
use super::*;
use crate::{SnapshotCauseKind, TakeoverRecord};

fn tx(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

/// When the replica's newest file (transaction 3) was written: the tick that staged it.
const TIP_WRITTEN: i64 = T0 + 1;

/// The redeploy of [`redeployed_with_a_new_meta_directory`] (replica 1 to 3, the database
/// equal to its state at 3, staging empty, the WAL restarted and empty), and then the
/// application commits once more while the replicator is down, so page 1 holds 4: what the lost
/// meta directory had staged and never uploaded. The database was last written at `written`.
fn ahead(written: Option<i64>) -> Rig {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.db.write_page(pg(1), &vec![4; PS.bytes()]);
    rig.wal.set_modified(written.map(ts));
    rig
}

fn stops(rig: &mut Rig, actor: &mut Actor) {
    let seen = rig.follow(actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), Some(tx(3)));
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::AheadOfReplica), 0);
}

/// Page 1 of the replica's state at `target`.
fn page_one(rig: &Rig, target: RestoreTarget) -> u8 {
    let sink = MemSink::new();
    block_on(restore(&rig.remote.inner, target, sink.clone())).unwrap();
    sink.published().unwrap().page(1).unwrap()[0]
}

/// The issue's case: the boot snapshots above the replica's newest transaction, names it in
/// the cause, and every transaction restores to what the database held then: 1 to 3 as
/// the replica had them, and the snapshot above them to the database as it is.
#[test]
fn a_new_meta_directory_over_the_replica_plus_more_snapshots_above_it() {
    let mut rig = ahead(Some(T0 + 2));
    let mut actor = Actor::new(go_like());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::AheadOfReplica { tip: tx(3) })
        }),
        "{boot:?}"
    );
    // Settled, not followed: maintenance later folds the transactions into a snapshot.
    let seen = rig.settle(&mut actor, T0 + 2);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), None);
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::AheadOfReplica), 1);
    for n in 1..=3 {
        let at = page_one(&rig, RestoreTarget::Txid(tx(n)));
        assert_eq!(u64::from(at), n, "transaction {n}");
    }
    assert_eq!(page_one(&rig, RestoreTarget::Txid(tx(4))), 4);
    assert_eq!(page_one(&rig, RestoreTarget::Latest), 4);
}

/// An empty database (a fresh volume) pointed at a real replica still stops: carrying on would
/// make it the replica's newest state.
#[test]
fn a_new_meta_directory_over_an_empty_database_still_stops() {
    let mut rig = ahead(Some(T0 + 2));
    rig.wal.remove_file();
    rig.db.set_page_count(0);
    stops(&mut rig, &mut Actor::new(go_like()));
}

/// An unrelated database of the same page size, written after the replica's newest file,
/// still stops: it holds none of the replica's pages.
#[test]
fn a_new_meta_directory_over_an_unrelated_database_still_stops() {
    let mut rig = ahead(Some(T0 + 2));
    for n in 1..=3 {
        rig.db.write_page(pg(n), &vec![0x5a; PS.bytes()]);
    }
    stops(&mut rig, &mut Actor::new(go_like()));
}

/// A database last written before the replica's newest file (an image put back with its
/// modified time, or a live second writer whose files keep landing) still stops, however many
/// pages it shares. So does one written at that very millisecond (no slack), and one whose
/// modified time cannot be read.
#[test]
fn a_new_meta_directory_over_a_database_not_written_after_the_replica_still_stops() {
    for written in [Some(T0), Some(TIP_WRITTEN), None] {
        let mut rig = ahead(written);
        stops(&mut rig, &mut Actor::new(go_like()));
    }
}

/// A takeover record through 2 is held, so the meta directory is not new: a restore on a new
/// host while the old host still uploads leaves a record short of the replica's newest
/// transaction, and the boot stops, whatever the database holds.
#[test]
fn a_takeover_record_short_of_the_replica_still_stops_over_a_database_ahead() {
    let mut rig = ahead(Some(T0 + 2));
    let mut actor = Actor::new(go_like());
    actor.remember_takeover(TakeoverRecord::through(tx(2)));
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

/// A database one page longer than the replica's state, its three pages all the replica's:
/// four pages, three equal, as in the reproduction of the issue. It carries on.
#[test]
fn a_four_page_database_sharing_three_pages_carries_on() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.db.set_page_count(4);
    rig.db.write_page(pg(4), &vec![4; PS.bytes()]);
    rig.wal.set_modified(Some(ts(T0 + 2)));
    let mut actor = Actor::new(go_like());
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::AheadOfReplica), 1);
}

/// A four-page database that shares none of the replica's pages stops.
#[test]
fn a_four_page_unrelated_database_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.db.set_page_count(4);
    for n in 1..=4 {
        rig.db.write_page(pg(n), &vec![0x77; PS.bytes()]);
    }
    rig.wal.set_modified(Some(ts(T0 + 2)));
    stops(&mut rig, &mut Actor::new(go_like()));
}

/// A database of two pages never passes, though both its pages are the replica's (two of
/// three, above the half the bar asks): a page or two match nearly anything.
#[test]
fn a_two_page_database_never_carries_on() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.wal.remove_file();
    rig.db.set_page_count(2);
    rig.wal.set_modified(Some(ts(T0 + 2)));
    stops(&mut rig, &mut Actor::new(go_like()));
}
