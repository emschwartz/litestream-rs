//! The live pages a scheduled verify compares a restore with, and the
//! restore point it pins.

use super::*;
use crate::verify::{Compared, verify_against};

fn newest_on_replica(rig: &Rig) -> Option<Txid> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max()
}

/// Nothing is read until asked, and once asked the pages are those of the
/// transaction the replica holds, which a restore of it reproduces.
#[test]
fn live_pages_are_read_once_asked_at_the_replica_s_transaction() {
    let (mut rig, mut actor) = caught_up();
    rig.tick(&mut actor, T0 + 1);
    assert_eq!(actor.take_live_pages(), None);
    actor.read_live_pages();
    rig.settle(&mut actor, T0 + 2);
    let live = actor.take_live_pages().unwrap();
    assert_eq!(Some(live.txid()), newest_on_replica(&rig));
    assert_eq!(live.commit(), nz(3));
    assert_eq!(actor.take_live_pages(), None, "handed over once");
    let verified = block_on(verify_against(&rig.remote.inner, &live)).unwrap();
    assert!(
        matches!(verified.compared(), Compared::Same { pages: 3, .. }),
        "{:?}",
        verified.compared()
    );
}

/// A commit not yet on the replica is waited for: the pages are read only
/// once its upload is confirmed, at its transaction.
#[test]
fn live_pages_wait_for_the_upload_of_a_new_commit() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(7);
    actor.read_live_pages();
    let sync = rig.tick(&mut actor, T0 + 1);
    assert_eq!(event(&sync), Some(&Event::Synced { files: 1 }));
    assert_eq!(actor.take_live_pages(), None, "the replica lacks it");
    rig.settle(&mut actor, T0 + 1);
    rig.tick(&mut actor, T0 + 2);
    let live = actor.take_live_pages().unwrap();
    assert_eq!(Some(live.txid()), newest_on_replica(&rig));
    assert_eq!(live.txid(), actor.staged().unwrap());
    let verified = block_on(verify_against(&rig.remote.inner, &live)).unwrap();
    assert!(
        matches!(verified.compared(), Compared::Same { .. }),
        "{:?}",
        verified.compared()
    );
}

/// A draining engine reads no live pages: a drain only syncs and uploads.
#[test]
fn a_drain_reads_no_live_pages() {
    let (mut rig, mut actor) = caught_up();
    actor.read_live_pages();
    actor.begin_shutdown(ts(T0 + 60_000));
    rig.follow(&mut actor, T0 + 1, 20);
    assert_eq!(actor.take_live_pages(), None);
}

/// A verify's pin is the restore point retention keeps; a degraded lane's
/// older one wins while both stand, and each goes on its own.
#[test]
fn a_verify_pin_is_kept_beside_a_lane_pin_and_the_older_wins() {
    let (mut rig, mut actor) = caught_up();
    assert_eq!(actor.retention_pin(), None);
    let first = newest_on_replica(&rig).unwrap();
    rig.commit(8);
    rig.settle(&mut actor, T0 + 1);
    let second = newest_on_replica(&rig).unwrap();
    assert!(second > first);
    actor.pin_for_verify(Some(second));
    assert_eq!(actor.retention_pin(), Some(second));
    actor.lane_pin = Some(first);
    actor.pinned = true;
    actor.apply_pin();
    assert_eq!(actor.retention_pin(), Some(first));
    actor.maintain.stale = false;
    actor.pin_for_verify(None);
    assert_eq!(actor.retention_pin(), Some(first));
    assert!(actor.maintain.stale, "a released pin lists again");
    actor.pin_for_verify(Some(second));
    actor.pinned = false;
    actor.apply_pin();
    assert_eq!(actor.retention_pin(), Some(second));
}
