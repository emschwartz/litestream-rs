//! The page checksum table in the actor: a database page that changed under
//! the engine is a degraded state that keeps backing up (the default), or a
//! stop before the page is shipped (strict).

use super::*;

/// The page the tests flip: the database file alone holds it (the rig's WAL
/// writes page 1 only), and the first snapshot recorded it.
fn three() -> PageNo {
    pg(3)
}

fn flip(rig: &mut Rig) {
    rig.db.write_page(three(), &vec![0xEE; PS.bytes()]);
}

fn mismatch() -> PersistentCause {
    PersistentCause::DatabasePageMismatch { pgno: three() }
}

fn caught_up_with(cfg: ActorConfig) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg);
    rig.settle(&mut actor, T0);
    (rig, actor)
}

fn staged_page(rig: &Rig, pgno: PageNo) -> Vec<Vec<u8>> {
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .filter_map(|info| {
            let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
            let bytes = block_on(rig.staging.inner.read(&info.key, 0, len)).unwrap();
            let mut decoder = ReadDecoder::new(bytes.as_slice());
            let mut found = None;
            while let Some(event) = decoder.next_event().unwrap() {
                if let lsr_ltx::Event::Page(page) = event
                    && page.pgno() == pgno
                {
                    found = Some(page.data().to_vec());
                }
            }
            found
        })
        .collect()
}

#[test]
fn a_flipped_database_page_degrades_the_sync_lane_ships_the_file_and_keeps_syncing() {
    let (mut rig, mut actor) = caught_up_with(go_like());
    let held = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max();
    flip(&mut rig);
    actor.request_snapshot();
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert!(
        seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Degraded { lane: Lane::Sync, cause, .. }) if *cause == mismatch()
        )),
        "{seen:?}"
    );
    // The database the host serves is what reached the replica.
    assert_eq!(
        staged_page(&rig, three()).last(),
        Some(&vec![0xEE; PS.bytes()])
    );
    // WAL syncs go on; the page stays reported and retention stays pinned.
    rig.commit(5);
    let seen = rig.follow(&mut actor, T0 + 10_000, 50);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Synced { files: 1 })),
        "{seen:?}"
    );
    assert_eq!(
        actor.degraded(Lane::Sync).map(|d| d.cause),
        Some(mismatch())
    );
    assert_eq!(actor.maintain.state.pin(), held);
}

#[test]
fn a_frame_that_rewrites_the_page_clears_the_degraded_state_and_the_pin() {
    let (mut rig, mut actor) = caught_up_with(go_like());
    flip(&mut rig);
    actor.request_snapshot();
    rig.follow(&mut actor, T0 + 10, 50);
    assert!(actor.degraded(Lane::Sync).is_some());
    rig.wal.append_tx(&[(three(), vec![7; PS.bytes()])], nz(3));
    rig.follow(&mut actor, T0 + 10_000, 50);
    rig.tick(&mut actor, T0 + 20_000);
    assert_eq!(actor.degraded(Lane::Sync), None);
    assert_eq!(actor.maintain.state.pin(), None);
}

#[test]
fn an_operator_accepting_the_file_clears_the_degraded_state() {
    let (mut rig, mut actor) = caught_up_with(go_like());
    flip(&mut rig);
    actor.request_snapshot();
    rig.follow(&mut actor, T0 + 10, 50);
    assert!(actor.degraded(Lane::Sync).is_some());
    actor.accept_database_pages();
    let tick = rig.tick(&mut actor, T0 + 100_000_000);
    assert_eq!(actor.degraded(Lane::Sync), None, "{tick:?}");
}

#[test]
fn a_flipped_database_page_stops_a_strict_engine_before_it_is_shipped() {
    let (mut rig, mut actor) = caught_up_with(cfg());
    flip(&mut rig);
    actor.request_snapshot();
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Halted(HaltCause::DatabasePageMismatch {
            pgno: three()
        })),
        "{seen:?}"
    );
    assert!(
        !staged_page(&rig, three()).contains(&vec![0xEE; PS.bytes()]),
        "the flipped page was staged"
    );
}

/// A boot that continues seeds the table from the staged files, so a page the
/// last run recorded is checked on its first read after the restart.
#[test]
fn a_boot_that_continues_checks_pages_the_last_run_recorded() {
    let (mut rig, _) = caught_up_with(cfg());
    flip(&mut rig);
    let mut actor = Actor::new(cfg());
    let booted = rig.tick(&mut actor, T0 + 10);
    assert_eq!(event(&booted), Some(&Event::Booted { cause: None }));
    actor.request_snapshot();
    let seen = rig.follow(&mut actor, T0 + 20, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Halted(HaltCause::DatabasePageMismatch {
            pgno: three()
        })),
        "{seen:?}"
    );
}

/// A boot that snapshots trusts the file afresh: the snapshot cause empties
/// the table, so the same flip raises nothing.
#[test]
fn a_boot_that_snapshots_does_not_seed() {
    let (mut rig, _) = caught_up_with(cfg());
    flip(&mut rig);
    rig.wal.force_restart();
    rig.commit(9);
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Halted(_))),
        "{seen:?}"
    );
}
