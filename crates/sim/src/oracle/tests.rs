use lsr_core::conformance::block_on;
use lsr_core::{MemStore, ObjectStore};
use lsr_ltx::PageNo;

use super::*;
use crate::model::{Model, PageImage};
use crate::sqlite::AppCommit;
use crate::store::Recorder;
use crate::testkit::{PAGE, SALTS, fkey, ltx, origin, pgno, ps, put, txid, wal_end};

/// Three one-frame commits of page 1 filled 10, 11, 12, and a replica that
/// holds them faithfully as three level-0 files.
struct Fixture {
    model: Model,
    recorder: crate::store::RecorderLog,
    store: Recorder<MemStore>,
    raw: MemStore,
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE).unwrap()]
}

fn fixture() -> Fixture {
    let raw = MemStore::new();
    let store = Recorder::new(raw.clone());
    let recorder = store.log();
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    for n in 1..=3_u32 {
        let fill = u8::try_from(n.saturating_add(9)).unwrap();
        let pages: Vec<(PageNo, Vec<u8>)> = vec![(pgno(1), page(fill))];
        model.commit(
            &pages,
            1,
            AppCommit {
                salts: SALTS,
                end: wal_end(n),
            },
            recorder.len(),
        );
        let t = u64::from(n);
        put(&store, 0, t, t, &ltx(t, t, 1, fill, Some(origin(n, n))));
    }
    Fixture {
        model,
        recorder,
        store,
        raw,
    }
}

fn run(f: &Fixture, acked: &Acked) -> Result<Checked, Violation> {
    check(
        &f.model,
        &f.recorder,
        &crate::remote::Bucket::Mem(f.raw.clone()),
        acked,
    )
}

#[test]
fn oracle_passes_a_faithful_replica() {
    let f = fixture();
    let mut acked = Acked::new();
    acked.record(txid(3));
    let done = run(&f, &acked).unwrap();
    assert_eq!(done.restored, 3);
    assert_eq!(done.inside_file, 0);
    assert_eq!(done.latest, Some(txid(3)));
}

#[test]
fn oracle_catches_planted_mid_tx_file() {
    let f = fixture();
    // A file whose WAL range stops a frame short of any commit's end.
    let cut = origin(3, 3);
    let torn =
        lsr_ltx::WalOrigin::new(cut.offset(), cut.size() / 2, SALTS.salt1, SALTS.salt2).unwrap();
    put(&f.store, 0, 4, 4, &ltx(4, 4, 1, 12, Some(torn)));
    let got = run(&f, &Acked::new()).unwrap_err();
    assert!(matches!(got, Violation::CoversMidTx { .. }), "{got:?}");
}

#[test]
fn oracle_catches_planted_wrong_page() {
    let f = fixture();
    block_on(f.raw.delete(&[fkey(0, 2, 2)])).unwrap();
    put(&f.raw, 0, 2, 2, &ltx(2, 2, 1, 99, Some(origin(2, 2))));
    let got = run(&f, &Acked::new()).unwrap_err();
    let Violation::RestoreMismatch {
        target, first_diff, ..
    } = got
    else {
        panic!("expected a mismatch, got {got:?}");
    };
    assert_eq!(target, RestoreTarget::Txid(txid(2)));
    assert_eq!(first_diff, 1);
}

#[test]
fn oracle_catches_lost_ack() {
    let f = fixture();
    let mut acked = Acked::new();
    acked.record(txid(4));
    assert_eq!(
        run(&f, &acked),
        Err(Violation::LatestLostAck {
            acked: 4,
            latest: Some(3)
        })
    );
    // Forgetting from 5 keeps the acknowledgement of 4; from 4 voids it.
    let mut above = acked;
    above.forget_from(txid(5));
    assert!(
        run(&f, &above).is_err(),
        "an ack below the loss still binds"
    );
    let mut lost = acked;
    lost.forget_from(txid(4));
    assert_eq!(lost.highest(), Some(txid(3)));
    run(&f, &lost).unwrap();
}

#[test]
fn oracle_accepts_compacted_interior_refusal() {
    let f = fixture();
    put(&f.store, 1, 2, 3, &ltx(2, 3, 1, 12, None));
    block_on(f.raw.delete(&[fkey(0, 2, 2), fkey(0, 3, 3)])).unwrap();
    let done = run(&f, &Acked::new()).unwrap();
    assert_eq!(done.inside_file, 1, "transaction 2 lies inside the file");
    assert_eq!(done.restored, 2);
    assert_eq!(done.latest, Some(txid(3)));
}

#[test]
fn oracle_refuses_a_hole_that_is_not_a_compacted_file() {
    let f = fixture();
    block_on(f.raw.delete(&[fkey(0, 2, 2)])).unwrap();
    let got = run(&f, &Acked::new()).unwrap_err();
    assert!(matches!(got, Violation::RestoreRefused { .. }), "{got:?}");
}

#[test]
fn oracle_catches_a_reused_transaction_id() {
    let f = fixture();
    // Transaction 2 again, now claiming the third commit's frames.
    put(&f.store, 0, 2, 2, &ltx(2, 2, 1, 12, Some(origin(3, 3))));
    let got = run(&f, &Acked::new()).unwrap_err();
    assert!(
        matches!(got, Violation::TxidReused { txid: 2, .. }),
        "{got:?}"
    );
}

/// A power loss that cut no commit a file covers does not license reusing an id: the run-wide
/// exemption the oracle used to grant after any power loss hid exactly this.
#[test]
fn a_power_loss_that_cut_nothing_covered_does_not_excuse_a_reused_id() {
    let mut f = fixture();
    // Every commit survives: the WAL kept all three frames.
    f.model.on_power_loss(SALTS, 3, f.recorder.len());
    put(&f.store, 0, 2, 2, &ltx(2, 2, 1, 12, Some(origin(3, 3))));
    let got = run(&f, &Acked::new()).unwrap_err();
    assert!(
        matches!(got, Violation::TxidReused { txid: 2, .. }),
        "{got:?}"
    );
}

/// An id whose commit a power loss cut may cover the commit the database made in its place:
/// nothing holds what the id first named any more.
#[test]
fn an_id_whose_commit_a_power_loss_cut_may_cover_its_replacement() {
    let mut f = fixture();
    // The loss keeps two frames: the third commit is gone, and the app commits again there.
    f.model.on_power_loss(SALTS, 2, f.recorder.len());
    f.model.commit(
        &[(pgno(1), page(13))],
        1,
        AppCommit {
            salts: SALTS,
            end: wal_end(3),
        },
        f.recorder.len(),
    );
    put(&f.store, 0, 3, 3, &ltx(3, 3, 1, 13, Some(origin(3, 3))));
    let done = run(&f, &Acked::new()).unwrap();
    assert_eq!(done.latest, Some(txid(3)));
}

/// A compacted key put again with other pages: no transaction id changes commit, so only the
/// write-once rule on keys sees it.
#[test]
fn a_key_put_again_with_other_bytes_is_a_rewrite() {
    let f = fixture();
    put(&f.store, 1, 1, 3, &ltx(1, 3, 1, 12, None));
    run(&f, &Acked::new()).unwrap();
    put(&f.store, 1, 1, 3, &ltx(1, 3, 1, 99, None));
    let got = run(&f, &Acked::new()).unwrap_err();
    assert!(matches!(got, Violation::KeyRewritten { .. }), "{got:?}");
}

/// The same bytes put again (an upload retried after an ambiguous commit) are no rewrite.
#[test]
fn a_key_put_again_with_the_same_bytes_is_not_a_rewrite() {
    let f = fixture();
    put(&f.store, 1, 1, 3, &ltx(1, 3, 1, 12, None));
    put(&f.store, 1, 1, 3, &ltx(1, 3, 1, 12, None));
    run(&f, &Acked::new()).unwrap();
}

/// A snapshot staged for transaction 1 was lost before it reached the replica
/// (a power loss tore it, or the staging directory was wiped), and the engine
/// took transaction 1 again from a later commit with an empty WAL, as after a
/// VACUUM. The replica holds only the second snapshot, so transaction 1 is
/// the later commit, not the first one noted.
#[test]
fn oracle_pairs_a_reissued_snapshot_with_its_newest_note() {
    let raw = MemStore::new();
    let store = Recorder::new(raw.clone());
    let recorder = store.log();
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    let mut commit = |fill: u8, frames: u32| {
        model.commit(
            &[(pgno(1), page(fill))],
            1,
            AppCommit {
                salts: SALTS,
                end: wal_end(frames),
            },
            recorder.len(),
        )
    };
    let lost = commit(10, 1);
    let kept = commit(20, 2);
    model.note_snapshot(txid(1), lost, false, recorder.len());
    model.note_snapshot(txid(1), kept, false, recorder.len());
    put(&store, 0, 1, 1, &ltx(1, 1, 1, 20, None));
    let done = check(
        &model,
        &recorder,
        &crate::remote::Bucket::Mem(raw.clone()),
        &Acked::new(),
    )
    .unwrap();
    assert_eq!(done.restored, 1);
    assert_eq!(done.latest, Some(txid(1)));
}

#[test]
fn within_flags_only_what_is_over() {
    within(Bound::WalLength, 4, 4).unwrap();
    assert_eq!(
        within(Bound::WalLength, 5, 4),
        Err(Violation::ResourceBound {
            what: Bound::WalLength,
            seen: 5,
            limit: 4
        })
    );
}

#[test]
fn acked_reads_a_step_result() {
    let mut acked = Acked::new();
    acked.on_did(&Did::Nothing);
    assert_eq!(acked.highest(), None);
    acked.on_did(&Did::Listed {
        newest: Some(txid(5)),
    });
    acked.on_did(&Did::Listed {
        newest: Some(txid(2)),
    });
    assert_eq!(acked.highest(), Some(txid(5)));
}

/// The judges' listing is the store's own contents, not the engine's listing code: a listing
/// fault queued for the engine neither stops it nor is spent by it, and it finds every file.
/// Before, the oracle listed through `Inventory::load`, so the fault failed the check as
/// "listing the replica" and was gone before the engine met it.
#[test]
fn the_judges_list_the_replica_without_the_engines_listing() {
    let f = fixture();
    let fault = lsr_core::StoreError::Transient {
        op: lsr_core::StoreOp::List,
        kind: std::io::ErrorKind::ConnectionReset,
    };
    f.raw.inject_failure(lsr_core::StoreOp::List, fault);
    let files = replica_files(&crate::remote::Bucket::Mem(f.raw.clone()));
    assert_eq!(
        files.iter().map(|file| file.key).collect::<Vec<_>>(),
        vec![fkey(0, 1, 1), fkey(0, 2, 2), fkey(0, 3, 3)]
    );
    assert_eq!(newest_of(&files), Some(txid(3)));
    let listed = block_on(f.raw.list(
        lsr_core::Level::new(0).unwrap(),
        None,
        lsr_core::PageLimit::MAX,
    ));
    assert!(listed.is_err(), "the engine's fault was spent by a judge");
}

/// What the level judge says of the last file the replica took.
fn judged(recorder: &crate::store::RecorderLog, raw: &MemStore) -> Result<LevelPut, Violation> {
    let events = recorder.events();
    let files = replica_files(&crate::remote::Bucket::Mem(raw.clone()));
    judge_level_put(&events, &files, events.last().unwrap())
}

/// A store whose level 1 holds transactions 1 to 2, 3 to 4 and 5 to 6, and level 2 the first.
fn three_level_one_files() -> (Recorder<MemStore>, MemStore) {
    let raw = MemStore::new();
    let store = Recorder::new(raw.clone());
    for (min, max) in [(1, 2), (3, 4), (5, 6)] {
        put(&store, 1, min, max, &ltx(min, max, 1, 7, None));
    }
    put(&store, 2, 1, 2, &ltx(1, 2, 1, 7, None));
    (store, raw)
}

/// The checker can fail: a level-2 file that starts two files past its level's newest
/// jumped over the level-1 file between them.
#[test]
fn a_file_that_jumps_over_a_file_of_the_level_below_is_a_level_hole() {
    let (store, raw) = three_level_one_files();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    assert_eq!(
        judged(&store.log(), &raw),
        Err(Violation::LevelHole {
            key: "2 5-6".to_owned(),
            skipped: "1 3-4".to_owned(),
        })
    );
}

#[test]
fn a_file_that_follows_its_level_is_judged_by_how_many_files_it_took() {
    let (store, raw) = three_level_one_files();
    assert_eq!(
        judged(&store.log(), &raw),
        Ok(LevelPut::Follows),
        "the first"
    );
    put(&store, 2, 3, 6, &ltx(3, 6, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::Joins));
    put(&store, 1, 7, 7, &ltx(7, 7, 1, 7, None));
    put(&store, 2, 7, 7, &ltx(7, 7, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::Follows));
    put(&store, 0, 8, 8, &ltx(8, 8, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::Skipped), "level 0");
    put(&store, 9, 1, 8, &ltx(1, 8, 1, 7, None));
    assert_eq!(
        judged(&store.log(), &raw),
        Ok(LevelPut::Skipped),
        "a snapshot"
    );
}

/// A file that closes a hole from its first transaction follows: it starts right after the
/// files before it, whatever comes after.
#[test]
fn a_file_that_closes_a_hole_from_its_start_follows() {
    let (store, raw) = three_level_one_files();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    put(&store, 2, 3, 4, &ltx(3, 4, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::Follows));
}

/// A range the level below no longer holds whole is not one the compaction jumped over.
#[test]
fn a_gap_with_no_file_of_the_level_below_inside_it_skipped_nothing() {
    let (store, raw) = three_level_one_files();
    block_on(raw.delete(&[fkey(1, 3, 4)])).unwrap();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::NothingSkipped));
}

/// The level's own file over the gap was taken from the store from outside: the engine
/// extended the level from the newest file it knew. The same gap after the engine's own
/// delete is not excused.
#[test]
fn only_a_file_lost_from_outside_excuses_a_gap() {
    let (store, raw) = three_level_one_files();
    put(&store, 2, 3, 4, &ltx(3, 4, 1, 7, None));
    block_on(raw.delete(&[fkey(2, 3, 4)])).unwrap();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::LostOutside));

    let (store, raw) = three_level_one_files();
    put(&store, 2, 3, 4, &ltx(3, 4, 1, 7, None));
    block_on(store.delete(&[fkey(2, 3, 4)])).unwrap();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    assert!(matches!(
        judged(&store.log(), &raw),
        Err(Violation::LevelHole { .. })
    ));
}

/// A file the replica no longer holds is not judged: a later tick's delete took it.
#[test]
fn a_put_the_replica_no_longer_holds_is_skipped() {
    let (store, raw) = three_level_one_files();
    put(&store, 2, 5, 6, &ltx(5, 6, 1, 7, None));
    block_on(raw.delete(&[fkey(2, 5, 6)])).unwrap();
    assert_eq!(judged(&store.log(), &raw), Ok(LevelPut::Skipped));
}
