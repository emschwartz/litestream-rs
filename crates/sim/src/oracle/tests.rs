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
    check(&f.model, &f.recorder, &f.raw, acked)
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
    let done = check(&model, &recorder, &raw, &Acked::new()).unwrap();
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
