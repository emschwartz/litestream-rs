use lsr_core::conformance::block_on;
use lsr_core::{MemStore, ObjectStore};
use lsr_ltx::PageNo;

use super::*;
use crate::model::{Model, PageImage};
use crate::sqlite::AppCommit;
use crate::store::Tap;
use crate::testkit::{PAGE, SALTS, fkey, ltx, origin, pgno, ps, put, txid, wal_end};

/// Three one-frame commits of page 1 filled 10, 11, 12, and a replica that
/// holds them faithfully as three level-0 files.
struct Fixture {
    model: Model,
    tap: crate::store::TapLog,
    store: Tap<MemStore>,
    raw: MemStore,
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE).unwrap()]
}

fn fixture() -> Fixture {
    let raw = MemStore::new();
    let store = Tap::new(raw.clone());
    let tap = store.log();
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
            tap.len(),
        );
        let t = u64::from(n);
        put(&store, 0, t, t, &ltx(t, t, 1, fill, Some(origin(n, n))));
    }
    Fixture {
        model,
        tap,
        store,
        raw,
    }
}

fn run(f: &Fixture, acked: &Acked) -> Result<Checked, Violation> {
    check(&f.model, &f.tap, &f.raw, acked)
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
    let mut rewound = acked;
    rewound.note_rewound();
    run(&f, &rewound).unwrap();
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
