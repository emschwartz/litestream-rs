use std::io::Cursor;
use std::num::NonZeroU32;

use lsr_ltx::{PageNo, PageSize, Timestamp, Txid, WalOrigin};
use lsr_wal::{FrameCursor, FrameRef, Salts, ScanLimit, TxOutcome, WalReader};
use proptest::prelude::*;

use super::*;
use crate::source::{CommittedRead, FakeWal};

const PS: PageSize = PageSize::MIN;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn salts(n: u32) -> Salts {
    Salts {
        salt1: n,
        salt2: n.wrapping_mul(7).wrapping_add(1),
    }
}

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

fn commit(wal: &FakeWal, pages: &[u32], size: u32) {
    let pages: Vec<(PageNo, Vec<u8>)> = pages
        .iter()
        .map(|n| (pg(*n), vec![u8::try_from(*n).unwrap(); PS.bytes()]))
        .collect();
    wal.append_tx(&pages, nz(size));
}

fn reader(bytes: Vec<u8>) -> WalReader<Cursor<Vec<u8>>> {
    WalReader::open(Cursor::new(bytes)).unwrap()
}

/// The end of the last commit in `bytes`: its cursor, commit frame and database size.
fn end_of(bytes: Vec<u8>) -> Option<(FrameCursor, FrameRef, u32)> {
    let mut wal = WalReader::open(Cursor::new(bytes)).ok()?;
    let mut at = wal.cursor_at_start();
    let mut last = None;
    while let Ok(TxOutcome::Tx(tx)) = wal.next_tx(&at, ScanLimit::FirstInvalidFrame) {
        at = *tx.end();
        last = Some((at, *tx.frames().last()?, tx.commit()));
    }
    last
}

fn position(wal: &FakeWal, id: u64, continuity: Continuity) -> Position {
    let (cursor, frame, size) = end_of(wal.file_bytes()).unwrap();
    Position::new(
        txid(id),
        cursor,
        nz(size),
        LastPage::of_frame(&frame),
        ts(1000),
        continuity,
    )
}

fn boot(wal: &FakeWal, id: u64) -> TipView {
    let bytes = wal.file_bytes();
    let (cursor, frame, size) = end_of(bytes.clone()).unwrap();
    let head = *reader(bytes).header();
    let end = cursor.next().get();
    let salts = head.salts();
    TipView::Readable {
        txid: txid(id),
        origin: WalOrigin::new(32, end.saturating_sub(32), salts.salt1, salts.salt2).unwrap(),
        commit: nz(size),
        page_size: PS,
        timestamp: ts(1000),
        last_page: LastPageCheck::Match {
            last_page: LastPage::of_frame(&frame),
            cursor,
        },
    }
}

fn view(wal: &FakeWal) -> WalView {
    let bytes = wal.file_bytes();
    WalView {
        header: WalReader::open(Cursor::new(bytes.clone()))
            .ok()
            .map(|r| *r.header()),
        len: Some(WalFileLen::new(u64::try_from(bytes.len()).unwrap())),
        committed: CommittedRead::new(wal.committed_end(), wal.backfilled()),
        epoch: wal.epoch(),
    }
}

fn probes(wal: &FakeWal, at: &Position) -> Probes {
    Probes::read(&mut reader(wal.file_bytes()), at).unwrap()
}

fn snapshot(after: Option<u64>, cause: SnapshotCause) -> Recovered {
    Recovered::Snapshot {
        after: after.map(txid),
        cause,
    }
}

fn fresh() -> FakeWal {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    commit(&wal, &[2, 3], 3);
    wal
}

fn run_running(wal: &FakeWal, pos: &Position, remote: Option<u64>) -> Verdict {
    recover_traced(
        Some(Tip::Running(pos, probes(wal, pos))),
        &view(wal),
        remote.map(txid),
    )
}

// Rule 1.
#[test]
fn rule_1_no_tip_is_a_first_sync() {
    let wal = fresh();
    assert_eq!(
        recover(None, &view(&wal), None),
        snapshot(None, SnapshotCause::FirstSync)
    );
}

#[test]
fn rule_1_no_tip_follows_the_remote() {
    let wal = fresh();
    assert_eq!(
        recover(None, &view(&wal), Some(txid(4))),
        snapshot(Some(4), SnapshotCause::FirstSync)
    );
}

// Rule 2.
#[test]
fn rule_2_boot_behind_the_remote_snapshots_after_it() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), Some(txid(5))),
        snapshot(Some(5), SnapshotCause::BehindRemote { local: txid(2) })
    );
}

#[test]
fn rule_2_a_remote_below_the_tip_is_pending_uploads() {
    let wal = fresh();
    let tip = boot(&wal, 5);
    assert!(matches!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), Some(txid(3))),
        Recovered::Continue(_)
    ));
}

#[test]
fn rule_2_running_behind_the_remote_halts() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    assert_eq!(
        run_running(&wal, &pos, Some(3)).recovered,
        Recovered::Halt(HaltCause::ForeignWriter)
    );
}

// Rule 3.
#[test]
fn rule_3_an_unreadable_tip_snapshots_after_the_newer_of_tip_and_remote() {
    let wal = fresh();
    let tip = TipView::Unreadable { txid: txid(4) };
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), Some(txid(2))),
        snapshot(Some(4), SnapshotCause::LocalTipUnreadable)
    );
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), None),
        snapshot(Some(4), SnapshotCause::LocalTipUnreadable)
    );
}

// Rule 4.
#[test]
fn rule_4_no_wal_file_is_wal_missing() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let mut gone = view(&wal);
    gone.header = None;
    gone.len = None;
    assert_eq!(
        recover(Some(Tip::Running(&pos, probes(&wal, &pos))), &gone, None),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
    let tip = boot(&wal, 2);
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &gone, None),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
}

#[test]
fn rule_4_a_torn_header_is_wal_missing() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    wal.corrupt_byte(9);
    assert_eq!(view(&wal).header, None);
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), None),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
}

// Rule 5.
#[test]
fn rule_5_a_new_page_size_snapshots() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let other = FakeWal::new(PageSize::new(1024).unwrap(), salts(10));
    assert_eq!(
        recover(
            Some(Tip::Running(&pos, probes(&wal, &pos))),
            &view(&other),
            None
        ),
        snapshot(Some(2), SnapshotCause::PageSizeChanged)
    );
}

// Rule 6 and F1.
fn restarted(wal: &FakeWal) {
    wal.force_restart();
    commit(wal, &[1], 3);
}

#[test]
fn f1_a_pinned_position_follows_a_restart_it_can_prove() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    restarted(&wal);
    let verdict = run_running(&wal, &pos, None);
    let Recovered::WalRestarted(next) = verdict.recovered else {
        panic!("{verdict:?}");
    };
    assert_eq!(verdict.event, None);
    assert_eq!(next.cursor().next().get(), 32);
    assert_eq!(next.cursor().salts(), wal.salts());
    assert_eq!(next.txid(), pos.txid());
    assert_eq!(next.commit(), pos.commit());
    assert_eq!(next.last_page(), pos.last_page());
    assert_eq!(next.timestamp(), pos.timestamp());
    assert_eq!(next.continuity(), pos.continuity());
}

#[test]
fn f1_an_unpinned_position_snapshots_on_a_restart() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    restarted(&wal);
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn f1_a_pin_from_another_epoch_proves_nothing() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    restarted(&wal);
    wal.set_epoch(wal.epoch().next());
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn f1_two_restarts_are_not_one() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    restarted(&wal);
    restarted(&wal);
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn f1_the_tripwire_catches_frames_abandoned_after_the_pin() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    // Frames appended after the pin, then a restart that rewrites the header in
    // place: the old frames' bytes are still in the file past the new end.
    commit(&wal, &[1], 3);
    wal.force_restart();
    let verdict = run_running(&wal, &pos, None);
    assert_eq!(
        verdict.recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
    assert_eq!(
        verdict.event,
        Some(RecoverEvent::PinViolated {
            synced: pos.synced().offset()
        })
    );
}

#[test]
fn f1_a_restart_that_has_grown_past_the_old_end_is_followed() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    wal.force_restart();
    commit(&wal, &[1], 3);
    commit(&wal, &[2], 3);
    commit(&wal, &[3], 3);
    assert!(matches!(
        run_running(&wal, &pos, None).recovered,
        Recovered::WalRestarted(_)
    ));
}

#[test]
fn go_parity_an_unknown_salt_snapshots() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    let other = FakeWal::new(PS, salts(99));
    commit(&other, &[1], 1);
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&other), None),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn go_parity_a_salt_reset_at_boot_snapshots() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    restarted(&wal);
    assert_eq!(
        recover(Some(Tip::Boot(&tip)), &view(&wal), None),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

// Rule 7 and F2.
#[test]
fn f2_a_committed_end_before_the_tip_is_a_rewind() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let mut seen = view(&wal);
    let end = lsr_wal::CommittedEnd::new(1, wal.salts(), PS);
    seen.committed = CommittedRead::new(end, 0);
    let verdict = recover_traced(Some(Tip::Running(&pos, probes(&wal, &pos))), &seen, None);
    let synced = pos.synced().offset();
    let committed = end.offset();
    assert_eq!(
        verdict.recovered,
        snapshot(Some(2), SnapshotCause::LocalRewound { synced, committed })
    );
    assert_eq!(
        verdict.event,
        Some(RecoverEvent::LocalRewound { synced, committed })
    );
}

#[test]
fn go_parity_a_wal_truncated_after_synced_to_end_is_now_a_snapshot() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    let bytes = wal.file_bytes();
    wal.truncate_file(bytes.len() - 100);
    let verdict = recover_traced(Some(Tip::Boot(&tip)), &view(&wal), None);
    assert!(matches!(
        verdict.recovered,
        Recovered::Snapshot {
            cause: SnapshotCause::LocalRewound { .. },
            ..
        }
    ));
    assert!(matches!(
        verdict.event,
        Some(RecoverEvent::LocalRewound { .. })
    ));
}

#[test]
fn rule_7_a_file_cut_by_a_frame_is_a_rewind_for_a_running_position_too() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let before = wal.file_bytes().len();
    wal.truncate_file(before - 1000);
    let verdict = run_running(&wal, &pos, None);
    assert!(matches!(
        verdict.recovered,
        Recovered::Snapshot {
            cause: SnapshotCause::LocalRewound { .. },
            ..
        }
    ));
}

// Rule 8.
#[test]
fn rule_8_a_frame_before_the_end_that_does_not_verify_is_prev_frame_mismatch() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let at = usize::try_from(pos.cursor().next().get()).unwrap();
    // A byte inside the last frame's page: its checksum fails.
    wal.corrupt_byte(at - 10);
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::PrevFrameMismatch)
    );
}

// Rule 9.
#[test]
fn rule_9_a_last_page_that_differs_snapshots() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Unpinned);
    let other = LastPage::new(pg(9), pos.last_page().digest());
    let moved = Position::new(
        pos.txid(),
        pos.cursor(),
        pos.commit(),
        other,
        pos.timestamp(),
        Continuity::Unpinned,
    );
    assert_eq!(
        run_running(&wal, &moved, None).recovered,
        snapshot(Some(2), SnapshotCause::LastPageMismatch)
    );
}

#[test]
fn rule_9_at_boot_a_mismatch_or_a_page_the_tip_lacks_snapshots() {
    let wal = fresh();
    let TipView::Readable {
        txid,
        origin,
        commit,
        page_size,
        timestamp,
        ..
    } = boot(&wal, 2)
    else {
        panic!("readable");
    };
    for last_page in [LastPageCheck::Mismatch, LastPageCheck::NotInTip] {
        let tip = TipView::Readable {
            txid,
            origin,
            commit,
            page_size,
            timestamp,
            last_page,
        };
        assert_eq!(
            recover(Some(Tip::Boot(&tip)), &view(&wal), None),
            snapshot(Some(2), SnapshotCause::LastPageMismatch)
        );
    }
}

// Rule 10.
#[test]
fn rule_10_a_running_position_continues_as_it_is() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    assert_eq!(
        run_running(&wal, &pos, Some(2)).recovered,
        Recovered::Continue(pos)
    );
}

#[test]
fn f1_a_boot_continue_at_the_committed_end_is_pinned() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    let Recovered::Continue(next) = recover(Some(Tip::Boot(&tip)), &view(&wal), None) else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Pinned(wal.epoch()));
    assert_eq!(next.cursor().next(), wal.committed_end().offset());
}

#[test]
fn f1_a_boot_continue_below_the_committed_end_is_unpinned() {
    let wal = fresh();
    let tip = boot(&wal, 2);
    commit(&wal, &[3], 3);
    let Recovered::Continue(next) = recover(Some(Tip::Boot(&tip)), &view(&wal), None) else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Unpinned);
}

#[test]
fn f1_a_pinned_position_from_an_older_epoch_at_the_committed_end_is_repinned() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    wal.set_epoch(wal.epoch().next());
    let Recovered::Continue(next) = run_running(&wal, &pos, None).recovered else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Pinned(wal.epoch()));
}

#[test]
fn f1_a_pinned_position_below_the_committed_end_stays_pinned() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Pinned(wal.epoch()));
    commit(&wal, &[3], 3);
    let Recovered::Continue(next) = run_running(&wal, &pos, None).recovered else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Pinned(wal.epoch()));
}

#[test]
fn go_parity_an_end_at_offset_32_continues() {
    let wal = FakeWal::new(PS, salts(10));
    let bytes = wal.file_bytes();
    let head = *reader(bytes.clone()).header();
    let cursor = reader(bytes).cursor_at_start();
    let last = LastPage::new(pg(1), lsr_ltx::Checksum::of_page(pg(1), &[0; 512]));
    let tip = TipView::Readable {
        txid: txid(3),
        origin: WalOrigin::new(32, 0, head.salts().salt1, head.salts().salt2).unwrap(),
        commit: nz(1),
        page_size: PS,
        timestamp: ts(5),
        last_page: LastPageCheck::Match {
            last_page: last,
            cursor,
        },
    };
    let Recovered::Continue(next) = recover(Some(Tip::Boot(&tip)), &view(&wal), None) else {
        panic!("continue");
    };
    assert_eq!(next.cursor().next().get(), 32);
    assert_eq!(next.last_page(), last);
}

#[test]
fn go_parity_an_end_one_frame_past_32_is_checked_not_skipped() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1], 1);
    let pos = position(&wal, 1, Continuity::Unpinned);
    assert_eq!(pos.cursor().next().frame_index(), 1);
    assert!(matches!(
        run_running(&wal, &pos, None).recovered,
        Recovered::Continue(_)
    ));
    let wrong = Position::new(
        pos.txid(),
        pos.cursor(),
        pos.commit(),
        LastPage::new(pos.last_page().pgno(), lsr_ltx::Checksum::EMPTY),
        pos.timestamp(),
        Continuity::Unpinned,
    );
    assert_eq!(
        run_running(&wal, &wrong, None).recovered,
        snapshot(Some(1), SnapshotCause::LastPageMismatch)
    );
}

// The loader.
fn encode_tip(wal: &FakeWal, id: u64, page_bytes: &[(u32, u8)], commit_size: u32) -> Vec<u8> {
    let (cursor, _frame, _size) = end_of(wal.file_bytes()).unwrap();
    let head = *reader(wal.file_bytes()).header();
    let origin = WalOrigin::new(
        32,
        cursor.next().get().saturating_sub(32),
        head.salts().salt1,
        head.salts().salt2,
    )
    .unwrap();
    let fields = crate::sync::header_fields(txid(id), nz(commit_size), PS, origin, ts(7), 0);
    let mut enc = lsr_ltx::Encoder::new(Vec::new(), lsr_ltx::Header::new(fields).unwrap()).unwrap();
    for (n, fill) in page_bytes {
        enc.write_page(pg(*n), &vec![*fill; PS.bytes()]).unwrap();
    }
    enc.finish(None).unwrap().0
}

async fn put(store: &crate::MemStore, id: u64, bytes: &[u8]) {
    use crate::{ObjectStore, Upload};
    let key = FileKey::new(Level::ZERO, lsr_ltx::TxidRange::single(txid(id)));
    let mut up = store.begin_upload(&key, ts(7)).await.unwrap();
    up.write(bytes).await.unwrap();
    up.commit().await.unwrap();
}

#[test]
fn the_loader_reads_the_newest_file_and_matches_the_last_page() {
    let wal = fresh();
    let store = crate::MemStore::new();
    let good = encode_tip(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    crate::conformance::block_on(async {
        put(&store, 1, &good).await;
        put(&store, 2, &good).await;
        let mut r = reader(wal.file_bytes());
        let tip = TipView::load(&store, Some(&mut r)).await.unwrap().unwrap();
        assert_eq!(tip.txid(), txid(2));
        assert!(matches!(
            tip,
            TipView::Readable {
                last_page: LastPageCheck::Match { .. },
                ..
            }
        ));
    });
}

#[test]
fn the_loader_calls_a_page_with_other_bytes_a_mismatch_and_a_missing_one_not_in_tip() {
    let wal = fresh();
    let store = crate::MemStore::new();
    let other = encode_tip(&wal, 1, &[(1, 1), (2, 2), (3, 9)], 3);
    let lacking = encode_tip(&wal, 2, &[(1, 1), (2, 2)], 3);
    crate::conformance::block_on(async {
        put(&store, 1, &other).await;
        let mut r = reader(wal.file_bytes());
        let tip = TipView::load(&store, Some(&mut r)).await.unwrap().unwrap();
        assert!(matches!(
            tip,
            TipView::Readable {
                last_page: LastPageCheck::Mismatch,
                ..
            }
        ));
        put(&store, 2, &lacking).await;
        let mut r = reader(wal.file_bytes());
        let tip = TipView::load(&store, Some(&mut r)).await.unwrap().unwrap();
        assert!(matches!(
            tip,
            TipView::Readable {
                last_page: LastPageCheck::NotInTip,
                ..
            }
        ));
    });
}

#[test]
fn the_loader_calls_garbage_unreadable_and_an_empty_directory_none() {
    let wal = fresh();
    let store = crate::MemStore::new();
    crate::conformance::block_on(async {
        let mut r = reader(wal.file_bytes());
        assert_eq!(TipView::load(&store, Some(&mut r)).await.unwrap(), None);
        put(&store, 3, b"not an ltx file").await;
        let mut r = reader(wal.file_bytes());
        assert_eq!(
            TipView::load(&store, Some(&mut r)).await.unwrap(),
            Some(TipView::Unreadable { txid: txid(3) })
        );
    });
}

// Generated triples.
#[derive(Clone, Debug)]
enum Op {
    Commit(Vec<u32>, u32),
    ForceRestart,
    Truncate(usize),
    Corrupt(usize),
    Remove,
    NewEpoch,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (prop::collection::vec(1_u32..6, 1..4), 6_u32..9).prop_map(|(p, c)| Op::Commit(p, c)),
        2 => Just(Op::ForceRestart),
        2 => (0_usize..2000).prop_map(Op::Truncate),
        2 => (0_usize..2000).prop_map(Op::Corrupt),
        1 => Just(Op::Remove),
        2 => Just(Op::NewEpoch),
    ]
}

fn apply(wal: &FakeWal, op: &Op) {
    match op {
        Op::Commit(pages, size) => commit(wal, pages, *size),
        Op::ForceRestart => wal.force_restart(),
        Op::Truncate(n) => {
            let len = wal.file_bytes().len();
            wal.truncate_file(len.saturating_sub(*n));
        }
        Op::Corrupt(at) => wal.corrupt_byte(*at),
        Op::Remove => wal.remove_file(),
        Op::NewEpoch => wal.set_epoch(wal.epoch().next()),
    }
}

#[derive(Clone, Copy, Debug)]
enum TipKind {
    Boot,
    BootUnreadable,
    BootMismatch,
    BootNotInTip,
    Pinned,
    Unpinned,
    NoTip,
}

#[derive(Debug)]
struct Triple {
    before: Vec<Op>,
    kind: TipKind,
    after: Vec<Op>,
    remote: Option<i8>,
    other_page_size: bool,
}

fn triple() -> impl Strategy<Value = Triple> {
    (
        prop::collection::vec(op(), 1..4),
        prop_oneof![
            Just(TipKind::Boot),
            Just(TipKind::BootUnreadable),
            Just(TipKind::BootMismatch),
            Just(TipKind::BootNotInTip),
            Just(TipKind::Pinned),
            Just(TipKind::Unpinned),
            Just(TipKind::NoTip),
        ],
        prop::collection::vec(op(), 0..3),
        prop::option::of(-2_i8..3),
        prop::bool::weighted(0.1),
    )
        .prop_map(|(before, kind, after, remote, other_page_size)| Triple {
            before,
            kind,
            after,
            remote,
            other_page_size,
        })
}

/// What a triple produced, by variant and cause only.
fn shape(recovered: &Recovered) -> String {
    match recovered {
        Recovered::Continue(_) => "Continue".into(),
        Recovered::WalRestarted(_) => "WalRestarted".into(),
        Recovered::Halt(cause) => format!("Halt({cause:?})"),
        Recovered::Snapshot { cause, .. } => match cause {
            SnapshotCause::BehindRemote { .. } => "BehindRemote".into(),
            SnapshotCause::LocalRewound { .. } => "LocalRewound".into(),
            SnapshotCause::FirstSync
            | SnapshotCause::LocalTipUnreadable
            | SnapshotCause::WalMissing
            | SnapshotCause::PageSizeChanged
            | SnapshotCause::GenerationChanged
            | SnapshotCause::PrevFrameMismatch
            | SnapshotCause::LastPageMismatch
            | SnapshotCause::CheckpointBoundary
            | SnapshotCause::OperatorRequested => format!("{cause:?}"),
        },
    }
}

fn other_size() -> PageSize {
    PageSize::new(1024).unwrap()
}

/// The tip a restarted process would load for `pos`, as `kind` says it reads.
fn boot_view(kind: TipKind, pos: &Position) -> TipView {
    let head = salts(10);
    let end = pos.cursor().next().get();
    let last_page = match kind {
        TipKind::BootMismatch => LastPageCheck::Mismatch,
        TipKind::BootNotInTip => LastPageCheck::NotInTip,
        TipKind::Boot
        | TipKind::BootUnreadable
        | TipKind::Pinned
        | TipKind::Unpinned
        | TipKind::NoTip => LastPageCheck::Match {
            last_page: pos.last_page(),
            cursor: pos.cursor(),
        },
    };
    match kind {
        TipKind::BootUnreadable => TipView::Unreadable { txid: pos.txid() },
        TipKind::Boot
        | TipKind::BootMismatch
        | TipKind::BootNotInTip
        | TipKind::Pinned
        | TipKind::Unpinned
        | TipKind::NoTip => TipView::Readable {
            txid: pos.txid(),
            origin: WalOrigin::new(32, end.saturating_sub(32), head.salt1, head.salt2).unwrap(),
            commit: pos.commit(),
            page_size: PS,
            timestamp: ts(1),
            last_page,
        },
    }
}

fn evaluate(triple: &Triple) -> Option<Recovered> {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    for op in &triple.before {
        apply(&wal, op);
    }
    let held = end_of(wal.file_bytes());
    let id = 5_u64;
    let epoch = wal.epoch();
    let held_pos = held.map(|(cursor, frame, size)| {
        let continuity = match triple.kind {
            TipKind::Pinned => Continuity::Pinned(epoch),
            TipKind::Boot
            | TipKind::BootUnreadable
            | TipKind::BootMismatch
            | TipKind::BootNotInTip
            | TipKind::Unpinned
            | TipKind::NoTip => Continuity::Unpinned,
        };
        Position::new(
            txid(id),
            cursor,
            nz(size.max(1)),
            LastPage::of_frame(&frame),
            ts(1),
            continuity,
        )
    });
    for op in &triple.after {
        apply(&wal, op);
    }
    let mut seen = view(&wal);
    if triple.other_page_size {
        seen = view(&FakeWal::new(other_size(), salts(10)));
    }
    let remote = triple
        .remote
        .and_then(|d| u64::try_from(i64::from(d).saturating_add(5)).ok())
        .and_then(Txid::new);
    let tip_view = held_pos.as_ref().map(|pos| boot_view(triple.kind, pos));
    let tip = match (triple.kind, held_pos.as_ref(), tip_view.as_ref()) {
        (TipKind::NoTip, ..) | (_, None, _) => None,
        (TipKind::Pinned | TipKind::Unpinned, Some(pos), _) => {
            let probes = WalReader::open(Cursor::new(wal.file_bytes()))
                .ok()
                .and_then(|mut r| Probes::read(&mut r, pos).ok())?;
            Some(Tip::Running(pos, probes))
        }
        (_, Some(_), Some(view)) => Some(Tip::Boot(view)),
        (_, Some(_), None) => None,
    };
    Some(recover(tip, &seen, remote))
}

#[test]
fn generated_triples_cover_every_outcome_recover_can_produce() {
    use proptest::test_runner::{Config, TestRunner};
    let mut runner = TestRunner::new_with_rng(
        Config::with_cases(4000),
        proptest::test_runner::TestRng::deterministic_rng(
            proptest::test_runner::RngAlgorithm::ChaCha,
        ),
    );
    let seen = std::cell::RefCell::new(std::collections::BTreeSet::new());
    runner
        .run(&triple(), |t| {
            if let Some(recovered) = evaluate(&t) {
                seen.borrow_mut().insert(shape(&recovered));
            }
            Ok(())
        })
        .unwrap();
    let seen = seen.into_inner();
    for want in [
        "Continue",
        "WalRestarted",
        "FirstSync",
        "BehindRemote",
        "LocalTipUnreadable",
        "WalMissing",
        "PageSizeChanged",
        "GenerationChanged",
        "PrevFrameMismatch",
        "LastPageMismatch",
        "LocalRewound",
        "Halt(ForeignWriter)",
    ] {
        assert!(seen.contains(want), "never produced {want}: {seen:?}");
    }
}

proptest! {
    #[test]
    fn recover_never_panics_and_always_answers(t in triple()) {
        let _ = evaluate(&t);
    }
}
