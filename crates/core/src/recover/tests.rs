use std::io::Cursor;
use std::num::NonZeroU32;

use lsr_ltx::{PageNo, PageSize, Timestamp, Txid, WalOrigin};
use lsr_wal::{FrameCursor, FrameRef, Salts, ScanLimit, TxOutcome, WalReader};
use proptest::prelude::*;

use super::*;
use crate::fault::role::Staging;
use crate::recover::BootWal;
use crate::source::{CommittedRead, FakeWal};
use crate::store::WriteMode;

/// The decision alone: these tests judge verdicts, and the events have tests of their own.
fn recover(
    source: Option<PositionSource<'_>>,
    wal: &crate::recover::WalView,
    remote: Option<lsr_ltx::Txid>,
) -> Recovered {
    crate::recover::recover_traced(source, wal, remote).recovered
}

const PS: PageSize = PageSize::MIN;

pub(super) fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

pub(super) fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

pub(super) fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

pub(super) fn salts(n: u32) -> Salts {
    Salts {
        salt1: n,
        salt2: n.wrapping_mul(7).wrapping_add(1),
    }
}

pub(super) fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

pub(super) fn commit(wal: &FakeWal, pages: &[u32], size: u32) {
    let pages: Vec<(PageNo, Vec<u8>)> = pages
        .iter()
        .map(|n| (pg(*n), vec![u8::try_from(*n).unwrap(); PS.bytes()]))
        .collect();
    wal.append_tx(&pages, nz(size));
}

pub(super) fn reader(bytes: Vec<u8>) -> WalReader<Cursor<Vec<u8>>> {
    WalReader::open(Cursor::new(bytes)).unwrap()
}

/// The end of the last commit in `bytes`: its cursor, commit frame and database size.
fn end_of(bytes: Vec<u8>) -> Option<(FrameCursor, FrameRef, u32)> {
    let mut wal = WalReader::open(Cursor::new(bytes)).ok()?;
    let mut at = wal.cursor_at_start();
    let mut last = None;
    while let Ok(TxOutcome::Tx(tx)) = wal.next_tx(&at, ScanLimit::FirstInvalidFrame) {
        at = *tx.end();
        last = Some((at, *tx.frames().last()?, tx.commit().get()));
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

fn boot(wal: &FakeWal, id: u64) -> NewestStagedFile {
    let bytes = wal.file_bytes();
    let (cursor, frame, size) = end_of(bytes.clone()).unwrap();
    let head = *reader(bytes).header();
    let end = cursor.next().get();
    let salts = head.salts();
    NewestStagedFile::Readable {
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

pub(super) fn view(wal: &FakeWal) -> WalView {
    let bytes = wal.file_bytes();
    WalView {
        header: WalReader::open(Cursor::new(bytes.clone()))
            .ok()
            .map(|r| *r.header()),
        len: Some(WalFileLen::new(u64::try_from(bytes.len()).unwrap())),
        committed: CommittedRead::new(wal.committed_end(), wal.backfilled(), wal.epoch()),
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

pub(super) fn fresh() -> FakeWal {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    commit(&wal, &[2, 3], 3);
    wal
}

fn run_running(wal: &FakeWal, pos: &Position, remote: Option<u64>) -> Verdict {
    recover_traced(
        Some(PositionSource::Running(pos, probes(wal, pos))),
        &view(wal),
        remote.map(txid),
    )
}

// Rule 1.
#[test]
fn rule_1_no_newest_staged_file_is_a_first_sync() {
    let wal = fresh();
    assert_eq!(
        recover(None, &view(&wal), None),
        snapshot(None, SnapshotCause::FirstSync)
    );
}

#[test]
fn rule_1_no_newest_staged_file_follows_the_remote() {
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
    let newest_staged = boot(&wal, 2);
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            Some(txid(5))
        ),
        snapshot(Some(5), SnapshotCause::BehindRemote { local: txid(2) })
    );
}

#[test]
fn rule_2_a_remote_below_the_newest_staged_file_is_pending_uploads() {
    let wal = fresh();
    let newest_staged = boot(&wal, 5);
    assert!(matches!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            Some(txid(3))
        ),
        Recovered::Continue(_)
    ));
}

#[test]
fn rule_2_running_behind_the_remote_stops() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    assert_eq!(
        run_running(&wal, &pos, Some(3)).recovered,
        Recovered::Stop(StopCause::OtherWriter)
    );
}

// Rule 3.
#[test]
fn rule_3_an_unreadable_newest_staged_file_snapshots_after_the_newer_of_newest_staged_file_and_remote()
 {
    let wal = fresh();
    let newest_staged = NewestStagedFile::Unreadable { txid: txid(4) };
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            Some(txid(2))
        ),
        snapshot(Some(4), SnapshotCause::NewestStagedFileUnreadable)
    );
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            None
        ),
        snapshot(Some(4), SnapshotCause::NewestStagedFileUnreadable)
    );
}

// Rule 4.
#[test]
fn rule_4_no_wal_file_is_wal_missing() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    let mut gone = view(&wal);
    gone.header = None;
    gone.len = None;
    assert_eq!(
        recover(
            Some(PositionSource::Running(&pos, probes(&wal, &pos))),
            &gone,
            None
        ),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
    let newest_staged = boot(&wal, 2);
    assert_eq!(
        recover(Some(PositionSource::Boot(&newest_staged)), &gone, None),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
}

#[test]
fn rule_4_a_torn_header_is_wal_missing() {
    let wal = fresh();
    let newest_staged = boot(&wal, 2);
    wal.corrupt_byte(9);
    assert_eq!(view(&wal).header, None);
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            None
        ),
        snapshot(Some(2), SnapshotCause::WalMissing)
    );
}

// Rule 5.
#[test]
fn rule_5_a_new_page_size_stops() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    let other = FakeWal::new(PageSize::new(1024).unwrap(), salts(10));
    assert_eq!(
        recover(
            Some(PositionSource::Running(&pos, probes(&wal, &pos))),
            &view(&other),
            None
        ),
        Recovered::Stop(StopCause::PageSizeChanged {
            was: PageSize::MIN,
            now: PageSize::new(1024).unwrap(),
        })
    );
}

// Rule 6: a restart the read-transaction continuity proof can follow.
fn restarted(wal: &FakeWal) {
    wal.force_restart();
    commit(wal, &[1], 3);
}

#[test]
fn a_pinned_position_follows_a_restart_it_can_prove() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
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
fn an_unpinned_position_snapshots_on_a_restart() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    restarted(&wal);
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn a_read_lock_from_another_epoch_proves_nothing() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    restarted(&wal);
    wal.set_epoch(wal.epoch().next());
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn two_restarts_are_not_one() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    restarted(&wal);
    restarted(&wal);
    assert_eq!(
        run_running(&wal, &pos, None).recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn the_abandoned_frames_check_catches_frames_abandoned_after_the_read_lock() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    // Frames appended after the read lock, then a restart that rewrites the header in
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
        Some(RecoverEvent::ReadLockViolated {
            synced: pos.synced().offset()
        })
    );
}

/// The same abandoned commit, with the restarted generation grown to exactly
/// the synced end: the abandoned frame starts right where the position ends.
#[test]
fn an_abandoned_commit_at_the_synced_end_of_a_restart_is_caught() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    commit(&wal, &[1], 3);
    wal.force_restart();
    commit(&wal, &[1, 2], 2);
    commit(&wal, &[2, 3], 3);
    let verdict = run_running(&wal, &pos, None);
    assert_eq!(
        verdict.recovered,
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
    assert_eq!(
        verdict.event,
        Some(RecoverEvent::ReadLockViolated {
            synced: pos.synced().offset()
        })
    );
}

#[test]
fn a_restart_that_has_grown_past_the_old_end_is_followed() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
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
    let newest_staged = boot(&wal, 2);
    let other = FakeWal::new(PS, salts(99));
    commit(&other, &[1], 1);
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&other),
            None
        ),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

#[test]
fn go_parity_a_salt_reset_at_boot_snapshots() {
    let wal = fresh();
    let newest_staged = boot(&wal, 2);
    restarted(&wal);
    assert_eq!(
        recover(
            Some(PositionSource::Boot(&newest_staged)),
            &view(&wal),
            None
        ),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

// Rule 7: a committed end before the newest staged file's end is a rewind.
#[test]
fn a_committed_end_before_the_newest_staged_file_is_a_rewind() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    let mut seen = view(&wal);
    let end = lsr_wal::CommittedEnd::new(1, wal.salts(), PS);
    seen.committed = CommittedRead::new(end, 0, wal.epoch());
    let verdict = recover_traced(
        Some(PositionSource::Running(&pos, probes(&wal, &pos))),
        &seen,
        None,
    );
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
    let newest_staged = boot(&wal, 2);
    let bytes = wal.file_bytes();
    wal.truncate_file(bytes.len() - 100);
    let verdict = recover_traced(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        None,
    );
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
    let pos = position(&wal, 2, Continuity::Broken);
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
    let pos = position(&wal, 2, Continuity::Broken);
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
    let pos = position(&wal, 2, Continuity::Broken);
    let other = LastPage::new(pg(9), pos.last_page().digest());
    let moved = Position::new(
        pos.txid(),
        pos.cursor(),
        pos.commit(),
        other,
        pos.timestamp(),
        Continuity::Broken,
    );
    assert_eq!(
        run_running(&wal, &moved, None).recovered,
        snapshot(Some(2), SnapshotCause::LastPageMismatch)
    );
}

#[test]
fn rule_9_at_boot_a_mismatch_or_a_page_the_newest_staged_file_lacks_snapshots() {
    let wal = fresh();
    let NewestStagedFile::Readable {
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
    for last_page in [
        LastPageCheck::Mismatch,
        LastPageCheck::NotInNewestStagedFile,
    ] {
        let newest_staged = NewestStagedFile::Readable {
            txid,
            origin,
            commit,
            page_size,
            timestamp,
            last_page,
        };
        assert_eq!(
            recover(
                Some(PositionSource::Boot(&newest_staged)),
                &view(&wal),
                None
            ),
            snapshot(Some(2), SnapshotCause::LastPageMismatch)
        );
    }
}

/// With the newest staged file's end at the WAL's start there is no frame before it, so the
/// miss a boot read reports names nothing rules 7 and 8 could judge: a boot newest staged file
/// that did not match is rule 9 whatever the miss.
#[test]
fn rule_9_at_boot_a_newest_staged_file_ending_at_the_wal_start_has_no_frame_to_miss() {
    let wal = fresh();
    let NewestStagedFile::Readable {
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
    let (salt1, salt2) = origin.salts();
    let origin = WalOrigin::new(32, 0, salt1, salt2).unwrap();
    for miss in [Miss::Truncated, Miss::NotACommit, Miss::Moved] {
        let newest_staged = NewestStagedFile::Readable {
            txid,
            origin,
            commit,
            page_size,
            timestamp,
            last_page: LastPageCheck::NoFrame(miss),
        };
        assert_eq!(
            recover(
                Some(PositionSource::Boot(&newest_staged)),
                &view(&wal),
                None
            ),
            snapshot(Some(2), SnapshotCause::LastPageMismatch),
            "{miss:?}"
        );
    }
}

// Rule 9a (#26): the wal-index reset ahead of the header.
fn reset_view(wal: &FakeWal) -> WalView {
    let mut seen = view(wal);
    let next = Salts {
        salt1: wal.salts().salt1.wrapping_add(1),
        salt2: 77,
    };
    seen.committed = CommittedRead::new(lsr_wal::CommittedEnd::new(0, next, PS), 0, wal.epoch());
    seen
}

fn under_reset(wal: &FakeWal, pos: &Position) -> Verdict {
    let seen = reset_view(wal);
    let index = seen.committed.unwrap();
    let probes = Probes::read_against(&mut reader(wal.file_bytes()), pos, &index).unwrap();
    recover_traced(Some(PositionSource::Running(pos, probes)), &seen, None)
}

#[test]
fn rule_9a_a_reset_index_with_nothing_after_the_position_continues_as_it_was() {
    let wal = fresh();
    for continuity in [Continuity::Held(wal.epoch()), Continuity::Broken] {
        let pos = position(&wal, 2, continuity);
        assert_eq!(under_reset(&wal, &pos), plain(Recovered::Continue(pos)));
    }
}

#[test]
fn rule_9a_a_commit_the_position_never_covered_under_a_reset_index_is_a_snapshot() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    let pos = position(&wal, 1, Continuity::Broken);
    commit(&wal, &[2, 3], 3);
    assert_eq!(
        under_reset(&wal, &pos),
        plain(snapshot(Some(1), SnapshotCause::GenerationChanged))
    );
}

#[test]
fn rule_9a_a_pinned_position_with_a_commit_after_it_under_a_reset_index_is_an_abandoned_frame() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    let pos = position(&wal, 1, Continuity::Held(wal.epoch()));
    commit(&wal, &[2, 3], 3);
    let verdict = under_reset(&wal, &pos);
    assert_eq!(
        verdict.recovered,
        snapshot(Some(1), SnapshotCause::GenerationChanged)
    );
    assert_eq!(
        verdict.event,
        Some(RecoverEvent::ReadLockViolated {
            synced: pos.synced().offset()
        })
    );
}

#[test]
fn rule_9a_a_rolled_back_spill_after_the_position_is_no_commit() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    let pos = position(&wal, 1, Continuity::Broken);
    wal.append_uncommitted(&[(pg(3), vec![3; PS.bytes()])]);
    assert_eq!(under_reset(&wal, &pos), plain(Recovered::Continue(pos)));
}

// Rule 10.
#[test]
fn rule_10_a_running_position_continues_as_it_is() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    assert_eq!(
        run_running(&wal, &pos, Some(2)).recovered,
        Recovered::Continue(pos)
    );
}

#[test]
fn a_boot_continue_at_the_committed_end_is_pinned() {
    let wal = fresh();
    let newest_staged = boot(&wal, 2);
    let Recovered::Continue(next) = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        None,
    ) else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Held(wal.epoch()));
    assert_eq!(next.cursor().next(), wal.committed_end().offset());
}

#[test]
fn a_boot_continue_below_the_committed_end_is_unpinned() {
    let wal = fresh();
    let newest_staged = boot(&wal, 2);
    commit(&wal, &[3], 3);
    let Recovered::Continue(next) = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        None,
    ) else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Broken);
}

#[test]
fn a_pinned_position_from_an_older_epoch_at_the_committed_end_is_repinned() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    wal.set_epoch(wal.epoch().next());
    let Recovered::Continue(next) = run_running(&wal, &pos, None).recovered else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Held(wal.epoch()));
}

#[test]
fn a_pinned_position_below_the_committed_end_stays_pinned() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    commit(&wal, &[3], 3);
    let Recovered::Continue(next) = run_running(&wal, &pos, None).recovered else {
        panic!("continue");
    };
    assert_eq!(next.continuity(), Continuity::Held(wal.epoch()));
}

#[test]
fn go_parity_an_end_at_offset_32_continues() {
    let wal = FakeWal::new(PS, salts(10));
    let bytes = wal.file_bytes();
    let head = *reader(bytes.clone()).header();
    let cursor = reader(bytes).cursor_at_start();
    let last = LastPage::new(pg(1), lsr_ltx::PageChecksum::of(pg(1), &[0; 512]));
    let newest_staged = NewestStagedFile::Readable {
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
    let Recovered::Continue(next) = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        None,
    ) else {
        panic!("continue");
    };
    assert_eq!(next.cursor().next().get(), 32);
    assert_eq!(next.last_page(), last);
}

#[test]
fn go_parity_an_end_one_frame_past_32_is_checked_not_skipped() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1], 1);
    let pos = position(&wal, 1, Continuity::Broken);
    assert_eq!(pos.cursor().next().frame_index(), 1);
    assert!(matches!(
        run_running(&wal, &pos, None).recovered,
        Recovered::Continue(_)
    ));
    let wrong = Position::new(
        pos.txid(),
        pos.cursor(),
        pos.commit(),
        LastPage::new(
            pos.last_page().pgno(),
            lsr_ltx::PageChecksum::of(pos.last_page().pgno(), &[]),
        ),
        pos.timestamp(),
        Continuity::Broken,
    );
    assert_eq!(
        run_running(&wal, &wrong, None).recovered,
        snapshot(Some(1), SnapshotCause::LastPageMismatch)
    );
}

// The loader.
pub(super) fn encode_newest_staged(
    wal: &FakeWal,
    id: u64,
    page_bytes: &[(u32, u8)],
    commit_size: u32,
) -> Vec<u8> {
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

pub(super) async fn put(store: &crate::MemStore, id: u64, bytes: &[u8]) {
    use crate::{ObjectStore, Upload};
    let key = FileKey::new(Level::ZERO, lsr_ltx::TxidRange::single(txid(id)));
    let mut up = store
        .begin_upload(&key, ts(7), WriteMode::Replace)
        .await
        .unwrap();
    up.write(bytes).await.unwrap();
    up.commit().await.unwrap();
}

#[test]
fn the_loader_reads_the_newest_file_and_matches_the_last_page() {
    let wal = fresh();
    let store = crate::MemStore::new();
    let good = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    crate::conformance::block_on(async {
        put(&store, 1, &good).await;
        put(&store, 2, &good).await;
        let mut r = reader(wal.file_bytes());
        let newest_staged = NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(newest_staged.txid(), txid(2));
        assert!(matches!(
            newest_staged,
            NewestStagedFile::Readable {
                last_page: LastPageCheck::Match { .. },
                ..
            }
        ));
    });
}

#[test]
fn the_loader_calls_a_page_with_other_bytes_a_mismatch_and_a_missing_one_not_in_newest_staged_file()
{
    let wal = fresh();
    let store = crate::MemStore::new();
    let other = encode_newest_staged(&wal, 1, &[(1, 1), (2, 2), (3, 9)], 3);
    let lacking = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2)], 3);
    crate::conformance::block_on(async {
        put(&store, 1, &other).await;
        let mut r = reader(wal.file_bytes());
        let newest_staged = NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            newest_staged,
            NewestStagedFile::Readable {
                last_page: LastPageCheck::Mismatch,
                ..
            }
        ));
        put(&store, 2, &lacking).await;
        let mut r = reader(wal.file_bytes());
        let newest_staged = NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            newest_staged,
            NewestStagedFile::Readable {
                last_page: LastPageCheck::NotInNewestStagedFile,
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
        assert_eq!(
            NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
                .await
                .unwrap(),
            None
        );
        put(&store, 3, b"not an ltx file").await;
        let mut r = reader(wal.file_bytes());
        assert_eq!(
            NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
                .await
                .unwrap(),
            Some(NewestStagedFile::Unreadable { txid: txid(3) })
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
enum NewestStagedFileKind {
    Boot,
    BootUnreadable,
    BootMismatch,
    BootNotInNewestStagedFile,
    Held,
    Broken,
    NoNewestStagedFile,
}

#[derive(Debug)]
struct Triple {
    before: Vec<Op>,
    kind: NewestStagedFileKind,
    after: Vec<Op>,
    remote: Option<i8>,
    other_page_size: bool,
}

fn triple() -> impl Strategy<Value = Triple> {
    (
        prop::collection::vec(op(), 1..4),
        prop_oneof![
            Just(NewestStagedFileKind::Boot),
            Just(NewestStagedFileKind::BootUnreadable),
            Just(NewestStagedFileKind::BootMismatch),
            Just(NewestStagedFileKind::BootNotInNewestStagedFile),
            Just(NewestStagedFileKind::Held),
            Just(NewestStagedFileKind::Broken),
            Just(NewestStagedFileKind::NoNewestStagedFile),
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
        Recovered::Stop(StopCause::PageSizeChanged { .. }) => "Stop(PageSizeChanged)".into(),
        Recovered::Stop(cause) => format!("Stop({cause:?})"),
        Recovered::Snapshot { cause, .. } => match cause {
            SnapshotCause::BehindRemote { .. } => "BehindRemote".into(),
            SnapshotCause::LocalRewound { .. } => "LocalRewound".into(),
            SnapshotCause::StagedUnverified { .. } => "StagedUnverified".into(),
            SnapshotCause::AheadOfReplica { .. } => "AheadOfReplica".into(),
            SnapshotCause::FirstSync
            | SnapshotCause::NewestStagedFileUnreadable
            | SnapshotCause::WalMissing
            | SnapshotCause::GenerationChanged
            | SnapshotCause::PrevFrameMismatch
            | SnapshotCause::LastPageMismatch
            | SnapshotCause::OperatorRequested => format!("{cause:?}"),
        },
    }
}

fn other_size() -> PageSize {
    PageSize::new(1024).unwrap()
}

/// The newest staged file a restarted process would load for `pos`, as `kind` says it reads.
fn boot_view(kind: NewestStagedFileKind, pos: &Position) -> NewestStagedFile {
    let head = salts(10);
    let end = pos.cursor().next().get();
    let last_page = match kind {
        NewestStagedFileKind::BootMismatch => LastPageCheck::Mismatch,
        NewestStagedFileKind::BootNotInNewestStagedFile => LastPageCheck::NotInNewestStagedFile,
        NewestStagedFileKind::Boot
        | NewestStagedFileKind::BootUnreadable
        | NewestStagedFileKind::Held
        | NewestStagedFileKind::Broken
        | NewestStagedFileKind::NoNewestStagedFile => LastPageCheck::Match {
            last_page: pos.last_page(),
            cursor: pos.cursor(),
        },
    };
    match kind {
        NewestStagedFileKind::BootUnreadable => NewestStagedFile::Unreadable { txid: pos.txid() },
        NewestStagedFileKind::Boot
        | NewestStagedFileKind::BootMismatch
        | NewestStagedFileKind::BootNotInNewestStagedFile
        | NewestStagedFileKind::Held
        | NewestStagedFileKind::Broken
        | NewestStagedFileKind::NoNewestStagedFile => NewestStagedFile::Readable {
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
            NewestStagedFileKind::Held => Continuity::Held(epoch),
            NewestStagedFileKind::Boot
            | NewestStagedFileKind::BootUnreadable
            | NewestStagedFileKind::BootMismatch
            | NewestStagedFileKind::BootNotInNewestStagedFile
            | NewestStagedFileKind::Broken
            | NewestStagedFileKind::NoNewestStagedFile => Continuity::Broken,
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
    let newest_staged = held_pos.as_ref().map(|pos| boot_view(triple.kind, pos));
    let source = match (triple.kind, held_pos.as_ref(), newest_staged.as_ref()) {
        (NewestStagedFileKind::NoNewestStagedFile, ..) | (_, None, _) => None,
        (NewestStagedFileKind::Held | NewestStagedFileKind::Broken, Some(pos), _) => {
            let probes = WalReader::open(Cursor::new(wal.file_bytes()))
                .ok()
                .and_then(|mut r| Probes::read(&mut r, pos).ok())?;
            Some(PositionSource::Running(pos, probes))
        }
        (_, Some(_), Some(view)) => Some(PositionSource::Boot(view)),
        (_, Some(_), None) => None,
    };
    Some(recover(source, &seen, remote))
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
        "NewestStagedFileUnreadable",
        "WalMissing",
        "Stop(PageSizeChanged)",
        "GenerationChanged",
        "PrevFrameMismatch",
        "LastPageMismatch",
        "LocalRewound",
        "Stop(OtherWriter)",
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

// The check that catches abandoned frames needs a whole committed transaction of the old
// generation; a rolled-back spill was never part of the database.
#[test]
fn the_abandoned_frames_check_ignores_a_rolled_back_spill_past_the_read_lock() {
    let wal = fresh();
    // An application transaction spills a frame past the committed end, then rolls back.
    wal.append_uncommitted(&[(pg(3), vec![9; PS.bytes()])]);
    wal.backfill(u32::MAX);
    // We re-read lock at the committed end (an idle step): the WAL is fully backfilled, so read mark 0.
    wal.set_epoch(wal.epoch().next());
    wal.hold_reader();
    assert_eq!(wal.read_mark(), Some(0));
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    // The next application write restarts the WAL once, as SQLite allows under read mark 0.
    wal.force_restart();
    commit(&wal, &[1], 3);
    let verdict = run_running(&wal, &pos, None);
    assert_eq!(
        verdict.event, None,
        "nothing committed was lost: {verdict:?}"
    );
    assert!(
        matches!(verdict.recovered, Recovered::WalRestarted(_)),
        "{verdict:?}"
    );
}

#[test]
fn the_abandoned_frames_check_still_fires_when_a_spill_follows_a_committed_transaction() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    commit(&wal, &[1], 3);
    wal.append_uncommitted(&[(pg(2), vec![9; PS.bytes()])]);
    wal.force_restart();
    let verdict = run_running(&wal, &pos, None);
    assert_eq!(
        verdict.event,
        Some(RecoverEvent::ReadLockViolated {
            synced: pos.synced().offset()
        })
    );
}

// Rule 6 does not look at checkpoint_seq: real SQLite writes the restarting connection's own
// count there, so only `salt1 + 1` in the shared wal-index header proves the restart.
#[test]
fn rule_6_a_restart_with_an_unrelated_checkpoint_seq_is_still_one_restart() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Held(wal.epoch()));
    let old = *reader(wal.file_bytes()).header();
    let next = Salts {
        salt1: old.salts().salt1.wrapping_add(1),
        salt2: 5,
    };
    // Another connection's own count: here the same as before, not old + 1.
    let bytes = FakeWal::new(PS, next).file_bytes();
    let header = *reader(bytes.clone()).header();
    assert_eq!(header.checkpoint_seq(), old.checkpoint_seq());
    let view = WalView {
        header: Some(header),
        len: Some(WalFileLen::new(u64::try_from(bytes.len()).unwrap())),
        committed: None,
        epoch: wal.epoch(),
    };
    let probes = Probes {
        prev: FrameCheck::Miss(Miss::NotACommit),
        stale_next: false,
        rechained: false,
    };
    assert!(matches!(
        recover(Some(PositionSource::Running(&pos, probes)), &view, None),
        Recovered::WalRestarted(_)
    ));
}

/// A running position at the first frame of its generation, pinned in this
/// hold (read mark 0 on a WAL with no frames, as after a crash and an empty-WAL
/// snapshot). The first commit writes a new header with fresh salts: SQLite
/// draws them at random when the checkpoint sequence is 0, so `salt1 + 1`
/// cannot prove it, but the generation it pinned held no commit to lose. The
/// running engine follows; it snapshotted again (seed 0xf4846). A position past
/// the first frame still needs `salt1 + 1`.
#[test]
fn rule_6_a_pinned_position_with_no_frames_follows_a_new_header_with_any_salts() {
    let old = fresh();
    let head = *reader(old.file_bytes()).header();
    let (_, frame, size) = end_of(old.file_bytes()).unwrap();
    let new = FakeWal::new(PS, salts(99));
    commit(&new, &[1], 3);
    let held = Continuity::Held(new.epoch());
    let empty = Position::new(
        txid(2),
        FrameCursor::at_start(&head),
        nz(size),
        LastPage::of_frame(&frame),
        ts(1000),
        held,
    );
    let probes = Probes {
        prev: FrameCheck::Miss(Miss::NotACommit),
        stale_next: false,
        rechained: false,
    };
    let verdict = recover_traced(
        Some(PositionSource::Running(&empty, probes)),
        &view(&new),
        None,
    );
    let Recovered::WalRestarted(next) = verdict.recovered else {
        panic!("{verdict:?}");
    };
    assert_eq!(verdict.event, None);
    assert_eq!(next.cursor().salts(), new.salts());
    assert_eq!(next.txid(), txid(2));
    let unpinned = empty.with_continuity(Continuity::Broken);
    assert_eq!(
        recover(
            Some(PositionSource::Running(&unpinned, probes)),
            &view(&new),
            None
        ),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
    let past_start = position(&old, 2, held);
    assert_eq!(
        recover(
            Some(PositionSource::Running(&past_start, probes)),
            &view(&new),
            None
        ),
        snapshot(Some(2), SnapshotCause::GenerationChanged)
    );
}

// Boot compares the whole newest staged file with the WAL range it covers, not only its last page.
/// Loads the newest staged file staged from `staged` (pages `newest_staged_file_pages`, database size `commit_size`)
/// against the WAL as it is now.
fn load_against(
    staged: &FakeWal,
    now: &FakeWal,
    newest_staged_file_pages: &[(u32, u8)],
    commit_size: u32,
) -> Recovered {
    let store = crate::MemStore::new();
    let bytes = encode_newest_staged(staged, 2, newest_staged_file_pages, commit_size);
    let newest_staged = crate::conformance::block_on(async {
        put(&store, 2, &bytes).await;
        let mut r = reader(now.file_bytes());
        NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap()
    });
    recover(Some(PositionSource::Boot(&newest_staged)), &view(now), None)
}

/// A WAL whose second transaction was dropped by power loss and regrown with the
/// same last page and a page 2 of other bytes.
pub(super) fn regrown() -> FakeWal {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    wal.append_tx(
        &[
            (pg(2), vec![0xEE; PS.bytes()]),
            (pg(3), vec![3; PS.bytes()]),
        ],
        nz(3),
    );
    wal
}

pub(super) fn is_rewound(recovered: Recovered) -> bool {
    matches!(
        recovered,
        Recovered::Snapshot {
            cause: SnapshotCause::LocalRewound { .. },
            ..
        }
    )
}

#[test]
fn a_rewound_tail_rewritten_with_the_same_last_page_is_not_continued() {
    let recovered = load_against(&fresh(), &regrown(), &[(1, 1), (2, 2), (3, 3)], 3);
    assert!(is_rewound(recovered), "{recovered:?}");
}

#[test]
fn a_range_regrown_with_another_commit_size_is_a_rewind() {
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[1, 2], 2);
    commit(&wal, &[2, 3], 5);
    let recovered = load_against(&fresh(), &wal, &[(1, 1), (2, 2), (3, 3)], 3);
    assert!(is_rewound(recovered), "{recovered:?}");
}

#[test]
fn a_newest_staged_file_lacking_a_page_the_range_holds_is_a_rewind() {
    let wal = fresh();
    let recovered = load_against(&wal, &wal, &[(2, 2), (3, 3)], 3);
    assert!(is_rewound(recovered), "{recovered:?}");
}

#[test]
fn a_newest_staged_file_with_a_page_from_the_database_file_still_continues() {
    // The grown-pages or snapshot step supplies page 1 from the database file.
    let wal = FakeWal::new(PS, salts(10));
    commit(&wal, &[2, 3], 3);
    let recovered = load_against(&wal, &wal, &[(1, 1), (2, 2), (3, 3)], 3);
    assert!(matches!(recovered, Recovered::Continue(_)), "{recovered:?}");
}

/// The original WAL (txs at frames 1-2 and 3-4) with its first two frames overwritten
/// by a shorter regrown transaction of the same generation: frames 3 and 4 stay as
/// residue past the new end, as after a power loss with synchronous=NORMAL.
fn residue_after_a_shorter_regrown_chain() -> (Vec<u8>, FakeWal) {
    let original = fresh();
    let regrown = FakeWal::new(PS, salts(10));
    regrown.append_tx(
        &[
            (pg(1), vec![1; PS.bytes()]),
            (pg(2), vec![0xEE; PS.bytes()]),
        ],
        nz(2),
    );
    let mut bytes = original.file_bytes();
    let new = regrown.file_bytes();
    bytes[..new.len()].copy_from_slice(&new);
    (bytes, regrown)
}

fn load_over_bytes(
    staged: &FakeWal,
    bytes: Vec<u8>,
    committed: Option<CommittedRead>,
) -> Recovered {
    let store = crate::MemStore::new();
    let newest_staged_file_bytes = encode_newest_staged(staged, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    let newest_staged = crate::conformance::block_on(async {
        put(&store, 2, &newest_staged_file_bytes).await;
        let mut r = reader(bytes.clone());
        NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap()
    });
    let seen = WalView {
        header: Some(*reader(bytes.clone()).header()),
        len: Some(WalFileLen::new(u64::try_from(bytes.len()).unwrap())),
        committed,
        epoch: staged.epoch(),
    };
    recover(Some(PositionSource::Boot(&newest_staged)), &seen, None)
}

#[test]
fn a_shorter_chain_regrown_in_front_of_residue_is_a_rewind_without_the_wal_index() {
    let (bytes, _) = residue_after_a_shorter_regrown_chain();
    let recovered = load_over_bytes(&fresh(), bytes, None);
    assert!(is_rewound(recovered), "{recovered:?}");
}

#[test]
fn the_same_schedule_with_the_wal_index_is_caught_by_rule_7() {
    let (bytes, regrown) = residue_after_a_shorter_regrown_chain();
    let committed = CommittedRead::new(
        regrown.committed_end(),
        regrown.backfilled(),
        regrown.epoch(),
    );
    let recovered = load_over_bytes(&fresh(), bytes, committed);
    assert!(is_rewound(recovered), "{recovered:?}");
}

#[test]
fn a_newest_staged_file_ending_at_frame_zero_continues_over_a_wal_that_has_since_grown() {
    // The range is empty (origin at the start of the WAL, size 0): nothing to compare.
    let wal = fresh();
    let head = *reader(wal.file_bytes()).header();
    let origin = WalOrigin::new(32, 0, head.salts().salt1, head.salts().salt2).unwrap();
    let fields = crate::sync::header_fields(txid(2), nz(1), PS, origin, ts(7), 0);
    let mut enc = lsr_ltx::Encoder::new(Vec::new(), lsr_ltx::Header::new(fields).unwrap()).unwrap();
    enc.write_page(pg(1), &vec![0x42; PS.bytes()]).unwrap();
    let bytes = enc.finish(None).unwrap().0;
    let store = crate::MemStore::new();
    let newest_staged = crate::conformance::block_on(async {
        put(&store, 2, &bytes).await;
        let mut r = reader(wal.file_bytes());
        NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap()
    });
    assert!(
        matches!(
            recover(
                Some(PositionSource::Boot(&newest_staged)),
                &view(&wal),
                None
            ),
            Recovered::Continue(_)
        ),
        "{newest_staged:?}"
    );
}

#[test]
fn a_range_missing_from_this_wal_does_not_veto() {
    let wal = fresh();
    let store = crate::MemStore::new();
    let bytes = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    let other = FakeWal::new(PS, salts(10));
    commit(&other, &[1, 2], 2);
    let newest_staged = crate::conformance::block_on(async {
        put(&store, 2, &bytes).await;
        let mut r = reader(other.file_bytes());
        NewestStagedFile::load(&store, Some(BootWal::recovered(&mut r)))
            .await
            .unwrap()
            .unwrap()
    });
    // The WAL ends before the newest staged file's end: rule 7 or the frame check speaks, not the range.
    assert!(
        !matches!(
            newest_staged,
            NewestStagedFile::Readable {
                last_page: LastPageCheck::Rewritten,
                ..
            }
        ),
        "{newest_staged:?}"
    );
}

#[test]
fn the_header_read_is_one_header_long() {
    assert_eq!(HEADER_READ.get(), u64::try_from(HEADER_LEN).unwrap());
    assert_eq!(NEWEST_STAGED_READ.get(), 1 << 20);
}

#[test]
fn a_staging_listing_whose_cursor_never_advances_is_a_fault_not_a_loop() {
    let staging = crate::stuck::StuckListing::new(crate::MemStore::new());
    let got = crate::conformance::block_on(newest_level_zero(&Staging(&staging)));
    assert_eq!(got, Err(Fault::LocalIo));
}

/// The frames under a running position were regrown byte for byte from a header written again
/// with the same salts and another seed: each frame verifies on its own and the last page
/// matches, but the frame before the end carries another running checksum than the position
/// scanned to it. The frames the position covers are not the ones it staged: a rewind, never a
/// continue that a later scan would meet as damage.
#[test]
fn frames_regrown_from_a_header_written_again_are_a_rewind() {
    let wal = fresh();
    let pos = position(&wal, 2, Continuity::Broken);
    let seq = wal.checkpoint_seq();
    wal.rewrite_header(seq.wrapping_add(5));
    commit(&wal, &[1, 2], 2);
    commit(&wal, &[2, 3], 3);
    assert_eq!(wal.salts(), pos.cursor().salts());
    let checked = probes(&wal, &pos);
    assert!(checked.rechained);
    assert!(matches!(checked.prev, FrameCheck::Frame(_)), "{checked:?}");
    let verdict = run_running(&wal, &pos, None);
    assert!(
        matches!(
            verdict.recovered,
            Recovered::Snapshot {
                cause: SnapshotCause::LocalRewound { .. },
                ..
            }
        ),
        "{verdict:?}"
    );
    assert!(matches!(
        verdict.event,
        Some(RecoverEvent::LocalRewound { .. })
    ));
}

/// A range past the live WAL's committed end is not proof either way, but its
/// generation names why: another generation's range is not in this WAL (the
/// seam the witness's checksum-chain rung would use), while this generation's
/// is residue past the commit, a rewrite.
#[test]
fn a_range_past_the_committed_end_is_named_by_its_generation() {
    let wal = FakeWal::new(PS, salts(1));
    commit(&wal, &[1], 1);
    commit(&wal, &[2], 2);
    let mut reader = reader(wal.file_bytes());
    let mut boot = BootWal::recovered(&mut reader);
    let start = lsr_wal::WalOffset::start(PS).get();
    let frame = 24 + u64::try_from(PS.bytes()).unwrap();
    let past = lsr_wal::WalOffset::new(start + 5 * frame, PS).unwrap();
    let other = wal_range(&mut boot, start, past, salts(9)).unwrap();
    assert!(matches!(other, Range::NotInThisWal), "another generation");
    let same = wal_range(&mut boot, start, past, salts(1)).unwrap();
    assert!(matches!(same, Range::Rewritten), "this generation");
}

#[test]
fn keep_newest_holds_the_newest_up_to_the_cap() {
    let key = |txid: u64| {
        FileKey::new(
            Level::ZERO,
            lsr_ltx::TxidRange::single(Txid::new(txid).unwrap()),
        )
    };
    let mut keys = VecDeque::new();
    for txid in 1..=3 {
        keep_newest(&mut keys, key(txid), 2);
    }
    assert_eq!(keys, VecDeque::from([key(2), key(3)]));
    let mut one = VecDeque::new();
    keep_newest(&mut one, key(1), 1);
    assert_eq!(one, VecDeque::from([key(1)]));
}
