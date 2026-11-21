use std::num::NonZeroU32;

use lsr_ltx::{PageNo, PageSize};
use lsr_wal::{Salts, ScanLimit, TxOutcome, WalReader};

use super::*;

fn ps() -> PageSize {
    PageSize::new(512).unwrap()
}

fn pg(n: u32) -> PageNo {
    PageNo::new(n, ps()).unwrap()
}

fn size(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn tx(fill: u8) -> Vec<(PageNo, Vec<u8>)> {
    vec![(pg(2), vec![fill; 512])]
}

fn wal() -> FakeWal {
    FakeWal::new(
        ps(),
        Salts {
            salt1: 10,
            salt2: 20,
        },
    )
}

fn committed(wal: &mut FakeWal) -> CommittedRead {
    wal.committed().unwrap()
}

#[test]
fn the_reader_accepts_what_the_fake_writes() {
    let mut fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.append_tx(&tx(2), size(2));
    let end = committed(&mut fake).end();
    let file = fake.open().unwrap().unwrap();
    let mut reader = WalReader::open(file).unwrap();
    let mut cursor = reader.cursor_at_start();
    let mut seen = 0;
    while let TxOutcome::Tx(found) = reader.next_tx(&cursor, ScanLimit::Committed(end)).unwrap() {
        cursor = *found.end();
        seen += 1;
    }
    assert_eq!(seen, 2);
    assert_eq!(cursor.next(), end.offset());
}

#[test]
fn fact_1_no_restart_under_a_read_mark_above_zero() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    let before = (fake.salts(), fake.checkpoint_seq());
    // Fully backfilled, but a reader holds mark 1: the writer cannot restart.
    fake.pin_reader_at(1);
    assert_eq!(fake.backfill(u32::MAX), 1);
    assert!(!fake.try_restart());
    for fill in 2..8 {
        fake.append_tx(&tx(fill), size(2));
    }
    assert_eq!((fake.salts(), fake.checkpoint_seq()), before);
    assert_eq!(fake.restarts(), 0);
    assert_eq!(fake.committed_end().frame_count(), 7);
}

#[test]
fn fact_1_holds_for_a_checkpoint_that_retakes_a_mark_above_zero() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.append_tx(&tx(2), size(2));
    fake.pin_reader_at(2);
    fake.backfill(1);
    fake.append_tx(&tx(3), size(2));
    assert_eq!(fake.restarts(), 0);
    assert_eq!(
        fake.salts(),
        Salts {
            salt1: 10,
            salt2: 20
        }
    );
}

#[test]
fn fact_2_under_mark_zero_the_next_write_restarts_once() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.backfill(u32::MAX);
    fake.pin_reader();
    assert_eq!(fake.read_mark(), Some(0));
    let old = fake.salts();
    fake.append_tx(&tx(2), size(2));
    assert_eq!(fake.restarts(), 1);
    assert_eq!(fake.salts().salt1, old.salt1.wrapping_add(1));
    assert_eq!(fake.checkpoint_seq(), 1);
    // The new generation is neither backfilled nor restarted while the pin holds.
    for fill in 3..9 {
        fake.append_tx(&tx(fill), size(2));
        assert_eq!(fake.backfill(u32::MAX), 0);
        assert!(!fake.try_restart());
    }
    assert_eq!(fake.restarts(), 1);
    assert_eq!(fake.committed_end().frame_count(), 7);
}

#[test]
fn fact_2_an_old_generation_that_grows_under_a_mark_zero_pin_is_never_restarted() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    // Not fully backfilled when the pin is taken on mark 0 by a reader of an older state.
    fake.pin_reader_at(0);
    let generation = fake.salts();
    for fill in 2..9 {
        fake.append_tx(&tx(fill), size(2));
        assert_eq!(fake.backfill(u32::MAX), 0, "the pin holds backfill at zero");
        assert!(!fake.try_restart());
    }
    assert_eq!(fake.salts(), generation);
    assert_eq!(fake.restarts(), 0);
    assert_eq!(fake.committed_end().frame_count(), 8);
}

#[test]
fn a_restart_leaves_the_old_frames_in_the_file_until_overwritten() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.append_tx(&tx(2), size(2));
    let len = fake.file_bytes().len();
    fake.force_restart();
    assert_eq!(fake.file_bytes().len(), len);
    assert_eq!(fake.committed_end().frame_count(), 0);
    fake.append_tx(&tx(3), size(2));
    assert_eq!(fake.file_bytes().len(), len);
}

#[test]
fn an_uncommitted_spill_is_in_the_file_below_no_committed_end() {
    let mut fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.append_uncommitted(&tx(2));
    assert_eq!(committed(&mut fake).end().frame_count(), 1);
    let frames = (fake.len().unwrap().unwrap().bytes() - 32) / (24 + 512);
    assert_eq!(frames, 2);
}

#[test]
fn a_truncating_checkpoint_empties_the_file_and_retakes_the_read_transaction() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    fake.pin_reader();
    let mut checkpointer = FakeCheckpointer::new(fake.clone());
    let before = fake.epoch();
    let run = checkpointer.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(!run.busy);
    assert_eq!(run.epoch, before.next());
    assert_eq!(fake.epoch(), run.epoch);
    assert!(fake.file_bytes().is_empty());
    assert_eq!(fake.restarts(), 1);
    assert_eq!(fake.read_mark(), Some(0));
    fake.append_tx(&tx(2), size(2));
    assert_eq!(fake.committed_end().frame_count(), 1);
}

#[test]
fn a_scripted_restart_changes_the_salts_by_one_and_the_sequence_by_one() {
    let fake = wal();
    fake.append_tx(&tx(1), size(2));
    let mut checkpointer = FakeCheckpointer::new(fake.clone());
    checkpointer.script(CheckpointScript {
        busy: false,
        effect: CheckpointEffect::Restart,
    });
    let run = checkpointer.checkpoint(CheckpointMode::Passive).unwrap();
    assert_eq!(run.checkpointed, 0);
    assert_eq!(fake.salts().salt1, 11);
    assert_eq!(fake.checkpoint_seq(), 1);
}

#[test]
fn the_checkpointer_records_calls_and_refuses_a_second_barrier() {
    let fake = wal();
    let mut checkpointer = FakeCheckpointer::new(fake);
    let barrier = checkpointer.begin_barrier().unwrap();
    assert_eq!(checkpointer.begin_barrier().unwrap_err(), SourceError::Busy);
    checkpointer.end_barrier(barrier).unwrap();
    let write = checkpointer.begin_write_lock().unwrap();
    checkpointer.end_write_lock(write).unwrap();
    checkpointer.bump_seq().unwrap();
    assert_eq!(
        checkpointer.calls(),
        [
            CheckpointCall::BeginBarrier,
            CheckpointCall::BeginBarrier,
            CheckpointCall::EndBarrier,
            CheckpointCall::BeginWriteLock,
            CheckpointCall::EndWriteLock,
            CheckpointCall::BumpSeq,
        ]
    );
}

#[test]
fn injected_errors_fire_once_in_the_named_operation() {
    let mut fake = wal();
    fake.inject(SourceOp::Committed, SourceError::Busy);
    assert_eq!(fake.committed().unwrap_err(), SourceError::Busy);
    fake.committed().unwrap();
    let mut checkpointer = FakeCheckpointer::new(fake);
    checkpointer.inject(SourceOp::Checkpoint, SourceError::ReadTxLost);
    assert_eq!(
        checkpointer
            .checkpoint(CheckpointMode::Passive)
            .unwrap_err(),
        SourceError::ReadTxLost
    );
    checkpointer.checkpoint(CheckpointMode::Passive).unwrap();
}

#[test]
fn a_removed_wal_has_no_file_and_no_length() {
    let mut fake = wal();
    fake.remove_file();
    assert!(fake.open().unwrap().is_none());
    assert!(fake.len().unwrap().is_none());
}

#[test]
fn a_scripted_committed_read_overrides_the_file() {
    let mut fake = wal();
    let odd =
        CommittedRead::new(CommittedEnd::new(5, fake.salts(), ps()), 2, fake.epoch()).unwrap();
    fake.script_committed(Some(odd));
    assert_eq!(committed(&mut fake), odd);
    fake.script_committed(None);
    assert_eq!(committed(&mut fake).end().frame_count(), 0);
}

#[test]
fn a_committed_read_never_backfills_past_its_frames() {
    let end = CommittedEnd::new(3, Salts { salt1: 1, salt2: 2 }, ps());
    let epoch = ReadEpoch::first();
    assert!(CommittedRead::new(end, 4, epoch).is_none());
    assert!(
        CommittedRead::new(end, 3, epoch)
            .unwrap()
            .fully_backfilled()
    );
    assert!(
        !CommittedRead::new(end, 2, epoch)
            .unwrap()
            .fully_backfilled()
    );
}

#[test]
fn the_fake_db_reads_pages_and_reports_short_ones() {
    let mut db = FakeDb::new(ps());
    db.write_page(pg(3), &[7; 512]);
    assert_eq!(db.page_count().unwrap(), 3);
    let mut buf = vec![0; 512];
    db.read_page(pg(3), &mut buf).unwrap();
    assert_eq!(buf, vec![7; 512]);
    db.read_page(pg(1), &mut buf).unwrap();
    assert_eq!(buf, vec![0; 512]);
    assert_eq!(
        db.read_page(pg(4), &mut buf).unwrap_err(),
        SourceError::Short { pgno: pg(4) }
    );
    db.inject(SourceOp::PageCount, SourceError::DiskFull);
    assert_eq!(db.page_count().unwrap_err(), SourceError::DiskFull);
    db.set_page_count(1);
    assert_eq!(db.page_count().unwrap(), 1);
}
