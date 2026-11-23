//! The check itself, over a fake WAL and database file: what counts as ahead of the WAL, and
//! every way the WAL can move under the check, each alone.

use std::io::Cursor;
use std::num::NonZeroU32;

use lsr_ltx::{PageChecksum, PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{CommittedEnd, Salts, ScanLimit, TxOutcome, WalReader};

use super::*;
use crate::conformance::block_on;
use crate::fault::role::Replica;
use crate::mem::MemStore;
use crate::position::{Continuity, LastPage};
use crate::source::{CommittedRead, FakeDb, FakeWal, PageSource, SourceError, SourceOp};

const PS: PageSize = PageSize::MIN;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn bytes(n: u8) -> Vec<u8> {
    vec![n; PS.bytes()]
}

/// A WAL whose one transaction writes page 1, over a three-page file whose pages hold their
/// numbers, and a table that says the same: nothing is ahead.
fn rig() -> (FakeWal, FakeDb, PageChecksumTable) {
    let wal = FakeWal::new(PS, Salts { salt1: 1, salt2: 2 });
    wal.append_tx(&[(pg(1), bytes(7))], NonZeroU32::new(3).unwrap());
    let mut db = FakeDb::new(PS);
    db.set_page_count(3);
    let mut table = PageChecksumTable::new();
    for n in 1..=3_u8 {
        db.write_page(pg(u32::from(n)), &bytes(n));
        table.record(
            pg(u32::from(n)),
            PageChecksum::of(pg(u32::from(n)), &bytes(n)),
        );
    }
    (wal, db, table)
}

/// The newest staged file's position: the end of the WAL's last transaction.
fn staged(wal: &FakeWal) -> Position {
    let mut reader = WalReader::open(Cursor::new(wal.file_bytes())).unwrap();
    let mut at = reader.cursor_at_start();
    let mut last = None;
    while let Ok(TxOutcome::Tx(tx)) = reader.next_tx(&at, ScanLimit::FirstInvalidFrame) {
        at = *tx.end();
        last = Some((*tx.frames().last().unwrap(), tx.commit()));
    }
    let (frame, commit) = last.unwrap();
    Position::new(
        Txid::new(4).unwrap(),
        at,
        commit,
        LastPage::of_frame(&frame),
        Timestamp::from_unix_millis(0),
        Continuity::Held(wal.epoch()),
    )
}

fn run<P: PageSource>(
    position: &Position,
    wal: &mut FakeWal,
    db: &mut P,
    table: &PageChecksumTable,
) -> Result<Option<Found>, Fault> {
    block_on(check(
        position,
        wal,
        db,
        table,
        &Replica(&MemStore::new()),
        None,
    ))
}

#[test]
fn a_file_that_holds_its_images_is_not_ahead() {
    let (mut wal, mut db, table) = rig();
    let position = staged(&wal);
    assert_eq!(run(&position, &mut wal, &mut db, &table), Ok(None));
}

/// Every uncovered page that differs counts, and the first is the lowest; the page the WAL
/// covers is not the file's to answer for, whatever the file holds.
#[test]
fn every_uncovered_page_that_differs_counts() {
    let (mut wal, mut db, table) = rig();
    let position = staged(&wal);
    db.write_page(pg(1), &bytes(9));
    db.write_page(pg(2), &bytes(9));
    db.write_page(pg(3), &bytes(9));
    assert_eq!(
        run(&position, &mut wal, &mut db, &table),
        Ok(Some(Found {
            pages: NonZeroU32::new(2).unwrap(),
            first: NonZeroU32::new(2).unwrap(),
        }))
    );
}

/// A WAL that restarted after the newest staged file: its header and the wal-index agree on new
/// salts, and only the position names the old ones.
#[test]
fn a_wal_restarted_since_the_staged_file_is_moved() {
    let (mut wal, mut db, table) = rig();
    let position = staged(&wal);
    wal.force_restart();
    wal.append_tx(&[(pg(1), bytes(8))], NonZeroU32::new(3).unwrap());
    assert_eq!(
        run(&position, &mut wal, &mut db, &table),
        Err(Fault::WalMoved)
    );
}

/// A wal-index that names other salts than the header the check opened, the position agreeing
/// with the header.
#[test]
fn a_wal_index_of_another_generation_is_moved() {
    let (mut wal, mut db, table) = rig();
    let position = staged(&wal);
    let end = wal.committed_end();
    let other = Salts { salt1: 9, salt2: 9 };
    let read = CommittedRead::new(
        CommittedEnd::new(end.frame_count(), other, PS),
        0,
        wal.epoch(),
    )
    .unwrap();
    wal.script_committed(Some(read));
    assert_eq!(
        run(&position, &mut wal, &mut db, &table),
        Err(Fault::WalMoved)
    );
}

/// A database file whose first read does something to the WAL first.
struct Meddling {
    db: FakeDb,
    wal: FakeWal,
    meddle: fn(&FakeWal),
    done: bool,
}

impl PageSource for Meddling {
    fn page_count(&mut self) -> Result<lsr_ltx::DbPages, SourceError> {
        self.db.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        if !std::mem::replace(&mut self.done, true) {
            (self.meddle)(&self.wal);
        }
        self.db.read_page(pgno, buf)
    }
}

fn meddled(meddle: fn(&FakeWal)) -> Result<Option<Found>, Fault> {
    let (mut wal, db, table) = rig();
    let position = staged(&wal);
    let mut db = Meddling {
        db,
        wal: wal.clone(),
        meddle,
        done: false,
    };
    run(&position, &mut wal, &mut db, &table)
}

/// The read transaction lost and taken again while the pages are read, the WAL's header
/// unchanged: what was read is not under one snapshot.
#[test]
fn a_read_transaction_lost_under_the_reads_is_moved() {
    let lost = meddled(|wal| wal.set_epoch(wal.epoch().next()));
    assert_eq!(lost, Err(Fault::WalMoved));
}

/// The WAL restarted in place while the pages are read (a TRUNCATE by another connection when
/// the engine's read mark is 0), the read transaction still held.
#[test]
fn a_wal_restarted_under_the_reads_is_moved() {
    let restarted = meddled(|wal| {
        wal.force_restart();
        wal.append_tx(&[(pg(1), bytes(8))], NonZeroU32::new(3).unwrap());
    });
    assert_eq!(restarted, Err(Fault::WalMoved));
}

/// A read of the database file that fails is that read's fault.
#[test]
fn a_failed_page_read_is_its_fault() {
    let (mut wal, mut db, table) = rig();
    let position = staged(&wal);
    db.inject(
        SourceOp::ReadPage,
        SourceError::Io(std::io::ErrorKind::Other),
    );
    assert_eq!(
        run(&position, &mut wal, &mut db, &table),
        Err(Fault::from_source(SourceError::Io(
            std::io::ErrorKind::Other
        )))
    );
}

/// A replica holding a snapshot at `txid` of a three-page file whose pages hold their numbers.
fn replica_at(txid: u64) -> MemStore {
    use lsr_ltx::{Encoder, Header, HeaderFields, TxidRange};

    use crate::layout::{FileKey, Level};
    use crate::store::{ObjectStore, Upload, WriteMode};

    let range = TxidRange::new(Txid::MIN, Txid::new(txid).unwrap()).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PS,
        commit: 3,
        txids: range,
        timestamp: Timestamp::from_unix_millis(1_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in 1..=3_u8 {
        encoder.write_page(pg(u32::from(n)), &bytes(n)).unwrap();
    }
    let file = encoder.finish(None).unwrap().0;
    let store = MemStore::new();
    block_on(async {
        let key = FileKey::new(Level::new(9).unwrap(), range);
        let mut upload = store
            .begin_upload(&key, Timestamp::from_unix_millis(1_000), WriteMode::Replace)
            .await
            .unwrap();
        upload.write(&file).await.unwrap();
        upload.commit().await.unwrap();
    });
    store
}

/// With nothing staged after the replica's newest transaction, the images come from the
/// replica alone: a file that holds them is clean, one page that does not is counted.
#[test]
fn the_images_come_from_the_replica_when_nothing_is_pending() {
    let (mut wal, mut db, _) = rig();
    let position = staged(&wal);
    let remote = replica_at(4);
    let empty = PageChecksumTable::new();
    let checked = |wal: &mut FakeWal, db: &mut FakeDb| {
        block_on(check(
            &position,
            wal,
            db,
            &empty,
            &Replica(&remote),
            Txid::new(4),
        ))
    };
    assert_eq!(checked(&mut wal, &mut db), Ok(None));
    db.write_page(pg(3), &bytes(9));
    assert_eq!(
        checked(&mut wal, &mut db),
        Ok(Some(Found {
            pages: NonZeroU32::MIN,
            first: NonZeroU32::new(3).unwrap(),
        }))
    );
}
