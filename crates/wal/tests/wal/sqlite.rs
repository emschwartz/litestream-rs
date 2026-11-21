//! Differential tests against SQLite itself.
//!
//! A database and its WAL are copied after each commit (the copy carries no
//! wal-index, so SQLite recovers from the WAL alone). SQLite's own answer is
//! the copy opened and checkpointed with TRUNCATE; ours is the main file with
//! the page map applied over it. They must agree page for page and in length.

use std::fs;
use std::num::NonZeroU32;
use std::path::Path;

use lsr_ltx::PageNo;
use lsr_wal::{CommittedEnd, PageMap, ScanLimit, Tail, WalError, WalOffset, WalReader};
use proptest::prelude::*;
use rusqlite::Connection;
use std::io::Cursor;
use tempfile::TempDir;

use crate::common::{scan, scan_from, size};

/// A database in WAL mode that never checkpoints by itself.
struct Db {
    dir: TempDir,
    conn: Connection,
}

#[derive(Clone)]
struct Copy {
    db: Vec<u8>,
    wal: Vec<u8>,
}

impl Db {
    fn new(ps: u32) -> Self {
        let dir = TempDir::new().unwrap();
        let conn = Connection::open(dir.path().join("test.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA page_size = {ps}; PRAGMA auto_vacuum = INCREMENTAL;
             PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; PRAGMA cache_size = 1;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"
        ))
        .unwrap();
        Self { dir, conn }
    }

    fn copy(&self) -> Copy {
        Copy {
            db: fs::read(self.dir.path().join("test.db")).unwrap(),
            wal: fs::read(self.dir.path().join("test.db-wal")).unwrap_or_default(),
        }
    }

    fn insert(&self, rows: u32, size: u32) {
        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO t (v) VALUES (randomblob(?1))")
            .unwrap();
        for _ in 0..rows {
            stmt.execute([size]).unwrap();
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert { rows: u32, size: u32 },
    Update { modulo: u32 },
    DeleteAll,
    Shrink,
    RollbackSpill { rows: u32, size: u32 },
}

fn run_op(db: &Db, op: &Op) {
    match op {
        Op::Insert { rows, size } => {
            db.conn.execute_batch("BEGIN").unwrap();
            db.insert(*rows, *size);
            db.conn.execute_batch("COMMIT").unwrap();
        }
        Op::Update { modulo } => {
            db.conn
                .execute(
                    "UPDATE t SET v = randomblob(length(v)) WHERE id % ?1 = 0",
                    [modulo],
                )
                .unwrap();
        }
        Op::DeleteAll => {
            db.conn.execute("DELETE FROM t", []).unwrap();
        }
        Op::Shrink => db.conn.execute_batch("PRAGMA incremental_vacuum").unwrap(),
        Op::RollbackSpill { rows, size } => {
            db.conn.execute_batch("BEGIN").unwrap();
            db.insert(*rows, *size);
            db.conn.execute_batch("ROLLBACK").unwrap();
        }
    }
}

/// SQLite's verdict on a copy: its committed frame count and the database it checkpoints to.
fn oracle(copy: &Copy) -> (u32, Vec<u8>) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    fs::write(&path, &copy.db).unwrap();
    fs::write(dir.path().join("test.db-wal"), &copy.wal).unwrap();
    let conn = Connection::open(&path).unwrap();
    let log: u32 = conn
        .query_row("PRAGMA wal_checkpoint(NOOP)", [], |row| row.get(1))
        .unwrap();
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    drop(conn);
    (log, fs::read(path).unwrap())
}

/// The main file with every page the map holds read from the WAL over it.
fn apply(map: &PageMap, db: &[u8], reader: &mut WalReader<Cursor<Vec<u8>>>) -> Vec<u8> {
    let ps = map.page_size();
    let mut out = Vec::new();
    for n in 1..=map.commit().map_or(0, NonZeroU32::get) {
        let pgno = PageNo::new(n, ps).unwrap();
        let mut page = vec![0; ps.bytes()];
        if let Some(frame) = map.get(pgno) {
            reader.read_page(frame, &mut page).unwrap();
        } else if let Some(main) = db.get((ps.bytes() * (usize::try_from(n).unwrap() - 1))..) {
            let take = main.len().min(page.len());
            page[..take].copy_from_slice(&main[..take]);
        }
        out.extend(page);
    }
    out
}

/// Checks one copy against SQLite and returns the tail our scan ended on.
fn check(copy: &Copy, ps: u32) -> Option<Tail> {
    let (log, expected) = oracle(copy);
    let Ok(mut reader) = WalReader::open(Cursor::new(copy.wal.clone())) else {
        assert_eq!(log, 0, "SQLite sees commits in a WAL we cannot open");
        return None;
    };
    assert_eq!(reader.header().page_size(), size(ps));
    let scanned = scan_from(&mut reader, None, ScanLimit::FirstInvalidFrame).unwrap();
    let end_frame = scanned
        .txs
        .last()
        .map_or(0, |tx| tx.end().next().frame_index());
    assert_eq!(end_frame, log, "our committed end is SQLite's mxFrame");
    let mut map = PageMap::new(size(ps));
    for tx in &scanned.txs {
        map.apply(tx).unwrap();
    }
    if log > 0 {
        assert!(
            apply(&map, &copy.db, &mut reader) == expected,
            "restored pages differ from SQLite's"
        );
    }
    let limit = ScanLimit::Committed(CommittedEnd::new(log, reader.header().salts(), size(ps)));
    let bounded = scan_from(&mut reader, None, limit).unwrap();
    assert_eq!(
        bounded.txs, scanned.txs,
        "the committed limit changes the result"
    );
    assert_eq!(bounded.tail, Tail::Clean);
    Some(scanned.tail)
}

fn workload(ps: u32, ops: &[Op]) -> Vec<Tail> {
    let db = Db::new(ps);
    let mut tails: Vec<Tail> = check(&db.copy(), ps).into_iter().collect();
    for op in ops {
        run_op(&db, op);
        tails.extend(check(&db.copy(), ps));
    }
    tails
}

fn every_op() -> Vec<Op> {
    vec![
        Op::Insert {
            rows: 30,
            size: 900,
        },
        Op::Update { modulo: 3 },
        Op::RollbackSpill {
            rows: 60,
            size: 1500,
        },
        Op::Insert {
            rows: 5,
            size: 3000,
        },
        Op::DeleteAll,
        Op::Shrink,
        Op::Insert {
            rows: 10,
            size: 200,
        },
    ]
}

#[test]
fn every_operation_at_every_page_size_matches_sqlite() {
    for ps in [512, 4096, 16384, 65536] {
        workload(ps, &every_op());
    }
}

/// SQLite rewrites a spilled page in place when the same transaction dirties it again and
/// repairs the checksums after it only at commit, so an open transaction's tail is either
/// valid uncommitted frames or a checksum failure. Both end the committed prefix.
fn open_tail(tail: &Tail) -> bool {
    matches!(tail, Tail::Uncommitted { frames } if *frames > 0)
        || matches!(
            tail,
            Tail::InvalidFrame(lsr_wal::FrameFault::ChecksumMismatch)
        )
}

#[test]
fn a_rolled_back_spill_leaves_an_uncommitted_tail() {
    let tails = workload(
        512,
        &[
            Op::Insert { rows: 4, size: 100 },
            Op::RollbackSpill {
                rows: 3,
                size: 100_000,
            },
        ],
    );
    assert!(matches!(tails.last(), Some(t) if open_tail(t)), "{tails:?}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    fn random_workloads_match_sqlite(
        ps in prop::sample::select(vec![512u32, 4096, 16384, 65536]),
        ops in proptest::collection::vec(
            prop_oneof![
                (1u32..40, 1u32..3000).prop_map(|(rows, size)| Op::Insert { rows, size }),
                (1u32..5).prop_map(|modulo| Op::Update { modulo }),
                Just(Op::DeleteAll),
                Just(Op::Shrink),
                (1u32..80, 100u32..4000).prop_map(|(rows, size)| Op::RollbackSpill { rows, size }),
            ],
            1..7,
        ),
    ) {
        workload(ps, &ops);
    }
}

#[test]
fn an_open_spilled_transaction_adds_no_pages() {
    let db = Db::new(512);
    db.insert(20, 400);
    let before = db.copy();
    db.conn.execute_batch("BEGIN").unwrap();
    db.insert(3, 100_000);
    let during = db.copy();
    db.conn.execute_batch("ROLLBACK").unwrap();

    let digests = |copy: &Copy| -> Vec<_> {
        let scanned = scan(&copy.wal, ScanLimit::FirstInvalidFrame);
        let mut map = PageMap::new(size(512));
        scanned.txs.iter().for_each(|tx| map.apply(tx).unwrap());
        map.pages().map(|f| (f.pgno(), f.digest())).collect()
    };
    let scanned = scan(&during.wal, ScanLimit::FirstInvalidFrame);
    assert!(open_tail(&scanned.tail), "{:?}", scanned.tail);
    assert_eq!(digests(&during), digests(&before));
    check(&during, 512);
}

#[test]
fn a_restarted_wal_ends_at_the_first_stale_frame() {
    let db = Db::new(512);
    db.insert(60, 400);
    let old = db.copy();
    db.conn
        .query_row("PRAGMA wal_checkpoint(RESTART)", [], |_| Ok(()))
        .unwrap();
    db.insert(1, 100);
    let new = db.copy();
    assert!(
        new.wal.len() >= old.wal.len(),
        "the WAL file is reused, old frames stay behind"
    );

    let old_salts = WalReader::open(Cursor::new(old.wal.clone()))
        .unwrap()
        .header()
        .salts();
    let mut reader = WalReader::open(Cursor::new(new.wal.clone())).unwrap();
    assert_ne!(reader.header().salts(), old_salts);
    let scanned = scan_from(&mut reader, None, ScanLimit::FirstInvalidFrame).unwrap();
    assert!(!scanned.txs.is_empty());
    assert_eq!(
        scanned.tail,
        Tail::InvalidFrame(lsr_wal::FrameFault::SaltMismatch { found: old_salts })
    );
    // A position remembered from the old generation is refused, at a stale frame and at the start.
    for at in [
        WalOffset::from_frame_index(40, size(512)),
        WalOffset::start(size(512)),
    ] {
        let err = reader.resume(at, old_salts).unwrap_err();
        assert!(
            matches!(err, lsr_wal::ResumeError::Restarted { expected, found }
                if expected == old_salts && found == reader.header().salts()),
            "{err}"
        );
    }
    check(&new, 512);
    // A cursor taken in the old generation is refused outright.
    let old_reader = WalReader::open(Cursor::new(old.wal)).unwrap();
    let old_cursor = old_reader.cursor_at_start();
    let err = reader
        .next_tx(&old_cursor, ScanLimit::FirstInvalidFrame)
        .unwrap_err();
    assert!(matches!(err, WalError::Restarted { .. }), "{err}");
}

/// Rewrites a little-endian WAL as a big-endian one: new magic, checksums redone.
fn to_big_endian(wal: &[u8], ps: usize) -> Vec<u8> {
    let mut out = wal.to_vec();
    out[3] = 0x83;
    let mut running = crate::common::sum(true, (0, 0), &out[..24]);
    out[24..28].copy_from_slice(&running.0.to_be_bytes());
    out[28..32].copy_from_slice(&running.1.to_be_bytes());
    let flen = ps + 24;
    let mut at = 32;
    while at + flen <= out.len() {
        running = crate::common::sum(true, running, &out[at..at + 8]);
        running = crate::common::sum(true, running, &out[at + 24..at + flen]);
        out[at + 16..at + 20].copy_from_slice(&running.0.to_be_bytes());
        out[at + 20..at + 24].copy_from_slice(&running.1.to_be_bytes());
        at += flen;
    }
    out
}

#[test]
fn a_big_endian_wal_matches_sqlite_reading_it() {
    let db = Db::new(512);
    db.insert(40, 300);
    run_op(&db, &Op::Update { modulo: 2 });
    let little = db.copy();
    let big = Copy {
        db: little.db.clone(),
        wal: to_big_endian(&little.wal, 512),
    };
    assert_eq!(check(&big, 512), check(&little, 512));
    let digests = |wal: &[u8]| -> Vec<_> {
        scan(wal, ScanLimit::FirstInvalidFrame)
            .txs
            .iter()
            .flat_map(|tx| tx.frames().iter().map(|f| f.digest()))
            .collect()
    };
    assert_eq!(digests(&big.wal), digests(&little.wal));
    assert!(Path::new(".").exists());
}
