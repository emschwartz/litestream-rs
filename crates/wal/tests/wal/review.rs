//! Review findings on resume, committed ends and the page map: each test failed on the code as reviewed.

use lsr_wal::{
    CommittedEnd, PageMap, PageMapError, ResumeError, Salts, ScanLimit, WalError, WalOffset,
};

use crate::common::{Wal, open, scan, size};

const PS: u32 = 512;
const SALTS: (u32, u32) = (0xaaaa_0001, 0xbbbb_0002);
/// What SQLite's walRestartHdr does: salt-1 plus one, a fresh random salt-2.
const RESTARTED: (u32, u32) = (SALTS.0 + 1, 0x1234_5678);

fn salts(pair: (u32, u32)) -> Salts {
    Salts {
        salt1: pair.0,
        salt2: pair.1,
    }
}

/// The caller replicated generation A up to frame 2, then the WAL restarted and
/// regrew past frame 2 with one transaction spanning it. The remembered
/// generation is refused, so no torn transaction (pages 1 and 3 lost, page 4 alone) is read.
#[test]
fn resume_refuses_a_wal_restarted_and_regrown_mid_transaction() {
    let mut old = Wal::new(PS, false, SALTS);
    old.tx(&[1, 2], 2, 10);
    let at = scan(&old.bytes, ScanLimit::FirstInvalidFrame).txs[0]
        .end()
        .next();
    let mut new = Wal::new(PS, false, RESTARTED);
    new.tx(&[1, 3, 4], 4, 50);
    let err = open(&new.bytes).resume(at, salts(SALTS)).unwrap_err();
    assert!(matches!(err, ResumeError::Restarted { .. }), "{err}");
}

/// Regrown so that frame 1 is a commit frame of generation B, and a caller at
/// the very start of generation A: both are refused, not silently continued in B.
#[test]
fn resume_refuses_a_wal_restarted_and_regrown_past_the_offset() {
    let mut new = Wal::new(PS, false, RESTARTED);
    new.tx(&[1, 2], 2, 50);
    new.tx(&[3], 3, 60);
    for index in [0, 2] {
        let at = WalOffset::from_frame_index(index, size(PS));
        let err = open(&new.bytes).resume(at, salts(SALTS)).unwrap_err();
        assert!(
            matches!(err, ResumeError::Restarted { .. }),
            "frame {index}: {err}"
        );
    }
}

/// An offset inside a transaction is refused, so its tail is never a CommittedTx.
#[test]
fn resume_refuses_an_offset_inside_a_transaction() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 2], 2, 10);
    let at = WalOffset::from_frame_index(1, size(PS));
    let err = open(&wal.bytes).resume(at, salts(SALTS)).unwrap_err();
    assert!(matches!(err, ResumeError::NotAtCommit { .. }), "{err}");
}

/// A skipped transaction leaves no trace in range(), so the map refuses the gap.
#[test]
fn page_map_refuses_a_gap() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1], 1, 0);
    wal.tx(&[2], 2, 0);
    wal.tx(&[1], 2, 0);
    let txs = scan(&wal.bytes, ScanLimit::FirstInvalidFrame).txs;
    let mut map = PageMap::new(size(PS));
    map.apply(&txs[0]).unwrap();
    let err = map.apply(&txs[2]).unwrap_err();
    assert!(
        matches!(err, PageMapError::Gap { expected, found }
            if expected.frame_index() == 1 && found.frame_index() == 2),
        "{err}"
    );
    assert_eq!(
        map.range().1.frame_index(),
        1,
        "the refused transaction left no trace"
    );
}

/// A transaction of another generation, even one that starts where the map ends, is refused.
#[test]
fn page_map_refuses_another_generation() {
    let mut old = Wal::new(PS, false, SALTS);
    old.tx(&[1], 1, 0);
    let mut new = Wal::new(PS, false, RESTARTED);
    new.tx(&[2], 2, 0);
    new.tx(&[3], 3, 0);
    let mut map = PageMap::new(size(PS));
    assert_eq!(map.salts(), None);
    map.apply(&scan(&old.bytes, ScanLimit::FirstInvalidFrame).txs[0])
        .unwrap();
    assert_eq!(map.salts(), Some(salts(SALTS)));
    let err = map
        .apply(&scan(&new.bytes, ScanLimit::FirstInvalidFrame).txs[1])
        .unwrap_err();
    assert_eq!(
        err,
        PageMapError::Generation {
            expected: salts(SALTS),
            found: salts(RESTARTED)
        }
    );
}

/// A mxFrame of the old generation against a file already in the new one is a
/// restart, never corruption below the committed end.
#[test]
fn an_old_generation_end_on_a_new_generation_file_is_restarted() {
    let mut old = Wal::new(PS, false, SALTS);
    old.tx(&[1, 2], 2, 10);
    old.tx(&[3], 3, 20);
    let mut new = Wal::new(PS, false, RESTARTED);
    new.tx(&[1], 1, 50);
    new.tx(&[2], 2, 60);
    let mut reader = open(&new.bytes);
    let end = CommittedEnd::new(3, salts(SALTS), size(PS));
    let cursor = reader.cursor_at_start();
    let outcome = reader.next_tx(&cursor, ScanLimit::Committed(end));
    assert!(
        matches!(outcome, Err(WalError::Restarted { .. })),
        "{outcome:?}"
    );
}
