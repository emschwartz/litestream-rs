//! The one known Go mismatch (#38): a Go build without upstream bcd69ae
//! (`GoBuild::reads_wal_index_end`) booting on WAL frames SQLite never committed.
//!
//! A process stopped mid-commit (its frames written, the wal-index not yet updated) leaves
//! valid frames past the wal-index's `mxFrame`. SQLite never treats them as committed while
//! the application keeps its connection open: the wal-index stays valid, so nobody recovers
//! the WAL, and the next commit overwrites them. Go v0.5.18 reads the WAL file to its last
//! valid frame instead, so if its first sync runs before the application's next commit, its
//! boot transaction holds that tail. In this run the tail is the `_litestream_seq` bump of a
//! Rust process caught by SIGSTOP; Rust itself reads the committed end from the wal-index.
//!
//! The evidence is read just before Go starts (the wal-index through the descriptor
//! `oracle::Held` keeps open), with no replicator running and the
//! application idle, so nothing changes the files between the read and Go's boot:
//! - the wal-index header (both copies equal, initialised) names `mxFrame` and the salts, and
//!   the WAL file's header has the same salts;
//! - the WAL's frames from the start through `mxFrame` verify as whole transactions (the end
//!   is a commit boundary), and at least one more whole transaction of valid frames follows;
//! - `committed`: the state SQLite recovers from the database file and the WAL cut at
//!   `mxFrame`, which must be the last recorded state on every page but `_litestream_seq`'s
//!   and hold the live `seq`: the tail sits on top of what the application has;
//! - `taken`: the state SQLite recovers from the whole WAL, which is what a reader of the
//!   file to its last valid frame sees. It must differ from `committed`, and only on pages the
//!   tail's frames name (a tail that shrank the database is not this case: no exception).
//!
//! The exception then accepts exactly one restore: the one at Go's boot transaction, for a
//! build without the fix, after the oracle refused it, when it equals `taken` on every page,
//! `_litestream_seq`'s included. Any other difference (a page missing, a page from another
//! commit, another size) is not `taken` and fails as before; the oracle is not advanced by the
//! accepted restore, so every later transaction is judged exactly as if it had not happened.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::unix::fs::FileExt;

use lsr_compat::{AppFixture, Outside, Shape};
use lsr_ltx::Txid;
use lsr_wal::{CommittedEnd, FrameCursor, Salts, ScanLimit, TxOutcome, WalReader};

use super::generation::wal_path;
use super::oracle::{Held, Oracle, State, capture, capture_wal, live_seq};

/// The wal-index header SQLite keeps twice at the start of the `-shm` file.
const INDEX_HEADER: usize = 48;

/// Valid frames past the wal-index's end, found before a Go boot.
#[derive(Debug)]
pub struct Tail {
    /// The transaction Go's boot writes: the position before it, plus one.
    pub boot: Txid,
    /// The wal-index's `mxFrame`.
    pub mx_frame: u32,
    /// Whole transactions' frames past it.
    pub frames: usize,
    /// The pages those frames write.
    pub pages: BTreeSet<u32>,
    /// What a reader of the WAL to its last valid frame restores.
    pub taken: State,
}

fn headers(held: &Held) -> Option<[u8; 2 * INDEX_HEADER]> {
    let mut bytes = [0; 2 * INDEX_HEADER];
    held.shm().read_exact_at(&mut bytes, 0).ok()?;
    Some(bytes)
}

/// `mxFrame` and the salts from the wal-index, when both header copies agree and are
/// initialised (no process is between its two writes).
fn wal_index(held: &Held) -> Option<(u32, Salts)> {
    let bytes = headers(held)?;
    let first = bytes.get(..INDEX_HEADER)?;
    if bytes.get(INDEX_HEADER..2 * INDEX_HEADER)? != first || first.get(12) != Some(&1) {
        return None;
    }
    let word = |at: usize| first.get(at..at + 4)?.try_into().ok();
    Some((
        u32::from_ne_bytes(word(16)?),
        Salts {
            salt1: u32::from_be_bytes(word(32)?),
            salt2: u32::from_be_bytes(word(36)?),
        },
    ))
}

/// The cursor at `mx_frame` after verifying every transaction below it, and the frames of the
/// whole transactions that follow it. (SQLite takes no POSIX lock on the WAL file, so this
/// descriptor may close.)
fn scan(fixture: &AppFixture, mx_frame: u32, salts: Salts) -> Option<(FrameCursor, Vec<u32>)> {
    let mut wal = WalReader::open(File::open(wal_path(fixture)).ok()?).ok()?;
    if wal.header().salts() != salts {
        return None;
    }
    let end = CommittedEnd::new(mx_frame, salts, wal.header().page_size());
    let mut at = wal.cursor_at_start();
    loop {
        match wal.next_tx(&at, ScanLimit::Committed(end)).ok()? {
            TxOutcome::Tx(tx) => at = *tx.end(),
            TxOutcome::End { at: stop, .. } => {
                at = stop;
                break;
            }
        }
    }
    if at.next().frame_index() != mx_frame {
        return None;
    }
    let committed = at;
    let mut pages = Vec::new();
    while let Ok(TxOutcome::Tx(tx)) = wal.next_tx(&at, ScanLimit::FirstInvalidFrame) {
        pages.extend(tx.frames().iter().map(|f| f.pgno().get()));
        at = *tx.end();
    }
    Some((committed, pages))
}

/// The tail Go's boot after `previous` may take, if the files hold one (see the module doc).
pub fn read(fixture: &AppFixture, held: &Held, oracle: &Oracle, previous: Txid) -> Option<Tail> {
    let (mx_frame, salts) = wal_index(held)?;
    let (end, frames) = scan(fixture, mx_frame, salts)?;
    if frames.is_empty() {
        return None;
    }
    let committed = capture_wal(fixture, held, "committed.db", Some(end.next().get()));
    let taken = capture_wal(fixture, held, "taken.db", None);
    let last = oracle.states.last()?;
    assert!(
        oracle.same_but_seq(&committed.pages, &last.pages) && committed.seq == live_seq(fixture),
        "the WAL cut at the wal-index's end ({mx_frame} frames) is not the database as recorded"
    );
    let pages: BTreeSet<u32> = frames.iter().copied().collect();
    let mut differs = (0..taken.pages.len().max(committed.pages.len()))
        .filter(|&i| taken.pages.get(i) != committed.pages.get(i))
        .map(|i| u32::try_from(i + 1).ok());
    // A tail that shrank the database differs on pages no frame names: not the case #38
    // describes, so no exception is claimed for it.
    if taken == committed || !differs.all(|pgno| pgno.is_some_and(|p| pages.contains(&p))) {
        return None;
    }
    Some(Tail {
        boot: previous.next()?,
        mx_frame,
        frames: frames.len(),
        pages,
        taken,
    })
}

/// The replicators' checkpoint bump (`oracle.rs`), the commit #38 caught half done.
const BUMP: &str = "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) \
                    ON CONFLICT (id) DO UPDATE SET seq = seq + 1";

fn index_header(held: &Held) -> [u8; INDEX_HEADER] {
    let bytes = headers(held).expect("read the wal-index");
    bytes[..INDEX_HEADER].try_into().expect("a header")
}

/// Writes `header` as both copies of the wal-index header, with its own checksum (SQLite's
/// `walChecksumBytes` over the first 40 bytes, native order).
fn set_index_header(held: &Held, mut header: [u8; INDEX_HEADER]) {
    let (mut s1, mut s2) = (0_u32, 0_u32);
    for pair in header[..40].as_chunks::<8>().0 {
        let word = |at: usize| u32::from_ne_bytes(pair[at..at + 4].try_into().expect("4"));
        s1 = s1.wrapping_add(word(0)).wrapping_add(s2);
        s2 = s2.wrapping_add(word(4)).wrapping_add(s1);
    }
    header[40..44].copy_from_slice(&s1.to_ne_bytes());
    header[44..48].copy_from_slice(&s2.to_ne_bytes());
    let mut bytes = [0; 2 * INDEX_HEADER];
    bytes[..INDEX_HEADER].copy_from_slice(&header);
    bytes[INDEX_HEADER..].copy_from_slice(&header);
    held.shm()
        .write_all_at(&bytes, 0)
        .expect("write the wal-index");
}

/// Runs the bump, then winds the wal-index back to before it, as a writer that wrote its frame
/// and was stopped before the index update leaves it: to `before` when the bump continued the
/// WAL, or to an empty new generation when it restarted it (`None`): `mxFrame` 0, and the
/// running checksum the next frame continues from is the WAL header's.
fn bump_unindexed(fixture: &AppFixture, held: &Held, before: Option<[u8; INDEX_HEADER]>) {
    fixture.app.execute(BUMP, []).expect("bump");
    let header = before.unwrap_or_else(|| {
        let mut header = index_header(held);
        let wal = fs::read(wal_path(fixture)).expect("wal");
        header[16..20].copy_from_slice(&0_u32.to_ne_bytes());
        for at in [24, 28] {
            let word = u32::from_be_bytes(wal[at..at + 4].try_into().expect("4"));
            header[at..at + 4].copy_from_slice(&word.to_ne_bytes());
        }
        header
    });
    set_index_header(held, header);
}

/// The evidence `read` needs is found in the two ways a stopped writer leaves it, and only
/// then: a bump past committed frames, and a bump that restarted the WAL and wrote its first
/// frame (the case #38 found). The tail's state holds the bump the application never saw.
pub fn a_tail_past_the_wal_index_is_recognised() {
    let mut fixture = AppFixture::create(4096, 38).expect("fixture");
    fixture.prepare_for_processes(20).expect("prepare");
    fixture.app.execute(BUMP, []).expect("a first bump");
    let held = Held::open(&fixture);
    let root = super::oracle::seq_root(&fixture);
    let mut oracle = Oracle::new(capture(&fixture, &held), root);
    for restart in [false, true] {
        fixture
            .commit(Shape::Insert { bytes: 300 })
            .expect("commit");
        let floor = live_seq(&fixture);
        oracle.record(capture(&fixture, &held), floor);
        let none = read(&fixture, &held, &oracle, Txid::MIN);
        assert!(none.is_none(), "nothing past the end");
        let before = index_header(&held);
        let frames = u32::from_ne_bytes(before[16..20].try_into().expect("4"));
        if restart {
            fixture.checkpoint(Outside::Restart).expect("restart");
        }
        bump_unindexed(&fixture, &held, (!restart).then_some(before));
        let tail = read(&fixture, &held, &oracle, Txid::MIN).expect("a tail past the end");
        let at = if restart { 0 } else { frames };
        assert_eq!((tail.mx_frame, tail.frames), (at, 1), "restart {restart}");
        assert_eq!(tail.pages, BTreeSet::from([root]), "restart {restart}");
        assert_eq!(tail.taken.seq, floor + 1, "restart {restart}: the bump");
        assert_eq!(
            live_seq(&fixture),
            floor,
            "restart {restart}: never committed"
        );
        let now = capture(&fixture, &held);
        assert!(oracle.same_but_seq(&tail.taken.pages, &now.pages));
        fixture
            .commit(Shape::Insert { bytes: 300 })
            .expect("overwrite");
        oracle.record(capture(&fixture, &held), floor);
        let none = read(&fixture, &held, &oracle, Txid::MIN);
        assert!(none.is_none(), "restart {restart}: overwritten");
        assert_eq!(
            live_seq(&fixture),
            floor,
            "restart {restart}: the bump is gone"
        );
    }
}
