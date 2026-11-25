//! The counter's reader against pages SQLite wrote, and against pages of every other shape.

use lsr_ltx::{PageNo, PageSize};
use proptest::prelude::*;
use rusqlite::Connection;

use super::{build, locate, value, varint};

/// The pages of a database file, one vector each.
fn pages_of(path: &std::path::Path, size: usize) -> Vec<Vec<u8>> {
    std::fs::read(path)
        .unwrap()
        .chunks(size)
        .map(<[u8]>::to_vec)
        .collect()
}

/// The root page and value of the counter in `pages`, as the identity check reads them.
fn read_counter(pages: &[Vec<u8>], size: PageSize) -> Option<(u32, Option<u64>)> {
    let count = u32::try_from(pages.len()).unwrap();
    let root = locate(size, count, &mut |pgno: PageNo, buf: &mut [u8]| {
        buf.copy_from_slice(&pages[usize::try_from(pgno.get() - 1).unwrap()]);
        Ok::<(), ()>(())
    })
    .unwrap()?;
    let page = &pages[usize::try_from(root.get() - 1).unwrap()];
    Some((root.get(), value(page)))
}

fn database(page_size: u32, tables: u32) -> (tempfile::TempDir, std::path::PathBuf, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(&format!("PRAGMA page_size = {page_size}; PRAGMA journal_mode = DELETE;"))
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for n in 0..tables {
        conn.execute_batch(&format!(
            "CREATE TABLE some_table_with_a_longer_name_{n} (id INTEGER PRIMARY KEY, value TEXT);"
        ))
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    conn.execute_batch("CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);")
        .unwrap();
    (dir, path, conn)
}

/// Real SQLite's table: empty reads 0, and each value the replicator's own statement writes
/// reads back, across the integer widths a record stores.
#[test]
fn reads_the_counter_sqlite_wrote() {
    let (_dir, path, conn) = database(4096, 3);
    let size = PageSize::new(4096).unwrap();
    let (root, empty) = read_counter(&pages_of(&path, 4096), size).unwrap();
    assert_eq!((root > 1, empty), (true, Some(0)));
    let bump = "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1";
    let mut seen = Vec::new();
    for want in [1_u64, 2, 127, 128, 40_000, 70_000, 20_000_000] {
        conn.execute_batch(&format!(
            "INSERT OR REPLACE INTO _litestream_seq (id, seq) VALUES (1, {});",
            want - 1
        ))
        .unwrap();
        conn.execute_batch(bump).unwrap();
        let (_, got) = read_counter(&pages_of(&path, 4096), size).unwrap();
        seen.push(got);
    }
    assert_eq!(
        seen,
        [1_u64, 2, 127, 128, 40_000, 70_000, 20_000_000].map(Some)
    );
    conn.execute_batch("UPDATE _litestream_seq SET seq = 5000000000;")
        .unwrap();
    assert_eq!(
        read_counter(&pages_of(&path, 4096), size).unwrap().1,
        Some(5_000_000_000)
    );
}

/// A schema too long for one page has interior pages: the walk finds the table through them,
/// at every page size down to the smallest.
#[test]
fn finds_the_counter_through_a_schema_with_interior_pages() {
    for page_size in [512_u32, 1024, 4096] {
        let tables = 300;
        let (_dir, path, conn) = database(page_size, tables);
        conn.execute_batch("INSERT INTO _litestream_seq VALUES (1, 77);")
            .unwrap();
        let size = PageSize::new(page_size).unwrap();
        let pages = pages_of(&path, usize::try_from(page_size).unwrap());
        assert_eq!(pages[0][100], 0x05, "the schema's root is an interior page");
        let (root, got) = read_counter(&pages, size).unwrap();
        let sql: u32 = conn
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE name = '_litestream_seq'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((root, got), (sql, Some(77)), "page size {page_size}");
    }
}

/// A schema whose counter row is not the first, the last cell of a leaf.
#[test]
fn finds_the_counter_among_other_tables() {
    let (_dir, path, conn) = database(4096, 12);
    conn.execute_batch("INSERT INTO _litestream_seq VALUES (1, 9); CREATE TABLE zzz (a); CREATE INDEX i ON zzz (a);")
        .unwrap();
    let size = PageSize::new(4096).unwrap();
    assert_eq!(read_counter(&pages_of(&path, 4096), size).unwrap().1, Some(9));
}

/// A database without the table has no counter.
#[test]
fn a_database_without_the_table_has_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE t (a); CREATE TABLE _litestream_lock (id INTEGER);")
        .unwrap();
    let size = PageSize::new(4096).unwrap();
    assert_eq!(read_counter(&pages_of(&path, 4096), size), None);
}

/// A table of the same name that is an index or a view, or a table named like it, is not the
/// counter: only a table named exactly so is.
#[test]
fn only_a_table_with_the_exact_name_counts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE _litestream_seq2 (id INTEGER PRIMARY KEY, seq INTEGER);
             CREATE TABLE x_litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);",
        )
        .unwrap();
    let size = PageSize::new(4096).unwrap();
    assert_eq!(read_counter(&pages_of(&path, 4096), size), None);
}

/// The hand-built pages the identity tests use read as they say, and an empty leaf reads 0.
#[test]
fn the_built_pages_read_back() {
    let size = PageSize::MIN;
    let pages = vec![
        build::schema_page(size, 2),
        build::counter_page(size, 300),
    ];
    assert_eq!(read_counter(&pages, size), Some((2, Some(300))));
    assert_eq!(value(&build::empty_counter_page(size)), Some(0));
    assert_eq!(value(&build::counter_page(size, 0x7fff_ffff)), Some(0x7fff_ffff));
}

/// A page that is not a leaf holding the row (zeros, an interior page, a leaf of another row,
/// a negative value, a value that is not an integer) has no value.
#[test]
fn pages_that_are_not_the_counter_have_no_value() {
    let size = PageSize::MIN;
    assert_eq!(value(&vec![0; size.bytes()]), None);
    let mut interior = build::counter_page(size, 5);
    interior[0] = 0x05;
    assert_eq!(value(&interior), None);
    let mut other_row = build::counter_page(size, 5);
    let at = size.bytes() - 9;
    assert_eq!(other_row[at + 1], 1, "the rowid byte");
    other_row[at + 1] = 2;
    assert_eq!(value(&other_row), None);
    let mut negative = build::counter_page(size, 5);
    let last = size.bytes() - 4;
    negative[last..].copy_from_slice(&(-3_i32).to_be_bytes());
    assert_eq!(value(&negative), None);
    let mut text = build::counter_page(size, 5);
    text[size.bytes() - 5] = 15;
    assert_eq!(value(&text), None);
    let mut reserved = build::counter_page(size, 5);
    reserved[size.bytes() - 5] = 10;
    assert_eq!(value(&reserved), None);
}

/// Serial types 8 and 9 are the constants 0 and 1; a one-byte negative is negative.
#[test]
fn constant_serial_types_read_as_zero_and_one() {
    let size = PageSize::MIN;
    for (serial, want) in [(8_u8, Some(0_u64)), (9, Some(1))] {
        let mut page = build::counter_page(size, 0);
        page[size.bytes() - 5] = serial;
        assert_eq!(value(&page), want, "serial {serial}");
    }
}

/// A walk over a schema that names pages past the database, loops, or never ends gives up.
#[test]
fn a_walk_over_a_broken_schema_gives_up() {
    let size = PageSize::MIN;
    // An interior page whose only child is itself.
    let mut looped = vec![0_u8; size.bytes()];
    looped[100] = 0x05;
    looped[108..112].copy_from_slice(&1_u32.to_be_bytes());
    assert_eq!(read_counter(&[looped.clone()], size), None);
    // A child past the end of the file.
    let mut past = looped;
    past[108..112].copy_from_slice(&9_u32.to_be_bytes());
    assert_eq!(read_counter(&[past], size), None);
    // A page that is no b-tree.
    assert_eq!(read_counter(&[vec![0xff; size.bytes()]], size), None);
    // The page size or count the caller gives does not fit page 1.
    let ok = vec![build::schema_page(size, 2), build::counter_page(size, 1)];
    let hidden = locate(size, 0, &mut |_: PageNo, _: &mut [u8]| Ok::<(), ()>(())).unwrap();
    assert_eq!(hidden, None);
    assert_eq!(read_counter(&ok, size), Some((2, Some(1))));
}

/// A read that fails ends the walk with its error.
#[test]
fn a_failed_read_is_the_callers_error() {
    let size = PageSize::MIN;
    let got = locate(size, 5, &mut |_: PageNo, _: &mut [u8]| Err::<(), _>("disk"));
    assert_eq!(got, Err("disk"));
}

/// Varints: one byte, many bytes, the ninth byte's eight bits, a run off the page.
#[test]
fn varints() {
    assert_eq!(varint(&[0x05], 0), Some((5, 1)));
    assert_eq!(varint(&[0x81, 0x00], 0), Some((128, 2)));
    assert_eq!(varint(&[0xff; 9], 0), Some((u64::MAX, 9)));
    assert_eq!(varint(&[0xff; 3], 0), None);
    assert_eq!(varint(&[0x05, 0x06], 1), Some((6, 2)));
    assert_eq!(varint(&[], 0), None);
}

proptest! {
    /// No page of any bytes makes the reader panic, and a value it returns is the cell's.
    #[test]
    fn arbitrary_pages_never_panic(bytes in proptest::collection::vec(any::<u8>(), 512)) {
        let size = PageSize::MIN;
        let _ = value(&bytes);
        let pages = vec![bytes.clone(), bytes];
        let _ = read_counter(&pages, size);
    }

    /// A built counter page reads back whatever it was built with.
    #[test]
    fn built_values_read_back(seq in 0..=i32::MAX.unsigned_abs()) {
        let size = PageSize::MIN;
        prop_assert_eq!(value(&build::counter_page(size, seq)), Some(u64::from(seq)));
    }
}
