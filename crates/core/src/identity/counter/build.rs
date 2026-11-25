//! Database pages in SQLite's format, built by hand: the schema page and the counter's leaf
//! the tests of the identity check give the fake source, which holds no SQLite.

use lsr_ltx::PageSize;

/// A page of `size` bytes with `cell` at its end, one cell pointer to it, and b-tree header
/// bytes at `header` for a table leaf.
fn leaf(size: PageSize, header: usize, cell: &[u8]) -> Vec<u8> {
    let mut page = vec![0_u8; size.bytes()];
    let at = size.bytes() - cell.len();
    page[at..].copy_from_slice(cell);
    page[header] = 0x0d;
    page[header + 3..header + 5].copy_from_slice(&1_u16.to_be_bytes());
    let start = u16::try_from(at).unwrap();
    page[header + 5..header + 7].copy_from_slice(&start.to_be_bytes());
    page[header + 8..header + 10].copy_from_slice(&start.to_be_bytes());
    page
}

/// Page 1 holding one schema row, `table _litestream_seq` rooted at `root` (below 128).
#[must_use]
pub(crate) fn schema_page(size: PageSize, root: u8) -> Vec<u8> {
    let name = b"_litestream_seq";
    let mut record = vec![6, 23, 43, 43, 1, 15];
    record.extend_from_slice(b"table");
    record.extend_from_slice(name);
    record.extend_from_slice(name);
    record.push(root);
    record.push(b'x');
    let mut cell = vec![u8::try_from(record.len()).unwrap(), 1];
    cell.extend(record);
    leaf(size, 100, &cell)
}

/// The counter's root page holding `seq` in its one row.
#[must_use]
pub(crate) fn counter_page(size: PageSize, seq: u32) -> Vec<u8> {
    let mut record = vec![3, 0, 4];
    record.extend_from_slice(&seq.to_be_bytes());
    let mut cell = vec![u8::try_from(record.len()).unwrap(), 1];
    cell.extend(record);
    leaf(size, 0, &cell)
}

/// The counter's root page of a table that was never bumped: a leaf with no cell.
#[must_use]
pub(crate) fn empty_counter_page(size: PageSize) -> Vec<u8> {
    let mut page = vec![0_u8; size.bytes()];
    page[0] = 0x0d;
    page
}
