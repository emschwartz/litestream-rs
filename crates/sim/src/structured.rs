//! Database pages laid out as SQLite lays out the replicator's counter, for the scenarios that
//! need the counter to be there: page 1 the schema, one row naming `_litestream_seq` rooted at
//! page 2, and page 2 the table, one row holding `seq`. Built here by hand, not by the engine's
//! reader, so a fault in that reader shows as a disagreement.

use lsr_ltx::PageSize;

/// The pages every `Op::Structured` database has.
pub const PAGES: u32 = 6;

fn leaf(size: PageSize, header: usize, cell: &[u8]) -> Vec<u8> {
    let mut page = vec![0_u8; size.bytes()];
    let at = size.bytes().saturating_sub(cell.len());
    if let Some(tail) = page.get_mut(at..) {
        tail.copy_from_slice(cell);
    }
    let start = u16::try_from(at).unwrap_or_default().to_be_bytes();
    let mut put = |from: usize, bytes: &[u8]| {
        if let Some(slot) = page.get_mut(from..from.saturating_add(bytes.len())) {
            slot.copy_from_slice(bytes);
        }
    };
    put(header, &[0x0d]);
    put(header.saturating_add(3), &1_u16.to_be_bytes());
    put(header.saturating_add(5), &start);
    put(header.saturating_add(8), &start);
    page
}

/// Page 1: the schema row `table _litestream_seq` rooted at page 2.
#[must_use]
pub fn schema_page(size: PageSize) -> Vec<u8> {
    let name = b"_litestream_seq";
    let mut record = vec![6, 23, 43, 43, 1, 15];
    record.extend_from_slice(b"table");
    record.extend_from_slice(name);
    record.extend_from_slice(name);
    record.extend_from_slice(&[2, b'x']);
    let mut cell = vec![u8::try_from(record.len()).unwrap_or_default(), 1];
    cell.extend(record);
    leaf(size, 100, &cell)
}

/// Page 2: the counter table holding `seq` (below 2^31) in row 1.
#[must_use]
pub fn counter_page(size: PageSize, seq: u32) -> Vec<u8> {
    let mut record = vec![3, 0, 4];
    record.extend_from_slice(&seq.to_be_bytes());
    let mut cell = vec![u8::try_from(record.len()).unwrap_or_default(), 1];
    cell.extend(record);
    leaf(size, 0, &cell)
}
