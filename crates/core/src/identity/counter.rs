//! The replicator's own counter, read from raw database pages.
//!
//! `_litestream_seq` (`id INTEGER PRIMARY KEY, seq INTEGER`) holds one row, id 1, that each
//! replicator checkpoint adds one to (Go's `bumpLitestreamSeq` and ours alike). Along one
//! database's history the value never goes down, so an image holding a lower value than another
//! is an ancestor of it, which two images' pages alone cannot show (`docs/decisions/D205.md`).
//!
//! Reading it takes no SQLite: the root page comes from a walk of the schema table's b-tree
//! (page 1 and its children), and the value from that root's one leaf cell. Anything the walk
//! does not understand reads as "no counter", never as a value: the caller then has no evidence
//! and decides as it did before. Every read of the walk is capped.

use lsr_ltx::{PageNo, PageSize};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
pub(crate) mod build;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;

/// The most pages the schema walk reads before it gives up.
const MAX_SCHEMA_PAGES: usize = 4096;

/// The counter table's name in the schema.
const TABLE_NAME: &[u8] = b"_litestream_seq";

/// A b-tree leaf page of a table.
const LEAF_TABLE: u8 = 0x0d;

/// An interior page of a table's b-tree.
const INTERIOR_TABLE: u8 = 0x05;

/// Where page 1's b-tree header starts: after the 100-byte database header.
const PAGE_ONE_HEADER: usize = 100;

/// The big-endian 16-bit number at `at`.
fn be16(page: &[u8], at: usize) -> Option<usize> {
    let bytes = page.get(at..at.checked_add(2)?)?;
    let (hi, lo) = (*bytes.first()?, *bytes.get(1)?);
    Some((usize::from(hi) << 8) | usize::from(lo))
}

/// The big-endian 32-bit number at `at`.
fn be32(page: &[u8], at: usize) -> Option<u32> {
    let bytes: [u8; 4] = page.get(at..at.checked_add(4)?)?.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

/// The SQLite varint at `at`: its value and the offset after it.
fn varint(page: &[u8], at: usize) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for i in 0..9_usize {
        let pos = at.checked_add(i)?;
        let byte = *page.get(pos)?;
        if i == 8 {
            return Some(((value << 8) | u64::from(byte), pos.checked_add(1)?));
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return Some((value, pos.checked_add(1)?));
        }
    }
    None
}

/// The bytes a record column of serial type `serial` takes.
fn serial_len(serial: u64) -> Option<usize> {
    match serial {
        0 | 8 | 9 => Some(0),
        1 => Some(1),
        2 => Some(2),
        3 => Some(3),
        4 => Some(4),
        5 => Some(6),
        6 | 7 => Some(8),
        10 | 11 => None,
        n => usize::try_from(n.saturating_sub(12) / 2).ok(),
    }
}

/// The integer a column holds, if its serial type is an integer type.
fn serial_int(serial: u64, bytes: &[u8]) -> Option<i64> {
    match serial {
        8 => return Some(0),
        9 => return Some(1),
        1..=6 => {}
        _ => return None,
    }
    let mut value = i64::from(i8::from_be_bytes([*bytes.first()?]) < 0).wrapping_neg();
    for byte in bytes {
        value = (value << 8) | i64::from(*byte);
    }
    Some(value)
}

/// A record's first `N` columns: each one's serial type and its bytes. The record is cut at
/// the end of the page, so a cell whose payload spills to an overflow page yields what its first
/// page holds, and `None` when that is too little.
fn columns<const N: usize>(page: &[u8], at: usize) -> Option<[(u64, &[u8]); N]> {
    let record = page.get(at..)?;
    let (header_len, mut cursor) = varint(record, 0)?;
    let header_len = usize::try_from(header_len).ok()?;
    let mut content = header_len;
    let mut out = [(0_u64, &[][..]); N];
    for slot in &mut out {
        if cursor >= header_len {
            return None;
        }
        let (serial, next) = varint(record, cursor)?;
        cursor = next;
        let len = serial_len(serial)?;
        let end = content.checked_add(len)?;
        *slot = (serial, record.get(content..end)?);
        content = end;
    }
    Some(out)
}

/// The cell pointers of the b-tree page whose header starts at `header`, and its type.
fn cells(page: &[u8], header: usize) -> Option<(u8, Vec<usize>)> {
    let kind = *page.get(header)?;
    let count = be16(page, header.checked_add(3)?)?;
    let first = match kind {
        LEAF_TABLE => header.checked_add(8)?,
        INTERIOR_TABLE => header.checked_add(12)?,
        _ => return None,
    };
    let mut out = Vec::new();
    for i in 0..count {
        let at = first.checked_add(i.checked_mul(2)?)?;
        out.push(be16(page, at)?);
    }
    Some((kind, out))
}

/// The root page of the counter table: the walk of the schema table's b-tree, reading each page
/// with `read`. `Ok(None)` when the schema holds no such table or the walk meets a page it does
/// not understand; `Err` is `read`'s own.
///
/// # Errors
/// What `read` returns.
pub(super) fn locate<E>(
    page_size: PageSize,
    pages: u32,
    read: &mut impl FnMut(PageNo, &mut [u8]) -> Result<(), E>,
) -> Result<Option<PageNo>, E> {
    let mut buf = vec![0_u8; page_size.bytes()];
    let mut todo = vec![1_u32];
    let mut reads = 0_usize;
    while let Some(n) = todo.pop() {
        reads = reads.saturating_add(1);
        if reads > MAX_SCHEMA_PAGES || n > pages {
            return Ok(None);
        }
        let Some(pgno) = PageNo::new(n, page_size) else {
            return Ok(None);
        };
        read(pgno, &mut buf)?;
        let header = if n == 1 { PAGE_ONE_HEADER } else { 0 };
        let Some((kind, pointers)) = cells(&buf, header) else {
            return Ok(None);
        };
        if kind == INTERIOR_TABLE {
            // The right-most child, then each cell's; the stack reads the left ones first.
            let right = header.checked_add(8).and_then(|at| be32(&buf, at));
            todo.extend(right);
            for at in pointers.iter().rev() {
                todo.extend(be32(&buf, *at));
            }
            continue;
        }
        for at in pointers {
            if let Some(root) = schema_row(&buf, at) {
                return Ok(PageNo::new(root, page_size));
            }
        }
    }
    Ok(None)
}

/// The root page a schema leaf cell at `at` names when it is the counter's table row.
fn schema_row(page: &[u8], at: usize) -> Option<u32> {
    let (_payload, rest) = varint(page, at)?;
    let (_rowid, rest) = varint(page, rest)?;
    let [(_, kind), (_, name), _, (serial, root)] = columns::<4>(page, rest)?;
    if kind != b"table" || name != TABLE_NAME {
        return None;
    }
    u32::try_from(serial_int(serial, root)?).ok()
}

/// The counter's value on its root page `page`: 0 for an empty table, the row's integer
/// otherwise. `None` for a page that is not a leaf holding that row or nothing, or a value
/// below zero.
#[must_use]
pub(super) fn value(page: &[u8]) -> Option<u64> {
    let (kind, pointers) = cells(page, 0)?;
    if kind != LEAF_TABLE {
        return None;
    }
    if pointers.is_empty() {
        return Some(0);
    }
    for at in pointers {
        let (_payload, rest) = varint(page, at)?;
        let (rowid, rest) = varint(page, rest)?;
        if rowid != 1 {
            continue;
        }
        let [_, (serial, bytes)] = columns::<2>(page, rest)?;
        return u64::try_from(serial_int(serial, bytes)?).ok();
    }
    None
}
