//! Page hashing and integrity checking of database files.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::CompatError;

/// The content of a database file as page hashes: what equality between a Go restore and a
/// Rust restore is defined on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageDigest {
    /// Page size in bytes.
    pub page_size: u32,
    /// Number of pages.
    pub page_count: u32,
    /// Lowercase hex SHA-256 of each page, in page order.
    pub page_hashes: Vec<String>,
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// The page size recorded in a SQLite header (offset 16, big-endian; 1 means 65536).
fn header_page_size(header: &[u8]) -> Option<u32> {
    let raw = u16::from_be_bytes(header.get(16..18)?.try_into().ok()?);
    let size = if raw == 1 { 65536 } else { u32::from(raw) };
    (size.is_power_of_two() && (512..=65536).contains(&size)).then_some(size)
}

/// Hashes every page of the database file at `path`, reading the page size from its header.
pub fn hash_pages(path: &Path) -> Result<PageDigest, CompatError> {
    let not_pages = |why: &str| CompatError::NotPages {
        path: path.to_path_buf(),
        why: why.to_owned(),
    };
    let mut file = File::open(path).map_err(CompatError::io(path))?;
    let mut head = [0u8; 100];
    file.read_exact(&mut head)
        .map_err(|_short| not_pages("shorter than a header"))?;
    let page_size = header_page_size(&head).ok_or_else(|| not_pages("header page size invalid"))?;
    let len = file.metadata().map_err(CompatError::io(path))?.len();
    let size64 = u64::from(page_size);
    if len == 0 || len.checked_rem(size64) != Some(0) {
        return Err(not_pages("length is not a whole number of pages"));
    }
    let page_count = u32::try_from(len.checked_div(size64).unwrap_or(0))
        .map_err(|_big| not_pages("more than u32 pages"))?;
    let mut file = File::open(path).map_err(CompatError::io(path))?;
    let mut buf = vec![0u8; usize::try_from(page_size).unwrap_or(0)];
    let mut page_hashes = Vec::new();
    for _ in 0..page_count {
        file.read_exact(&mut buf).map_err(CompatError::io(path))?;
        page_hashes.push(sha256_hex(&buf));
    }
    Ok(PageDigest {
        page_size,
        page_count,
        page_hashes,
    })
}

/// Runs `PRAGMA integrity_check` on a copy-safe read-only connection; anything but a single
/// `ok` row is an error carrying SQLite's report.
pub fn integrity_check(path: &Path) -> Result<(), CompatError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    if rows == ["ok"] {
        Ok(())
    } else {
        Err(CompatError::Integrity {
            path: path.to_path_buf(),
            report: rows.join("; "),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Digests computed outside this code with `shasum -a 256` over the same bytes.
    const PAGE1_SHA: &str = "c6fee191f4aa19c140b2245c287303d617f504e824c7fd028ecaa1b5f119fee9";
    const PAGE2_SHA: &str = "847c7abf4f64e13f1641564318260d6b134fa1d065830bd260a7cc0012744c31";

    /// Two 512-byte pages: a SQLite header page and a page of 0xAB.
    fn two_page_file() -> Vec<u8> {
        let mut p1 = vec![0u8; 512];
        p1[..16].copy_from_slice(b"SQLite format 3\0");
        p1[16..18].copy_from_slice(&512u16.to_be_bytes());
        let mut file = p1;
        file.extend(std::iter::repeat_n(0xABu8, 512));
        file
    }

    #[test]
    fn hashes_a_hand_built_two_page_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("two.db");
        std::fs::write(&path, two_page_file()).expect("write");
        let d = hash_pages(&path).expect("hash");
        assert_eq!((d.page_size, d.page_count), (512, 2));
        assert_eq!(d.page_hashes, [PAGE1_SHA, PAGE2_SHA]);
    }

    #[test]
    fn rejects_ragged_and_headerless_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ragged = dir.path().join("ragged.db");
        let mut bytes = two_page_file();
        bytes.push(0);
        std::fs::write(&ragged, bytes).expect("write");
        assert!(matches!(
            hash_pages(&ragged),
            Err(CompatError::NotPages { .. })
        ));
        let short = dir.path().join("short.db");
        std::fs::write(&short, b"tiny").expect("write");
        assert!(matches!(
            hash_pages(&short),
            Err(CompatError::NotPages { .. })
        ));
    }

    #[test]
    fn integrity_check_accepts_a_real_database_and_rejects_garbage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let good = dir.path().join("good.db");
        let conn = Connection::open(&good).expect("open");
        conn.execute_batch("CREATE TABLE t(a); INSERT INTO t VALUES (1);")
            .expect("seed");
        drop(conn);
        integrity_check(&good).expect("ok");
        let bad = dir.path().join("bad.db");
        std::fs::write(&bad, vec![0x55u8; 4096]).expect("write");
        assert!(integrity_check(&bad).is_err());
    }
}
