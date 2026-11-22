//! SQLite's own WAL recovery over a database's and a WAL's bytes: the ground truth of a power
//! loss, for the simulator's rules and for the real-SQLite fault suite alike.
//!
//! The rules' database pages are not a SQLite database (they are [`lsr_wal::testing::page`]s),
//! so no SQL statement can read them. Recovery does not need one. The probe writes the database
//! and the WAL into a temporary directory; for the rules' bytes ([`Header::Fake`]) the
//! database's first 100 bytes become a SQLite file header naming the page size, so the pager
//! opens at that size. A statement then
//! takes the shared lock, which opens the WAL (`pagerOpenWalIfPresent`) and runs
//! `walIndexRecover` on the empty wal-index, before the B-tree layer reads page 1 and gives up.
//! The probe reads the recovered wal-index header from the `-shm` file (its layout is SQLite's
//! file format, walformat.html: `isInit` at byte 12, `mxFrame` at 16, the database size at 20
//! and the last frame's checksum at 24, in native byte order; the salts at 32, copied from the
//! WAL header as they were, big-endian), then closes the connection, whose close checkpoints every recovered frame into the
//! database file. That file is the database as SQLite recovered it, page for page; page 1 keeps
//! the made-up header unless a recovered frame wrote page 1.

use std::io;

use lsr_ltx::PageSize;
use lsr_wal::{Salts, WalChecksum};
use rusqlite::{Connection, OpenFlags};

/// The database file's header, as given or made up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Header {
    /// The bytes are a SQLite database: its header stays.
    Real,
    /// The bytes are the rules' pages: page 1's first 100 bytes become a header naming the page
    /// size, and stay in the result unless a recovered frame wrote page 1.
    Fake,
}

/// What SQLite's recovery made of a database and its WAL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// The recovered `mxFrame`: the frames up to the last valid commit.
    pub mx_frame: u32,
    /// The database's size in pages after it.
    pub db_pages: u32,
    /// The salts the wal-index holds: the WAL header's when recovery took it.
    pub salts: Salts,
    /// The running checksum after the last recovered frame (the header's when there is none).
    pub checksum: WalChecksum,
    /// The database file after a full checkpoint of the recovered frames.
    pub image: Vec<u8>,
}

/// Why the probe has no answer.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// A file in the temporary directory.
    #[error("probe I/O: {0}")]
    Io(#[from] io::Error),
    /// SQLite refused to open the database at all.
    #[error("probe open: {0}")]
    Open(#[from] rusqlite::Error),
    /// The wal-index holds no header after the statement.
    #[error("the probe's wal-index holds no header")]
    NoIndex,
}

/// The header bytes a pager needs to open a file at `page_size`.
fn file_header(page_size: PageSize) -> [u8; 100] {
    let mut head = [0_u8; 100];
    if let Some(magic) = head.get_mut(..16) {
        magic.copy_from_slice(b"SQLite format 3\0");
    }
    // The 2-byte field holds 1 for 65536.
    let field = u16::try_from(page_size.get()).unwrap_or(1);
    if let Some(size) = head.get_mut(16..18) {
        size.copy_from_slice(&field.to_be_bytes());
    }
    // WAL read and write versions, then the payload fractions every database carries.
    for (at, byte) in [(18, 2), (19, 2), (21, 64), (22, 32), (23, 32)] {
        if let Some(slot) = head.get_mut(at) {
            *slot = byte;
        }
    }
    head
}

fn word(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    bytes.get(at..end)?.try_into().ok().map(u32::from_ne_bytes)
}

fn be_word(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    bytes.get(at..end)?.try_into().ok().map(u32::from_be_bytes)
}

/// The recovered wal-index header: `mxFrame`, the size, the salts and the checksum.
fn index_header(shm: &[u8]) -> Option<(u32, u32, Salts, WalChecksum)> {
    if shm.get(12).copied().unwrap_or(0) == 0 {
        return None;
    }
    let salts = Salts {
        salt1: be_word(shm, 32)?,
        salt2: be_word(shm, 36)?,
    };
    let checksum = WalChecksum::new(word(shm, 24)?, word(shm, 28)?);
    Some((word(shm, 16)?, word(shm, 20)?, salts, checksum))
}

/// Runs SQLite's recovery over `db` and `wal` (module doc); `wal` is `None` when there is no
/// WAL file.
///
/// # Errors
/// [`ProbeError`].
pub fn recover(
    page_size: PageSize,
    db: &[u8],
    wal: Option<&[u8]>,
    header: Header,
) -> Result<Recovered, ProbeError> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("probe.db");
    let mut probe = db.to_vec();
    if header == Header::Fake {
        let head = file_header(page_size);
        if probe.len() < page_size.bytes() {
            probe.resize(page_size.bytes(), 0);
        }
        if let Some(first) = probe.get_mut(..head.len()) {
            first.copy_from_slice(&head);
        }
    }
    std::fs::write(&path, &probe)?;
    if let Some(wal) = wal {
        std::fs::write(dir.path().join("probe.db-wal"), wal)?;
    }
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    // Fails reading page 1 when the WAL wrote the rules' bytes there; recovery ran first.
    let _ignored = conn.query_row("SELECT count(*) FROM sqlite_master", [], |_row| Ok(()));
    let shm = std::fs::read(dir.path().join("probe.db-shm"))?;
    let (mx_frame, db_pages, salts, checksum) = index_header(&shm).ok_or(ProbeError::NoIndex)?;
    conn.close().map_err(|(_conn, err)| err)?;
    let image = std::fs::read(&path)?;
    Ok(Recovered {
        mx_frame,
        db_pages,
        salts,
        checksum,
        image,
    })
}
