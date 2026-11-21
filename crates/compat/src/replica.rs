//! Reading a file replica directory the way Go lays it out.

use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::error::CompatError;
use lsr_core::{CreatedAt, ListedTime};
use lsr_core::{FileInfo, FileKey, Level, parse_file_name};
use lsr_ltx::{Event, HEADER_LEN, Header, ReadDecoder, Timestamp, Txid, lock_page};

/// One LTX file of a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFile {
    /// Path relative to the replica root, `ltx/<level>/<min>-<max>.ltx`.
    pub rel: String,
    /// The parsed key.
    pub key: FileKey,
    /// Size in bytes.
    pub size: u64,
    /// Modification time, Unix milliseconds.
    pub mtime_ms: i64,
}

/// A replica directory listing: its LTX files and the names that are not LTX keys.
#[derive(Debug, Default)]
pub struct Listing {
    /// The LTX files, sorted by key.
    pub files: Vec<ReplicaFile>,
    /// Relative names that are not LTX keys (temp files of an upload in flight, for one).
    pub unrecognized: Vec<String>,
}

/// The key of a path relative to a file replica, `ltx/<level>/<min>-<max>.ltx` with the level as
/// one decimal digit, exactly as Go's file replica writes it; `None` for any other name.
pub fn parse_rel(rel: &str) -> Option<FileKey> {
    let (level, name) = rel.strip_prefix("ltx/")?.split_once('/')?;
    let one_digit = level.len() == 1 && level.bytes().all(|b| b.is_ascii_digit());
    let level = Level::new(level.parse().ok().filter(|_| one_digit)?)?;
    Some(FileKey::new(level, parse_file_name(name).ok()?))
}

/// The path of a key relative to a file replica; the inverse of [`parse_rel`].
pub fn rel_of(key: &FileKey) -> String {
    format!("ltx/{}/{}", key.level(), key.file_name())
}

/// The planner's view of a listed file: its key, size and creation time.
pub fn info_of(file: &ReplicaFile) -> FileInfo {
    FileInfo {
        key: file.key,
        size: file.size,
        listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(file.mtime_ms))),
    }
}

fn mtime_ms(meta: &fs::Metadata, path: &Path) -> Result<i64, CompatError> {
    let since = meta
        .modified()
        .map_err(CompatError::io(path))?
        .duration_since(UNIX_EPOCH)
        .map_err(|e| CompatError::Fixture(format!("{} predates 1970: {e}", path.display())))?;
    i64::try_from(since.as_millis())
        .map_err(|e| CompatError::Fixture(format!("time overflow: {e}")))
}

/// Lists `<replica>/ltx/<level>/*`; a missing `ltx` directory is an empty replica.
pub fn list(replica: &Path) -> Result<Listing, CompatError> {
    let root = replica.join("ltx");
    let mut out = Listing::default();
    let Ok(levels) = fs::read_dir(&root) else {
        return Ok(out);
    };
    for level in levels {
        let level_path = level.map_err(CompatError::io(&root))?.path();
        let level_name = level_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_owned();
        if !level_path.is_dir() {
            out.unrecognized.push(format!("ltx/{level_name}"));
            continue;
        }
        for entry in fs::read_dir(&level_path).map_err(CompatError::io(&level_path))? {
            let path = entry.map_err(CompatError::io(&level_path))?.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("?")
                .to_owned();
            let rel = format!("ltx/{level_name}/{name}");
            let Some(key) = parse_rel(&rel) else {
                out.unrecognized.push(rel);
                continue;
            };
            // Compaction deletes level-0 files while a live replica is listed.
            let meta = match fs::metadata(&path) {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(CompatError::io(&path)(e)),
            };
            out.files.push(ReplicaFile {
                rel,
                key,
                size: meta.len(),
                mtime_ms: mtime_ms(&meta, &path)?,
            });
        }
    }
    out.files.sort_by_key(|f| f.key);
    out.unrecognized.sort();
    Ok(out)
}

/// The newest level-0 file and its header; `None` when there is none yet or it vanished
/// between listing and reading (compaction deletes level-0 files).
pub fn newest_l0(replica: &Path) -> Result<Option<Header>, CompatError> {
    let listing = list(replica)?;
    let Some(file) = listing
        .files
        .iter()
        .filter(|f| f.key.level() == Level::ZERO)
        .max_by_key(|f| f.key.range().max())
    else {
        return Ok(None);
    };
    let path = replica.join(&file.rel);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(None);
    };
    let head: Option<&[u8; HEADER_LEN]> = bytes.get(..HEADER_LEN).and_then(|b| b.try_into().ok());
    Ok(head.and_then(|h| Header::decode(h).ok()))
}

/// One LTX file read whole and verified: its header and how many pages it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// The file's header.
    pub header: Header,
    /// The number of pages in the file.
    pub pages: u32,
}

impl Summary {
    /// Whether the file holds every page of the database (its commit size, less the lock
    /// page when the database reaches it): what a snapshot holds, whatever its transaction
    /// range. A level-0 file that continues a WAL holds only the pages its transactions wrote.
    pub fn holds_every_page(&self) -> bool {
        let commit = self.header.fields().commit;
        let lock = u32::from(commit >= lock_page(self.header.page_size()));
        self.pages.saturating_add(lock) >= commit
    }
}

/// Reads and verifies the LTX file at `path`; `None` when it vanished first (compaction and
/// retention delete files while a live replica is read).
///
/// # Errors
/// [`CompatError::Fixture`] when the file does not decode or verify.
pub fn summarize(path: &Path) -> Result<Option<Summary>, CompatError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(CompatError::io(path)(e)),
    };
    summarize_bytes(&bytes, &path.display().to_string()).map(Some)
}

/// Decodes and verifies one LTX file held in memory; `name` labels a failure.
///
/// # Errors
/// [`CompatError::Fixture`] when the bytes do not decode or verify.
pub fn summarize_bytes(bytes: &[u8], name: &str) -> Result<Summary, CompatError> {
    let bad = |e: lsr_ltx::DecodeError| CompatError::Fixture(format!("{name}: {e}"));
    let mut decoder = ReadDecoder::new(bytes);
    let mut header = None;
    let mut pages: u32 = 0;
    while let Some(event) = decoder.next_event().map_err(bad)? {
        match event {
            Event::Header(h) => header = Some(h),
            Event::Page(_) => pages = pages.saturating_add(1),
        }
    }
    decoder.finish().map_err(bad)?;
    let header = header.ok_or_else(|| CompatError::Fixture(format!("{name}: no header")))?;
    Ok(Summary { header, pages })
}

/// The level-0 file whose transactions start at `min`, from the first of `dirs` that holds
/// one (a meta directory and a replica share the layout), read whole; `None` when no
/// directory has it yet.
///
/// # Errors
/// [`CompatError`] when a listing fails or the file does not decode.
pub fn l0_starting_at(dirs: &[&Path], min: Txid) -> Result<Option<Summary>, CompatError> {
    for dir in dirs {
        let found = list(dir)?
            .files
            .into_iter()
            .find(|f| f.key.level() == Level::ZERO && f.key.range().min() == min);
        if let Some(file) = found
            && let Some(summary) = summarize(&dir.join(&file.rel))?
        {
            return Ok(Some(summary));
        }
    }
    Ok(None)
}

/// The newest transaction any file of any level in `dirs` covers.
///
/// # Errors
/// [`CompatError`] when a listing fails.
pub fn newest_txid(dirs: &[&Path]) -> Result<Option<Txid>, CompatError> {
    let mut newest = None;
    for dir in dirs {
        let here = list(dir)?.files.iter().map(|f| f.key.range().max()).max();
        newest = newest.max(here);
    }
    Ok(newest)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "0000000000000001-0000000000000a0b.ltx";

    #[test]
    fn go_file_keys_round_trip() {
        let rel = format!("ltx/9/{NAME}");
        let key = parse_rel(&rel).expect("a key");
        assert_eq!(key.level(), Level::SNAPSHOT);
        assert_eq!(rel_of(&key), rel);
    }

    #[test]
    fn names_go_would_not_write_are_unrecognized() {
        for rel in [
            format!("ltx/10/{NAME}"),
            format!("ltx/00/{NAME}"),
            format!("ltx/x/{NAME}"),
            format!("0009/{NAME}"),
            format!("0/{NAME}"),
            "ltx/0/0000000000000001-0000000000000002.tmp".to_owned(),
            "ltx/0/0000000000000003-0000000000000002.ltx".to_owned(),
            "ltx/0/0000000000000000-0000000000000002.ltx".to_owned(),
        ] {
            assert!(parse_rel(&rel).is_none(), "{rel}");
        }
    }
}
