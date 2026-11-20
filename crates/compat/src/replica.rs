//! Reading a file replica directory the way Go lays it out.

use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::error::CompatError;
use lsr_core::{FileInfo, FileKey, Level, parse_file_name};
use lsr_core::{ListedStamp, StoredStamp};
use lsr_ltx::{HEADER_LEN, Header, Timestamp};

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
    pub foreign: Vec<String>,
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
        listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(
            file.mtime_ms,
        ))),
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
            out.foreign.push(format!("ltx/{level_name}"));
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
                out.foreign.push(rel);
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
    out.foreign.sort();
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
    fn names_go_would_not_write_are_foreign() {
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
