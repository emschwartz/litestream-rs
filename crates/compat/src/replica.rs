//! Reading a file replica directory the way Go lays it out.

use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::error::CompatError;
use crate::ltxhdr::{HEADER_LEN, LtxHeader};
use crate::ltxkey::{KeyLayout, LocalKey};

/// One LTX file of a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFile {
    /// Path relative to the replica root, `ltx/<level>/<min>-<max>.ltx`.
    pub rel: String,
    /// The parsed key.
    pub key: LocalKey,
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
            let Ok(key) = LocalKey::parse(&rel, KeyLayout::File) else {
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
pub fn newest_l0(replica: &Path) -> Result<Option<LtxHeader>, CompatError> {
    let listing = list(replica)?;
    let Some(file) = listing
        .files
        .iter()
        .filter(|f| f.key.level == 0)
        .max_by_key(|f| f.key.max)
    else {
        return Ok(None);
    };
    let path = replica.join(&file.rel);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(None);
    };
    Ok(bytes.get(..HEADER_LEN).and_then(LtxHeader::parse))
}
