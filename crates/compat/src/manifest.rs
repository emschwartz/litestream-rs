//! The fixture manifest: what is in a replica directory and what Go restored from it.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::binaries::workspace_root;
use crate::error::CompatError;
use crate::ltxkey::{KeyLayout, LocalKey};
use crate::pages::PageDigest;

/// The manifest file name inside a fixture directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// The version of the manifest layout.
pub const FIXTURE_FORMAT: u32 = 1;

/// A restore target. TEMPORARY shape: `lsr-core`'s `RestoreTarget` replaces it (task 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    /// The newest state.
    Latest,
    /// Up to and including this TXID.
    Txid {
        /// The TXID.
        txid: u64,
    },
    /// As of this time.
    AsOf {
        /// Unix milliseconds.
        unix_ms: i64,
    },
}

/// What a restore produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    /// The pages that version's own `litestream restore` wrote.
    Restored {
        /// Page size in bytes.
        page_size: u32,
        /// Number of pages.
        page_count: u32,
        /// Hex SHA-256 of each page.
        page_hashes: Vec<String>,
    },
    /// The chain has a hole; the expected answer is a gap, never an earlier state.
    Gap {
        /// Highest TXID reachable contiguously from the snapshot.
        have_through: u64,
        /// Lowest TXID of a file beyond the hole.
        next_starts: u64,
        /// Whether Go's restore exited non-zero. False records that Go silently answered.
        go_refused: bool,
    },
}

impl Outcome {
    /// A restored outcome from a page digest.
    pub fn restored(digest: PageDigest) -> Self {
        Self::Restored {
            page_size: digest.page_size,
            page_count: digest.page_count,
            page_hashes: digest.page_hashes,
        }
    }
}

/// One replica file, relative to the fixture's `replica` directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Go file-replica key, `ltx/<level>/<min>-<max>.ltx`.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Modification time in Unix milliseconds: the replica's creation time for the file, which
    /// git does not keep (see [`Manifest::apply_mtimes`]).
    pub created_at_ms: i64,
}

/// A restore target with the answer recorded for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetEntry {
    /// The target.
    pub target: Target,
    /// The answer.
    pub outcome: Outcome,
}

/// Everything a later test needs to use a fixture without Go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest layout version, [`FIXTURE_FORMAT`].
    pub format: u32,
    /// The Go version that wrote the replica and produced the outcomes (its name).
    pub go_version: String,
    /// The scenario name.
    pub scenario: String,
    /// The workload seed.
    pub seed: u64,
    /// Database page size.
    pub page_size: u32,
    /// The largest page count the database reached.
    pub max_page_count: u32,
    /// The page count at the end.
    pub final_page_count: u32,
    /// Every file of the replica, in key order.
    pub files: Vec<FileEntry>,
    /// The targets and their answers.
    pub targets: Vec<TargetEntry>,
}

/// `crates/compat/fixtures`.
pub fn fixtures_root() -> PathBuf {
    workspace_root()
        .join("crates")
        .join("compat")
        .join("fixtures")
}

/// The directory of one fixture.
pub fn fixture_dir(go_version: &str, scenario: &str) -> PathBuf {
    fixtures_root().join(go_version).join(scenario)
}

/// Every fixture directory (a directory holding a manifest), sorted.
pub fn all_fixtures() -> Result<Vec<PathBuf>, CompatError> {
    let root = fixtures_root();
    let mut out = Vec::new();
    for version in sorted_dirs(&root)? {
        for scenario in sorted_dirs(&version)? {
            if scenario.join(MANIFEST_FILE).is_file() {
                out.push(scenario);
            }
        }
    }
    Ok(out)
}

fn sorted_dirs(dir: &Path) -> Result<Vec<PathBuf>, CompatError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(CompatError::io(dir))? {
        let path = entry.map_err(CompatError::io(dir))?.path();
        if path.is_dir() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

impl Manifest {
    /// Loads `manifest.json` from a fixture directory.
    pub fn load(fixture: &Path) -> Result<Self, CompatError> {
        let path = fixture.join(MANIFEST_FILE);
        let text = fs::read_to_string(&path).map_err(CompatError::io(&path))?;
        serde_json::from_str(&text).map_err(|source| CompatError::Json { path, source })
    }

    /// Writes `manifest.json` into a fixture directory.
    pub fn save(&self, fixture: &Path) -> Result<(), CompatError> {
        let path = fixture.join(MANIFEST_FILE);
        let mut text = serde_json::to_string_pretty(self).map_err(|source| CompatError::Json {
            path: path.clone(),
            source,
        })?;
        text.push('\n');
        fs::write(&path, text).map_err(CompatError::io(&path))
    }

    /// The replica directory of a fixture.
    pub fn replica_dir(fixture: &Path) -> PathBuf {
        fixture.join("replica")
    }

    /// Sets every replica file's mtime to its recorded `created_at_ms`; a file replica reads
    /// creation time from mtime, and a git checkout resets it.
    pub fn apply_mtimes(&self, fixture: &Path) -> Result<(), CompatError> {
        let replica = Self::replica_dir(fixture);
        for entry in &self.files {
            let path = replica.join(&entry.key);
            let millis = u64::try_from(entry.created_at_ms).map_err(|_negative| {
                CompatError::Fixture(format!("{} has a negative time", entry.key))
            })?;
            let when = UNIX_EPOCH
                .checked_add(Duration::from_millis(millis))
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let file = fs::File::options()
                .write(true)
                .open(&path)
                .map_err(CompatError::io(&path))?;
            file.set_modified(when).map_err(CompatError::io(&path))?;
        }
        Ok(())
    }

    /// Self-consistency: the files listed are exactly the files on disk with the listed sizes,
    /// every key parses as Go's layout, the targets cover latest, three TXIDs and two times,
    /// and every restored outcome's hash list matches its page count.
    pub fn check(&self, fixture: &Path) -> Result<(), CompatError> {
        let bad = |msg: String| CompatError::Fixture(format!("{}: {msg}", fixture.display()));
        if self.format != FIXTURE_FORMAT {
            return Err(bad(format!(
                "format {} is not {FIXTURE_FORMAT}",
                self.format
            )));
        }
        let listed = self.check_files(fixture, &bad)?;
        let on_disk = list_replica_files(&Self::replica_dir(fixture))?;
        if listed != on_disk {
            return Err(bad("manifest files and replica files differ".to_owned()));
        }
        self.check_targets(&bad)
    }

    fn check_files(
        &self,
        fixture: &Path,
        bad: &dyn Fn(String) -> CompatError,
    ) -> Result<BTreeSet<String>, CompatError> {
        let mut listed = BTreeSet::new();
        for entry in &self.files {
            LocalKey::parse(&entry.key, KeyLayout::File)?;
            let path = Self::replica_dir(fixture).join(&entry.key);
            let size = fs::metadata(&path).map_err(CompatError::io(&path))?.len();
            if size != entry.size {
                return Err(bad(format!(
                    "{} is {size} bytes, manifest says {}",
                    entry.key, entry.size
                )));
            }
            if !listed.insert(entry.key.clone()) {
                return Err(bad(format!("{} listed twice", entry.key)));
            }
        }
        Ok(listed)
    }

    fn check_targets(&self, bad: &dyn Fn(String) -> CompatError) -> Result<(), CompatError> {
        let count = |f: fn(&Target) -> bool| self.targets.iter().filter(|t| f(&t.target)).count();
        let latest = count(|t| matches!(t, Target::Latest));
        let txids = count(|t| matches!(t, Target::Txid { .. }));
        let times = count(|t| matches!(t, Target::AsOf { .. }));
        if latest != 1 || txids < 3 || times < 2 {
            return Err(bad(format!(
                "targets: {latest} latest, {txids} txid, {times} as-of"
            )));
        }
        for t in &self.targets {
            if let Outcome::Restored {
                page_size,
                page_count,
                page_hashes,
            } = &t.outcome
            {
                let counted = u32::try_from(page_hashes.len()).ok();
                if counted != Some(*page_count) || *page_size != self.page_size {
                    return Err(bad(format!(
                        "{:?}: hashes do not match its page count or size",
                        t.target
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The keys of every file under a replica directory; a file that is not an LTX key is an error.
fn list_replica_files(replica: &Path) -> Result<BTreeSet<String>, CompatError> {
    let listing = crate::replica::list(replica)?;
    if let Some(stray) = listing.foreign.first() {
        return Err(CompatError::BadKey(stray.clone()));
    }
    Ok(listing.files.into_iter().map(|f| f.rel).collect())
}
