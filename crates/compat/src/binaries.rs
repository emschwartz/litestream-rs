//! Locating the Go binaries and the workspace, and the table of which Go builds the live
//! tests run against.
//!
//! [`GoVersion`] is the closed pinned set (committed fixtures need static names). The live
//! tests take a [`GoBuild`] instead: any release tag, a commit or `main`, with the
//! [`PageEncoding`] its name implies. [`builds_under_test`] is the one place that chooses the set.

use std::path::{Path, PathBuf};

use crate::error::CompatError;

/// The Go versions of the compatibility matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GoVersion {
    /// The first 0.5 release: LZ4 frame pages, ltx v0.5.0.
    V0_5_0,
    /// The last release that writes LZ4 frame pages.
    V0_5_15,
    /// The first release that writes LZ4 block pages.
    V0_5_16,
    /// A later release on the block encoding.
    V0_5_18,
    /// The pinned upstream commit 041a8c3 the research read.
    Head,
}

impl GoVersion {
    /// Every version, oldest first.
    pub const ALL: [Self; 5] = [
        Self::V0_5_0,
        Self::V0_5_15,
        Self::V0_5_16,
        Self::V0_5_18,
        Self::Head,
    ];

    /// The name used in binary and fixture paths (tag, or short commit for `Head`).
    pub const fn name(self) -> &'static str {
        match self {
            Self::V0_5_0 => "v0.5.0",
            Self::V0_5_15 => "v0.5.15",
            Self::V0_5_16 => "v0.5.16",
            Self::V0_5_18 => "v0.5.18",
            Self::Head => "041a8c3",
        }
    }

    /// The version a name denotes.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.name() == name)
    }

    /// Whether this version writes the legacy LZ4 frame page encoding.
    pub const fn writes_lz4_frames(self) -> bool {
        match self {
            Self::V0_5_0 | Self::V0_5_15 => true,
            Self::V0_5_16 | Self::V0_5_18 | Self::Head => false,
        }
    }
}

/// Which page encoding a Go build writes and reads, the one fact every live expectation
/// depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PageEncoding {
    /// LZ4 frame pages (`v0.5.N`, N up to 15): reads only frame-encoded files, so it refuses
    /// a replica Rust wrote.
    Frame,
    /// LZ4 block pages (v0.5.16 and later, any commit, `main`).
    Block,
}

/// The last release tag on the frame encoding.
const LAST_FRAMES_PATCH: u32 = 15;

/// The first release tag whose VFS takes `LITESTREAM_REPLICA_URL`, so reads a file replica.
const FIRST_FILE_VFS_PATCH: u32 = 3;

/// The first release tag that retries a snapshot the database was not ready for, instead of
/// waiting a whole snapshot interval (upstream feb65fb, #888).
const FIRST_SNAPSHOT_RETRY_PATCH: u32 = 4;

/// The first release tag that stops reading the WAL at the wal-index's committed end
/// (upstream [`WAL_INDEX_END_COMMIT`], #1581), or `None` while no release contains that
/// commit. When upstream tags one, this becomes `Some(N)`: `wal_index_end_follows_the_clone`
/// in `tests/compat/releases.rs` fails until it does.
pub const FIRST_WAL_INDEX_END_PATCH: Option<u32> = None;

/// The upstream commit that made Go read the committed WAL end from the wal-index:
/// "fix(db): do not replicate WAL frames past the wal-index mxFrame (#1581)", 2026-11-19.
pub const WAL_INDEX_END_COMMIT: &str = "bcd69ae";

/// A Go build the live tests run: its binary name (a tag, a short commit or `main`) and
/// the page encoding derived from it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GoBuild {
    name: String,
    page_encoding: PageEncoding,
}

impl GoBuild {
    /// The build named `name`. A release tag `v0.5.N` with N up to 15 is [`PageEncoding::Frame`];
    /// anything else (a later tag, a commit, `main`) is [`PageEncoding::Block`].
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            page_encoding: page_encoding_of(name),
        }
    }

    /// The name used in the binary path.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The page encoding this build writes and reads.
    pub const fn page_encoding(&self) -> PageEncoding {
        self.page_encoding
    }

    /// The release quirk recorded for this build, if any.
    pub fn quirk(&self) -> Option<&'static QuirkEntry> {
        QUIRKS.iter().find(|q| q.name == self.name)
    }

    /// Where the built binary lives; an error naming the build script when absent.
    pub fn binary(&self) -> Result<PathBuf, CompatError> {
        binary_named(&self.name)
    }

    /// Whether this build's read-replica VFS can open a file replica. Every 0.5 release
    /// ships the VFS; before v0.5.3 it builds an S3 client from `LITESTREAM_S3_*` variables
    /// and nothing else (`cmd/litestream-vfs/litestream-vfs.go` at v0.5.0), so the build
    /// script does not build it.
    pub fn vfs_reads_files(&self) -> bool {
        release_patch(&self.name).is_none_or(|n| n >= FIRST_FILE_VFS_PATCH)
    }

    /// Whether this build writes its first snapshot soon after it starts. Every 0.5 release
    /// tries one at start; before v0.5.4 a try made before the database's first sync fails
    /// and the next waits the whole snapshot interval (`store.go`, `monitorCompactionLevel`,
    /// fixed upstream in feb65fb), so a short run may never see a snapshot from it.
    pub fn snapshots_soon_after_start(&self) -> bool {
        release_patch(&self.name).is_none_or(|n| n >= FIRST_SNAPSHOT_RETRY_PATCH)
    }

    /// Whether this build replicates only WAL frames SQLite committed. A frame is committed
    /// once the wal-index's `mxFrame` covers it; a writer stopped after writing its frames but
    /// before updating the wal-index leaves valid frames past that end, which the next commit
    /// overwrites. Releases through v0.5.18 read the WAL file to its last valid frame, so one
    /// that boots on such a tail while the application keeps its connection open can replicate
    /// a transaction that never happened (#38). Upstream [`WAL_INDEX_END_COMMIT`] reads
    /// `mxFrame` first (`walIndexMaxFrame`, `db.go`); no release contains it yet
    /// ([`FIRST_WAL_INDEX_END_PATCH`]).
    ///
    /// A commit or `main` counts as containing it: the pinned 041a8c3 and upstream's newest commit both
    /// do. A commit older than the fix is then held to the strict expectation, so it can fail
    /// loudly on the bug but never pass by an exception it should not get.
    pub fn reads_wal_index_end(&self) -> bool {
        release_patch(&self.name)
            .is_none_or(|n| FIRST_WAL_INDEX_END_PATCH.is_some_and(|first| n >= first))
    }

    /// Where the built VFS extension lives; an error naming the build script when absent.
    pub fn vfs_library(&self) -> Result<PathBuf, CompatError> {
        file_in_bin(&format!("litestream-vfs-{}.so", self.name))
    }
}

impl From<GoVersion> for GoBuild {
    fn from(version: GoVersion) -> Self {
        Self::new(version.name())
    }
}

/// N of a release tag `v0.5.N`; `None` for anything else (a commit, `main`, a pre-release).
fn release_patch(name: &str) -> Option<u32> {
    name.strip_prefix("v0.5.")
        .filter(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()))
        .and_then(|n| n.parse::<u32>().ok())
}

fn page_encoding_of(name: &str) -> PageEncoding {
    match release_patch(name) {
        Some(n) if n <= LAST_FRAMES_PATCH => PageEncoding::Frame,
        Some(_) | None => PageEncoding::Block,
    }
}

/// How a release differs from its page encoding in a way a test sees. Tests assert the behaviour, they
/// never skip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quirk {
    /// The current Go toolchain cannot build the tag (default `GOTOOLCHAIN`, no source patch).
    Unbuildable,
}

/// One row of [`QUIRKS`].
#[derive(Debug, Clone, Copy)]
pub struct QuirkEntry {
    /// The release tag.
    pub name: &'static str,
    /// What differs.
    pub quirk: Quirk,
    /// Why, read from upstream's source or history (or the build error line), with the
    /// commit or issue when found.
    pub reason: &'static str,
}

/// Every release that differs from its page encoding, or cannot be built. Empty means every release
/// behaves as its page encoding says.
pub const QUIRKS: &[QuirkEntry] = &[];

/// Every upstream release tag `v0.5.N` (no pre-release suffix) the table knows. A tag in the
/// clone that is not here fails `every_release_tag_is_known`, so a new upstream release
/// needs a page-encoding decision here, never a silent drop.
pub const KNOWN_RELEASES: [&str; 19] = [
    "v0.5.0", "v0.5.1", "v0.5.2", "v0.5.3", "v0.5.4", "v0.5.5", "v0.5.6", "v0.5.7", "v0.5.8",
    "v0.5.9", "v0.5.10", "v0.5.11", "v0.5.12", "v0.5.13", "v0.5.14", "v0.5.15", "v0.5.16",
    "v0.5.17", "v0.5.18",
];

/// The environment variable naming the builds to test, space separated.
pub const VERSIONS_ENV: &str = "LSR_COMPAT_VERSIONS";

/// The module every live test sits in, which `just compat-live` selects by
/// (`test(/(^|::)live::/)`).
pub const LIVE_MODULE: &str = "live";

/// Whether a test path (`handover`'s `live::go::rust_restores_what_go_wrote`) is in a
/// [`LIVE_MODULE`], so `just compat-live` runs it.
#[must_use]
pub fn is_live_test(path: &str) -> bool {
    in_module(path, LIVE_MODULE)
}

/// The module the live tests over MinIO sit in. `just compat-s3` runs them on their own,
/// since they need a MinIO server, so `just compat-live` leaves them out on purpose.
pub const S3_MODULE: &str = "s3";

/// Whether a test path names `module` among the modules it sits in.
fn in_module(path: &str, module: &str) -> bool {
    let modules = path
        .rsplit_once("::")
        .map_or("", |(modules, _test)| modules);
    modules.split("::").any(|m| m == module)
}

/// The builds the live tests run against: the names in `LSR_COMPAT_VERSIONS` when set
/// (`releases` stands for [`KNOWN_RELEASES`]; minus any the quirk table lists as unbuildable),
/// else the pinned five.
///
/// Only a test in a [`LIVE_MODULE`] may ask (the test harness names the thread after the
/// test). A live test moved or renamed out of one would drop out of `just compat-live`
/// unnoticed; this fails it instead, the first time `just compat` runs it.
#[expect(
    clippy::panic,
    reason = "the guard fails the misplaced test itself; returning no builds would pass it"
)]
pub fn builds_under_test() -> Vec<GoBuild> {
    let caller = std::thread::current().name().unwrap_or_default().to_owned();
    if !is_live_test(&caller) && !in_module(&caller, S3_MODULE) {
        panic!(
            "{caller:?} runs the Go builds under test but is not in a `mod {LIVE_MODULE}` (or `mod {S3_MODULE}` for MinIO), so `just compat-live` would skip it"
        );
    }
    match std::env::var(VERSIONS_ENV) {
        Ok(names) if !names.trim().is_empty() => names
            .split_whitespace()
            .flat_map(|n| {
                if n == "releases" {
                    KNOWN_RELEASES.to_vec()
                } else {
                    vec![n]
                }
            })
            .map(GoBuild::new)
            .filter(|b| b.quirk().is_none_or(|q| q.quirk != Quirk::Unbuildable))
            .collect(),
        Ok(_) | Err(_) => GoVersion::ALL.into_iter().map(GoBuild::from).collect(),
    }
}

/// The builds of `builds_under_test` in `page_encoding`, with its guard.
pub fn builds_in(page_encoding: PageEncoding) -> Vec<GoBuild> {
    builds_under_test()
        .into_iter()
        .filter(|b| b.page_encoding() == page_encoding)
        .collect()
}

/// The workspace root (the worktree this crate was built in).
pub fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest.to_path_buf(), Path::to_path_buf)
}

/// The directory holding `.upstream`: `LSR_UPSTREAM`'s parent when set, else the nearest
/// ancestor of the workspace root that has one (a worktree shares its main checkout's).
fn upstream_bin_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("LSR_UPSTREAM") {
        return Some(PathBuf::from(dir).join("bin"));
    }
    workspace_root()
        .ancestors()
        .map(|a| a.join(".upstream").join("bin"))
        .find(|p| p.is_dir())
}

/// Where the built binary of `version` lives; an error naming the build script when absent.
pub fn go_binary(version: GoVersion) -> Result<PathBuf, CompatError> {
    binary_named(version.name())
}

fn binary_named(name: &str) -> Result<PathBuf, CompatError> {
    file_in_bin(&format!("litestream-{name}"))
}

fn file_in_bin(file: &str) -> Result<PathBuf, CompatError> {
    let path = upstream_bin_dir().map_or_else(|| PathBuf::from(file), |d| d.join(file));
    if path.is_file() {
        Ok(path)
    } else {
        Err(CompatError::MissingBinary { path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_test_is_one_in_a_live_module() {
        assert!(is_live_test("live::go_vfs_reads_rust_replica"));
        assert!(is_live_test("live::go::rust_restores_what_go_wrote"));
        assert!(is_live_test("tests::live::x"));
        assert!(!is_live_test("live"));
        assert!(!is_live_test("live_go::go_resumes_from_rust_meta"));
        assert!(!is_live_test("process::live"));
        assert!(!is_live_test("go_vfs_reads_rust_replica"));
        assert!(in_module("s3::over_s3_go_restores_rust_replica", S3_MODULE));
        assert!(!is_live_test("s3::over_s3_go_restores_rust_replica"));
    }

    /// The guard: a test outside a live module that asks for the builds fails, so moving
    /// one out of `just compat-live`'s filter cannot pass unnoticed.
    #[test]
    #[should_panic(expected = "is not in a `mod live`")]
    fn the_builds_are_refused_outside_a_live_module() {
        drop(builds_under_test());
    }

    mod live {
        use super::super::builds_under_test;

        #[test]
        fn a_live_module_gets_the_builds() {
            assert!(!builds_under_test().is_empty());
        }
    }

    #[test]
    fn names_round_trip() {
        for v in GoVersion::ALL {
            assert_eq!(GoVersion::parse(v.name()), Some(v));
        }
        assert_eq!(GoVersion::parse("v9"), None);
    }

    #[test]
    fn era_follows_the_name() {
        for (name, page_encoding) in [
            ("v0.5.0", PageEncoding::Frame),
            ("v0.5.9", PageEncoding::Frame),
            ("v0.5.15", PageEncoding::Frame),
            ("v0.5.16", PageEncoding::Block),
            ("v0.5.100", PageEncoding::Block),
            ("041a8c3", PageEncoding::Block),
            ("main", PageEncoding::Block),
            ("v0.5.3-beta3", PageEncoding::Block),
        ] {
            assert_eq!(GoBuild::new(name).page_encoding(), page_encoding, "{name}");
        }
    }

    #[test]
    fn the_vfs_reads_files_from_v0_5_3() {
        for (name, files) in [
            ("v0.5.0", false),
            ("v0.5.2", false),
            ("v0.5.3", true),
            ("v0.5.15", true),
            ("v0.5.18", true),
            ("041a8c3", true),
            ("main", true),
        ] {
            assert_eq!(GoBuild::new(name).vfs_reads_files(), files, "{name}");
        }
    }

    #[test]
    fn only_builds_with_the_upstream_fix_read_the_wal_index_end() {
        for (name, fixed) in [
            ("v0.5.0", false),
            ("v0.5.16", false),
            ("v0.5.18", false),
            ("041a8c3", true),
            ("main", true),
        ] {
            assert_eq!(GoBuild::new(name).reads_wal_index_end(), fixed, "{name}");
        }
    }

    #[test]
    fn pinned_versions_agree_with_their_era() {
        for v in GoVersion::ALL {
            let frames = GoBuild::from(v).page_encoding() == PageEncoding::Frame;
            assert_eq!(frames, v.writes_lz4_frames(), "{}", v.name());
        }
    }

    #[test]
    fn the_known_table_is_ordered_and_complete() {
        for (patch, name) in KNOWN_RELEASES.iter().enumerate() {
            assert_eq!(*name, format!("v0.5.{patch}"));
        }
    }

    #[test]
    fn frame_writers_are_the_first_two() {
        let frames: Vec<_> = GoVersion::ALL
            .into_iter()
            .filter(|v| v.writes_lz4_frames())
            .collect();
        assert_eq!(frames, [GoVersion::V0_5_0, GoVersion::V0_5_15]);
    }
}
