//! Locating the pinned Go binaries and the workspace.

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
    let file = format!("litestream-{}", version.name());
    let path = upstream_bin_dir().map_or_else(|| PathBuf::from(&file), |d| d.join(&file));
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
    fn names_round_trip() {
        for v in GoVersion::ALL {
            assert_eq!(GoVersion::parse(v.name()), Some(v));
        }
        assert_eq!(GoVersion::parse("v9"), None);
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
