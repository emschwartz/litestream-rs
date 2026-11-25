//! Integration tests for lsr-compat: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod alternate;
#[cfg(test)]
mod alternation;
#[cfg(test)]
mod behavior;
#[cfg(test)]
mod compaction;
#[cfg(test)]
mod dropin_cli;
#[cfg(test)]
mod dropin_config;
#[cfg(test)]
mod dropin_creds;
#[cfg(test)]
mod dropin_dir;
#[cfg(test)]
mod dropin_inspect;
#[cfg(test)]
mod dropin_metrics;
#[cfg(test)]
mod dropin_once;
#[cfg(test)]
mod dropin_restore;
#[cfg(test)]
mod dropin_retention;
#[cfg(test)]
mod dropin_roles;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod handover;
#[cfg(test)]
mod layout;
#[cfg(test)]
mod live_matches_sim;
#[cfg(test)]
mod live_source;
#[cfg(test)]
mod matrix;
#[cfg(test)]
mod merged_snapshot;
#[cfg(test)]
mod pinned;
#[cfg(test)]
mod releases;
#[cfg(test)]
mod rust_chains;
#[cfg(test)]
mod s3;
#[cfg(test)]
mod seeded;
#[cfg(test)]
mod sidecar;
#[cfg(test)]
mod tracked_checksums;
#[cfg(test)]
mod upstream_test_map;
#[cfg(test)]
mod verify;
#[cfg(test)]
mod vfs;
