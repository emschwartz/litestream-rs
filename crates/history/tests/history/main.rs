//! The history judge's tests: the judge itself over hand-built trees, its independence from the
//! engine, and the standalone runs that drive the real engine on real SQLite and judge real
//! restores (`rig`: through power losses, `replicator`: through the public API, on a file
//! replica and on MinIO).

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod command;
#[cfg(test)]
mod judge;
#[cfg(test)]
mod manifest;
#[cfg(test)]
mod real;
#[cfg(test)]
mod replicator;
#[cfg(test)]
mod rig;
