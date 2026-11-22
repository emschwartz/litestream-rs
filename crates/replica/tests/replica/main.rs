//! Integration tests for lsr-replica: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod common;
#[cfg(test)]
mod gates;
#[cfg(test)]
mod rebuild;
#[cfg(test)]
mod replicator;
#[cfg(test)]
mod restored;
#[cfg(test)]
mod second_writer;
#[cfg(test)]
mod verify;
// Schedule exploration, only under `--cfg shuttle` (`just shuttle`).
#[cfg(test)]
#[cfg(shuttle)]
mod shuttle;
