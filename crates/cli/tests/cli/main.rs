//! Integration tests for lsr-cli: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod ahead_of_replica;
mod older_copy;
#[cfg(test)]
mod cli;
#[cfg(test)]
mod common;
#[cfg(test)]
mod crash_loop;
#[cfg(test)]
mod daemon;
#[cfg(test)]
mod provider;
#[cfg(test)]
mod s3;
#[cfg(test)]
mod sidecar;
#[cfg(test)]
mod soak_tigris;
#[cfg(test)]
mod tigris;
