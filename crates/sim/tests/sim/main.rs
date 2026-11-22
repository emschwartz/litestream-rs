//! Integration tests for lsr-sim: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod after_restart;
#[cfg(test)]
mod crash_law;
#[cfg(test)]
mod dropped_created_at;
#[cfg(test)]
mod faults;
#[cfg(test)]
mod fixed_seeds;
#[cfg(test)]
mod replay;
#[cfg(test)]
mod retention;
#[cfg(test)]
mod snapshot_causes;
#[cfg(test)]
mod soak;
#[cfg(test)]
mod sqlite_rules;
#[cfg(test)]
mod tally;
#[cfg(test)]
mod unaided;
