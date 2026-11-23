//! Integration tests for lsr-sim: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod after_restart;
#[cfg(test)]
mod clock_reads;
#[cfg(test)]
mod crash_law;
#[cfg(test)]
mod differential;
#[cfg(test)]
mod dropped_created_at;
#[cfg(test)]
mod evicted_cure;
#[cfg(test)]
mod failstop;
#[cfg(test)]
mod faults;
#[cfg(test)]
mod fixed_seeds;
#[cfg(test)]
mod histories;
#[cfg(test)]
mod kill_after_fault;
#[cfg(test)]
mod known_wrong;
#[cfg(test)]
mod liveness;
#[cfg(test)]
mod marks;
#[cfg(test)]
mod replay;
#[cfg(test)]
mod restored_staging;
#[cfg(test)]
mod retention;
#[cfg(test)]
mod short_write;
#[cfg(test)]
mod snapshot_causes;
#[cfg(test)]
mod soak;
#[cfg(test)]
mod sqlite_rules;
#[cfg(test)]
mod stall_liveness;
#[cfg(test)]
mod survival;
#[cfg(test)]
mod tally;
#[cfg(test)]
mod unaided;
#[cfg(test)]
mod verify;
