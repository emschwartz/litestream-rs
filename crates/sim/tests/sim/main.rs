//! Integration tests for lsr-sim: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod after_restart;
#[cfg(test)]
#[cfg(test)]
mod clock_reads;
#[cfg(test)]
mod crash_law;
#[cfg(test)]
mod create_only;
#[cfg(test)]
mod differential;
#[cfg(test)]
mod dropped_created_at;
#[cfg(test)]
mod empty_replica_boot;
#[cfg(test)]
#[cfg(test)]
#[cfg(test)]
mod failstop;
#[cfg(test)]
mod faults;
mod final_retention;
mod final_snapshot;
#[cfg(test)]
mod fixed_seeds;
#[cfg(test)]
mod histories;
#[cfg(test)]
mod kill_after_fault;
#[cfg(test)]
mod level_holes;
#[cfg(test)]
mod marks;
#[cfg(test)]
mod needless_snapshot;
#[cfg(test)]
mod parts;
#[cfg(test)]
mod remote_s3;
#[cfg(test)]
mod replay;
#[cfg(test)]
mod restored_staging;
#[cfg(test)]
mod retention;
#[cfg(test)]
mod retention_windows;
#[cfg(test)]
mod short_write;
#[cfg(test)]
mod sliced;
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
