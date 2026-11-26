//! Integration tests for lsr-sqlite: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

use std::path::Path;

/// The child processes' entry, the one test `lsr_sqlite::testing`'s separate-process application and
/// `common::spawn` start this binary again to run (by its name, `--exact`): a no-op in the test
/// run; in a child, the application or the role a module's `child` names.
#[test]
fn child_process_entry() {
    if lsr_sqlite::testing::serve_app() {
        return;
    }
    let Some((role, db)) = common::role() else {
        return;
    };
    assert!(child(&role, &db), "unknown role {role}");
}

fn child(role: &str, db: &Path) -> bool {
    #[cfg(target_os = "linux")]
    if sidecar_read_lock::child(role, db) {
        return true;
    }
    faults::child(role, db) || kill::child(role, db) || truncate_proof::child(role, db)
}

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod app_at_defaults;
#[cfg(test)]
mod big;
#[cfg(test)]
mod common;
#[cfg(test)]
mod crash_cycles;
#[cfg(test)]
mod descriptors;
#[cfg(test)]
mod errors;
#[cfg(test)]
mod faults;
#[cfg(test)]
mod kill;
#[cfg(test)]
mod leaving_wal;
#[cfg(test)]
mod own_checkpoint;
#[cfg(test)]
mod power_loss;
#[cfg(test)]
mod sidecar_read_lock;
#[cfg(test)]
mod sim_model;
#[cfg(test)]
mod sim_real;
#[cfg(test)]
mod truncate_proof;
#[cfg(test)]
mod wal_index;
#[cfg(test)]
mod wal_size;
