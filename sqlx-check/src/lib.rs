//! An application's connection setup on sqlx: sqlx's raw handle goes to
//! `lsr_replica::ConnectionSetup::apply_raw`. It compiles only when both crates link the same
//! libsqlite3-sys, whose `sqlite3` is then one type.

use lsr_replica::ConnectionSetup;
use sqlx::SqliteConnection;

/// Sets up one of the application's sqlx connections with `setup`
/// (`Replicator::connection_setup`, captured once by the pool's `after_connect`).
///
/// # Errors
/// sqlx's, when the handle cannot be locked, or the setup's.
pub async fn set_up(
    conn: &mut SqliteConnection,
    setup: &ConnectionSetup,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handle = conn.lock_handle().await?;
    let raw = handle.as_raw_handle().as_ptr();
    // SAFETY: the handle is open and locked to this task for the call, and sqlx links the same
    // SQLite as lsr-replica (one libsqlite3-sys in the graph).
    unsafe { setup.apply_raw(raw) }?;
    Ok(())
}
