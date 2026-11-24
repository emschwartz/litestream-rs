//! Fail-stop in the daemon (`if-replication-stops`):
//! what `/healthz`, `/metrics` and the log say of a trip, in one place.
//!
//! Every name and line here was approved by the owner (`docs/decisions.md` D84); they are kept
//! together so a change touches this module and nothing else. The exit code `-exec` ends with is the CLI's
//! (`Exit::AppStopped` in `lsr-cli`).
//!
//! **Every form.** The replicator holds the database's write lock while writes are held
//! (`lsr_replica::Health::writes`): every writer, in any process, gets SQLite's "database is
//! locked", and the log says `writes blocked` and later `writes allowed`
//! (`docs/decisions/D192.md`). A trip also makes `/healthz` 503, and the daemon stays up (an
//! exit would invite a restart that fixes nothing). So a plain sidecar may use `stop-app`.
//!
//! **`-exec`** ([`crate::supervise`]): the child starts once every database's gate is open; a
//! held gate keeps it from starting, and the daemon stays up answering 503. Under `stop-app` a
//! gate held while the child runs stops it: SIGTERM, the shutdown grace, SIGKILL, one last
//! drain, then the CLI's own exit code (decision D19).

use lsr_replica::{Gate, Trip, WhyBlocked};

use crate::health::stop_name;

/// The health and metric cause name of a trip: a stop's own name, or the trip's.
#[must_use]
pub(crate) const fn trip_name(trip: Trip) -> &'static str {
    match trip {
        Trip::Stopped(cause) => stop_name(cause),
        Trip::UnreplicatedTooLong { .. } => "unreplicated_too_long",
        Trip::RefusedAtStart(_) => "refused_at_start",
    }
}

/// The gate's name in `/healthz`.
#[must_use]
pub(crate) const fn gate_name(gate: Gate) -> &'static str {
    match gate {
        Gate::Pending => "pending",
        Gate::Allowed => "allowed",
        Gate::Held(_) => "held",
    }
}

/// Why writes are held, as the `writes blocked` line names it: `booting`, or the trip's name.
#[must_use]
pub(crate) const fn why_name(why: WhyBlocked) -> &'static str {
    match why {
        WhyBlocked::Booting => "booting",
        WhyBlocked::Tripped(trip) => trip_name(trip),
    }
}

/// The log line when the replicator takes the database's write lock.
pub(crate) const WRITES_BLOCKED: &str = "writes blocked";

/// The log line when it lets go of it.
pub(crate) const WRITES_ALLOWED: &str = "writes allowed";

/// The log line when a held gate keeps the `-exec` child from starting.
pub(crate) const START_REFUSED: &str = "start refused";

/// The log line when a held gate stops the running `-exec` child.
pub(crate) const APP_STOPPED: &str = "app stopped";