//! Fail-stop in the daemon (`if-replication-stops`, `docs/proposals/fail-stop.md` section 3):
//! what `/healthz`, `/metrics` and the log say of a trip, in one place.
//!
//! PROVISIONAL(#115): every name and line here is a proposal from the design's section 4,
//! awaiting the owner's approval one by one. They are kept together so the approval, or a
//! change, touches this module and nothing else. The exit code `-exec` ends with is the CLI's
//! (`Exit::AppStopped` in `lsr-cli`).
//!
//! **Plain sidecar.** The daemon cannot reach the application: a trip makes `/healthz` 503 and
//! the daemon stays up (an exit would invite a restart that fixes nothing). `stop-app` without
//! `-exec` is refused when the config is checked (decision D20).
//!
//! **`-exec`** ([`crate::supervise`]): the child starts once every database's gate is open; a
//! held gate keeps it from starting, and the daemon stays up answering 503. Under `stop-app` a
//! gate held while the child runs stops it: SIGTERM, the shutdown grace, SIGKILL, one last
//! drain, then the CLI's own exit code (decision D19).

use lsr_replica::{Gate, Trip};

use crate::health::stop_name;

/// The health and metric cause name of a trip: a stop's own name, or the trip's.
#[must_use]
pub(crate) const fn trip_name(trip: Trip) -> &'static str {
    match trip {
        Trip::Stopped(cause) => stop_name(cause),
        // PROVISIONAL(#115): `unreplicated_too_long`.
        Trip::UnreplicatedTooLong { .. } => "unreplicated_too_long",
        // PROVISIONAL(#115): `not_yet_reachable`.
        Trip::NotYetReachable(_) => "not_yet_reachable",
    }
}

/// The gate's name in `/healthz`.
#[must_use]
pub(crate) const fn gate_name(gate: Gate) -> &'static str {
    match gate {
        Gate::Pending => "pending",
        Gate::Open => "open",
        Gate::Held(_) => "held",
    }
}

/// The metric for a held application: `lsr_app_held{db,cause}` 1 while held.
/// PROVISIONAL(#115).
pub(crate) const APP_HELD_METRIC: &str = "lsr_app_held";

/// The log line when a held gate keeps the `-exec` child from starting. PROVISIONAL(#115).
pub(crate) const START_REFUSED: &str = "start refused";

/// The log line when a held gate stops the running `-exec` child. PROVISIONAL(#115).
pub(crate) const APP_STOPPED: &str = "app stopped";

/// The config-check refusal of `stop-app` without `-exec`. PROVISIONAL(#115).
pub(crate) const STOP_APP_NEEDS_EXEC: &str = "if-replication-stops: stop-app";

/// What to do instead, for [`STOP_APP_NEEDS_EXEC`]. PROVISIONAL(#115).
pub(crate) const STOP_APP_INSTEAD: &str =
    "it stops the application only under -exec (or the exec key); use hold-start or report";
