//! What `/healthz` and `/metrics` read: the [`Board`], one entry per database in config
//! order, and the `/healthz` body.
//!
//! The daemon updates an entry when a database starts, fails or begins waiting. A request
//! copies the entries under the board's lock (a path and one `Arc` per replicator), lets go,
//! and only then asks each replicator for its `Health`, itself a copy taken under the
//! replicator's own short lock. No lock is held across a socket write.
//!
//! **Status.** 200 while every database is booting, running, draining, degraded or waiting
//! for its file to appear: a degraded task keeps retrying and recovers on its own, and an
//! orchestrator that restarted on it would only repeat the boot. 503 once any database has
//! stopped (a stop cause, or the drain's end), its thread is gone, it failed to start, or
//! fail-stop tripped for it (`crate::failstop`); the body's `down` lists which.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_replica::{
    AppNotice, DegradedCause, Denied, Gone, Health, JournalSignal, Phase, Replicator,
    SnapshotCause, SnapshotCauseKind, Stall, StopCause, Task, Timestamp,
};

use crate::json::string as json_string;

/// Where one database stands, as the board holds it.
#[derive(Clone, Debug)]
pub(crate) enum Entry {
    /// Its file does not exist yet.
    Waiting,
    /// Its store, restore or start failed.
    Failed,
    /// Its replicator.
    Running(Replicator),
}

#[derive(Debug)]
struct Row {
    path: PathBuf,
    replica_type: &'static str,
    entry: Entry,
}

/// Every database the daemon was given, and where each stands.
#[derive(Clone, Debug, Default)]
pub(crate) struct Board(Arc<Mutex<Vec<Row>>>);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Each critical section leaves plain data behind.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Board {
    /// Adds a database at the end.
    pub(crate) fn push(&self, path: PathBuf, replica_type: &'static str, entry: Entry) {
        lock(&self.0).push(Row {
            path,
            replica_type,
            entry,
        });
    }

    /// Changes the `index`th database's entry.
    pub(crate) fn set(&self, index: usize, entry: Entry) {
        if let Some(row) = lock(&self.0).get_mut(index) {
            row.entry = entry;
        }
    }

    /// A copy of every database's state, the replicators' health read after the lock.
    #[must_use]
    pub(crate) fn views(&self) -> Vec<DbView> {
        let rows: Vec<_> = lock(&self.0)
            .iter()
            .map(|row| (row.path.clone(), row.replica_type, row.entry.clone()))
            .collect();
        rows.into_iter()
            .map(|(path, replica_type, entry)| DbView {
                path,
                replica_type,
                state: match entry {
                    Entry::Waiting => DbState::Waiting,
                    Entry::Failed => DbState::Failed,
                    Entry::Running(replicator) => DbState::Running(Box::new(replicator.health())),
                },
            })
            .collect()
    }
}

/// One database, read for a request.
#[derive(Clone, Debug)]
pub(crate) struct DbView {
    /// The database path.
    pub path: PathBuf,
    /// Go's `replica_type` label: `file` or `s3`.
    pub replica_type: &'static str,
    /// Where it stands.
    pub state: DbState,
}

/// Where one database stands, read for a request.
#[derive(Clone, Debug)]
pub(crate) enum DbState {
    /// Its file does not exist yet.
    Waiting,
    /// It failed to start.
    Failed,
    /// Its replicator's health.
    Running(Box<Health>),
}

impl DbView {
    /// The state's name: the replicator's phase, or `waiting` or `failed`.
    #[must_use]
    pub(crate) fn state_name(&self) -> &'static str {
        match &self.state {
            DbState::Waiting => "waiting",
            DbState::Failed => "failed",
            DbState::Running(health) => phase_name(health.phase),
        }
    }

    /// Whether this database makes `/healthz` 503.
    #[must_use]
    pub(crate) fn down(&self) -> bool {
        match &self.state {
            DbState::Waiting => false,
            DbState::Failed => true,
            DbState::Running(health) => match health.phase {
                // A fail-stop trip is 503 under every mode (`crate::failstop`).
                Phase::Booting | Phase::Running | Phase::Draining => health.held_by.trip.is_some(),
                Phase::Stopped(_) | Phase::Gone(_) => true,
            },
        }
    }
}

/// A phase's name.
#[must_use]
pub(crate) const fn phase_name(phase: Phase) -> &'static str {
    match phase {
        Phase::Booting => "booting",
        Phase::Running => "running",
        Phase::Draining => "draining",
        Phase::Stopped(_) => "stopped",
        Phase::Gone(Gone::ThreadDied) => "gone",
        Phase::Gone(Gone::Forked) => "forked",
    }
}

/// A task's name.
#[must_use]
pub(crate) const fn task_name(task: Task) -> &'static str {
    match task {
        Task::Sync => "sync",
        Task::Checkpoint => "checkpoint",
        Task::Upload => "upload",
        Task::Prune => "prune",
        Task::Maintenance => "maintenance",
    }
}

/// A degraded cause's name (the page number and sizes are in `/healthz` only).
#[must_use]
pub(crate) const fn cause_name(cause: DegradedCause) -> &'static str {
    match cause {
        DegradedCause::StoreDenied(why) => denied_name(why),
        DegradedCause::StoreRefused => "store_refused",
        DegradedCause::RemoteCorrupt => "remote_corrupt",
        DegradedCause::OtherWriter => "other_writer",
        DegradedCause::RemoteRegressed => "remote_regressed",
        DegradedCause::WalCorrupt => "wal_corrupt",
        DegradedCause::WalTruncated => "wal_truncated",
        DegradedCause::WalUnlinked => "wal_unlinked",
        DegradedCause::WalSyncFailed => "wal_sync_failed",
        DegradedCause::PageSizeChanged { .. } => "page_size_changed",
        DegradedCause::DatabasePageMismatch { .. } => "database_page_mismatch",
        DegradedCause::DatabaseAheadOfWal { .. } => "database_ahead_of_wal",
        DegradedCause::ConditionalWritesUnsupported => "conditional_writes_unsupported",
        DegradedCause::Bug => "bug",
        DegradedCause::Stalled(stall) => stall_name(stall),
    }
}

/// A stalled task's cause: the retried fault that kept failing.
#[must_use]
pub(crate) const fn stall_name(stall: Stall) -> &'static str {
    match stall {
        Stall::StoreTransient => "store_transient",
        Stall::StoreThrottled => "store_throttled",
        Stall::RemoteObjectMissing => "remote_object_missing",
        Stall::UploadAmbiguous => "upload_ambiguous",
        Stall::UploadChecksumMismatch => "upload_checksum_mismatch",
        Stall::StagedLost => "staged_lost",
        Stall::LocalDiskFull => "local_disk_full",
        Stall::LocalIo => "local_io",
        Stall::LocalFsyncFailed => "local_fsync_failed",
        Stall::SqliteBusy => "sqlite_busy",
        Stall::ReadersHeld => "readers_held",
        Stall::WalMoved => "wal_moved",
        Stall::WalIndexInconsistent => "wal_index_inconsistent",
        Stall::WalChangedUnderRead => "wal_changed_under_read",
        Stall::NothingToSnapshot => "nothing_to_snapshot",
        Stall::ReadTxLost => "read_tx_lost",
    }
}

/// An access refusal's name: what an operator must fix.
#[must_use]
pub(crate) const fn denied_name(why: Denied) -> &'static str {
    match why {
        Denied::Credentials => "credentials",
        Denied::Permission => "permission_denied",
        Denied::ClockSkew => "clock_skew",
        Denied::BucketMissing => "bucket_missing",
        Denied::Tls => "tls",
    }
}

/// A stop cause's name: the degraded cause it stopped on, or `shutdown` for a drain's end.
#[must_use]
pub(crate) const fn stop_name(cause: StopCause) -> &'static str {
    match DegradedCause::of(cause) {
        Some(cause) => cause_name(cause),
        None => "shutdown",
    }
}

/// A snapshot cause's name.
#[must_use]
pub(crate) fn snapshot_name(cause: SnapshotCause) -> &'static str {
    snapshot_kind_name(SnapshotCauseKind::from(&cause))
}

/// A journal signal's name in `/healthz` and metrics, without its payload.
#[must_use]
pub(crate) const fn journal_name(signal: JournalSignal) -> &'static str {
    match signal {
        JournalSignal::Missing => "missing",
        JournalSignal::Unreadable(_) => "unreadable",
        JournalSignal::Newer { .. } => "newer_version",
        JournalSignal::SlotUnreadable => "slot_unreadable",
        JournalSignal::Unwritable(_) => "unwritable",
    }
}

/// A snapshot cause's name, without its payload.
#[must_use]
pub(crate) const fn snapshot_kind_name(kind: SnapshotCauseKind) -> &'static str {
    match kind {
        SnapshotCauseKind::FirstSync => "first_sync",
        SnapshotCauseKind::BehindRemote => "behind_remote",
        SnapshotCauseKind::NewestStagedFileUnreadable => "newest_staged_file_unreadable",
        SnapshotCauseKind::WalMissing => "wal_missing",
        SnapshotCauseKind::GenerationChanged => "generation_changed",
        SnapshotCauseKind::PrevFrameMismatch => "prev_frame_mismatch",
        SnapshotCauseKind::LastPageMismatch => "last_page_mismatch",
        SnapshotCauseKind::LocalRewound => "local_rewound",
        SnapshotCauseKind::CheckpointBoundary => "checkpoint_boundary",
        SnapshotCauseKind::OperatorRequested => "operator_requested",
        SnapshotCauseKind::StagedUnverified => "staged_unverified",
        SnapshotCauseKind::WalSyncFailed => "wal_sync_failed",
    }
}

/// An application notice's kind.
#[must_use]
pub(crate) const fn notice_name(notice: AppNotice) -> &'static str {
    match notice {
        AppNotice::Checkpointing { .. } => "checkpointing",
        AppNotice::Stalled { .. } => "stalled",
    }
}

/// A JSON object, built field by field.
struct Object(String);

impl Object {
    fn new() -> Self {
        Self(String::from("{"))
    }

    fn raw(&mut self, key: &str, value: &str) {
        if self.0.len() > 1 {
            self.0.push(',');
        }
        self.0.push_str(&json_string(key));
        self.0.push(':');
        self.0.push_str(value);
    }

    fn text(&mut self, key: &str, value: &str) {
        self.raw(key, &json_string(value));
    }

    fn num(&mut self, key: &str, value: impl std::fmt::Display) {
        self.raw(key, &value.to_string());
    }

    fn maybe(&mut self, key: &str, value: Option<String>) {
        match value {
            Some(text) => self.text(key, &text),
            None => self.raw(key, "null"),
        }
    }

    fn end(mut self) -> String {
        self.0.push('}');
        self.0
    }
}

fn ahead_json(health: &Health) -> String {
    let Some(ahead) = health.ahead_of_wal else {
        return "null".to_owned();
    };
    let mut o = Object::new();
    o.num("pages", ahead.pages);
    o.num("first_page", ahead.first_page);
    o.text("restore_point", &ahead.restore_point.to_string());
    o.text("kept_until", &ahead.kept_until.to_string());
    o.end()
}

fn degraded_json(health: &Health) -> String {
    let tasks: Vec<String> = Task::ALL
        .iter()
        .filter_map(|task| {
            let degraded = health.task(*task)?;
            let mut o = Object::new();
            o.text("task", task_name(*task));
            o.text("cause", cause_name(degraded.cause));
            o.text("detail", &format!("{:?}", degraded.cause));
            o.text("since", &degraded.since.to_string());
            o.num("retries", degraded.retries);
            Some(o.end())
        })
        .collect();
    format!("[{}]", tasks.join(","))
}

fn running_json(o: &mut Object, health: &Health) {
    match health.phase {
        Phase::Stopped(cause) => {
            o.text("stop_cause", stop_name(cause));
            o.text("stop_detail", &format!("{cause:?}"));
        }
        Phase::Booting | Phase::Running | Phase::Draining | Phase::Gone(_) => {
            o.raw("stop_cause", "null");
        }
    }
    o.raw("degraded", &degraded_json(health));
    o.maybe("synced_txid", health.synced.map(|(t, _)| t.to_string()));
    o.maybe("synced_at", health.synced.map(|(_, at)| at.to_string()));
    o.maybe(
        "replicated_txid",
        health.replicated.map(|(t, _)| t.to_string()),
    );
    o.maybe(
        "replicated_at",
        health.replicated.map(|(_, at)| at.to_string()),
    );
    o.maybe(
        "unreplicated_since",
        health.unreplicated_since.map(|at| at.to_string()),
    );
    o.num("wal_bytes", health.wal_bytes);
    o.num("db_bytes", health.db_bytes);
    o.maybe(
        "resnapshot",
        health.resnapshot.map(|c| snapshot_name(c).to_owned()),
    );
    o.maybe("app_notice", health.app_notice().map(|n| n.to_string()));
    o.maybe(
        "journal",
        health.journal.map(|s| journal_name(s).to_owned()),
    );
    o.maybe("journal_detail", health.journal.map(|s| format!("{s:?}")));
    o.raw("database_ahead_of_wal", &ahead_json(health));
    o.maybe(
        "held_by",
        health
            .held_by
            .trip
            .map(|trip| crate::failstop::trip_name(trip).to_owned()),
    );
    o.text("app", crate::failstop::gate_name(health.held_by.gate));
    o.text("reported_at", &health.reported_at.to_string());
}

/// The `/healthz` body, and whether every database is up.
#[must_use]
pub(crate) fn healthz(views: &[DbView], now: Timestamp) -> (bool, String) {
    let down: Vec<String> = views
        .iter()
        .filter(|v| v.down())
        .map(|v| json_string(&v.path.display().to_string()))
        .collect();
    let dbs: Vec<String> = views
        .iter()
        .map(|view| {
            let mut o = Object::new();
            o.text("path", &view.path.display().to_string());
            o.text("replica_type", view.replica_type);
            o.text("state", view.state_name());
            if let DbState::Running(health) = &view.state {
                running_json(&mut o, health);
            }
            o.end()
        })
        .collect();
    let up = down.is_empty();
    let mut o = Object::new();
    o.text("status", if up { "ok" } else { "down" });
    o.text("time", &now.to_string());
    o.raw("down", &format!("[{}]", down.join(",")));
    o.raw("dbs", &format!("[{}]", dbs.join(",")));
    (up, o.end())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fail-stop trip is 503 whatever the phase and the mode: a database still running, its
    /// commit off the replica too long, is down; the same database untripped is up.
    #[test]
    fn a_trip_is_503_while_the_database_runs() {
        let at = Timestamp::from_unix_millis(1_700_000_000_000);
        let mut health = Health::new(at);
        health.phase = Phase::Running;
        let view = |health: &Health| DbView {
            path: PathBuf::from("/app.db"),
            replica_type: "file",
            state: DbState::Running(Box::new(health.clone())),
        };
        assert!(!view(&health).down());
        health.held_by.trip = Some(lsr_replica::Trip::UnreplicatedTooLong { since: at });
        health.held_by.gate = lsr_replica::Gate::Allowed;
        assert!(view(&health).down(), "report and hold-start still say 503");
        let (up, body) = healthz(&[view(&health)], at);
        assert!(!up);
        assert!(
            body.contains("\"held_by\":\"unreplicated_too_long\""),
            "{body}"
        );
        assert!(body.contains("\"app\":\"allowed\""), "{body}");
    }

    /// A database file found ahead of its WAL is a notice in both outputs while the state
    /// before it is kept, with `/healthz` still 200, and both drop it once it ends.
    #[test]
    fn a_database_ahead_of_its_wal_is_a_notice_in_both_outputs() {
        let at = Timestamp::from_unix_millis(1_700_000_000_000);
        let mut health = Health::new(at);
        health.phase = Phase::Running;
        let view = |health: &Health| DbView {
            path: PathBuf::from("/app.db"),
            replica_type: "file",
            state: DbState::Running(Box::new(health.clone())),
        };
        health.ahead_of_wal = Some(lsr_replica::AheadOfWal::of(lsr_core::Ahead {
            restore_point: lsr_replica::Txid::new(7).unwrap(),
            pages: std::num::NonZeroU32::new(2).unwrap(),
            first: std::num::NonZeroU32::new(5).unwrap(),
            made: at,
        }));
        let (up, body) = healthz(&[view(&health)], at);
        assert!(up, "{body}");
        let kept_until = at.saturating_add(lsr_core::AHEAD_KEPT_FOR);
        assert!(
            body.contains(&format!(
                "\"database_ahead_of_wal\":{{\"pages\":2,\"first_page\":5,\"restore_point\":\"{}\",\"kept_until\":\"{kept_until}\"}}",
                lsr_replica::Txid::new(7).unwrap()
            )),
            "{body}"
        );
        let metrics = crate::metrics::render(&[view(&health)], at);
        assert!(
            metrics.contains("lsr_database_ahead_of_wal{db=\"/app.db\"} 1"),
            "{metrics}"
        );
        health.ahead_of_wal = None;
        let (_, body) = healthz(&[view(&health)], at);
        assert!(body.contains("\"database_ahead_of_wal\":null"), "{body}");
        let metrics = crate::metrics::render(&[view(&health)], at);
        assert!(
            metrics.contains("lsr_database_ahead_of_wal{db=\"/app.db\"} 0"),
            "{metrics}"
        );
    }

    /// Every kind of access refusal reports under a name of its own: one name for
    /// all of them sent operators to rotate keys for a wrong clock or a missing bucket.
    #[test]
    fn each_access_refusal_has_its_own_name() {
        let all = [
            Denied::Credentials,
            Denied::Permission,
            Denied::ClockSkew,
            Denied::BucketMissing,
            Denied::Tls,
        ];
        let names: std::collections::BTreeSet<&str> = all
            .into_iter()
            .map(|why| cause_name(DegradedCause::StoreDenied(why)))
            .collect();
        assert_eq!(names.len(), all.len(), "{names:?}");
        assert!(names.contains("credentials"));
    }

    /// Each journal signal has the owner-approved name, one per variant: an operator acts
    /// differently on a lost journal and on one that cannot be written.
    #[test]
    fn each_journal_signal_has_its_approved_name() {
        let named = [
            (JournalSignal::Missing, "missing"),
            (
                JournalSignal::Unreadable(lsr_journal::Why::Tie),
                "unreadable",
            ),
            (JournalSignal::Newer { version: 2 }, "newer_version"),
            (JournalSignal::SlotUnreadable, "slot_unreadable"),
            (
                JournalSignal::Unwritable(std::io::ErrorKind::StorageFull),
                "unwritable",
            ),
        ];
        for (signal, name) in named {
            assert_eq!(journal_name(signal), name, "{signal:?}");
        }
    }
}
