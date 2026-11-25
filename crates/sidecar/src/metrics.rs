//! `/metrics` in the Prometheus text format (version 0.0.4), written by hand.
//!
//! **Go's series, where the replicator has the value** (names, types and labels as Go's
//! `db.go:3527-3585` and `internal/internal.go:151-176` define them; `db` is the database
//! path, as Go's `db.path`):
//! - `litestream_db_size{db}`, gauge: the database file's size (`db.go:1434`).
//! - `litestream_txid{db}`, gauge: the newest transaction staged locally, Go's position after
//!   a sync (`db.go:1430`); 0 before the first.
//! - `litestream_wal_size{db}`, gauge: the WAL offset the newest staged file ends at, Go's
//!   offset after a sync (`db.go:1436`); 0 before the first. `lsr_wal_bytes` is the file.
//! - `litestream_total_wal_bytes{db}`, counter: WAL bytes the sync steps staged (`db.go:2256`).
//! - `litestream_sync_count{db}`, `litestream_sync_error_count{db}` and
//!   `litestream_sync_seconds{db}`, counters: sync steps, the ones that failed, and their time
//!   (`db.go:1365-1376`). A boot attempt the store fails is a step that failed, as Go's failed
//!   `init` is (`db.go:1210`): the series where a store that refuses the credentials or cannot be
//!   reached shows; Go counts no later upload failure there. The engine steps when the WAL moved or a checkpoint wants proof;
//!   Go syncs on every one-second poll, so Go's rate is higher on an idle database (D163).
//! - `litestream_disk_full{db}`, gauge: 1 while the last sync step failed for a full disk.
//! - `litestream_checkpoint_count{db,mode}`, `litestream_checkpoint_error_count{db,mode}` and
//!   `litestream_checkpoint_seconds{db,mode}`, counters: the checkpoints the replicator asked
//!   SQLite for, `PASSIVE`, `FULL` or `TRUNCATE`, the ones that failed, and their time
//!   (`db.go:2808-2820`). Served for every mode from the start; Go creates each at first use.
//! - `litestream_replica_operation_total{replica_type,operation}`, counter, and
//!   `litestream_replica_operation_duration_seconds{replica_type,operation}`, histogram,
//!   with Go's operation names: `GET` (reads), `PUT` (committed uploads), `DELETE` (delete
//!   calls; Go's S3 client adds the number of objects deleted), `LIST` (listing pages).
//!   The histogram has only the `+Inf` bucket: the replicator keeps a total time per
//!   operation, not a distribution, so `_sum / _count` is right and Go's buckets are absent.
//!   Go's file client reports none of these; this daemon reports them for both types.
//! - `litestream_replica_operation_bytes{replica_type,operation="PUT"}`, counter.
//! - `litestream_compaction_verify_error_count{db}`, counter: always 0, as Go's with its
//!   default, since the verification it counts is a setting this daemon refuses.
//!
//! **Go's series not built, and why** (D163). `litestream_replica_operation_errors_total`
//! counts per-object failures of an S3 batch delete by the response's error code, which the
//! store does not report (`lsr_replica_operation_errors_total` counts failed calls).
//! `litestream_l0_retention_files_total` has no per-pass counts. The `litestream_validation_*`
//! series are per compaction level, and the scheduled verify that replaces Go's validation
//! checks the replica as a whole (`lsr_verify_*` below).
//!
//! **New series** under `lsr_`, for what Go has no series for:
//! - `lsr_db_state{db,state}` 1, the state `/healthz` names (`booting`, `running`,
//!   `draining`, `stopped`, `gone`, `forked`, `waiting`, `failed`).
//! - `lsr_degraded{db,task,cause}` 1 while a task keeps failing, and
//!   `lsr_degraded_since_timestamp_seconds{db,task,cause}`.
//! - `lsr_stopped{db,cause}` 1 once stopped (`shutdown` after a drain).
//! - `lsr_replicated_txid{db}`, the newest transaction on the replica.
//! - `lsr_unreplicated_since_timestamp_seconds{db}` while a commit is not on the replica,
//!   and `lsr_unreplicated_age_seconds{db}` (0 when caught up): the lag to alert on.
//! - `lsr_wal_bytes{db}`: the WAL file's size, which a read lock held while uploads fail grows.
//! - `lsr_app_checkpoints_total{db,kind}` (`backfilled`, `restarted`, `stalled`) and
//!   `lsr_app_notice{db,kind}` 1 while the application-checkpoint notice stands.
//! - `lsr_resnapshot_pending{db,cause}` 1 when the next sync is a snapshot.
//! - `lsr_snapshots_total{db,cause}`: snapshots staged since the replicator started, every
//!   cause listed, zeros included. Each uploads the whole database: a cause other than
//!   `first_sync`, `checkpoint_boundary` or `operator_requested` that keeps counting up means
//!   restarts or faults the replicator cannot see past.
//! - `lsr_syncs_staged_total{db}`: sync steps that staged a file.
//! - `lsr_checkpoints_total{db}`, `lsr_checkpoints_busy_total{db}`, `lsr_uploads_total{db}`,
//!   `lsr_retries_total{db}`, `lsr_panics_total{db}`.
//! - `lsr_read_lock_violations_total{db}`: recoveries that found commits lost to a WAL restart
//!   the engine's read lock should have prevented, a broken lock or a bug. Each takes a snapshot
//!   counted as `generation_changed`; any value above zero is worth a report.
//! - `lsr_scratch_sweeps_denied_total{db}`: 1 once the replica refused the start-up sweep of
//!   leftover multipart uploads for want of permission (Go's published S3 policy allows neither
//!   the list nor the abort); the sweep is skipped, replication is unaffected.
//! - `lsr_replica_operation_errors_total{replica_type,operation}`.
//! - `lsr_health_report_age_seconds{db}`: how old the replicator's last health write is; a large
//!   value means its thread is stuck in a store call.
//! - The scheduled verify (`validation.interval`): `lsr_verify_runs_total{db,result}`
//!   (`passed`, `failed`, `merged`, `incomplete`), `lsr_verify_failing{db}` 1 while a run
//!   found the replica restores something the database never held (a degraded state), with
//!   `lsr_verify_failing_txid{db}` and `lsr_verify_failing_page{db}` (the lowest page that
//!   differs) while it stands, and `lsr_verify_last_success_timestamp_seconds{db}`.

use std::fmt::Display;

use lsr_replica::{
    CallTotals, CheckpointMode, Health, Mismatch, OpCounters, Phase, StoreCounters, Task,
    Timestamp, VerifyFailure,
};

use crate::health::{
    DbState, DbView, cause_name, notice_name, snapshot_kind_name, snapshot_name, stop_name,
    task_name,
};

/// The rendered text.
struct Out(String);

impl Out {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        self.0
            .push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl Display) {
        self.0.push_str(name);
        if !labels.is_empty() {
            let labels: Vec<String> = labels
                .iter()
                .map(|(key, text)| format!("{key}=\"{}\"", escape(text)))
                .collect();
            self.0.push_str(&format!("{{{}}}", labels.join(",")));
        }
        self.0.push_str(&format!(" {value}\n"));
    }
}

/// The label value escapes of the text format: backslash, double quote, newline.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// Milliseconds as decimal seconds, without a float.
fn seconds(millis: u64) -> String {
    format!("{}.{:03}", millis / 1000, millis % 1000)
}

fn since(now: Timestamp, then: Timestamp) -> String {
    let ms = now.saturating_duration_since(then).as_millis();
    seconds(u64::try_from(ms).unwrap_or(u64::MAX))
}

fn unix_seconds(at: Timestamp) -> String {
    let ms = at.unix_millis();
    seconds(u64::try_from(ms).unwrap_or(0))
}

/// The running databases with their path, as label text.
fn running(views: &[DbView]) -> Vec<(String, &Health)> {
    views
        .iter()
        .filter_map(|view| match &view.state {
            DbState::Running(health) => Some((view.path.display().to_string(), &**health)),
            DbState::Waiting | DbState::Failed => None,
        })
        .collect()
}

/// A per-database series over every running database.
fn per_db(
    out: &mut Out,
    dbs: &[(String, &Health)],
    (name, kind, help): (&str, &str, &str),
    value: impl Fn(&Health) -> String,
) {
    out.family(name, kind, help);
    for (db, health) in dbs {
        out.sample(name, &[("db", db)], value(health));
    }
}

fn go_db_series(out: &mut Out, dbs: &[(String, &Health)]) {
    per_db(
        out,
        dbs,
        (
            "litestream_db_size",
            "gauge",
            "The current size of the real DB",
        ),
        // Go sets it after a sync (`db.go:1434`): 0 until one has staged.
        |h| h.synced.map_or(0, |_| h.db_bytes).to_string(),
    );
    per_db(
        out,
        dbs,
        ("litestream_txid", "gauge", "The current transaction ID"),
        |h| h.synced.map_or(0, |(t, _)| t.get()).to_string(),
    );
    per_db(
        out,
        dbs,
        (
            "litestream_wal_size",
            "gauge",
            "The current size of the real WAL",
        ),
        |h| h.wal_synced_offset.to_string(),
    );
    per_db(
        out,
        dbs,
        (
            "litestream_total_wal_bytes",
            "counter",
            "Total number of bytes written to shadow WAL",
        ),
        |h| h.counters.work.wal_bytes().to_string(),
    );
    go_sync_series(out, dbs);
    go_checkpoint_series(out, dbs);
    // Go's post-compaction verification is off by default, and `verify-compaction: true` is
    // refused here (docs/config-keys.md), so its failure count is zero on both sides: served
    // so an alert written against the series finds it (`db.go:406`, `db.go:3582`).
    per_db(
        out,
        dbs,
        (
            "litestream_compaction_verify_error_count",
            "counter",
            "Number of post-compaction verification failures",
        ),
        |_| "0".to_owned(),
    );
}

fn go_sync_series(out: &mut Out, dbs: &[(String, &Health)]) {
    per_db(
        out,
        dbs,
        (
            "litestream_sync_count",
            "counter",
            "Number of sync operations performed",
        ),
        |h| h.counters.work.sync().calls().to_string(),
    );
    per_db(
        out,
        dbs,
        (
            "litestream_sync_error_count",
            "counter",
            "Number of sync errors that have occurred",
        ),
        |h| h.counters.work.sync().errors().to_string(),
    );
    per_db(
        out,
        dbs,
        (
            "litestream_sync_seconds",
            "counter",
            "Time spent syncing shadow WAL, in seconds",
        ),
        |h| seconds(h.counters.work.sync().millis()),
    );
    per_db(
        out,
        dbs,
        (
            "litestream_disk_full",
            "gauge",
            "Whether replication is paused because the local disk is full",
        ),
        |h| u8::from(h.counters.work.disk_full()).to_string(),
    );
}

/// Go's checkpoint series, per database and mode (Go's `mode` label is SQLite's name).
fn go_checkpoint_series(out: &mut Out, dbs: &[(String, &Health)]) {
    let series: [CallSeries; 3] = [
        (
            "litestream_checkpoint_count",
            "Number of checkpoint operations performed",
            |c| c.calls().to_string(),
        ),
        (
            "litestream_checkpoint_error_count",
            "Number of checkpoint errors that have occurred",
            |c| c.errors().to_string(),
        ),
        (
            "litestream_checkpoint_seconds",
            "Time spent checkpointing WAL, in seconds",
            |c| seconds(c.millis()),
        ),
    ];
    for (name, help, value) in series {
        out.family(name, "counter", help);
        for (db, health) in dbs {
            for (mode, label) in [
                (CheckpointMode::Passive, "PASSIVE"),
                (CheckpointMode::Full, "FULL"),
                (CheckpointMode::Truncate, "TRUNCATE"),
            ] {
                let totals = health.counters.work.checkpoints(mode);
                out.sample(name, &[("db", db), ("mode", label)], value(totals));
            }
        }
    }
}

/// Go's operation names and their totals.
type Ops = [(&'static str, OpCounters); 4];

/// One replica type's totals and bytes uploaded.
type TypeSum = (&'static str, Ops, u64);

/// A series over one kind of call's totals: name, help, value.
type CallSeries = (&'static str, &'static str, fn(CallTotals) -> String);

/// A per-database counter: name, help, value.
type CounterSeries = (&'static str, &'static str, fn(&Health) -> u64);

/// Go's operations, from the replicator's per-call totals.
fn operations(store: &StoreCounters) -> Ops {
    let read = OpCounters {
        calls: store.read.calls.saturating_add(store.created_at.calls),
        errors: store.read.errors.saturating_add(store.created_at.errors),
        millis: store.read.millis.saturating_add(store.created_at.millis),
    };
    [
        ("GET", read),
        ("PUT", store.commit),
        ("DELETE", store.delete),
        ("LIST", store.list),
    ]
}

/// Every database's store totals summed by replica type, in a fixed order.
fn by_type(views: &[DbView]) -> Vec<TypeSum> {
    let mut sums: Vec<TypeSum> = Vec::new();
    for view in views {
        let DbState::Running(health) = &view.state else {
            continue;
        };
        let store = &health.counters.store;
        let ops = operations(store);
        match sums
            .iter_mut()
            .find(|(kind, _, _)| *kind == view.replica_type)
        {
            Some((_, sum, bytes)) => {
                for ((_, total), (_, add)) in sum.iter_mut().zip(ops) {
                    total.calls = total.calls.saturating_add(add.calls);
                    total.errors = total.errors.saturating_add(add.errors);
                    total.millis = total.millis.saturating_add(add.millis);
                }
                *bytes = bytes.saturating_add(store.bytes_uploaded);
            }
            None => sums.push((view.replica_type, ops, store.bytes_uploaded)),
        }
    }
    sums.sort_by_key(|(kind, _, _)| *kind);
    sums
}

fn go_replica_series(out: &mut Out, views: &[DbView]) {
    let sums = by_type(views);
    let name = "litestream_replica_operation_total";
    out.family(
        name,
        "counter",
        "The number of replica operations performed",
    );
    for (kind, ops, _) in &sums {
        for (op, c) in ops {
            out.sample(name, &[("replica_type", kind), ("operation", op)], c.calls);
        }
    }
    let name = "litestream_replica_operation_bytes";
    out.family(
        name,
        "counter",
        "The number of bytes used by replica operations",
    );
    for (kind, _, bytes) in &sums {
        out.sample(name, &[("replica_type", kind), ("operation", "PUT")], bytes);
    }
    let name = "litestream_replica_operation_duration_seconds";
    out.family(
        name,
        "histogram",
        "Duration of replica operations by type and operation",
    );
    for (kind, ops, _) in &sums {
        for (op, c) in ops {
            let labels = [("replica_type", *kind), ("operation", *op)];
            let bucket = [("replica_type", *kind), ("operation", *op), ("le", "+Inf")];
            out.sample(&format!("{name}_bucket"), &bucket, c.calls);
            out.sample(&format!("{name}_sum"), &labels, seconds(c.millis));
            out.sample(&format!("{name}_count"), &labels, c.calls);
        }
    }
    let name = "lsr_replica_operation_errors_total";
    out.family(name, "counter", "Replica operations that returned an error");
    for (kind, ops, _) in &sums {
        for (op, c) in ops {
            out.sample(name, &[("replica_type", kind), ("operation", op)], c.errors);
        }
    }
}

fn state_series(out: &mut Out, views: &[DbView], now: Timestamp) {
    let name = "lsr_db_state";
    out.family(name, "gauge", "1 for the state each database is in");
    for view in views {
        let db = view.path.display().to_string();
        out.sample(name, &[("db", &db), ("state", view.state_name())], 1);
    }
    let dbs = running(views);
    out.family("lsr_degraded", "gauge", "1 while a task keeps failing");
    let at = "lsr_degraded_since_timestamp_seconds";
    let mut since_lines = Vec::new();
    for (db, health) in &dbs {
        for task in Task::ALL {
            if let Some(d) = health.task(task) {
                let labels = [
                    ("db", db.as_str()),
                    ("task", task_name(task)),
                    ("cause", cause_name(d.cause)),
                ];
                out.sample("lsr_degraded", &labels, 1);
                since_lines.push((labels, unix_seconds(d.since)));
            }
        }
    }
    out.family(at, "gauge", "When the task began failing this way");
    for (labels, value) in since_lines {
        out.sample(at, &labels, value);
    }
    out.family(
        "lsr_stopped",
        "gauge",
        "1 once the replicator stopped, by cause",
    );
    for (db, health) in &dbs {
        if let Phase::Stopped(cause) = health.phase {
            out.sample("lsr_stopped", &[("db", db), ("cause", stop_name(cause))], 1);
        }
    }
    let name = "lsr_health_report_age_seconds";
    out.family(name, "gauge", "Age of the replicator's last health report");
    for (db, health) in &dbs {
        out.sample(name, &[("db", db)], since(now, health.reported_at));
    }
}

fn lag_series(out: &mut Out, dbs: &[(String, &Health)], now: Timestamp) {
    per_db(
        out,
        dbs,
        (
            "lsr_replicated_txid",
            "gauge",
            "The newest transaction on the replica",
        ),
        |h| h.replicated.map_or(0, |(t, _)| t.get()).to_string(),
    );
    let name = "lsr_unreplicated_since_timestamp_seconds";
    out.family(
        name,
        "gauge",
        "When the oldest commit not on the replica was seen",
    );
    for (db, health) in dbs {
        if let Some(at) = health.unreplicated_since {
            out.sample(name, &[("db", db)], unix_seconds(at));
        }
    }
    per_db(
        out,
        dbs,
        (
            "lsr_unreplicated_age_seconds",
            "gauge",
            "How long the oldest commit not on the replica has waited; 0 when caught up",
        ),
        |h| {
            h.unreplicated_since
                .map_or_else(|| "0".to_owned(), |at| since(now, at))
        },
    );
    per_db(
        out,
        dbs,
        ("lsr_wal_bytes", "gauge", "The WAL file's size"),
        |h| h.wal_bytes.to_string(),
    );
}

fn app_series(out: &mut Out, dbs: &[(String, &Health)]) {
    let name = "lsr_app_checkpoints_total";
    out.family(
        name,
        "counter",
        "Checkpoints the application ran, or tried to, by kind",
    );
    for (db, health) in dbs {
        let app = &health.app_checkpoints;
        for (kind, n) in [
            ("backfilled", app.backfilled),
            ("restarted", app.restarted),
            ("stalled", app.stalled),
        ] {
            out.sample(name, &[("db", db), ("kind", kind)], n);
        }
    }
    let name = "lsr_app_notice";
    out.family(
        name,
        "gauge",
        "1 while the application-checkpoint notice stands",
    );
    for (db, health) in dbs {
        if let Some(notice) = health.app_notice() {
            out.sample(name, &[("db", db), ("kind", notice_name(notice))], 1);
        }
    }
    let name = "lsr_resnapshot_pending";
    out.family(
        name,
        "gauge",
        "1 when the next sync is a snapshot, by cause",
    );
    for (db, health) in dbs {
        if let Some(cause) = health.resnapshot {
            out.sample(name, &[("db", db), ("cause", snapshot_name(cause))], 1);
        }
    }
    let name = "lsr_snapshots_total";
    out.family(name, "counter", "Snapshots staged, by cause");
    for (db, health) in dbs {
        for (kind, n) in health.counters.snapshots.iter() {
            out.sample(name, &[("db", db), ("cause", snapshot_kind_name(kind))], n);
        }
    }
}

fn counter_series(out: &mut Out, dbs: &[(String, &Health)]) {
    let series: [CounterSeries; 8] = [
        (
            "lsr_syncs_staged_total",
            "Sync steps that staged a file",
            |h| h.counters.syncs,
        ),
        ("lsr_checkpoints_total", "Checkpoints run", |h| {
            h.counters.checkpoints
        }),
        (
            "lsr_checkpoints_busy_total",
            "Checkpoints SQLite reported busy",
            |h| h.counters.checkpoints_busy,
        ),
        ("lsr_uploads_total", "Files uploaded", |h| {
            h.counters.uploads
        }),
        (
            "lsr_retries_total",
            "Failed attempts that were retried",
            |h| h.counters.retries,
        ),
        (
            "lsr_panics_total",
            "Panics the replicator thread caught",
            |h| h.counters.panics,
        ),
        (
            "lsr_read_lock_violations_total",
            "Recoveries that found commits lost to a WAL restart the read lock should have prevented",
            |h| h.counters.read_lock_violations,
        ),
        (
            "lsr_scratch_sweeps_denied_total",
            "1 once the replica refused the start-up sweep of leftover uploads for want of permission (skipped; replication is unaffected)",
            |h| h.counters.scratch_sweeps_denied,
        ),
    ];
    for (name, help, value) in series {
        per_db(out, dbs, (name, "counter", help), |h| value(h).to_string());
    }
}

fn verify_series(out: &mut Out, dbs: &[(String, &Health)]) {
    let name = "lsr_verify_runs_total";
    out.family(name, "counter", "Scheduled verify runs, by how they ended");
    for (db, health) in dbs {
        let runs = health.verification.runs;
        for (result, n) in [
            ("passed", runs.passed),
            ("failed", runs.failed),
            ("merged", runs.merged),
            ("incomplete", runs.incomplete),
        ] {
            out.sample(name, &[("db", db), ("result", result)], n);
        }
    }
    per_db(
        out,
        dbs,
        (
            "lsr_verify_failing",
            "gauge",
            "1 while the scheduled verify finds the replica restores something the database never held",
        ),
        |h| u8::from(h.verification.failing.is_some()).to_string(),
    );
    let failing = |h: &Health| h.verification.failing;
    let name = "lsr_verify_failing_txid";
    out.family(name, "gauge", "The transaction the failing verify compared");
    for (db, health) in dbs {
        if let Some(failure) = failing(health) {
            out.sample(name, &[("db", db)], failure.txid.get());
        }
    }
    let name = "lsr_verify_failing_page";
    out.family(
        name,
        "gauge",
        "The lowest page the failing verify found different",
    );
    for (db, health) in dbs {
        if let Some(VerifyFailure {
            found: Mismatch::Pages { first, .. },
            ..
        }) = failing(health)
        {
            out.sample(name, &[("db", db)], first);
        }
    }
    let name = "lsr_verify_last_success_timestamp_seconds";
    out.family(name, "gauge", "When a scheduled verify last passed");
    for (db, health) in dbs {
        if let Some((_, at)) = health.verification.last_good {
            out.sample(name, &[("db", db)], unix_seconds(at));
        }
    }
}

/// The `/metrics` body.
#[must_use]
pub(crate) fn render(views: &[DbView], now: Timestamp) -> String {
    let mut out = Out(String::new());
    let dbs = running(views);
    go_db_series(&mut out, &dbs);
    go_replica_series(&mut out, views);
    state_series(&mut out, views, now);
    lag_series(&mut out, &dbs, now);
    app_series(&mut out, &dbs);
    counter_series(&mut out, &dbs);
    verify_series(&mut out, &dbs);
    out.0
}
