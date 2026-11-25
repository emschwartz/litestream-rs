//! Log lines in Go's `log/slog` formats, written by hand (no new dependency).
//!
//! - `text` is slog's `TextHandler`: `time=<RFC 3339, ms, UTC> level=INFO
//!   msg="..." key=value`, a value quoted (Go's `strconv.Quote` escapes) when it
//!   is empty or holds a space, `=`, `"` or a control character.
//! - `json` is slog's `JSONHandler`: one object per line, `time`, `level`, `msg`
//!   first, numbers and booleans unquoted.
//!
//! Go writes `time` in the local zone; this writes UTC (`Z`), which is the same
//! on the usual container. Levels are Go's, `TRACE` included
//! (`internal/log.go`). The keys match Go's where Go logs the same thing
//! (`path`, `db`, `type`, `signal`, `error`); the messages are this daemon's own.
//!
//! [`DbEvents`] turns each replicator event into one line for that database,
//! with `db` (the file name, as Go's database logger) and the cause as a field,
//! so a grep for forced snapshots keeps working on `cause=`.

use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_config::{LogFormat, LogLevel};
use lsr_core::Task;
use lsr_core::{AppCheckpoint, Clock, Event as EngineEvent, MaintainedEvent, SnapshotCause};
use lsr_replica::{DegradedCause, Event, EventSink, Timestamp, Txid, VerifyOutcome};

use crate::failstop;
use crate::json::string as json_string;

/// One field's value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    /// A string.
    Text(String),
    /// A number.
    Int(i64),
    /// A boolean.
    Bool(bool),
}

impl From<&str> for Value {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<String> for Value {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&Path> for Value {
    fn from(path: &Path) -> Self {
        Self::Text(path.display().to_string())
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Self::Int(i64::try_from(n).unwrap_or(i64::MAX))
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

const fn rank(level: LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warn => 3,
        LogLevel::Error => 4,
    }
}

const fn name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Trace => "TRACE",
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

/// Where lines go, at what level, in what format.
pub(crate) struct Logger {
    level: LogLevel,
    format: LogFormat,
    clock: Arc<dyn Clock>,
    out: Mutex<Box<dyn Write + Send>>,
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("level", &self.level)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl Logger {
    /// A logger to stdout or stderr.
    #[must_use]
    pub(crate) fn new(
        level: LogLevel,
        format: LogFormat,
        stderr: bool,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let out: Box<dyn Write + Send> = if stderr {
            Box::new(io::stderr())
        } else {
            Box::new(io::stdout())
        };
        Self::to(level, format, clock, out)
    }

    /// A logger to any writer.
    #[must_use]
    pub(crate) fn to(
        level: LogLevel,
        format: LogFormat,
        clock: Arc<dyn Clock>,
        out: Box<dyn Write + Send>,
    ) -> Self {
        Self {
            level,
            format,
            clock,
            out: Mutex::new(out),
        }
    }

    /// Writes one line if `level` is enabled. A write that fails is dropped: there
    /// is nowhere else to say so, and replication must not stop for it.
    pub(crate) fn log(&self, level: LogLevel, msg: &str, fields: &[(&str, Value)]) {
        if rank(level) < rank(self.level) {
            return;
        }
        let time = self.clock.now().to_string();
        let line = match self.format {
            LogFormat::Text => text_line(&time, level, msg, fields),
            LogFormat::Json => json_line(&time, level, msg, fields),
        };
        let mut out = self.out.lock().unwrap_or_else(PoisonError::into_inner);
        if out
            .write_all(line.as_bytes())
            .and_then(|()| out.flush())
            .is_err()
        {
            // The stream is gone; the exit code still reports the outcome.
        }
    }

    /// [`Logger::log`] at info.
    pub(crate) fn info(&self, msg: &str, fields: &[(&str, Value)]) {
        self.log(LogLevel::Info, msg, fields);
    }

    /// [`Logger::log`] at warn.
    pub(crate) fn warn(&self, msg: &str, fields: &[(&str, Value)]) {
        self.log(LogLevel::Warn, msg, fields);
    }

    /// [`Logger::log`] at error.
    pub(crate) fn error(&self, msg: &str, fields: &[(&str, Value)]) {
        self.log(LogLevel::Error, msg, fields);
    }
}

fn needs_quoting(text: &str) -> bool {
    text.is_empty()
        || text
            .chars()
            .any(|c| c == ' ' || c == '=' || c == '"' || c.is_control())
}

/// Go's `strconv.Quote` for the characters a log value holds.
fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(2));
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() => {
                out.push_str(&format!("\\x{:02x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn text_value(value: &Value) -> String {
    match value {
        Value::Text(text) if needs_quoting(text) => quote(text),
        Value::Text(text) => text.clone(),
        Value::Int(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
    }
}

/// slog's `TextHandler` line.
pub(crate) fn text_line(
    time: &str,
    level: LogLevel,
    msg: &str,
    fields: &[(&str, Value)],
) -> String {
    let mut line = format!(
        "time={time} level={} msg={}",
        name(level),
        text_value(&msg.into())
    );
    for (key, value) in fields {
        line.push_str(&format!(" {key}={}", text_value(value)));
    }
    line.push('\n');
    line
}

/// slog's `JSONHandler` line.
pub(crate) fn json_line(
    time: &str,
    level: LogLevel,
    msg: &str,
    fields: &[(&str, Value)],
) -> String {
    let mut line = format!(
        "{{\"time\":{},\"level\":\"{}\",\"msg\":{}",
        json_string(time),
        name(level),
        json_string(msg)
    );
    // A key `group.leaf` is slog's text spelling of a group; the JSON handler nests the
    // consecutive keys of one group in an object.
    let mut open: Option<&str> = None;
    for (key, value) in fields {
        let (group, leaf) = key
            .split_once('.')
            .map_or((None, *key), |(group, leaf)| (Some(group), leaf));
        let value = match value {
            Value::Text(text) => json_string(text),
            Value::Int(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
        };
        if group.is_some() && group == open {
            line.push_str(&format!(",{}:{value}", json_string(leaf)));
            continue;
        }
        if open.is_some() {
            line.push('}');
        }
        match group {
            Some(group) => line.push_str(&format!(
                ",{}:{{{}:{value}",
                json_string(group),
                json_string(leaf)
            )),
            None => line.push_str(&format!(",{}:{value}", json_string(key))),
        }
        open = group;
    }
    if open.is_some() {
        line.push('}');
    }
    line.push_str("}\n");
    line
}

/// One database's events, as log lines.
#[derive(Debug)]
pub(crate) struct DbEvents {
    log: Arc<Logger>,
    db: String,
}

impl DbEvents {
    /// The sink for the database at `path`.
    #[must_use]
    pub(crate) fn new(log: Arc<Logger>, path: &Path) -> Self {
        let db = path.file_name().map_or_else(
            || path.display().to_string(),
            |n| n.to_string_lossy().into_owned(),
        );
        Self { log, db }
    }
}

fn debug(value: &impl std::fmt::Debug) -> Value {
    Value::Text(format!("{value:?}"))
}

impl DbEvents {
    /// Logs a finished snapshot or compaction as Go does, with its `system=store` field, and
    /// says whether the event was one.
    fn log_store(&mut self, event: &Event) -> bool {
        let Event::Engine(EngineEvent::Maintained { event, .. }) = event else {
            return false;
        };
        let Some((msg, rest)) = store_line(event) else {
            return false;
        };
        let mut all = vec![
            ("system", Value::from("store")),
            ("db", Value::Text(self.db.clone())),
        ];
        all.extend(rest);
        self.log.log(LogLevel::Info, msg, &all);
        true
    }
}

impl EventSink for DbEvents {
    fn event(&mut self, event: &Event) {
        if self.log_store(event) {
            return;
        }
        let db = ("db", Value::Text(self.db.clone()));
        let (level, msg, fields): (LogLevel, &str, Vec<(&str, Value)>) = match event {
            Event::Engine(EngineEvent::Booted {
                cause: Some(SnapshotCause::AheadOfReplica { tip }),
            }) => {
                let msg = ahead_message(*tip);
                let tip = ("tip", Value::from(tip.get()));
                self.log.log(LogLevel::Warn, &msg, &[db, tip]);
                return;
            }
            Event::Engine(engine) => match engine_line(engine) {
                Some(line) => line,
                None => return,
            },
            Event::AppCheckpoint { seen, total } => (
                LogLevel::Warn,
                app_checkpoint_message(*seen),
                vec![("seen", debug(seen)), ("total", (*total).into())],
            ),
            Event::ScratchSweepDenied => (
                LogLevel::Warn,
                "the replica refused the sweep of leftover uploads at start (needs s3:ListBucketMultipartUploads and s3:AbortMultipartUpload); replication continues, and open multipart uploads a killed process left stay until a bucket lifecycle rule or an operator removes them",
                vec![],
            ),
            Event::Panicked { rebuilt } => (
                LogLevel::Error,
                "replicator panicked",
                vec![("rebuilt", (*rebuilt).into())],
            ),
            Event::Stopped(cause) => (
                LogLevel::Error,
                "replication stopped",
                vec![("cause", debug(cause))],
            ),
            Event::Refused { through } => {
                let msg = refused_message(*through);
                let through = ("through", Value::from(through.get()));
                self.log.log(LogLevel::Error, &msg, &[db, through]);
                return;
            }
            Event::TakeoverRecordUnusable(unusable) => (
                LogLevel::Warn,
                "the takeover record in the meta directory (lsr-takeover) cannot be used and is ignored; the boot builds only on what the WAL vouches for",
                vec![("problem", debug(unusable))],
            ),
            Event::TakeoverNotRecorded { kind } => (
                LogLevel::Error,
                "the takeover record could not be written; staged files are kept until it is",
                vec![("kind", debug(kind))],
            ),
            Event::TakeoverNotRemoved { kind } => (
                LogLevel::Error,
                "a spent takeover record could not be deleted from the meta directory; retrying",
                vec![("kind", debug(kind))],
            ),
            Event::Verified { txid, outcome } => verified_line(*txid, *outcome),
            Event::WritesBlocked { why } => (
                failstop::writes_blocked_level(*why),
                failstop::WRITES_BLOCKED,
                vec![("held_by", failstop::why_name(*why).into())],
            ),
            Event::WritesAllowed => (LogLevel::Info, failstop::WRITES_ALLOWED, vec![]),
        };
        let mut all = vec![db];
        all.extend(fields);
        self.log.log(level, msg, &all);
    }
}

/// The text for a boot with a brand-new meta directory that found the database ahead of the
/// replica: not its newest state, but written after it and holding most of its pages
/// (`lsr_core::SnapshotCause::AheadOfReplica`). A warning, not an error: the engine goes on.
fn ahead_message(tip: Txid) -> String {
    format!(
        "the meta directory has no record of the replica's newest transaction {tip}, but this database was written after it and holds most of its pages: replicating it as the same database with commits that never reached the replica, starting with a snapshot above {tip}. If another replicator writes to this prefix, stop it."
    )
}

/// The approved text for a boot that refused the replica's newest transactions: nothing
/// local vouched for them (`lsr_replica::Event::Refused`).
fn refused_message(through: Txid) -> String {
    format!(
        "replicate refused: the replica holds transactions up to {through} that this database has no record of writing. Another replicator may be using this prefix. If this database is the right one, run `lsr trust-local <db> --replica <url>` and start again. If the replica is the right one, restore from it with `lsr restore`."
    )
}

type Line = (LogLevel, &'static str, Vec<(&'static str, Value)>);

/// A scheduled verify's line; Go logs its validation problems as warnings and a check that
/// could not run as an error, and a restore that differs from the database is worse than both.
fn verified_line(txid: Txid, outcome: VerifyOutcome) -> Line {
    let txid = ("txid", Value::from(txid.get()));
    match outcome {
        VerifyOutcome::Passed { pages } => (
            LogLevel::Debug,
            "replica verified against the database",
            vec![txid, ("pages", u64::from(pages).into())],
        ),
        VerifyOutcome::Failed(found) => (
            LogLevel::Error,
            "replica does not restore the database",
            vec![txid, ("found", debug(&found))],
        ),
        VerifyOutcome::Merged => (
            LogLevel::Debug,
            "verify skipped: transaction already compacted",
            vec![txid],
        ),
        VerifyOutcome::Incomplete => (LogLevel::Warn, "verify could not finish", vec![txid]),
    }
}

/// A task's degraded event: the two causes that report lost commits are logged on their own.
fn degraded_line(task: Task, cause: DegradedCause, until: Timestamp) -> Line {
    match cause {
        DegradedCause::StoreDenied(_)
        | DegradedCause::StoreRefused
        | DegradedCause::RemoteCorrupt
        | DegradedCause::OtherWriter
        | DegradedCause::RemoteRegressed
        | DegradedCause::WalCorrupt
        | DegradedCause::WalTruncated
        | DegradedCause::WalUnlinked
        | DegradedCause::WalSyncFailed
        | DegradedCause::PageSizeChanged { .. }
        | DegradedCause::DatabasePageMismatch { .. }
        | DegradedCause::ConditionalWritesUnsupported
        | DegradedCause::Bug
        | DegradedCause::Stalled(_) => (
            LogLevel::Warn,
            "replication degraded",
            vec![
                ("task", debug(&task)),
                ("cause", debug(&cause)),
                ("retry_at", until.to_string().into()),
            ],
        ),
    }
}

/// Go's `store` logs a finished compaction and a finished snapshot at info level (`store.go:828`,
/// `store.go:849`), which a deploy script may grep for: the same message and attribute names, after
/// `system=store db=<name>`. A level-9 file is Go's snapshot, its `txid` the newest transaction in
/// it; any other level is a compaction with its `txid` group (slog's `txid.min`, `txid.max`).
fn store_line(event: &MaintainedEvent) -> Option<(&'static str, Vec<(&'static str, Value)>)> {
    Some(match event {
        MaintainedEvent::Compacted(info) => {
            let range = info.key.range();
            (
                "compaction complete",
                vec![
                    ("level", u64::from(info.key.level().get()).into()),
                    ("txid.min", range.min().to_string().into()),
                    ("txid.max", range.max().to_string().into()),
                    ("size", info.size.into()),
                ],
            )
        }
        MaintainedEvent::Snapshot(info) => (
            "snapshot complete",
            vec![
                ("txid", info.key.range().max().to_string().into()),
                ("size", info.size.into()),
            ],
        ),
        MaintainedEvent::Deleted(_)
        | MaintainedEvent::Replan
        | MaintainedEvent::Repaired(_)
        | MaintainedEvent::Waiting { .. } => return None,
    })
}

/// What an observation of the application's checkpoints says happened. A stall is the one
/// where nothing was checkpointed, by the application or anyone: the line says what was read
/// (a WAL past the threshold, nothing new copied) and not that a checkpoint ran.
const fn app_checkpoint_message(seen: AppCheckpoint) -> &'static str {
    match seen {
        AppCheckpoint::Backfilled { .. } | AppCheckpoint::Restarted => {
            "the application checkpointed the database"
        }
        AppCheckpoint::Stalled { .. } => {
            "the WAL grew past the application's automatic checkpoint threshold with nothing new checkpointed"
        }
    }
}

fn engine_line(event: &EngineEvent) -> Option<Line> {
    Some(match event {
        EngineEvent::Idle => return None,
        EngineEvent::Booted { cause: None } => {
            (LogLevel::Info, "replica position recovered", vec![])
        }
        EngineEvent::Booted { cause: Some(cause) } => (
            LogLevel::Info,
            "replica position recovered; snapshot required",
            vec![("cause", debug(cause))],
        ),
        EngineEvent::Synced { files } => (
            LogLevel::Debug,
            "wal synced",
            vec![("files", u64::from(*files).into())],
        ),
        EngineEvent::Checkpointed { mode, busy } => (
            LogLevel::Debug,
            "checkpoint",
            vec![("mode", debug(mode)), ("busy", (*busy).into())],
        ),
        EngineEvent::Uploaded(did) => (LogLevel::Debug, "uploaded", vec![("did", debug(did))]),
        EngineEvent::Maintained { event, .. } => (
            LogLevel::Debug,
            "maintenance",
            vec![("event", debug(event))],
        ),
        EngineEvent::Degraded {
            task, cause, until, ..
        } => degraded_line(*task, *cause, *until),
        EngineEvent::Retrying { fault, until } => (
            LogLevel::Warn,
            "retrying",
            vec![
                ("error", debug(fault)),
                ("retry_at", until.to_string().into()),
            ],
        ),
        EngineEvent::CheckpointDeferred { until } => (
            LogLevel::Debug,
            "checkpoint skipped: the database's write lock is busy",
            vec![("retry_at", until.to_string().into())],
        ),
        EngineEvent::WritesHeld { until } => (
            LogLevel::Debug,
            "checkpoint waits while writes are blocked",
            vec![("retry_at", until.to_string().into())],
        ),
        EngineEvent::Resnapshot(cause) => (
            LogLevel::Info,
            "snapshot required",
            vec![("cause", debug(cause))],
        ),
        EngineEvent::Clamped(clamped) => (
            LogLevel::Warn,
            "clock went backwards",
            vec![("clamped", debug(clamped))],
        ),
        EngineEvent::CheckingIdentity { tip } => (
            LogLevel::Info,
            "comparing the database with the replica before starting",
            vec![("tip", tip.get().into())],
        ),
        EngineEvent::TakeoverWanted(record) => (
            LogLevel::Info,
            "takeover record kept before dropping staged files",
            vec![("through", record.txid().get().into())],
        ),
    })
}

#[cfg(test)]
mod tests;
