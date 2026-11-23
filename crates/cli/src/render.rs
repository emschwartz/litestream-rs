//! Text and JSON output, and the one-line error text.
//!
//! JSON is written by hand: keys are fixed and values are numbers, booleans,
//! hex transaction ids, RFC 3339 times, paths and messages, so a ten-line
//! escaper is all the format needs. Tests parse it back with `serde_json`.
//!
//! Restore's JSON and its dry-run text keep Go's keys and layout
//! (cmd/litestream/restore.go: `RestoreResult`, `RestorePlan`,
//! `printDryRunPlan`), then add what this restore also knows. JSON is one line,
//! as for verify and files, where Go indents. A skip prints one object too,
//! where Go prints only a log line, so a JSON reader always gets an answer.

use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{
    ContiguousChain, Depth, FileInfo, FileKey, Finding, Inventory, Level, LevelSummary,
    PlanOutcome, Report, Restored, Severity,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::args::{
    CONFIG_CHECK_SUMMARY, CONFIG_CHECK_USAGE, CommandName, DAEMON_SUMMARY, DAEMON_USAGE, FLAGS,
};
use crate::run::{CliError, Done, Exit, Finished, RestoreDone, Skip, Source};
use lsr_core::ListedTime;
use lsr_replica::local::IntegrityCheck;
use lsr_sidecar::json_string as quote;

/// Appends formatted text to a `String`, which cannot fail.
macro_rules! put {
    ($out:expr, $($arg:tt)*) => {
        $out.push_str(&format!($($arg)*))
    };
}

/// [`put`] and a newline.
macro_rules! putln {
    ($out:expr, $($arg:tt)*) => {{
        $out.push_str(&format!($($arg)*));
        $out.push('\n');
    }};
}

/// The stdout text of a finished command.
pub fn done(done: &Done) -> String {
    match done {
        Done::Restore(done) => restore(done),
        Done::Verified {
            depth,
            report,
            json: false,
        } => verify_text(report, *depth),
        Done::Verified {
            depth,
            report,
            json: true,
        } => verify_json(report, *depth),
        Done::Files {
            inventory,
            created_at_fallbacks,
            json: false,
        } => files_text(inventory, *created_at_fallbacks),
        Done::Files {
            inventory,
            created_at_fallbacks,
            json: true,
        } => files_json(inventory, *created_at_fallbacks),
        Done::TakenOver { through, meta } => taken_over_text(*through, meta),
        Done::Daemon(_) => String::new(),
        Done::DaemonHelp(text) => (*text).to_owned(),
        Done::ConfigChecked(checked) => checked.to_string(),
        Done::Help => help(),
        Done::Version => format!("lsr {}\n", env!("CARGO_PKG_VERSION")),
    }
}

/// `lsr take-over`'s line.
fn taken_over_text(through: Option<Txid>, meta: &Path) -> String {
    match through {
        Some(txid) => format!("takeover recorded through {txid} in {}\n", meta.display()),
        None => "the replica is empty: there is nothing to take over and nothing was recorded\n"
            .to_owned(),
    }
}

/// The one-line error for stderr.
pub fn error_line(error: &CliError) -> String {
    let mut line = format!("lsr: {error}");
    if let CliError::Usage(_) = error {
        line.push_str(" (try lsr --help)");
    }
    line.replace('\n', " ")
}

/// The help text, generated from the flag table and the exit codes.
pub fn help() -> String {
    let mut out = String::from("lsr: restore, verify and list a Litestream file replica\n");
    for command in CommandName::ALL {
        let mut usage = format!("\nlsr {}", command.word());
        if let Some(positional) = command.positional() {
            put!(usage, " {positional}");
        }
        for spec in FLAGS.iter().filter(|s| s.commands.contains(&command)) {
            let value = spec.value.map_or(String::new(), |v| format!(" {v}"));
            let flag = format!("{}{value}", spec.name);
            put!(
                usage,
                " {}",
                if spec.required {
                    flag
                } else {
                    format!("[{flag}]")
                }
            );
        }
        putln!(out, "{usage}\n  {}", command.summary());
        for spec in FLAGS.iter().filter(|s| s.commands.contains(&command)) {
            putln!(out, "  {:<20}{}", spec.name, spec.help);
        }
    }
    putln!(out, "\n{DAEMON_USAGE}\n  {DAEMON_SUMMARY}");
    putln!(out, "\n{CONFIG_CHECK_USAGE}\n  {CONFIG_CHECK_SUMMARY}");
    out.push_str(
        "\nlsr --help, lsr -h: this text; lsr --version, lsr -V: the version\n\nexit codes:\n",
    );
    for exit in Exit::ALL {
        putln!(out, "  {:<3}{}", exit.code(), exit.meaning());
    }
    out.push_str("  replicate exits 1 on any failure but 4, as Go Litestream does\n");
    out
}

/// `a, b, c`, for an error line.
pub fn paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn key_path(key: &FileKey) -> String {
    format!("ltx/{}/{}", key.level(), key.file_name())
}

/// Why a replica has (or has not) an exact answer, as one clause.
pub fn plan_outcome(outcome: &PlanOutcome) -> String {
    match outcome {
        PlanOutcome::Plan(chain) => format!(
            "restorable through transaction {} from {} files",
            chain.reaches(),
            chain.files().len()
        ),
        PlanOutcome::EmptyReplica => "the replica holds no LTX file".to_owned(),
        PlanOutcome::NoSnapshot {
            oldest_snapshot,
            malformed_snapshots,
        } => {
            let mut text = "no snapshot can start a restore of this target".to_owned();
            if let Some(oldest) = oldest_snapshot {
                put!(text, "; the oldest snapshot ends at transaction {oldest}");
            }
            if !malformed_snapshots.is_empty() {
                put!(
                    text,
                    "; {} level-9 files do not start at transaction 1",
                    malformed_snapshots.len()
                );
            }
            text
        }
        PlanOutcome::BeyondNewest { requested, newest } => {
            format!("transaction {requested} is beyond the newest, {newest}")
        }
        PlanOutcome::AsOfInsideFile { chain, file } => {
            format!(
                "the instant falls inside {}; the newest state before it ends at transaction {}",
                key_path(file),
                chain.reaches()
            ) + &format!(
                "; rerun with --allow-earlier to restore that state, or with --txid {}",
                chain.reaches()
            )
        }
        PlanOutcome::Gap {
            have_through,
            next_starts,
        } => format!(
            "gap: the history reaches transaction {have_through} and resumes at {next_starts}"
        ),
        PlanOutcome::InsideFile { requested, file } => format!(
            "transaction {requested} is inside {} and the finer files for it are gone",
            key_path(file)
        ),
        PlanOutcome::MalformedSnapshot { have_through, file } => format!(
            "the history reaches transaction {have_through}; the rest is in {}, a level-9 file that does not start at transaction 1",
            key_path(file)
        ),
    }
}

fn restore(done: &RestoreDone) -> String {
    let RestoreDone {
        output,
        source,
        json,
        finished,
        ..
    } = done;
    match (finished, *json) {
        (
            Finished::Restored {
                restored,
                cleared,
                integrity_check,
                ..
            },
            false,
        ) => restored_text(output, restored, cleared, *integrity_check),
        (
            Finished::Restored {
                restored,
                cleared,
                integrity_check,
                elapsed,
            },
            true,
        ) => restored_json(
            output,
            source,
            restored,
            cleared,
            *integrity_check,
            *elapsed,
        ),
        (
            Finished::Planned {
                chain,
                created_at_read,
                journal_files_to_remove,
            },
            false,
        ) => plan_text(
            output,
            source,
            chain,
            *created_at_read,
            journal_files_to_remove,
        ),
        (
            Finished::Planned {
                chain,
                created_at_read,
                journal_files_to_remove,
            },
            true,
        ) => plan_json(
            output,
            source,
            chain,
            *created_at_read,
            journal_files_to_remove,
        ),
        (Finished::Skipped { skip, cleared }, false) => {
            skipped_text(output, source, *skip, cleared)
        }
        (Finished::Skipped { skip, cleared }, true) => skipped_json(output, source, *skip, cleared),
    }
}

/// The stderr lines of a finished command: a restore that accepted an instant
/// inside a compacted file says so, since its database may lack transactions
/// committed before the instant.
pub fn warnings(done: &Done) -> String {
    let Done::Restore(done) = done else {
        return String::new();
    };
    let parallelism = if done.parallelism {
        "warning: -parallelism is deprecated and has no effect; accepted for compatibility only\n"
    } else {
        ""
    };
    let rest = match &done.finished {
        Finished::Restored { restored, .. } => {
            let mut out = restored.inside_file().map_or_else(String::new, |file| {
                inside_warning(&file, restored.reached())
            });
            if restored.writer_outpaced() {
                out.push_str(&outpaced_warning(restored.reached()));
            }
            out
        }
        Finished::Planned { .. } | Finished::Skipped { .. } => String::new(),
    };
    format!("{parallelism}{rest}")
}

fn outpaced_warning(reached: Txid) -> String {
    format!(
        "warning: a writer kept adding files while the replica was listed; restored the state at \
         transaction {reached}, which is committed but may be behind the writer by now\n"
    )
}

fn inside_warning(file: &FileKey, reached: Txid) -> String {
    format!(
        "warning: the instant falls inside {}; restored the state at transaction {reached}, which may leave out transactions committed before the instant\n",
        key_path(file)
    )
}

/// Go's `-integrity-check` word for the check.
const fn integrity_word(check: IntegrityCheck) -> &'static str {
    match check {
        IntegrityCheck::Off => "none",
        IntegrityCheck::Quick => "quick",
        IntegrityCheck::Full => "full",
    }
}

const fn skip_word(skip: Skip) -> &'static str {
    match skip {
        Skip::OutputExists => "output_exists",
        Skip::EmptyReplica => "empty_replica",
        Skip::NoLtxFolder => "no_ltx_folder",
    }
}

fn path_array(paths: &[PathBuf]) -> String {
    let items: Vec<String> = paths
        .iter()
        .map(|path| quote(&path.display().to_string()))
        .collect();
    array(&items)
}

fn restored_json(
    output: &Path,
    source: &Source,
    restored: &Restored,
    cleared: &[PathBuf],
    integrity_check: IntegrityCheck,
    elapsed: Duration,
) -> String {
    let mut fields = vec![
        ("db_path", quote(&output.display().to_string())),
        ("replica", quote(source.kind().word())),
        ("txid", quote(&restored.reached().to_string())),
        ("duration_ms", elapsed.as_millis().to_string()),
        ("integrity_check", quote(integrity_word(integrity_check))),
        ("page_size", restored.page_size().get().to_string()),
        ("pages", restored.commit().to_string()),
        ("files", restored.files().to_string()),
        ("bytes_read", restored.bytes_read().to_string()),
        ("created_at_read", restored.created_at_read().to_string()),
        (
            "checksum_verified",
            restored.post_apply().is_some().to_string(),
        ),
        ("journal_files_removed", path_array(cleared)),
    ];
    if let Some(file) = restored.inside_file() {
        fields.push(("inside_file", quote(&key_path(&file))));
    }
    if restored.writer_outpaced() {
        fields.push(("writer_outpaced", "true".to_owned()));
    }
    let mut out = object(&fields);
    out.push('\n');
    out
}

fn skipped_text(output: &Path, source: &Source, skip: Skip, cleared: &[PathBuf]) -> String {
    let mut out = String::new();
    for path in cleared {
        putln!(out, "removed stale {}", path.display());
    }
    match skip {
        Skip::OutputExists => putln!(
            out,
            "skipped: {} already exists (--if-db-not-exists)",
            output.display()
        ),
        Skip::EmptyReplica => putln!(
            out,
            "skipped: the replica holds no backup (--if-replica-exists); nothing written to {}",
            output.display()
        ),
        Skip::NoLtxFolder => putln!(
            out,
            "skipped: {} holds no backup, it has no ltx/ folder (--if-replica-exists); nothing written to {}",
            source.name(),
            output.display()
        ),
    }
    out
}

fn skipped_json(output: &Path, source: &Source, skip: Skip, cleared: &[PathBuf]) -> String {
    let mut out = object(&[
        ("skipped", quote(skip_word(skip))),
        ("db_path", quote(&output.display().to_string())),
        ("replica", quote(source.kind().word())),
        ("journal_files_removed", path_array(cleared)),
    ]);
    out.push('\n');
    out
}

/// One row of Go's dry-run table: level, file, min_txid, max_txid, size, timestamp.
fn plan_row(info: &FileInfo) -> [String; 6] {
    let range = info.key.range();
    let (time, exact) = listed_time(info);
    let note = if exact { "" } else { " (listed)" };
    [
        info.key.level().to_string(),
        info.key.file_name(),
        range.min().to_string(),
        range.max().to_string(),
        info.size.to_string(),
        format!("{time}{note}"),
    ]
}

/// Left-aligned columns two spaces apart, as Go's tabwriter prints them.
fn columns(rows: &[[String; 6]]) -> String {
    let mut widths = [0_usize; 6];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let mut out = String::new();
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .zip(widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        putln!(out, "{}", cells.join("  ").trim_end());
    }
    out
}

fn plan_text(
    output: &Path,
    source: &Source,
    chain: &ContiguousChain,
    created_at_read: u64,
    journal_files_to_remove: &[PathBuf],
) -> String {
    let mut out = String::new();
    for path in journal_files_to_remove {
        putln!(out, "would remove stale {}", path.display());
    }
    out.push_str("Restore plan:\n");
    putln!(out, "  source: {}", source.name());
    putln!(out, "  target: {}", output.display());
    putln!(out, "  replica: {}", source.kind().word());
    putln!(
        out,
        "  txid range: {} - {}",
        chain.snapshot().key.range().min(),
        chain.reaches()
    );
    if created_at_read > 0 {
        putln!(out, "  created-at times read to plan: {created_at_read}");
    }
    out.push_str("\nFiles to fetch:\n");
    let header = ["level", "file", "min_txid", "max_txid", "size", "timestamp"].map(str::to_owned);
    let rows: Vec<[String; 6]> = std::iter::once(header)
        .chain(chain.files().iter().map(plan_row))
        .collect();
    out.push_str(&columns(&rows));
    out
}

fn plan_file_json(info: &FileInfo) -> String {
    let range = info.key.range();
    let (time, exact) = listed_time(info);
    object(&[
        ("level", info.key.level().get().to_string()),
        ("name", quote(&info.key.file_name())),
        ("min_txid", quote(&range.min().to_string())),
        ("max_txid", quote(&range.max().to_string())),
        ("size", info.size.to_string()),
        ("timestamp", quote(&time.to_string())),
        ("created_at_exact", exact.to_string()),
    ])
}

fn plan_json(
    output: &Path,
    source: &Source,
    chain: &ContiguousChain,
    created_at_read: u64,
    journal_files_to_remove: &[PathBuf],
) -> String {
    let files: Vec<String> = chain.files().iter().map(plan_file_json).collect();
    let mut out = object(&[
        ("source", quote(source.name())),
        ("target_path", quote(&output.display().to_string())),
        ("replica", quote(source.kind().word())),
        (
            "min_txid",
            quote(&chain.snapshot().key.range().min().to_string()),
        ),
        ("max_txid", quote(&chain.reaches().to_string())),
        ("files", array(&files)),
        ("created_at_read", created_at_read.to_string()),
        (
            "journal_files_to_remove",
            path_array(journal_files_to_remove),
        ),
    ]);
    out.push('\n');
    out
}

fn restored_text(
    output: &Path,
    restored: &Restored,
    cleared: &[PathBuf],
    integrity_check: IntegrityCheck,
) -> String {
    let checksum = if restored.post_apply().is_some() {
        "database checksum verified"
    } else {
        "database checksum not verified (the chain did not track it)"
    };
    let mut out = String::new();
    for path in cleared {
        putln!(out, "removed stale {}", path.display());
    }
    putln!(
        out,
        "restored {}: transaction {}, {} pages of {} bytes, {} files and {} bytes read, {checksum}",
        output.display(),
        restored.reached(),
        restored.commit(),
        restored.page_size().get(),
        restored.files(),
        restored.bytes_read(),
    );
    putln!(out, "takeover recorded through {}", restored.reached());
    // Only an as-of restore over a lazily listed store reads created-at times; the line
    // for every other restore stays as it was.
    if restored.created_at_read() > 0 {
        putln!(
            out,
            "{} created-at times read to plan",
            restored.created_at_read()
        );
    }
    if let Some(pragma) = integrity_check.pragma() {
        putln!(out, "SQLite {pragma} passed before publishing");
    }
    out
}

const fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Note => "note",
        Severity::Degraded => "degraded",
        Severity::Unrestorable => "unrestorable",
    }
}

const fn depth_name(depth: Depth) -> &'static str {
    match depth {
        Depth::Names => "names",
        Depth::Contents => "contents",
    }
}

/// A finding's machine name and its message.
#[expect(
    clippy::too_many_lines,
    reason = "one arm per Finding variant, each a single message; the match must stay exhaustive"
)]
fn finding_parts(finding: &Finding) -> (&'static str, String) {
    match finding {
        Finding::NoSnapshot => (
            "no_snapshot",
            "files exist but none can start a restore".to_owned(),
        ),
        Finding::Gap {
            snapshot,
            have_through,
            next_starts,
            at_newest,
        } => (
            "gap",
            format!(
                "from {} the history reaches transaction {have_through} and resumes at {next_starts}{}",
                key_path(snapshot),
                if *at_newest {
                    " (the newest state is cut off)"
                } else {
                    " (older history)"
                }
            ),
        ),
        Finding::MalformedSnapshot { key } => (
            "malformed_snapshot",
            format!(
                "{} is a level-9 file that does not start at transaction 1",
                key_path(key)
            ),
        ),
        Finding::UnrecognizedNames { level, count } => (
            "unrecognized_names",
            format!("{count} names in level {level} are not LTX files"),
        ),
        Finding::Unreadable { key, error, .. } => (
            "unreadable",
            format!("{} does not decode: {error}", key_path(key)),
        ),
        Finding::RangeMismatch { key, header, .. } => (
            "range_mismatch",
            format!("{} has a header covering {header}", key_path(key)),
        ),
        Finding::NotASnapshot { key, .. } => (
            "not_a_snapshot",
            format!(
                "{} is at level 9 but its header is not a snapshot",
                key_path(key)
            ),
        ),
        Finding::PageSizeDiffers {
            key,
            expected,
            found,
            ..
        } => (
            "page_size_differs",
            format!(
                "{} has page size {} where {} was expected",
                key_path(key),
                found.get(),
                expected.get()
            ),
        ),
        Finding::SizeDiffers { key, listed, read } => (
            "size_differs",
            format!(
                "{} is listed at {listed} bytes but holds {read}",
                key_path(key)
            ),
        ),
        Finding::TimestampsDiffer {
            files,
            first,
            largest,
        } => (
            "timestamps_differ",
            format!(
                "{files} files have header times that differ from the store's (first {}, by up to {} ms); restores to a point in time follow the store's times, the latest state is unaffected",
                key_path(first),
                largest.as_millis()
            ),
        ),
        Finding::ChecksumChain {
            key,
            expected,
            found,
            ..
        } => (
            "checksum_chain",
            format!(
                "{} starts from checksum {found}, the file before it ended at {expected}",
                key_path(key)
            ),
        ),
        Finding::Vanished { key } => (
            "vanished",
            format!(
                "{} was listed and then gone (compaction or retention)",
                key_path(key)
            ),
        ),
        Finding::LatestRestoreFails { key, error } => (
            "latest_restore_fails",
            match key {
                Some(key) => format!(
                    "restoring the latest state fails at {}: {error}",
                    key_path(key)
                ),
                None => format!("restoring the latest state fails: {error}"),
            },
        ),
        Finding::Truncated { more } => ("truncated", format!("{more} more findings were not kept")),
    }
}

fn level_range(summary: &LevelSummary) -> Option<TxidRange> {
    summary.range()
}

const LEVEL_HEADERS: [&str; 6] = [
    "level",
    "files",
    "bytes",
    "first transaction",
    "last transaction",
    "unrecognized",
];

/// Columns padded to the widest of header and values, two spaces apart; the
/// numeric columns (all but the two transaction ids) are right-aligned.
fn levels_table(rows: &[[String; 6]]) -> String {
    let mut widths = LEVEL_HEADERS.map(str::len);
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let mut out = String::new();
    let header = LEVEL_HEADERS.map(str::to_owned);
    for row in std::iter::once(&header).chain(rows) {
        let cells: Vec<String> = row
            .iter()
            .zip(widths)
            .enumerate()
            .map(|(column, (cell, width))| {
                if matches!(column, 3 | 4) {
                    format!("{cell:<width$}")
                } else {
                    format!("{cell:>width$}")
                }
            })
            .collect();
        putln!(out, "{}", cells.join("  ").trim_end());
    }
    out
}

fn verify_text(report: &Report, depth: Depth) -> String {
    let mut rows = Vec::new();
    for (level, summary) in Level::ALL.iter().zip(report.levels()) {
        if summary.files() == 0 && summary.skipped() == 0 {
            continue;
        }
        let range = level_range(summary);
        rows.push([
            level.to_string(),
            summary.files().to_string(),
            summary.bytes().to_string(),
            range.map_or("-".to_owned(), |r| r.min().to_string()),
            range.map_or("-".to_owned(), |r| r.max().to_string()),
            summary.skipped().to_string(),
        ]);
    }
    let mut out = levels_table(&rows);
    putln!(out, "latest: {}", plan_outcome(report.latest()));
    match depth {
        Depth::Names => out.push_str("depth: names (no file was read)\n"),
        Depth::Contents => {
            putln!(
                out,
                "depth: contents ({} files, {} bytes read)",
                report.files_read(),
                report.bytes_read()
            );
        }
    }
    if report.findings().is_empty() {
        out.push_str("findings: none\n");
    }
    for finding in report.findings() {
        let (kind, message) = finding_parts(finding);
        putln!(
            out,
            "{}: {kind}: {message}",
            severity_name(finding.severity())
        );
    }
    putln!(
        out,
        "result: {}",
        report.worst().map_or("clean", severity_name)
    );
    out
}

fn object(fields: &[(&str, String)]) -> String {
    let body: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("{}:{value}", quote(key)))
        .collect();
    format!("{{{}}}", body.join(","))
}

fn array(items: &[String]) -> String {
    format!("[{}]", items.join(","))
}

fn hex_or_null(txid: Option<Txid>) -> String {
    txid.map_or("null".to_owned(), |txid| quote(&txid.to_string()))
}

fn level_json(level: Level, summary: &LevelSummary) -> String {
    let range = level_range(summary);
    object(&[
        ("level", level.get().to_string()),
        ("files", summary.files().to_string()),
        ("bytes", summary.bytes().to_string()),
        ("first", hex_or_null(range.map(TxidRange::min))),
        ("last", hex_or_null(range.map(TxidRange::max))),
        ("unrecognized_names", summary.skipped().to_string()),
    ])
}

fn finding_json(finding: &Finding) -> String {
    let (kind, message) = finding_parts(finding);
    object(&[
        ("kind", quote(kind)),
        ("severity", quote(severity_name(finding.severity()))),
        ("message", quote(&message)),
    ])
}

fn verify_json(report: &Report, depth: Depth) -> String {
    let reaches = match report.latest() {
        PlanOutcome::Plan(chain) => Some(chain.reaches()),
        PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. } => None,
    };
    let latest = object(&[
        ("restorable", reaches.is_some().to_string()),
        ("reaches", hex_or_null(reaches)),
        ("description", quote(&plan_outcome(report.latest()))),
    ]);
    let levels: Vec<String> = Level::ALL
        .iter()
        .zip(report.levels())
        .map(|(level, summary)| level_json(*level, summary))
        .collect();
    let findings: Vec<String> = report.findings().iter().map(finding_json).collect();
    let worst = report
        .worst()
        .map_or("null".to_owned(), |severity| quote(severity_name(severity)));
    let mut out = object(&[
        ("depth", quote(depth_name(depth))),
        ("worst", worst),
        ("files_read", report.files_read().to_string()),
        ("bytes_read", report.bytes_read().to_string()),
        ("latest", latest),
        ("levels", array(&levels)),
        ("findings", array(&findings)),
    ]);
    out.push('\n');
    out
}

fn files_text(inventory: &Inventory, created_at_fallbacks: Option<u64>) -> String {
    let mut out = String::new();
    for info in inventory.files() {
        let (time, exact) = listed_time(info);
        let note = if exact { "" } else { " (listed)" };
        putln!(out, "{} {} {time}{note}", key_path(&info.key), info.size);
    }
    if inventory.skipped_total() > 0 {
        putln!(
            out,
            "unrecognized names skipped: {}",
            inventory.skipped_total()
        );
    }
    if let Some(count) = created_at_fallbacks {
        putln!(out, "stored times taken from LastModified: {count}");
    }
    out
}

/// The time a listing gave for a file, and whether it is the exact created-at time or
/// only the bucket's `LastModified`.
fn listed_time(info: &FileInfo) -> (Timestamp, bool) {
    match info.listed {
        ListedTime::Created(created_at) => (created_at.at(), true),
        ListedTime::LastModified(at) => (at, false),
    }
}

fn file_json(info: &FileInfo) -> String {
    let range = info.key.range();
    let (created, exact) = listed_time(info);
    object(&[
        ("level", info.key.level().get().to_string()),
        ("key", quote(&key_path(&info.key))),
        ("min", quote(&range.min().to_string())),
        ("max", quote(&range.max().to_string())),
        ("size", info.size.to_string()),
        ("created_at", quote(&created.to_string())),
        ("created_at_ms", created.unix_millis().to_string()),
        ("created_at_exact", exact.to_string()),
    ])
}

fn files_json(inventory: &Inventory, created_at_fallbacks: Option<u64>) -> String {
    let files: Vec<String> = inventory.files().iter().map(file_json).collect();
    let mut fields = vec![
        ("files", array(&files)),
        ("unrecognized_names", inventory.skipped_total().to_string()),
    ];
    if let Some(count) = created_at_fallbacks {
        fields.push(("created_at_from_last_modified", count.to_string()));
    }
    let mut out = object(&fields);
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::{LEVEL_HEADERS, levels_table};

    fn row(cells: [&str; 6]) -> [String; 6] {
        cells.map(str::to_owned)
    }

    /// A 1-digit level, 3-digit file count and 12-digit byte count once ran the columns together.
    #[test]
    fn level_table_cells_survive_splitting_on_two_spaces() {
        let tx = "000000000000000f";
        let rows = [
            row(["0", "624", "123456789012", tx, tx, "0"]),
            row(["9", "1", "7", "-", "-", "12"]),
        ];
        let table = levels_table(&rows);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines.len(), 3);
        for (line, want) in lines.iter().zip(
            std::iter::once(row([
                "level",
                "files",
                "bytes",
                "first transaction",
                "last transaction",
                "unrecognized",
            ]))
            .chain(rows),
        ) {
            let cells: Vec<&str> = line
                .split("  ")
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .collect();
            assert_eq!(cells.len(), LEVEL_HEADERS.len(), "{line:?}");
            assert_eq!(
                cells,
                want.iter().map(String::as_str).collect::<Vec<_>>(),
                "{line:?}"
            );
        }
    }
}
