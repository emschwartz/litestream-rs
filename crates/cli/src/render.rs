//! Text and JSON output, and the one-line error text.
//!
//! JSON is written by hand: keys are fixed and values are numbers, booleans,
//! hex transaction ids, RFC 3339 times, paths and messages, so a ten-line
//! escaper is all the format needs. Tests parse it back with `serde_json`.

use std::path::{Path, PathBuf};

use lsr_core::{
    Depth, FileInfo, FileKey, Finding, Inventory, Level, LevelSummary, PlanOutcome, Report,
    Restored, Severity,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::args::{CommandName, FLAGS};
use crate::run::{CliError, Done, Exit};

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
        Done::Restored {
            output,
            restored,
            cleared,
        } => restored_text(output, restored, cleared),
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
            stamp_fallbacks,
            json: false,
        } => files_text(inventory, *stamp_fallbacks),
        Done::Files {
            inventory,
            stamp_fallbacks,
            json: true,
        } => files_json(inventory, *stamp_fallbacks),
        Done::Help => help(),
        Done::Version => format!("lsr {}\n", env!("CARGO_PKG_VERSION")),
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
    out.push_str(
        "\nlsr --help, lsr -h: this text; lsr --version, lsr -V: the version\n\nexit codes:\n",
    );
    for exit in Exit::ALL {
        putln!(out, "  {:<3}{}", exit.code(), exit.meaning());
    }
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
        PlanOutcome::AsOfInsideFile { chain, file } => format!(
            "the instant falls inside {}; the newest state before it ends at transaction {}",
            key_path(file),
            chain.reaches()
        ),
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

fn restored_text(output: &Path, restored: &Restored, cleared: &[PathBuf]) -> String {
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
            at_tip,
        } => (
            "gap",
            format!(
                "from {} the history reaches transaction {have_through} and resumes at {next_starts}{}",
                key_path(snapshot),
                if *at_tip {
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
        Finding::ForeignNames { level, count } => (
            "foreign_names",
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
    "foreign",
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

/// A JSON string literal.
fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(2));
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control.is_control() => {
                put!(out, "\\u{:04x}", u32::from(control));
            }
            plain => out.push(plain),
        }
    }
    out.push('"');
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
        ("foreign_names", summary.skipped().to_string()),
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

fn files_text(inventory: &Inventory, stamp_fallbacks: Option<u64>) -> String {
    let mut out = String::new();
    for info in inventory.files() {
        putln!(
            out,
            "{} {} {}",
            key_path(&info.key),
            info.size,
            info.created_at
        );
    }
    if inventory.skipped_total() > 0 {
        putln!(out, "foreign names skipped: {}", inventory.skipped_total());
    }
    if let Some(count) = stamp_fallbacks {
        putln!(out, "stored times taken from LastModified: {count}");
    }
    out
}

fn file_json(info: &FileInfo) -> String {
    let range = info.key.range();
    let created: Timestamp = info.created_at;
    object(&[
        ("level", info.key.level().get().to_string()),
        ("key", quote(&key_path(&info.key))),
        ("min", quote(&range.min().to_string())),
        ("max", quote(&range.max().to_string())),
        ("size", info.size.to_string()),
        ("created_at", quote(&created.to_string())),
        ("created_at_ms", created.unix_millis().to_string()),
    ])
}

fn files_json(inventory: &Inventory, stamp_fallbacks: Option<u64>) -> String {
    let files: Vec<String> = inventory.files().iter().map(file_json).collect();
    let mut fields = vec![
        ("files", array(&files)),
        ("foreign_names", inventory.skipped_total().to_string()),
    ];
    if let Some(count) = stamp_fallbacks {
        fields.push(("stamps_from_last_modified", count.to_string()));
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
                "foreign",
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
