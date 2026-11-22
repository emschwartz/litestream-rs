//! `lsr-history`: the judge as a command, for drivers outside Rust (the LazyFS rig,
//! `scripts/lazyfs.py`).
//!
//! ```text
//! lsr-history digest RESTORED_DB     print the chain tip's hex digest, or exit 3 if the file is broken
//! lsr-history judge LOG [--ahead-counts]
//! ```
//!
//! The log is text, one event per line, appended by the application and the driver on storage
//! no fault touches. `<at>` is the driver's clock in milliseconds and `<hex>` a full 64-digit
//! chain digest; fields after the ones named are ignored.
//!
//! - `E <at> <hex>`: the application started and found the database at this state (a recovery;
//!   a state off the head's branch is a rewind).
//! - `I <at> <hex>`: the application is about to commit the row that makes this state.
//! - `A <at>`: that commit was acknowledged.
//! - `L <at> <hex>` or `L <at> broken <why>`: a restore of the newest state, judged against the
//!   tree as the log stands at that line.
//! - `F <at> <hex>`: the same, after a clean stop and drain; it must also be the head.
//! - Any other line (and a last line with no newline, cut by a kill) is skipped.
//!
//! An intended commit that a later `E` follows without an `A` is a commit whose outcome is
//! unknown. One that a restore already shows landed counts as acknowledged at that restore.
//!
//! A restore that holds commits the database lost is an anomaly unless `--ahead-counts`: the
//! engine syncs the WAL before staging from it, so with an honest disk it never happens.
//! `judge` prints one line per anomaly as `line <n>: <anomaly>`, then a `counts` line, and exits
//! 1 when there was any anomaly, 2 on a usage or I/O error.

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use lsr_history::{AheadMode, Anomaly, Digest, Judge, Log, Target, chain};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    match refs.as_slice() {
        ["digest", db] => digest(Path::new(db)),
        ["judge", log] => judge(Path::new(log), AheadMode::Fatal),
        ["judge", log, "--ahead-counts"] => judge(Path::new(log), AheadMode::Count),
        _ => {
            eprintln!(
                "usage: lsr-history digest RESTORED_DB | lsr-history judge LOG [--ahead-counts]"
            );
            ExitCode::from(2)
        }
    }
}

fn digest(db: &Path) -> ExitCode {
    match chain::read(db) {
        Ok(d) => {
            println!("{}", hex(&d));
            ExitCode::SUCCESS
        }
        Err(why) => {
            println!("broken {why}");
            ExitCode::from(3)
        }
    }
}

fn hex(d: &Digest) -> String {
    d.bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_digest(text: &str) -> Option<Digest> {
    if text.len() != 64 {
        return None;
    }
    let bytes: Option<Vec<u8>> = text
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
        })
        .collect();
    bytes.and_then(|b| Digest::from_slice(&b))
}

/// The replay: the tree so far, the judge's state, and a commit sent but not yet answered.
struct Replay {
    log: Option<Log>,
    judge: Judge,
    sent: Option<(Digest, i64)>,
    anomalies: u32,
}

impl Replay {
    fn log(&mut self, at: i64) -> &mut Log {
        self.log
            .get_or_insert_with(|| Log::new(Digest::genesis(), at))
    }

    fn report(&mut self, line: usize, what: &str) {
        self.anomalies = self.anomalies.saturating_add(1);
        println!("line {line}: {what}");
    }

    /// The commit in flight, if any, as one whose outcome is unknown.
    fn settle_unknown(&mut self) {
        if let Some((d, at)) = self.sent.take() {
            self.log(at).pending(d, at);
        }
    }

    fn recovered(&mut self, line: usize, d: Digest, at: i64) {
        self.settle_unknown();
        let log = self.log(at);
        match log.find_from(&d, log.head()) {
            Some(node) => log.recover(node, at),
            None => self.report(
                line,
                &format!("the database came back at {d}, a state no commit made"),
            ),
        }
    }

    fn restored(&mut self, line: usize, d: Digest, at: i64, last: bool) {
        // A restore can show the commit in flight before its acknowledgement is logged.
        if self.sent.is_some_and(|(s, _)| s == d) {
            self.sent = None;
            self.log(at).commit(d, at);
        }
        self.log(at);
        let Some(log) = self.log.as_ref() else { return };
        let mut verdict = self.judge.latest(log, d).map(drop);
        if last && verdict.is_ok() {
            verdict = self.judge.lag(log, d, at, 0);
        }
        if let Err(anomaly) = verdict {
            self.report(line, &anomaly.to_string());
        }
    }

    fn event(&mut self, line: usize, fields: &[&str]) {
        let at = fields.get(1).and_then(|t| t.parse::<i64>().ok());
        let d = fields.get(2).and_then(|h| parse_digest(h));
        match (fields.first().copied(), at, d) {
            (Some("E"), Some(at), Some(d)) => self.recovered(line, d, at),
            (Some("I"), Some(at), Some(d)) => {
                self.settle_unknown();
                self.sent = Some((d, at));
            }
            (Some("A"), Some(at), _) => {
                if let Some((d, _)) = self.sent.take() {
                    self.log(at).commit(d, at);
                }
            }
            (Some(tag @ ("L" | "F")), Some(at), Some(d)) => self.restored(line, d, at, tag == "F"),
            (Some("L" | "F"), Some(_), None) if fields.get(2) == Some(&"broken") => {
                let why = fields.get(3..).map(|w| w.join(" ")).unwrap_or_default();
                let broken = Anomaly::Broken {
                    target: Target::Latest,
                    why,
                };
                self.report(line, &broken.to_string());
            }
            _ => {}
        }
    }
}

fn judge(path: &Path, mode: AheadMode) -> ExitCode {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(err) => {
            eprintln!("lsr-history: {}: {err}", path.display());
            return ExitCode::from(2);
        }
    };
    let mut replay = Replay {
        log: None,
        judge: Judge::new(mode),
        sent: None,
        anomalies: 0,
    };
    for (n, line) in text.split_inclusive('\n').enumerate() {
        if !line.ends_with('\n') {
            break; // cut by a kill mid-line
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        replay.event(n.saturating_add(1), &fields);
    }
    let rewinds = replay.log.as_ref().map_or(0, |l| l.rewinds().len());
    println!("counts {:?} rewinds={rewinds}", replay.judge.counts());
    if replay.anomalies == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
