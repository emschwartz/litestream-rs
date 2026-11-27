//! `lsr-history`, the judge as a command over a text commit log (the LazyFS rig's judge):
//! each line kind replays into the tree, and the verdicts match the library's.

use std::path::Path;
use std::process::{Command, Output};

use lsr_history::chain::{self, Chain};

fn hex(chain: &Chain) -> String {
    hex::encode(chain.digest().bytes())
}

fn judge(lines: &[String]) -> Output {
    judge_with(lines, &[])
}

fn judge_with(lines: &[String], flags: &[&str]) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("commits.log");
    std::fs::write(&log, lines.join("\n") + "\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_lsr-history"))
        .arg("judge")
        .arg(&log)
        .args(flags)
        .output()
        .unwrap()
}

fn text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A start at the genesis and two acknowledged commits; returns the log and the two states.
fn two_commits() -> (Vec<String>, Chain, Chain) {
    let g = Chain::new();
    let one = g.next(b"one");
    let two = one.next(b"two");
    let lines = vec![
        format!("E 1 {} 0", hex(&g)),
        format!("I 2 {} 1", hex(&one)),
        "A 3 1".to_owned(),
        format!("I 4 {} 2", hex(&two)),
        "A 5 2".to_owned(),
    ];
    (lines, one, two)
}

#[test]
fn a_restore_on_the_live_branch_passes_and_a_final_one_must_be_the_head() {
    let (mut lines, one, two) = two_commits();
    lines.push(format!("L 6 {}", hex(&one)));
    lines.push(format!("F 7 {}", hex(&two)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));

    let (mut lines, one, _) = two_commits();
    lines.push(format!("F 6 {}", hex(&one)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(
        text(&out).starts_with("line 6: latest gave node"),
        "{}",
        text(&out)
    );
}

#[test]
fn a_restore_of_a_branch_a_rewind_cut_off_is_an_anomaly_unless_counted() {
    let (mut lines, one, two) = two_commits();
    let other = one.next(b"another two");
    lines.push(format!("E 6 {} 1", hex(&one)));
    lines.push(format!("I 7 {} 2", hex(&other)));
    lines.push("A 8 2".to_owned());
    lines.push(format!("L 9 {}", hex(&two)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(text(&out).contains("line 9:"), "{}", text(&out));
    assert!(text(&out).contains("which a rewind cut"), "{}", text(&out));

    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("commits.log");
    std::fs::write(&log, lines.join("\n") + "\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_lsr-history"))
        .args(["judge", log.to_str().unwrap(), "--ahead-counts"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(text(&out).contains("ahead_latest: 1"), "{}", text(&out));
}

#[test]
fn a_restore_may_show_the_commit_in_flight_before_its_acknowledgement() {
    let (mut lines, _, two) = two_commits();
    let three = two.next(b"three");
    lines.push(format!("I 6 {} 3", hex(&three)));
    lines.push(format!("L 7 {}", hex(&three)));
    lines.push("A 8 3".to_owned());
    lines.push(format!("F 9 {}", hex(&three)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
}

#[test]
fn an_unacknowledged_commit_that_landed_is_found_at_the_next_start() {
    let (mut lines, _, two) = two_commits();
    let three = two.next(b"three");
    lines.push(format!("I 6 {} 3", hex(&three)));
    lines.push(format!("E 7 {} 3", hex(&three)));
    lines.push(format!("F 8 {}", hex(&three)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
}

#[test]
fn states_no_commit_made_and_broken_restores_are_anomalies() {
    let (mut lines, _, two) = two_commits();
    let stranger = two.next(b"never logged");
    lines.push(format!("L 6 {}", hex(&stranger)));
    lines.push("L 7 broken integrity_check: page 3 is never used".to_owned());
    lines.push(format!("E 8 {} 3", hex(&stranger)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    let t = text(&out);
    assert!(
        t.contains("line 6: restoring Latest gave a state no commit made"),
        "{t}"
    );
    assert!(
        t.contains("line 7: restoring Latest gave a broken database: integrity_check"),
        "{t}"
    );
    assert!(t.contains("line 8: the database came back at"), "{t}");
}

fn digest(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lsr-history"))
        .arg("digest")
        .arg(path)
        .output()
        .unwrap()
}

#[test]
fn digest_prints_a_sound_chain_s_tip_and_refuses_a_broken_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("restored.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(chain::SCHEMA, []).unwrap();
    let one = Chain::new().next(b"one");
    one.insert(&conn, b"one").unwrap();
    let out = digest(&db);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(text(&out).trim(), hex(&one));

    conn.execute("UPDATE chain SET payload = x'00'", [])
        .unwrap();
    drop(conn);
    let out = digest(&db);
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));
    assert!(text(&out).starts_with("broken "), "{}", text(&out));
}

#[test]
fn restore_points_are_judged_in_the_replica_s_order() {
    let (mut lines, one, two) = two_commits();
    lines.push(format!("P 6 0000000000000001 {}", hex(&one)));
    lines.push(format!("P 6 0000000000000002 {}", hex(&two)));
    lines.push("W 6".to_owned());
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(text(&out).contains("points: 2"), "{}", text(&out));

    // The checker can fail: the same points in the other order went back on a branch.
    let (mut lines, one, two) = two_commits();
    lines.push(format!("P 6 0000000000000002 {}", hex(&two)));
    lines.push(format!("P 6 0000000000000003 {}", hex(&one)));
    lines.push("W 6".to_owned());
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(
        text(&out).starts_with("line 8: latest went back"),
        "{}",
        text(&out)
    );

    // A point no commit made, and a point that would not restore.
    let (mut lines, _, two) = two_commits();
    let stranger = two.next(b"never logged");
    lines.push(format!("P 6 0000000000000009 {}", hex(&stranger)));
    lines.push("W 6".to_owned());
    lines.push("P 7 000000000000000a broken integrity_check: page 3".to_owned());
    let out = judge(&lines);
    let t = text(&out);
    assert_eq!(out.status.code(), Some(1), "{t}");
    assert!(
        t.contains("line 7: restoring Point(9) gave a state no commit made"),
        "{t}"
    );
    assert!(
        t.contains("line 8: restoring Point(10) gave a broken database"),
        "{t}"
    );
}

#[test]
fn a_restore_as_of_an_instant_may_not_hold_a_commit_begun_after_it() {
    let (mut lines, one, two) = two_commits();
    lines.push(format!("T 6 3 {}", hex(&one)));
    lines.push(format!("T 6 10 {}", hex(&two)));
    let out = judge(&lines);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(text(&out).contains("as_of: 2"), "{}", text(&out));

    // Commit two began at 4, so it cannot be the state as of 4, and a broken one is reported.
    let (mut lines, _, two) = two_commits();
    lines.push(format!("T 6 4 {}", hex(&two)));
    lines.push("T 7 9 broken integrity_check: page 3".to_owned());
    let out = judge(&lines);
    let t = text(&out);
    assert_eq!(out.status.code(), Some(1), "{t}");
    assert!(
        t.contains("line 6: restoring as of 4 gave node 2, begun at 4"),
        "{t}"
    );
    assert!(
        t.contains("line 7: restoring AsOf(9) gave a broken database"),
        "{t}"
    );
}

#[test]
fn with_signals_required_a_rewind_needs_the_engine_s_own_report() {
    let (mut lines, one, two) = two_commits();
    let other = one.next(b"another two");
    lines.push(format!("L 5 {}", hex(&two)));
    lines.push(format!("E 6 {} 1", hex(&one)));
    lines.push(format!("I 7 {} 2", hex(&other)));
    lines.push("A 8 2".to_owned());
    let mut quiet = lines.clone();
    quiet.push(format!("L 9 {}", hex(&other)));
    // The driver's record alone explains it, unless the engine's report is required.
    let out = judge_with(&quiet, &[]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let out = judge_with(&quiet, &["--signals-required"]);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(
        text(&out).contains("the engine reported no loss"),
        "{}",
        text(&out)
    );

    lines.push("S 9".to_owned());
    lines.push(format!("L 10 {}", hex(&other)));
    let out = judge_with(&lines, &["--signals-required"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
}
