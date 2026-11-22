//! `lsr-history`, the judge as a command over a text commit log (the LazyFS rig's judge):
//! each line kind replays into the tree, and the verdicts match the library's.

use std::path::Path;
use std::process::{Command, Output};

use lsr_history::chain::{self, Chain};

fn hex(chain: &Chain) -> String {
    chain
        .digest()
        .bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn judge(lines: &[String]) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("commits.log");
    std::fs::write(&log, lines.join("\n") + "\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_lsr-history"))
        .arg("judge")
        .arg(&log)
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
