//! Source guard: no tracked file holds a credential-shaped string. The repository
//! is public, and the Tigris run (docs/spikes.md) uses a real key, so the shapes
//! that key and an AWS secret assignment take must never reach a commit. The
//! planted examples and the shapes are built by concatenation so this file
//! passes its own scan.

use std::fs;
use std::path::Path;
use std::process::Command;

const KEY_RUN: usize = 20;

fn is_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// A Tigris key id or secret: the prefix then at least `KEY_RUN` key characters.
fn has_tigris_key(text: &str) -> bool {
    [concat!("ti", "d_"), concat!("tse", "c_")]
        .iter()
        .any(|prefix| {
            text.match_indices(prefix).any(|(at, _)| {
                let rest = text
                    .get(at..)
                    .and_then(|t| t.strip_prefix(prefix))
                    .unwrap_or("");
                rest.chars().take_while(|c| is_key_char(*c)).count() >= KEY_RUN
            })
        })
}

/// `AWS_SECRET_ACCESS_KEY = <40 base64 characters>`, in shell, TOML, YAML or Rust.
fn has_aws_secret_assignment(text: &str) -> bool {
    let name = concat!("aws_secret", "_access_key");
    let lower = text.to_ascii_lowercase();
    lower.match_indices(name).any(|(at, _)| {
        let rest = text
            .get(at..)
            .and_then(|t| t.get(name.len()..))
            .unwrap_or("");
        let value = rest.trim_start_matches([' ', '\t', '=', ':', '"', '\'']);
        let skipped = value.len() < rest.len();
        skipped
            && value
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '+' | '='))
                .count()
                >= 40
    })
}

fn flagged(text: &str) -> bool {
    has_tigris_key(text) || has_aws_secret_assignment(text)
}

#[test]
fn the_guard_catches_planted_examples() {
    let filler = "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8S9t0";
    assert!(has_tigris_key(&format!(
        "{}{}",
        concat!("ti", "d_"),
        filler.chars().take(24).collect::<String>()
    )));
    assert!(has_tigris_key(&format!(
        "x={}{}",
        concat!("tse", "c_"),
        filler
    )));
    assert!(has_aws_secret_assignment(&format!(
        "{}={}",
        concat!("AWS_SECRET", "_ACCESS_KEY"),
        filler
    )));
    assert!(has_aws_secret_assignment(&format!(
        "{}: \"{}\"",
        concat!("aws_secret", "_access_key"),
        filler
    )));
    // Too short, or no value: not a secret.
    assert!(!has_tigris_key(&format!("{}short", concat!("ti", "d_"))));
    assert!(!has_aws_secret_assignment(concat!(
        "AWS_SECRET",
        "_ACCESS_KEY is read from the environment"
    )));
    assert!(!flagged("let tid = 7; // a transaction id"));
}

#[test]
fn no_tracked_file_holds_a_credential() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let listing = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(listing.status.success(), "git ls-files failed");
    let mut offenders = Vec::new();
    for name in String::from_utf8_lossy(&listing.stdout).split('\0') {
        if name.is_empty() {
            continue;
        }
        // A file deleted in the work tree but still tracked has nothing to scan.
        let Ok(bytes) = fs::read(root.join(name)) else {
            continue;
        };
        if flagged(&String::from_utf8_lossy(&bytes)) {
            offenders.push(name.to_owned());
        }
    }
    assert!(
        offenders.is_empty(),
        "credential-shaped text in: {offenders:?}"
    );
}
