//! The judge's independence is a build fact: it may not depend on the engine it judges. The
//! engine is a dev-dependency only, for the standalone tests that drive it.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn the_judge_depends_on_no_engine_crate() {
    let text = fs::read_to_string(root().join("Cargo.toml")).unwrap();
    assert!(
        text.contains("name = \"lsr-history\""),
        "not the judge's manifest"
    );
    let mut table = String::new();
    let mut seen = 0;
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            table = line.to_owned();
            continue;
        }
        if table == "[dependencies]" {
            seen += 1;
            assert!(
                !line.starts_with("lsr-"),
                "lsr-history depends on {line}: the judge could reach the engine"
            );
        }
    }
    assert!(seen > 0, "found the [dependencies] table");
}

#[test]
fn the_judge_names_no_engine_crate() {
    let mut files = Vec::new();
    let mut dirs = vec![root().join("src")];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    assert!(files.len() >= 4, "found the sources: {files:?}");
    for file in files {
        let text = fs::read_to_string(&file).unwrap();
        for name in ["lsr_core", "lsr_wal", "lsr_ltx", "lsr_sqlite", "lsr_sim"] {
            assert!(!text.contains(name), "{} names {name}", file.display());
        }
    }
}
