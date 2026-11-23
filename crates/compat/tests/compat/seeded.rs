//! Every generated run in the compat suite is seeded, and every Go differential runs through
//! [`lsr_compat::Differential`] (its module doc says why): a source scan, so a new
//! differential that builds its own `TestRunner` or an unseeded `ProptestConfig` fails here
//! before it can flake on CI.

use std::path::{Path, PathBuf};

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// What is wrong with one file's text: a `TestRunner` built outside the helper, or a
/// `ProptestConfig` literal without a fixed seed.
fn unseeded(text: &str) -> Vec<String> {
    let mut wrong = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let at = n.saturating_add(1);
        if line.contains(concat!("TestRunner", "::")) || line.contains(concat!("Test", "Runner {"))
        {
            wrong.push(format!(
                "line {at}: a TestRunner outside lsr_compat::Differential"
            ));
        }
        if line.contains(concat!("ProptestConfig", " {")) && !line.contains("rng_seed") {
            wrong.push(format!("line {at}: a ProptestConfig without rng_seed"));
        }
    }
    wrong
}

#[test]
fn every_generated_run_is_seeded() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    sources(&root.join("tests"), &mut files);
    sources(&root.join("src"), &mut files);
    assert!(files.len() > 10, "found only {files:?}");
    let helper = root.join("src").join("differential.rs");
    let wrong: Vec<String> = files
        .iter()
        .filter(|path| **path != helper)
        .flat_map(|path| {
            let text = std::fs::read_to_string(path).unwrap();
            unseeded(&text)
                .into_iter()
                .map(move |why| format!("{}: {why}", path.display()))
        })
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_scan_catches_both_shapes() {
    let runner = concat!("let mut r = Test", "Runner::new(Config::default());");
    let config = concat!(
        "#![proptest_config(Proptest",
        "Config { cases: 8, ..Default::default() })]"
    );
    let seeded = concat!(
        "Proptest",
        "Config { cases: 8, rng_seed: RngSeed::Fixed(1), ..Default::default() }"
    );
    assert_eq!(unseeded(runner).len(), 1);
    assert_eq!(unseeded(config).len(), 1);
    assert!(unseeded(seeded).is_empty());
}
