//! Replays every committed corpus file through its target, so the corpus runs
//! in `just check` without libFuzzer.

use std::fs;
use std::path::PathBuf;

#[cfg(test)]
fn corpus(target: &str) -> Vec<(String, Vec<u8>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz/corpus")
        .join(target);
    let mut files: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("corpus {} is missing: {error}", dir.display()))
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    assert!(!files.is_empty(), "corpus {} is empty", dir.display());
    files
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path).expect("a readable corpus file");
            (path.display().to_string(), bytes)
        })
        .collect()
}

#[cfg(test)]
fn replay(target: &str, run: fn(&[u8])) {
    for (name, bytes) in corpus(target) {
        run(&bytes);
        eprintln!("ok {name}");
    }
}

#[test]
#[cfg(test)]
fn decode_corpus_replays() {
    replay("decode", lsr_fuzzing::decode);
}

#[test]
#[cfg(test)]
fn index_corpus_replays() {
    replay("index", lsr_fuzzing::index);
}

#[test]
#[cfg(test)]
fn plan_corpus_replays() {
    replay("plan", lsr_fuzzing::plan);
}

#[test]
#[cfg(test)]
fn key_corpus_replays() {
    replay("key", lsr_fuzzing::key);
}
