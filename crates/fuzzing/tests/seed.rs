//! Writes the seed corpus from the Go-written fixtures. Ignored: it changes
//! the tree. Run it with `just fuzz-seed`; `cargo fuzz cmin` then minimises.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::fs;
use std::path::{Path, PathBuf};

use lsr_core::FileKey;
use lsr_ltx::{HEADER_LEN, Header, INDEX_TAIL_LEN, PageIndex, TRAILER_LEN};

/// Fixture files above this size are not seeds: libFuzzer inputs stay small.
const MAX_SEED: u64 = 20 * 1024;

#[cfg(test)]
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[cfg(test)]
fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .expect("a readable directory")
        .map(|entry| entry.expect("an entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[cfg(test)]
fn ltx_files() -> Vec<PathBuf> {
    let mut all = Vec::new();
    walk(&root().join("crates/ltx/tests/fixtures"), &mut all);
    walk(&root().join("crates/compat/fixtures"), &mut all);
    all.retain(|path| {
        path.extension().is_some_and(|ext| ext == "ltx")
            && fs::metadata(path).is_ok_and(|meta| meta.len() <= MAX_SEED)
    });
    all
}

#[cfg(test)]
fn put(target: &str, name: &str, bytes: &[u8]) {
    let dir = root().join("fuzz/corpus").join(target);
    fs::create_dir_all(&dir).expect("create corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed");
}

/// The compact header, index region and first frame of a file, via its own index.
#[cfg(test)]
fn index_seed(file: &[u8]) -> Option<Vec<u8>> {
    let header = Header::decode(file.get(..HEADER_LEN)?.try_into().ok()?).ok()?;
    let spec = lsr_fuzzing::header_spec(&header)?;
    let body_end = file.len().checked_sub(TRAILER_LEN)?;
    let tail: &[u8; INDEX_TAIL_LEN] = file
        .get(file.len().checked_sub(INDEX_TAIL_LEN)?..)?
        .try_into()
        .ok()?;
    let region_len = usize::try_from(PageIndex::region_len(tail)).ok()?;
    let region = file.get(body_end.checked_sub(region_len)?..body_end)?;
    let index = PageIndex::decode(region, &header).ok()?;
    let first = index.entries().first()?;
    let start = usize::try_from(first.offset()).ok()?;
    let end = start.checked_add(usize::try_from(first.size()).ok()?)?;
    let mut seed = spec.to_vec();
    seed.extend(u16::try_from(region.len()).ok()?.to_be_bytes());
    seed.extend(region);
    seed.extend(file.get(start..end)?);
    Some(seed)
}

/// Every replica file of the compat fixtures: scenario, level, range.
#[cfg(test)]
fn replica_files() -> Vec<(String, u8, u64, u64)> {
    let mut all = Vec::new();
    walk(&root().join("crates/compat/fixtures"), &mut all);
    let mut out = Vec::new();
    for path in all {
        let text = path.to_string_lossy().into_owned();
        let Some((scenario, rest)) = text.split_once("/replica/ltx/") else {
            continue;
        };
        let Some((level, name)) = rest.split_once('/') else {
            continue;
        };
        let Some((min, max)) = name.strip_suffix(".ltx").and_then(|n| n.split_once('-')) else {
            continue;
        };
        if let (Ok(level), Ok(min), Ok(max)) = (
            level.parse::<u8>(),
            u64::from_str_radix(min, 16),
            u64::from_str_radix(max, 16),
        ) {
            out.push((scenario.to_owned(), level, min, max));
        }
    }
    out
}

#[test]
#[ignore = "writes fuzz/corpus; run through just fuzz-seed"]
#[cfg(test)]
fn write_seeds() {
    for (n, path) in ltx_files().iter().enumerate() {
        let bytes = fs::read(path).expect("a fixture");
        let mut decode_seed = vec![0_u8];
        decode_seed.extend(&bytes);
        put("decode", &format!("seed-{n:04}"), &decode_seed);
        if let Some(seed) = index_seed(&bytes) {
            put("index", &format!("seed-{n:04}"), &seed);
        }
    }
    let files = replica_files();
    key_seeds(&files);
    plan_seeds(&files);
}

#[cfg(test)]
fn key_seeds(files: &[(String, u8, u64, u64)]) {
    let keys: std::collections::BTreeSet<String> = files
        .iter()
        .map(|(_, level, min, max)| format!("{level:04x}/{min:016x}-{max:016x}.ltx"))
        .collect();
    for (n, key) in keys.iter().enumerate() {
        assert!(key.parse::<FileKey>().is_ok(), "{key} is a key");
        put("key", &format!("seed-{n:04}"), key.as_bytes());
    }
    put("key", "txid", b"0000000000000001");
    put("key", "range", b"0000000000000001-0000000000000002.ltx");
}

/// Replica listings as plan inputs, one per scenario and target. Transaction
/// ids that do not fit the input format skip the scenario.
#[cfg(test)]
fn plan_seeds(files: &[(String, u8, u64, u64)]) {
    let mut by_scenario: std::collections::BTreeMap<&str, Vec<(u8, u64, u64)>> = Default::default();
    for (scenario, level, min, max) in files {
        by_scenario
            .entry(scenario)
            .or_default()
            .push((*level, *min, *max));
    }
    for (n, files) in by_scenario.values().enumerate() {
        if files
            .iter()
            .any(|(_, min, max)| *min > 64 || max - min > 31)
        {
            continue;
        }
        let mut records = Vec::new();
        for (order, (level, min, max)) in files.iter().enumerate() {
            let created = u8::try_from(order % 256).expect("fits");
            let first = u8::try_from(min - 1).expect("checked above");
            let span = u8::try_from(max - min).expect("checked above");
            records.extend([*level, first, span, created]);
        }
        let newest = files.iter().map(|(_, _, max)| *max).max().unwrap_or(1);
        let newest = u16::try_from(newest - 1).expect("small");
        for (kind, value) in [
            (0_u8, 0_u16),
            (1, newest),
            (1, 2),
            (2, 40),
            (2, 0),
            (2, 299),
        ] {
            let mut input = vec![kind];
            input.extend(value.to_be_bytes());
            input.push(0);
            input.extend(&records);
            put("plan", &format!("seed-{n:03}-{kind}-{value}"), &input);
        }
    }
}
