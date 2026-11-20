//! The compactor against Go's own compactions: every level 1 or 2 file Go wrote whose
//! range the files one level down still tile is rebuilt with [lsr_core::compact] and
//! compared on header fields and pages (never on bytes, which differ in framing).

#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::fs;
use std::path::Path;

use lsr_compat::{Manifest, all_fixtures};
use lsr_core::conformance::block_on;
use lsr_core::{
    CompactionInputs, FileInfo, FileKey, Level, MemStore, ObjectStore, PageLimit, Upload, compact,
    parse_file_name,
};
use lsr_ltx::{Decoder, Event, Timestamp, TxidRange};

/// Floor (90 measured) from the committed fixtures; a drop means the walk stopped finding Go's files.
const FLOOR: usize = 60;

type Pages = Vec<(u32, Vec<u8>)>;

fn decode(bytes: &[u8]) -> (lsr_ltx::Header, Pages) {
    let mut decoder = Decoder::new();
    let mut pages = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (used, event) = decoder.feed(rest).expect("Go's file decodes");
        if let Some(Event::Page { pgno, data }) = event {
            pages.push((pgno.get(), data.to_vec()));
        }
        rest = &rest[used..];
    }
    (*decoder.finish().expect("verified").header(), pages)
}

fn files(replica: &Path) -> Vec<(FileKey, Vec<u8>)> {
    let mut out = Vec::new();
    for level_dir in fs::read_dir(replica.join("ltx")).expect("ltx dir") {
        let level_dir = level_dir.expect("entry").path();
        let level = level_dir
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.parse::<u8>().ok())
            .and_then(Level::new)
            .expect("level directory");
        for file in fs::read_dir(&level_dir).expect("level dir") {
            let path = file.expect("entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).expect("name");
            let range = parse_file_name(name).expect("file name");
            out.push((FileKey::new(level, range), fs::read(&path).expect("read")));
        }
    }
    out.sort_by_key(|(key, _)| (key.level(), key.range().min(), key.range().max()));
    out
}

fn put(store: &MemStore, key: FileKey, bytes: &[u8]) -> FileInfo {
    block_on(async {
        let mut upload = store
            .begin_upload(&key, Timestamp::from_unix_millis(0))
            .await
            .expect("begin");
        upload.write(bytes).await.expect("write");
        upload.commit().await.expect("commit")
    })
}

/// The files one level down that tile exactly the range, if there are such.
fn tiling(below: &[FileInfo], range: TxidRange) -> Option<Vec<FileInfo>> {
    let mut next = range.min();
    let mut chosen = Vec::new();
    while next <= range.max() {
        let step = below
            .iter()
            .filter(|f| f.key.range().min() == next && f.key.range().max() <= range.max())
            .max_by_key(|f| f.key.range().max())?;
        chosen.push(*step);
        if step.key.range().max() == range.max() {
            return Some(chosen);
        }
        next = step.key.range().max().next()?;
    }
    None
}

#[test]
fn go_compacted_files_equal_ours_on_header_and_pages() {
    let mut compared = 0;
    for dir in all_fixtures().expect("fixtures") {
        let replica = Manifest::replica_dir(&dir);
        let on_disk = files(&replica);
        let store = MemStore::new();
        let infos: Vec<FileInfo> = on_disk.iter().map(|(k, b)| put(&store, *k, b)).collect();
        for (go_key, go_bytes) in &on_disk {
            let level = go_key.level().get();
            if !(1..=2).contains(&level) {
                continue;
            }
            let below: Vec<FileInfo> = infos
                .iter()
                .filter(|f| f.key.level().get() == level - 1)
                .copied()
                .collect();
            let Some(tiles) = tiling(&below, go_key.range()) else {
                continue;
            };
            if tiles.len() > lsr_core::MAX_INPUTS {
                continue;
            }
            let dest = MemStore::new();
            let inputs = CompactionInputs::new(tiles).expect("contiguous");
            let done = block_on(compact(&store, &dest, &inputs, go_key.level()))
                .unwrap_or_else(|err| panic!("{}: {err}", go_key.range()));
            let ours_key = done.info().key;
            let ours = block_on(dest.list(go_key.level(), None, PageLimit::MAX))
                .expect("list")
                .files;
            assert_eq!(ours.len(), 1);
            let (_, ours_bytes) = {
                let len = lsr_core::ReadLen::new(lsr_core::ReadLen::MAX_RAW).expect("len");
                let mut all = Vec::new();
                loop {
                    let chunk =
                        block_on(dest.read(&ours_key, u64::try_from(all.len()).expect("len"), len))
                            .expect("read");
                    if chunk.is_empty() {
                        break;
                    }
                    all.extend_from_slice(&chunk);
                }
                (ours_key, all)
            };
            let (go_header, go_pages) = decode(go_bytes);
            let (our_header, our_pages) = decode(&ours_bytes);
            let at = format!("{} {go_key:?}", dir.display());
            assert_eq!(our_header.commit(), go_header.commit(), "{at}");
            assert_eq!(our_header.txids(), go_header.txids(), "{at}");
            assert_eq!(
                our_header.fields().timestamp,
                go_header.fields().timestamp,
                "{at}"
            );
            assert_eq!(our_header.tracked(), go_header.tracked(), "{at}");
            assert_eq!(
                our_header.fields().pre_apply,
                go_header.fields().pre_apply,
                "{at}"
            );
            assert_eq!(our_pages, go_pages, "{at}");
            compared += 1;
        }
    }
    assert!(compared >= FLOOR, "only {compared} Go files compared");
}
