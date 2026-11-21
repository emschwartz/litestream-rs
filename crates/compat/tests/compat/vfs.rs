//! The Go read-replica VFS reads a replica the Rust actor wrote: opened read-only through
//! upstream's `cmd/litestream-vfs` extension, every table reads back the rows of the database
//! at the replica's newest transaction, every page the VFS serves hashes as that database's
//! page does, and `PRAGMA integrity_check` through the VFS is ok.
//!
//! The chain is the one `rust_chains.rs` restores with Go: page-count growth, a delete and a
//! VACUUM that shrink the file, compaction to levels 1 and 2 and a snapshot. Each build under
//! test with a file-reading VFS runs (`just compat`: the pinned five; `just compat-live`: the
//! names given). The frame era's VFS reads only frame-encoded files, so it refuses, and the
//! refusal is asserted; before v0.5.3 the VFS reads S3 only and has nothing to open here.

use std::collections::BTreeSet;
use std::path::Path;

use lsr_compat::{
    Chain, Era, Site, VfsRead, Written, build_vfs_reader, builds_under_test, clock_ending_now,
    hash_pages, integrity_check, read_plain, read_with_vfs, restore_replica,
};
use lsr_core::RestoreTarget;

const COMMITS: u32 = 300;

fn write_chain() -> Written {
    let site = Site::create(4096, 11).expect("site");
    let mut chain = Chain::start(site, clock_ending_now(COMMITS).expect("clock")).expect("start");
    chain.run(COMMITS).expect("run");
    chain.finish().expect("finish")
}

fn level_of(path: &Path) -> Option<u8> {
    path.file_name()?.to_str()?.split('-').next()?.parse().ok()
}

/// The run reached the shapes the claim is about; otherwise a pass would mean less.
fn assert_shapes(written: &Written) {
    let levels: BTreeSet<u8> = written.archive.iter().filter_map(|p| level_of(p)).collect();
    for level in [1, 2, 9] {
        assert!(levels.contains(&level), "no level {level} file: {levels:?}");
    }
    let counts: Vec<usize> = written.points.iter().map(|p| p.page_hashes.len()).collect();
    let steps: Vec<(usize, usize)> = counts
        .iter()
        .copied()
        .zip(counts.iter().copied().skip(1))
        .collect();
    assert!(
        steps.iter().any(|(a, b)| b < a),
        "the database never shrank"
    );
    assert!(steps.iter().any(|(a, b)| b > a), "the database never grew");
}

/// The tests that run the Go builds under test (`LSR_COMPAT_VERSIONS`).
/// `just compat-live` selects the tests by this module (`test(/(^|::)live::/)`).
mod live {
    use super::*;

    #[test]
    #[ignore = "runs the pinned Go binaries; just compat"]
    fn go_vfs_reads_rust_replica() {
        let written = write_chain();
        assert_shapes(&written);
        let newest = written.points.last().expect("points");
        let dir = written.site.dir();
        let reader = build_vfs_reader(dir).expect("the C host builds (cc and SQLite headers)");
        // The reference: the newest transaction restored by Rust, checked against the source.
        let reference = dir.join("reference.db");
        restore_replica(&written.site.replica, RestoreTarget::Latest, &reference)
            .expect("sink")
            .expect("latest restores");
        integrity_check(&reference).expect("reference integrity");
        assert_eq!(
            hash_pages(&reference).expect("hash").page_hashes,
            newest.page_hashes
        );
        let plain = dir.join("plain");
        std::fs::create_dir_all(&plain).expect("mkdir");
        let want = read_plain(&reader, &reference, &plain).expect("the host reads the reference");
        assert!(want.rows.len() > 10, "only {} rows", want.rows.len());
        let mut read = 0;
        for build in builds_under_test() {
            let name = build.name();
            if !build.vfs_reads_files() {
                println!("{name}: the VFS reads S3 only; no file replica to open");
                continue;
            }
            let library = build
                .vfs_library()
                .expect("the VFS of the build under test is built");
            let scratch = dir.join(format!("vfs-{name}"));
            std::fs::create_dir_all(&scratch).expect("mkdir");
            let result = read_with_vfs(&reader, &library, &written.site.replica, &scratch)
                .expect("the host runs");
            match (build.era(), result) {
                (Era::Blocks, VfsRead::Read(view)) => {
                    assert_eq!(view.integrity, ["ok"], "{name}");
                    assert_eq!(view.txid, Some(newest.txid.get()), "{name}");
                    assert!(view.rows == want.rows, "{name}: the rows differ");
                    let pages = view.pages_as(&reference).expect("pages");
                    assert_eq!(pages.page_hashes, newest.page_hashes, "{name}");
                    println!(
                        "{name}: read txid {:#x}, {} rows, {} pages",
                        newest.txid.get(),
                        view.rows.len(),
                        pages.page_count
                    );
                    read += 1;
                }
                (Era::Blocks, VfsRead::Refused { detail }) => {
                    panic!("{name} refused a Rust replica: {detail}")
                }
                (Era::Frames, VfsRead::Refused { detail }) => {
                    // What matters is that a build predating block-encoded pages refuses
                    // instead of reading wrong data. The reason is the page encoding (an
                    // LZ4 frame decoder handed a block), the same one these builds give Go
                    // v0.5.16's own replicas; v0.5.7 to v0.5.15 log it, and v0.5.3 to v0.5.6
                    // refuse with only "SQL logic error" and no such log line (issue #30),
                    // so the log line is reported, not required.
                    let log = std::fs::read_to_string(scratch.join("vfs.log")).unwrap_or_default();
                    let why = if log.contains("lz4: bad magic number") {
                        "lz4 frame decoder"
                    } else {
                        "no reason logged"
                    };
                    println!("{name} on a Rust replica: refused ({why}), {detail}");
                }
                (Era::Frames, VfsRead::Read(_)) => panic!("{name} read a block-encoded replica"),
            }
        }
        println!("{read} builds read the replica through the VFS");
    }
}
