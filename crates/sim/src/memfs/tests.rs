use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, SystemTime};

use lsr_store_fs::{Fs, FsFile};

use super::*;
use crate::crash_law::BlockSize;

fn p(s: &str) -> &Path {
    Path::new(s)
}

/// A file written, synced, renamed into a synced directory: the store's commit, in full.
fn commit(fs: &MemFs, name: &str, bytes: &[u8], sync_file: bool, sync_dir: bool) {
    let scratch = format!("/d/{name}.tmp");
    let mut f = fs.create_new(p(&scratch)).unwrap();
    f.write_all(bytes).unwrap();
    if sync_file {
        f.sync_all().unwrap();
    }
    fs.rename(p(&scratch), &Path::new("/d").join(name)).unwrap();
    if sync_dir {
        fs.sync_dir(p("/d")).unwrap();
    }
}

fn fresh(law: CrashLaw) -> MemFs {
    let fs = MemFs::new(law);
    fs.create_dir(p("/d")).unwrap();
    fs.sync_dir(p("/")).unwrap();
    fs
}

fn names(fs: &MemFs) -> BTreeSet<String> {
    fs.read_dir(p("/d"))
        .unwrap()
        .iter()
        .map(|e| e.name().to_string_lossy().into_owned())
        .collect()
}

fn bytes(fs: &MemFs, path: &str) -> Vec<u8> {
    fs.open(p(path)).unwrap().read_at(0, 1 << 20).unwrap()
}

#[test]
fn a_synced_commit_survives_a_power_loss_whole() {
    let fs = fresh(CrashLaw::AllOrNothing);
    commit(&fs, "a", b"hello", true, true);
    assert!(fs.all_durable());
    assert_eq!(fs.power_loss(), PowerLoss::default());
    assert_eq!(bytes(&fs, "/d/a"), b"hello");
}

#[test]
fn a_rename_with_no_directory_sync_is_undone_under_all_or_nothing() {
    let fs = fresh(CrashLaw::AllOrNothing);
    commit(&fs, "a", b"hello", true, false);
    assert!(!fs.all_durable());
    let loss = fs.power_loss();
    assert_eq!(
        loss.undone, 1,
        "the new name; the scratch name was never durable"
    );
    assert!(names(&fs).is_empty());
}

#[test]
fn a_rename_synced_over_unsynced_bytes_keeps_the_name_and_loses_the_bytes() {
    let fs = fresh(CrashLaw::AllOrNothing);
    commit(&fs, "a", b"hello", false, true);
    let loss = fs.power_loss();
    assert_eq!(loss.changed, 1);
    assert_eq!(names(&fs), BTreeSet::from(["a".to_owned()]));
    assert_eq!(bytes(&fs, "/d/a"), b"");
}

/// All-or-nothing keeps deletions done; the block law puts an unsynced unlink back or not.
#[test]
fn an_unlink_with_no_directory_sync_may_come_back_under_the_block_law() {
    let fs = fresh(CrashLaw::AllOrNothing);
    commit(&fs, "a", b"hello", true, true);
    fs.remove_file(p("/d/a")).unwrap();
    fs.power_loss();
    assert!(names(&fs).is_empty());
    let mut seen = BTreeSet::new();
    for seed in 0..32 {
        let fs = fresh(CrashLaw::Blocks {
            block: BlockSize::B512,
            seed,
        });
        commit(&fs, "a", b"hello", true, true);
        fs.remove_file(p("/d/a")).unwrap();
        fs.power_loss();
        seen.insert(names(&fs).into_iter().collect::<Vec<_>>());
        drop(fs.remove_file(p("/d/a")));
        fs.sync_dir(p("/d")).unwrap();
        fs.power_loss();
        assert!(names(&fs).is_empty(), "a synced unlink stays done");
    }
    assert_eq!(seen, BTreeSet::from([vec![], vec!["a".to_owned()]]));
}

#[test]
fn an_unsynced_directory_goes_with_everything_in_it() {
    let fs = MemFs::new(CrashLaw::AllOrNothing);
    fs.create_dir(p("/d")).unwrap();
    commit(&fs, "a", b"x", true, true);
    fs.power_loss();
    assert!(!fs.try_exists(p("/d")).unwrap());
    assert!(!fs.try_exists(p("/d/a")).unwrap());
}

/// Each name of an unsynced rename keeps or loses its change on its own coin, the shared
/// directory-entry rule: the rename can part into both names or neither.
#[test]
fn the_block_law_keeps_each_entry_of_an_unsynced_rename_apart() {
    let mut kept = BTreeSet::new();
    for seed in 0..64 {
        let fs = fresh(CrashLaw::Blocks {
            block: BlockSize::B512,
            seed,
        });
        let mut f = fs.create_new(p("/d/a.tmp")).unwrap();
        f.write_all(b"x").unwrap();
        f.sync_all().unwrap();
        fs.sync_dir(p("/d")).unwrap();
        fs.rename(p("/d/a.tmp"), p("/d/a")).unwrap();
        fs.power_loss();
        kept.insert(names(&fs).into_iter().collect::<Vec<_>>());
    }
    let expected: BTreeSet<Vec<String>> = [
        vec![],
        vec!["a".to_owned()],
        vec!["a.tmp".to_owned()],
        vec!["a".to_owned(), "a.tmp".to_owned()],
    ]
    .into_iter()
    .collect();
    assert_eq!(kept, expected);
}

#[test]
fn the_block_law_tears_unsynced_bytes_and_keeps_or_loses_the_mtime() {
    let mut lens = BTreeSet::new();
    let mut mtimes = BTreeSet::new();
    let later = SystemTime::UNIX_EPOCH + Duration::from_secs(9);
    for seed in 0..64 {
        let fs = fresh(CrashLaw::Blocks {
            block: BlockSize::B512,
            seed,
        });
        let mut f = fs.create_new(p("/d/a")).unwrap();
        fs.sync_dir(p("/d")).unwrap();
        f.write_all(&[7; 1536]).unwrap();
        f.set_modified(later).unwrap();
        fs.power_loss();
        let meta = fs.metadata(p("/d/a")).unwrap();
        lens.insert(meta.len());
        mtimes.insert(meta.modified());
    }
    assert_eq!(lens, BTreeSet::from([0, 512, 1024, 1536]));
    assert_eq!(mtimes, BTreeSet::from([SystemTime::UNIX_EPOCH, later]));
}

#[test]
fn a_handle_from_before_a_power_loss_is_dead() {
    let fs = fresh(CrashLaw::AllOrNothing);
    let mut f = fs.create_new(p("/d/a")).unwrap();
    fs.sync_dir(p("/d")).unwrap();
    fs.power_loss();
    assert!(f.write_all(b"x").is_err());
    assert!(f.sync_all().is_err());
}

#[test]
fn errors_carry_the_kinds_the_store_matches_on() {
    let fs = fresh(CrashLaw::AllOrNothing);
    commit(&fs, "a", b"x", true, true);
    let kind = |r: io::Result<()>| r.unwrap_err().kind();
    assert_eq!(
        kind(fs.create_new(p("/d/a")).map(drop)),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(kind(fs.create_dir(p("/d"))), io::ErrorKind::AlreadyExists);
    assert_eq!(kind(fs.remove_file(p("/d/b"))), io::ErrorKind::NotFound);
    assert_eq!(
        kind(fs.read_dir(p("/e")).map(drop)),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        kind(fs.read_dir(p("/d/a")).map(drop)),
        io::ErrorKind::NotADirectory
    );
    assert_eq!(kind(fs.remove_file(p("/d"))), io::ErrorKind::IsADirectory);
    assert_eq!(
        kind(fs.rename(p("/d/b"), p("/d/c"))),
        io::ErrorKind::NotFound
    );
    assert!(!fs.try_exists(p("/d/b")).unwrap());
}
