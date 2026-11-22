//! The store crashed at every durability step of its writes, under the crash law.
//!
//! [`capture`] records the steps a real upload or delete takes on a real directory; [`Disk`]
//! replays a prefix of them and answers what a power loss right there can leave: a file's
//! bytes are kept only if it was synced after its last write, and each name a directory
//! operation changed since its directory's last sync holds either its old or its new entry.
//! Every crash point and both outcomes of every name are judged:
//!
//! - an object name that may be present names a file whose bytes are durable (no torn object
//!   is ever visible);
//! - a name whose commit returned success is present in every outcome, and so is every
//!   directory the store created on its way;
//! - a name whose delete returned success is absent in every outcome.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow or a bad index panics, which fails the test"
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use lsr_core::conformance::block_on;
use lsr_core::{FileKey, Level, ObjectStore, Upload};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use tempfile::TempDir;

use super::{Step, capture};
use crate::FsStore;

/// What a name holds: a file (by inode number in the replay) or a directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Entry {
    File(usize),
    Dir,
}

/// The disk after a prefix of the steps.
#[derive(Default)]
struct Disk {
    /// Every name as the running process sees it.
    now: BTreeMap<PathBuf, Option<Entry>>,
    /// Every name as its directory was last synced: what a loss keeps for certain.
    synced: BTreeMap<PathBuf, Option<Entry>>,
    /// Whether each file has bytes written since its last sync.
    dirty: Vec<bool>,
    committed: Vec<PathBuf>,
    deleted: Vec<PathBuf>,
    created_dirs: Vec<PathBuf>,
}

impl Disk {
    fn apply(&mut self, step: &Step) {
        match step {
            Step::Create(path) => {
                self.dirty.push(true);
                let id = self.dirty.len() - 1;
                self.now.insert(path.clone(), Some(Entry::File(id)));
            }
            Step::Write(path) => {
                if let Some(Some(Entry::File(id))) = self.now.get(path) {
                    self.dirty[*id] = true;
                }
            }
            Step::SyncFile(path) => {
                if let Some(Some(Entry::File(id))) = self.now.get(path) {
                    self.dirty[*id] = false;
                }
            }
            Step::Rename { from, to } => {
                let moved = self.now.insert(from.clone(), None).flatten();
                self.now.insert(to.clone(), moved);
            }
            Step::Remove(path) => {
                self.now.insert(path.clone(), None);
                // A delete under way takes back the commit's promise for this name.
                self.committed.retain(|name| name != path);
            }
            Step::CreateDir(path) => {
                self.now.insert(path.clone(), Some(Entry::Dir));
                self.created_dirs.push(path.clone());
            }
            Step::SyncDir(dir) => {
                for (name, entry) in &self.now {
                    if name.parent() == Some(dir.as_path()) {
                        self.synced.insert(name.clone(), *entry);
                    }
                }
            }
            Step::Committed(path) => self.committed.push(path.clone()),
            Step::Deleted(path) => self.deleted.push(path.clone()),
        }
    }

    /// What a loss now can leave at `name`: its synced entry, or the one it has now. A name
    /// never synced was absent before the run.
    fn outcomes(&self, name: &Path) -> [Option<Entry>; 2] {
        let synced = self.synced.get(name).copied().flatten();
        let now = self.now.get(name).copied().flatten();
        [synced, now]
    }

    /// The first rule a loss right now could break, if any.
    fn broken(&self, deletes: bool) -> Option<String> {
        for name in self.now.keys().chain(self.synced.keys()) {
            let object = name.extension().is_some_and(|ext| ext == "ltx");
            for entry in self.outcomes(name) {
                if let (true, Some(Entry::File(id))) = (object, entry)
                    && self.dirty[id]
                {
                    return Some(format!(
                        "{} may be visible with bytes never synced",
                        name.display()
                    ));
                }
            }
        }
        for name in &self.committed {
            if self.outcomes(name).contains(&None) {
                return Some(format!("{} was committed and may be gone", name.display()));
            }
        }
        for dir in &self.created_dirs {
            let holds_a_commit = self.committed.iter().any(|name| name.starts_with(dir));
            if holds_a_commit && self.outcomes(dir).contains(&None) {
                return Some(format!("directory {} may be gone", dir.display()));
            }
        }
        if deletes {
            for name in &self.deleted {
                if self.outcomes(name).iter().any(Option::is_some) {
                    return Some(format!("{} was deleted and may come back", name.display()));
                }
            }
        }
        None
    }
}

/// The first crash point whose loss breaks a rule, with the steps up to it.
fn first_broken(steps: &[Step], deletes: bool) -> Option<String> {
    let mut disk = Disk::default();
    for at in 0..=steps.len() {
        if let Some(why) = disk.broken(deletes) {
            return Some(format!("after {at} of {steps:#?}: {why}"));
        }
        if let Some(step) = steps.get(at) {
            disk.apply(step);
        }
    }
    None
}

fn key(min: u64, max: u64) -> FileKey {
    let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
    FileKey::new(Level::ZERO, range)
}

fn upload(store: &FsStore, key: FileKey, bytes: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(&key, Timestamp::from_unix_millis(1_700_000_000_000))
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap();
    });
}

/// Two uploads, the first creating the level's directories, the second writing over the
/// first's key, then a third key: every crash point leaves no torn object visible, keeps
/// every committed object and every directory made for it.
#[test]
fn a_crash_at_any_step_of_a_commit_loses_nothing_committed_and_shows_nothing_torn() {
    let dir = TempDir::new().unwrap();
    let store = FsStore::new(dir.path().join("replica"));
    let ((), steps) = capture(|| {
        upload(&store, key(1, 1), b"first");
        upload(&store, key(1, 1), b"first, again");
        upload(&store, key(2, 2), b"second");
    });
    assert!(steps.iter().any(|step| matches!(step, Step::CreateDir(_))));
    assert_eq!(
        steps
            .iter()
            .filter(|s| matches!(s, Step::Committed(_)))
            .count(),
        3
    );
    if let Some(why) = first_broken(&steps, false) {
        panic!("{why}");
    }
}

/// A delete syncs the level directory of every key before it returns, so after a power loss
/// at any step a deleted file is either still there because the delete had not returned, or
/// gone for good (D11 in `docs/BEHAVIOR.md`). Without the sync a deleted staged file can come
/// back.
#[test]
fn a_crash_after_a_delete_never_brings_the_file_back() {
    let dir = TempDir::new().unwrap();
    let store = FsStore::new(dir.path().join("replica"));
    let ((), steps) = capture(|| {
        upload(&store, key(1, 1), b"first");
        block_on(store.delete(&[key(1, 1)])).unwrap();
        // A retry of a delete that already removed the name still syncs before it returns.
        block_on(store.delete(&[key(1, 1), key(2, 2)])).unwrap();
    });
    if let Some(why) = first_broken(&steps, true) {
        panic!("{why}");
    }
}

/// The checker can fail: a rename before the file's sync, and a commit with no directory
/// sync, each break a rule.
#[test]
fn the_crash_checker_fails_a_rename_before_the_sync_and_a_commit_without_a_directory_sync() {
    let (dir, scratch, target) = (
        PathBuf::from("/r/ltx/0"),
        PathBuf::from("/r/ltx/0/a.ltx.1-0.tmp"),
        PathBuf::from("/r/ltx/0/a.ltx"),
    );
    let rename = Step::Rename {
        from: scratch.clone(),
        to: target.clone(),
    };
    let unsynced = [
        Step::Create(scratch.clone()),
        Step::Write(scratch.clone()),
        rename.clone(),
        Step::SyncFile(target.clone()),
        Step::SyncDir(dir.clone()),
        Step::Committed(target.clone()),
    ];
    assert!(first_broken(&unsynced, false).is_some());
    let no_dir_sync = [
        Step::Create(scratch.clone()),
        Step::Write(scratch.clone()),
        Step::SyncFile(scratch),
        rename,
        Step::Committed(target),
    ];
    assert!(first_broken(&no_dir_sync, false).is_some());
}
