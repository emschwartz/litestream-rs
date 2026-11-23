//! Each filesystem call the store tolerates one error kind from (a file already gone, a
//! directory already made, a hard link the filesystem cannot make), failed with that kind and
//! with one it must not tolerate: the first is absorbed, the second reaches the caller.

use super::*;

/// The call [`Faulty`] fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    ReadDir,
    Metadata,
    CreateDir,
    SyncDir,
    HardLink,
    RemoveFile,
    CreateNew,
}

/// The real filesystem, with every call of `op` failing with `kind`.
#[derive(Clone, Copy, Debug)]
struct Faulty {
    op: Op,
    kind: io::ErrorKind,
}

impl Faulty {
    fn check(self, op: Op) -> io::Result<()> {
        if self.op == op {
            return Err(self.kind.into());
        }
        Ok(())
    }
}

impl Fs for Faulty {
    type File = <RealFs as Fs>::File;

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>> {
        self.check(Op::ReadDir)?;
        RealFs.read_dir(dir)
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        self.check(Op::Metadata)?;
        RealFs.metadata(path)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealFs.try_exists(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.check(Op::CreateDir)?;
        RealFs.create_dir(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.check(Op::SyncDir)?;
        RealFs.sync_dir(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.check(Op::HardLink)?;
        RealFs.hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.check(Op::RemoveFile)?;
        RealFs.remove_file(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File> {
        self.check(Op::CreateNew)?;
        RealFs.create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<Self::File> {
        RealFs.open(path)
    }
}

const DENIED: io::ErrorKind = io::ErrorKind::PermissionDenied;

/// Commits `key` through a store on the real filesystem.
fn put(root: &Path, key: FileKey) {
    block_on(async {
        let store = FsStore::new(root);
        let mut up = store
            .begin_upload(&key, Timestamp::from_unix_millis(1), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"data").await.unwrap();
        up.commit().await.unwrap();
    });
}

/// A store on `root` whose `op` fails with `kind`.
fn faulty(root: &Path, op: Op, kind: io::ErrorKind) -> FsStore<Faulty> {
    FsStore::with_fs(root, Faulty { op, kind })
}

/// Starts an upload of `key` on `store`, leaving its scratch file.
fn begun(store: &FsStore<Faulty>, key: FileKey) -> Result<FsUpload<Faulty>, StoreError> {
    block_on(store.begin_upload(&key, Timestamp::from_unix_millis(1), WriteMode::New))
}

#[test]
fn a_missing_directory_already_made_is_fine_and_a_refused_one_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a/b");
    let made = Faulty {
        op: Op::CreateDir,
        kind: io::ErrorKind::AlreadyExists,
    };
    create_dirs_durably(&made, &path, &mut |_| Ok(())).unwrap();
    let denied = Faulty {
        op: Op::CreateDir,
        kind: DENIED,
    };
    create_dirs_durably(&denied, &path, &mut |_| Ok(())).unwrap_err();
}

#[test]
fn a_listing_absorbs_only_a_gone_level_or_a_gone_file() {
    let dir = tempfile::tempdir().unwrap();
    put(dir.path(), key(0, 1, 1));
    let list =
        |op, kind| block_on(faulty(dir.path(), op, kind).list(Level::ZERO, None, PageLimit::MAX));
    assert_eq!(
        list(Op::ReadDir, io::ErrorKind::NotFound).unwrap().files,
        vec![]
    );
    list(Op::ReadDir, DENIED).unwrap_err();
    // A file deleted between the directory read and its stat is gone, not an error.
    assert_eq!(
        list(Op::Metadata, io::ErrorKind::NotFound).unwrap().files,
        vec![]
    );
    list(Op::Metadata, DENIED).unwrap_err();
}

#[test]
fn a_taken_scratch_name_is_skipped_and_a_refused_one_ends_the_upload() {
    let dir = tempfile::tempdir().unwrap();
    let taken = begun(
        &faulty(dir.path(), Op::CreateNew, io::ErrorKind::AlreadyExists),
        key(0, 1, 1),
    );
    assert!(
        matches!(taken, Err(StoreError::ScratchExhausted { .. })),
        "{taken:?}"
    );
    let denied = begun(&faulty(dir.path(), Op::CreateNew, DENIED), key(0, 1, 1));
    assert!(
        matches!(denied, Err(ref err) if !matches!(err, StoreError::ScratchExhausted { .. })),
        "{denied:?}"
    );
}

#[test]
fn removing_scratch_absorbs_only_what_is_already_gone() {
    let dir = tempfile::tempdir().unwrap();
    // The level directory is made and the dropped upload leaves its scratch file behind.
    drop(begun(&faulty(dir.path(), Op::HardLink, DENIED), key(0, 1, 1)).unwrap());
    let remove = |op, kind| block_on(faulty(dir.path(), op, kind).remove_scratch());
    remove(Op::ReadDir, DENIED).unwrap_err();
    assert_eq!(remove(Op::RemoveFile, io::ErrorKind::NotFound), Ok(0));
    remove(Op::RemoveFile, DENIED).unwrap_err();
}

#[test]
fn a_delete_absorbs_only_a_file_or_directory_already_gone() {
    for (op, kind, ok) in [
        (Op::RemoveFile, io::ErrorKind::NotFound, true),
        (Op::RemoveFile, DENIED, false),
        (Op::SyncDir, io::ErrorKind::NotFound, true),
        (Op::SyncDir, DENIED, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        put(dir.path(), key(0, 1, 1));
        let got = block_on(faulty(dir.path(), op, kind).delete(&[key(0, 1, 1)]));
        assert_eq!(got.is_ok(), ok, "{op:?} {kind:?}: {got:?}");
    }
}

#[test]
fn a_create_only_commit_tells_a_taken_key_from_a_filesystem_without_links() {
    let commit = |kind| {
        let dir = tempfile::tempdir().unwrap();
        let store = faulty(dir.path(), Op::HardLink, kind);
        let result = block_on(async {
            let mut up = begun(&store, key(0, 1, 1)).unwrap();
            up.write(b"data").await.unwrap();
            up.commit().await
        });
        let published = fs::read(store.file_path(&key(0, 1, 1))).ok();
        (result, published)
    };
    let (taken, _) = commit(io::ErrorKind::AlreadyExists);
    assert!(
        matches!(taken, Err(StoreError::AlreadyExists { .. })),
        "{taken:?}"
    );
    // Without hard links the commit falls back to a rename, which still publishes.
    let (renamed, published) = commit(io::ErrorKind::Unsupported);
    assert!(renamed.is_ok(), "{renamed:?}");
    assert_eq!(published.as_deref(), Some(&b"data"[..]));
    let (denied, published) = commit(DENIED);
    assert!(
        matches!(denied, Err(ref err) if !matches!(err, StoreError::AlreadyExists { .. })),
        "{denied:?}"
    );
    assert_eq!(published, None);
}

#[test]
fn an_abort_absorbs_only_a_scratch_file_already_gone() {
    let abort = |kind| {
        let dir = tempfile::tempdir().unwrap();
        let store = faulty(dir.path(), Op::RemoveFile, kind);
        block_on(begun(&store, key(0, 1, 1)).unwrap().abort())
    };
    assert_eq!(abort(io::ErrorKind::NotFound), Ok(()));
    abort(DENIED).unwrap_err();
}
