use std::collections::BTreeSet;
use std::fs;

use lsr_core::{FileKey, Level, conformance::block_on};
use lsr_core::{MemStore, ObjectStore, RestoreError, RestoreTarget, Upload, restore};
use lsr_ltx::{Encoder, Header, HeaderFields, Timestamp, Txid, TxidRange};
use tempfile::TempDir;

use super::*;
use lsr_core::WriteMode;

fn ps(bytes: u32) -> PageSize {
    PageSize::new(bytes).unwrap()
}

fn pgno(n: u32) -> PageNo {
    PageNo::new(n, ps(512)).unwrap()
}

fn page(n: u32, data: &[u8]) -> PageRef<'_> {
    PageRef::new(pgno(n), ps(512), data).unwrap()
}

#[test]
fn a_page_lands_where_its_page_size_puts_it() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    let size = ps(1024);
    let data = [5; 1024];
    let second = PageRef::new(PageNo::new(2, size).unwrap(), size, &data).unwrap();
    sink.write_page(second).unwrap();
    sink.publish().unwrap();
    let mut want = vec![0; 1024];
    want.extend([5; 1024]);
    assert_eq!(fs::read(&target).unwrap(), want);
}

fn names(dir: &TempDir) -> BTreeSet<String> {
    fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

#[test]
fn create_refuses_a_target_that_exists() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"precious").unwrap();
    assert!(matches!(
        FileSink::create(&target),
        Err(CreateError::TargetExists(path)) if path == target
    ));
    assert_eq!(fs::read(&target).unwrap(), b"precious");
    let dangling = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path().join("nowhere"), &dangling).unwrap();
    assert!(matches!(
        FileSink::create(&dangling),
        Err(CreateError::TargetExists(_))
    ));
    assert_eq!(
        names(&dir),
        BTreeSet::from(["db".to_owned(), "link".to_owned()])
    );
}

#[test]
fn the_scratch_is_the_target_name_plus_restore() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let sink = FileSink::create(&target).unwrap();
    assert_eq!(sink.scratch_path(), dir.path().join("db.restore"));
    assert!(!target.exists());
}

#[test]
fn a_second_restore_of_one_target_is_refused_while_the_first_lives() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut first = FileSink::create(&target).unwrap();
    first.write_page(page(1, &[1; 512])).unwrap();
    assert!(matches!(
        FileSink::create(&target),
        Err(CreateError::RestoreInProgress(path)) if path == dir.path().join("db.restore")
    ));
    // The refused attempt did not disturb the first restore's scratch.
    assert_eq!(fs::read(first.scratch_path()).unwrap(), vec![1; 512]);
    drop(first);
    assert!(names(&dir).is_empty());
    drop(FileSink::create(&target).unwrap());
}

#[test]
fn a_scratch_left_by_a_crashed_restore_is_reclaimed() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(dir.path().join("db.restore"), vec![9; 4096]).unwrap();
    let mut sink = FileSink::create(&target).unwrap();
    assert_eq!(fs::metadata(sink.scratch_path()).unwrap().len(), 0);
    sink.write_page(page(1, &[1; 512])).unwrap();
    sink.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![1; 512]);
    assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
}

#[test]
fn the_scratch_is_removed_on_drop_unless_published() {
    let dir = TempDir::new().unwrap();
    let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
    sink.write_page(page(1, &[1; 512])).unwrap();
    assert_eq!(names(&dir).len(), 1);
    drop(sink);
    assert!(names(&dir).is_empty());
}

#[test]
fn published_bytes_are_the_pages_written_and_only_the_target_remains() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    sink.write_page(page(3, &[3; 512])).unwrap();
    sink.write_page(page(1, &[1; 512])).unwrap();
    sink.truncate(DbPages::new(4), ps(512)).unwrap();
    sink.publish().unwrap();
    let mut want = [1_u8; 512].to_vec();
    want.extend([0; 512]);
    want.extend([3; 512]);
    want.extend([0; 512]);
    assert_eq!(fs::read(&target).unwrap(), want);
    assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
}

#[test]
fn truncate_sets_the_exact_length_and_growth_reads_as_zeros() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    sink.write_page(page(2, &[9; 512])).unwrap();
    sink.truncate(DbPages::new(1), ps(512)).unwrap();
    sink.truncate(DbPages::new(2), ps(512)).unwrap();
    sink.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![0; 1024]);
}

#[test]
fn checksum_reads_back_what_the_file_holds() {
    let dir = TempDir::new().unwrap();
    let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
    sink.write_page(page(3, &[3; 512])).unwrap();
    sink.truncate(DbPages::new(4), ps(512)).unwrap();
    let mut want = DbChecksum::EMPTY;
    for (n, seed) in [(1, 0_u8), (2, 0), (3, 3), (4, 0)] {
        want = want.toggle(PageChecksum::of(pgno(n), &[seed; 512]));
    }
    assert_eq!(sink.checksum(DbPages::new(4), ps(512)).unwrap(), want);
}

#[test]
fn reset_empties_the_scratch() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    sink.write_page(page(1, &[1; 512])).unwrap();
    sink.reset().unwrap();
    sink.write_page(page(1, &[2; 512])).unwrap();
    sink.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![2; 512]);
}

#[test]
fn create_refuses_each_journal_file_and_removes_nothing() {
    for suffix in ["-wal", "-shm", "-journal"] {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let journal_file = dir.path().join(format!("db{suffix}"));
        fs::write(&journal_file, b"stale").unwrap();
        assert!(matches!(
            FileSink::create(&target),
            Err(CreateError::JournalFile { path }) if path == journal_file
        ));
        assert_eq!(fs::read(&journal_file).unwrap(), b"stale");
        assert_eq!(names(&dir).len(), 1, "{suffix}: no scratch left behind");
    }
}

#[test]
fn preflight_reports_what_create_and_clear_would_meet_and_touches_nothing() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    assert_eq!(preflight(&target).unwrap(), Vec::<PathBuf>::new());
    fs::write(dir.path().join("db-wal"), b"stale").unwrap();
    fs::write(dir.path().join("db-journal"), b"stale").unwrap();
    assert_eq!(
        preflight(&target).unwrap(),
        vec![dir.path().join("db-wal"), dir.path().join("db-journal")]
    );
    fs::write(&target, b"live").unwrap();
    assert!(matches!(
        preflight(&target),
        Err(CreateError::TargetExists(path)) if path == target
    ));
    assert_eq!(names(&dir).len(), 3, "nothing created or removed");
}

#[test]
fn clear_journal_files_removes_the_three_and_reports_them() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    for suffix in ["-wal", "-shm", "-journal"] {
        fs::write(dir.path().join(format!("db{suffix}")), b"stale").unwrap();
    }
    fs::write(dir.path().join("db-other"), b"not ours").unwrap();
    let mut removed = clear_journal_files(&target).unwrap();
    removed.sort();
    assert_eq!(
        removed,
        ["db-journal", "db-shm", "db-wal"].map(|n| dir.path().join(n))
    );
    assert_eq!(names(&dir), BTreeSet::from(["db-other".to_owned()]));
    assert!(clear_journal_files(&target).unwrap().is_empty());
}

#[test]
fn clear_journal_files_refuses_a_target_that_exists() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"live").unwrap();
    fs::write(dir.path().join("db-wal"), b"its wal").unwrap();
    assert!(matches!(
        clear_journal_files(&target),
        Err(ClearError::TargetExists(path)) if path == target
    ));
    assert_eq!(fs::read(dir.path().join("db-wal")).unwrap(), b"its wal");
}

/// Review: a stale rollback journal beside the target is a hot journal to
/// SQLite, which rolls its old pages back over the restored database on
/// the first open ("database disk image is malformed", reproduced with
/// sqlite3). Create refuses; after clear_journal_files the restore publishes
/// exactly the database and nothing else.
#[test]
fn a_stale_journal_blocks_the_restore_until_cleared() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(dir.path().join("db-journal"), b"stale hot journal").unwrap();
    assert!(matches!(
        FileSink::create(&target),
        Err(CreateError::JournalFile { .. })
    ));
    clear_journal_files(&target).unwrap();
    let mut sink = FileSink::create(&target).unwrap();
    sink.write_page(page(1, &[1; 512])).unwrap();
    sink.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![1; 512]);
    assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
}

/// A journal file that appears after create is a rival's (or a stale file's);
/// publish refuses and leaves it, and the target, alone.
#[test]
fn a_journal_file_appearing_after_create_makes_publish_refuse() {
    for suffix in ["-wal", "-shm", "-journal"] {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        let journal_file = dir.path().join(format!("db{suffix}"));
        fs::write(&journal_file, b"theirs").unwrap();
        let err = sink.publish().unwrap_err();
        assert!(already_exists(&err), "{err}");
        assert!(err.to_string().contains(&format!("db{suffix}")), "{err}");
        assert_eq!(fs::read(&journal_file).unwrap(), b"theirs");
        assert_eq!(names(&dir), BTreeSet::from([format!("db{suffix}")]));
    }
}

/// A refusal before the link because something is already at a path.
fn already_exists(err: &PublishError) -> bool {
    matches!(
        err,
        PublishError::NotVisible { step: PublishStep::CheckAbsent, source, .. }
            if source.kind() == io::ErrorKind::AlreadyExists
    )
}

#[test]
fn a_target_created_during_the_restore_is_not_overwritten() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    sink.write_page(page(1, &[1; 512])).unwrap();
    fs::write(&target, b"someone else's").unwrap();
    fs::write(dir.path().join("db-wal"), b"their wal").unwrap();
    let err = sink.publish().unwrap_err();
    assert!(already_exists(&err), "{err}");
    assert_eq!(fs::read(&target).unwrap(), b"someone else's");
    // Their WAL belongs to their database: it is not ours to remove.
    assert_eq!(fs::read(dir.path().join("db-wal")).unwrap(), b"their wal");
    assert_eq!(
        names(&dir),
        BTreeSet::from(["db".to_owned(), "db-wal".to_owned()])
    );
}

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn snapshot_bytes(commit: u32, seed: u8) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps(512),
        commit,
        txids: TxidRange::single(txid(1)),
        timestamp: Timestamp::from_unix_millis(1000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in 1..=commit {
        encoder.write_page(pgno(n), &[seed; 512]).unwrap();
    }
    encoder.finish(None).unwrap().0
}

fn replica(bytes: &[u8]) -> MemStore {
    let store = MemStore::new();
    block_on(async {
        let key = FileKey::new(Level::SNAPSHOT, TxidRange::single(txid(1)));
        let mut upload = store
            .begin_upload(&key, Timestamp::from_unix_millis(1000), WriteMode::Replace)
            .await
            .unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap();
    });
    store
}

#[test]
fn a_restore_publishes_the_database_and_nothing_else() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let store = replica(&snapshot_bytes(3, 7));
    let sink = FileSink::create(&target).unwrap();
    let restored = block_on(restore(&store, RestoreTarget::Latest, sink)).unwrap();
    assert_eq!(restored.commit(), DbPages::new(3));
    assert_eq!(fs::read(&target).unwrap(), vec![7; 1536]);
    assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
}

/// A real SQLite database in WAL mode (its header says so, as a
/// replicated one does), 512-byte pages, a table and an index over it.
fn sqlite_pages() -> Vec<u8> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("source.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size = 512; PRAGMA journal_mode = WAL;
         CREATE TABLE t (x TEXT); CREATE INDEX t_x ON t (x);",
    )
    .unwrap();
    for n in 0..200 {
        conn.execute("INSERT INTO t VALUES (?1)", [format!("{n:0>40}")])
            .unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.get(18..20), Some(&[2, 2][..]), "WAL-mode header");
    bytes
}

/// A snapshot holding exactly [db]'s pages: valid LTX whatever they say.
fn snapshot_of(db: &[u8]) -> Vec<u8> {
    let commit = u32::try_from(db.len() / 512).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps(512),
        commit,
        txids: TxidRange::single(txid(1)),
        timestamp: Timestamp::from_unix_millis(1000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for (n, page) in (1..=commit).zip(db.as_chunks::<512>().0) {
        encoder.write_page(pgno(n), page).unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// Page 2, the table's root, overwritten: every LTX check passes, the
/// b-tree is not one.
fn corrupt_b_tree() -> Vec<u8> {
    let mut db = sqlite_pages();
    db[512..1024].fill(0xDE);
    db
}

fn checked_sink(target: &Path, check: IntegrityCheck) -> FileSink {
    FileSink::create(target)
        .unwrap()
        .with_integrity_check(check)
}

fn restore_with(db: &[u8], check: IntegrityCheck, target: &Path) -> Result<(), RestoreError> {
    let store = replica(&snapshot_of(db));
    let sink = checked_sink(target, check);
    block_on(restore(&store, RestoreTarget::Latest, sink)).map(drop)
}

fn integrity_failure(result: Result<(), RestoreError>) -> IntegrityFailure {
    match result {
        Err(RestoreError::Publish { source, .. }) => match source.downcast::<PublishError>() {
            Ok(failure) => match *failure {
                PublishError::Integrity(failure) => failure,
                other @ (PublishError::NotVisible { .. }
                | PublishError::VisibleNotDurable { .. }) => {
                    panic!("not an integrity failure: {other}")
                }
            },
            Err(source) => panic!("not the sink's error: {source}"),
        },
        other => panic!("expected a refused publish, got {other:?}"),
    }
}

#[test]
fn every_check_publishes_a_valid_database_and_nothing_beside_it() {
    let db = sqlite_pages();
    for check in [
        IntegrityCheck::Off,
        IntegrityCheck::Quick,
        IntegrityCheck::Full,
    ] {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        restore_with(&db, check, &target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), db, "{check:?}");
        // No -wal or -shm beside the scratch or the target, though the
        // header says WAL mode: the check opened it immutable.
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]), "{check:?}");
    }
}

#[test]
fn quick_and_full_refuse_a_corrupt_b_tree_and_leave_nothing() {
    let db = corrupt_b_tree();
    for (check, pragma) in [
        (IntegrityCheck::Quick, "quick_check"),
        (IntegrityCheck::Full, "integrity_check"),
    ] {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let failure = integrity_failure(restore_with(&db, check, &target));
        assert_eq!(failure.pragma(), pragma);
        assert!(
            failure.messages().iter().any(|m| m.contains("page 2")),
            "{failure}"
        );
        assert!(failure.to_string().contains(pragma), "{failure}");
        assert!(names(&dir).is_empty(), "{check:?}: {:?}", names(&dir));
    }
}

#[test]
fn off_publishes_the_corrupt_b_tree_as_restored() {
    let db = corrupt_b_tree();
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    restore_with(&db, IntegrityCheck::Off, &target).unwrap();
    assert_eq!(fs::read(&target).unwrap(), db);
    assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
}

/// Pages that are no database at all: SQLite refuses to open them, which
/// is a verdict on the file, so the restore is refused, not failed.
#[test]
fn a_file_that_is_not_a_database_is_refused_like_a_failed_check() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let failure = integrity_failure(restore_with(&[7; 1536], IntegrityCheck::Quick, &target));
    assert_eq!(failure.messages().len(), 1);
    assert!(!failure.more());
    assert!(names(&dir).is_empty());
}

/// Only the first problems are kept, and a cut list says so.
#[test]
fn a_long_list_of_problems_keeps_the_first_few() {
    let mut db = sqlite_pages();
    let len = db.len();
    db[512..len].fill(0xDE);
    let dir = TempDir::new().unwrap();
    let failure = integrity_failure(restore_with(
        &db,
        IntegrityCheck::Full,
        &dir.path().join("db"),
    ));
    assert_eq!(failure.messages().len(), integrity::KEPT_MESSAGES);
    assert!(failure.more());
    assert!(failure.to_string().ends_with("; and more"), "{failure}");
}

/// The check opens the scratch by URI; a name with URI syntax in it is
/// still that file.
#[test]
fn a_target_name_with_uri_syntax_is_checked_as_named() {
    let dir = TempDir::new().unwrap();
    let name = "a?mode=rwc#b%41 c";
    restore_with(
        &sqlite_pages(),
        IntegrityCheck::Quick,
        &dir.path().join(name),
    )
    .unwrap();
    let failure = integrity_failure(restore_with(
        &corrupt_b_tree(),
        IntegrityCheck::Quick,
        &dir.path().join("x?y"),
    ));
    assert!(!failure.messages().is_empty());
    assert_eq!(names(&dir), BTreeSet::from([name.to_owned()]));
}

/// The check runs on the bytes in the scratch at publish, whatever wrote
/// them: a page changed after the restore's own writes is caught.
#[test]
fn the_check_reads_the_scratch_as_it_is_at_publish() {
    let db = sqlite_pages();
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target)
        .unwrap()
        .with_integrity_check(IntegrityCheck::Full);
    for (n, bytes) in (1..).zip(db.as_chunks::<512>().0) {
        sink.write_page(page(n, bytes)).unwrap();
    }
    sink.write_page(page(2, &[0xDE; 512])).unwrap();
    let err = sink.publish().unwrap_err();
    assert!(matches!(err, PublishError::Integrity(_)), "{err}");
    assert!(names(&dir).is_empty());
}

#[test]
fn a_failed_restore_leaves_no_target_and_no_scratch() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut bytes = snapshot_bytes(3, 7);
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let store = replica(&bytes);
    let sink = FileSink::create(&target).unwrap();
    let result = block_on(restore(&store, RestoreTarget::Latest, sink));
    assert!(matches!(result, Err(RestoreError::File { .. })));
    assert!(names(&dir).is_empty());
}

/// A target named with no directory syncs the current one; the empty parent is not a path.
/// A named directory is the one synced, so one that does not exist fails.
#[test]
fn a_bare_target_name_syncs_the_current_directory() {
    sync_dir(Path::new("bare.db")).unwrap();
    let dir = TempDir::new().unwrap();
    sync_dir(&dir.path().join("missing").join("db")).unwrap_err();
}

/// A restore that starts over leaves nothing of the first attempt in the scratch.
#[test]
fn a_reset_empties_the_scratch() {
    let dir = TempDir::new().unwrap();
    let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
    sink.write_page(page(1, &[7; 512])).unwrap();
    sink.reset().unwrap();
    assert_eq!(fs::metadata(sink.scratch_path()).unwrap().len(), 0);
}

/// Only a missing file counts as absent: any other answer about the target or a journal file
/// (here a path under a regular file, a name too long, a journal that is a directory) stops the
/// restore rather than let it publish over something it could not look at.
#[test]
fn only_a_missing_file_counts_as_absent() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("file");
    fs::write(&file, b"x").unwrap();
    blocking(&file.join("db"), Existing::Refuse).unwrap_err();
    let too_long = dir.path().join("j".repeat(300));
    exists(&too_long).unwrap_err();
    let target = dir.path().join("db");
    let sink = FileSink::create_over(&target, Existing::Any).unwrap();
    fs::create_dir(suffixed(&target, "-wal")).unwrap();
    ReplaceFs::remove_journals(&sink).unwrap_err();
}
