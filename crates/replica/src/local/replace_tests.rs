//! Replacing a file at the target (Go's `restore` over an empty file, or `-force`): what
//! each mode replaces and refuses, that a restore which never publishes leaves the old
//! file, and the order of the publish steps.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use lsr_core::DatabaseSink;
use lsr_ltx::{PageNo, PageRef, PageSize};
use tempfile::TempDir;

use super::{
    CreateError, Existing, FileSink, PublishError, PublishStep, ReplaceFs, preflight_over,
    replace_steps,
};

fn one_page(target: &Path, existing: Existing) -> Result<FileSink, CreateError> {
    FileSink::create_over(target, existing).map(write_one)
}

fn write_one(mut sink: FileSink) -> FileSink {
    let size = PageSize::new(512).unwrap();
    let page = [7_u8; 512];
    sink.write_page(PageRef::new(PageNo::new(1, size).unwrap(), size, &page).unwrap())
        .unwrap();
    sink
}

#[test]
fn empty_mode_replaces_an_empty_file_and_refuses_anything_else() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"").unwrap();
    one_page(&target, Existing::Empty)
        .unwrap()
        .publish()
        .unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![7_u8; 512]);
    // Now not empty: refused, and kept.
    assert!(matches!(
        one_page(&target, Existing::Empty),
        Err(CreateError::TargetExists(_))
    ));
    assert_eq!(fs::read(&target).unwrap(), vec![7_u8; 512]);
    // A journal beside an empty file is still refused without -force.
    let other = dir.path().join("other");
    fs::write(&other, b"").unwrap();
    fs::write(dir.path().join("other-wal"), b"stale").unwrap();
    assert!(matches!(
        one_page(&other, Existing::Empty),
        Err(CreateError::JournalFile { .. })
    ));
}

#[test]
fn any_mode_replaces_a_database_and_removes_its_journal_files() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"old database").unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        fs::write(dir.path().join(format!("db{suffix}")), b"old").unwrap();
    }
    assert!(preflight_over(&target, Existing::Any).unwrap().is_empty());
    one_page(&target, Existing::Any).unwrap().publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), vec![7_u8; 512]);
    let left: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(left, vec!["db".to_owned()]);
}

#[test]
fn a_restore_that_never_publishes_leaves_the_old_database() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"old database").unwrap();
    fs::write(dir.path().join("db-wal"), b"old wal").unwrap();
    drop(one_page(&target, Existing::Any).unwrap());
    assert_eq!(fs::read(&target).unwrap(), b"old database");
    assert_eq!(fs::read(dir.path().join("db-wal")).unwrap(), b"old wal");
    assert!(!dir.path().join("db.restore").exists());
}

#[test]
fn a_directory_is_never_replaced() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::create_dir(&target).unwrap();
    for existing in [Existing::Empty, Existing::Any] {
        assert!(matches!(
            one_page(&target, existing),
            Err(CreateError::TargetExists(_))
        ));
    }
}

#[test]
fn a_target_filled_during_the_restore_is_not_replaced_in_empty_mode() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    fs::write(&target, b"").unwrap();
    let sink = one_page(&target, Existing::Empty).unwrap();
    fs::write(&target, b"written meanwhile").unwrap();
    let err = sink.publish().unwrap_err();
    assert!(matches!(
        err,
        PublishError::NotVisible {
            step: PublishStep::CheckReplaceable,
            ..
        }
    ));
    assert_eq!(fs::read(&target).unwrap(), b"written meanwhile");
}

/// Records each step and fails the one named.
struct Traced {
    fail: Option<PublishStep>,
    ops: RefCell<Vec<&'static str>>,
}

impl Traced {
    fn op(&self, name: &'static str, step: PublishStep) -> io::Result<()> {
        if self.fail == Some(step) {
            return Err(io::Error::other("injected"));
        }
        self.ops.borrow_mut().push(name);
        Ok(())
    }

    fn renamed(&self) -> bool {
        self.ops.borrow().contains(&"rename")
    }
}

impl ReplaceFs for Traced {
    fn sync_scratch(&mut self) -> io::Result<()> {
        self.op("sync scratch", PublishStep::SyncScratch)
    }

    fn not_replaceable(&self) -> io::Result<Option<PathBuf>> {
        self.op("check", PublishStep::CheckReplaceable)
            .map(|()| None)
    }

    fn remove_journals(&self) -> io::Result<()> {
        self.op("remove journals", PublishStep::RemoveJournals)
    }

    fn sync_dir(&self) -> io::Result<()> {
        let step = if self.renamed() {
            PublishStep::SyncName
        } else {
            PublishStep::SyncJournals
        };
        self.op("sync dir", step)
    }

    fn rename(&self) -> io::Result<()> {
        self.op("rename", PublishStep::Rename)
    }
}

#[test]
fn journal_removals_are_synced_before_the_rename() {
    let mut fs = Traced {
        fail: None,
        ops: RefCell::new(Vec::new()),
    };
    replace_steps(&mut fs, Path::new("/t/db")).unwrap();
    assert_eq!(
        *fs.ops.borrow(),
        [
            "sync scratch",
            "check",
            "remove journals",
            "sync dir",
            "rename",
            "sync dir"
        ]
    );
}

#[test]
fn each_failed_replace_step_says_whether_the_target_is_visible() {
    for step in [
        PublishStep::SyncScratch,
        PublishStep::CheckReplaceable,
        PublishStep::RemoveJournals,
        PublishStep::SyncJournals,
        PublishStep::Rename,
        PublishStep::SyncName,
    ] {
        let mut fs = Traced {
            fail: Some(step),
            ops: RefCell::new(Vec::new()),
        };
        let err = replace_steps(&mut fs, Path::new("/t/db")).unwrap_err();
        let visible = matches!(err, PublishError::VisibleNotDurable { .. });
        assert_eq!(visible, fs.renamed(), "{step:?}: {err}");
        assert_eq!(step.after_link(), fs.renamed(), "{step:?}");
    }
}
