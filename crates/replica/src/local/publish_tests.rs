//! Each publish step failing in turn: the error says whether the target is visible, and
//! it is exactly when the link had happened.

use std::cell::Cell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use lsr_core::{DatabaseSink, PublishFailure, PublishLeft};
use lsr_ltx::{PageNo, PageRef, PageSize};
use tempfile::TempDir;

use super::{FileSink, PublishError, PublishFs, PublishStep, publish_steps};

const STEPS: [PublishStep; 6] = [
    PublishStep::SyncScratch,
    PublishStep::CheckAbsent,
    PublishStep::Link,
    PublishStep::SyncName,
    PublishStep::RemoveScratch,
    PublishStep::SyncRemoval,
];

/// A file system that fails one step and records whether the target was linked.
struct Failing {
    fail: PublishStep,
    syncs: Cell<u32>,
    linked: Cell<bool>,
}

impl Failing {
    fn new(fail: PublishStep) -> Self {
        Self {
            fail,
            syncs: Cell::new(0),
            linked: Cell::new(false),
        }
    }

    fn step(&self, step: PublishStep) -> io::Result<()> {
        if step == self.fail {
            Err(io::Error::other(format!("injected at {step:?}")))
        } else {
            Ok(())
        }
    }
}

impl PublishFs for Failing {
    fn sync_scratch(&mut self) -> io::Result<()> {
        self.step(PublishStep::SyncScratch)
    }

    fn first_present(&self) -> io::Result<Option<PathBuf>> {
        self.step(PublishStep::CheckAbsent).map(|()| None)
    }

    fn link(&self) -> io::Result<()> {
        self.step(PublishStep::Link)?;
        self.linked.set(true);
        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        let first = self.syncs.get() == 0;
        self.syncs.set(self.syncs.get().saturating_add(1));
        self.step(if first {
            PublishStep::SyncName
        } else {
            PublishStep::SyncRemoval
        })
    }

    fn remove_scratch(&self) -> io::Result<()> {
        self.step(PublishStep::RemoveScratch)
    }
}

#[test]
fn each_failed_step_says_whether_the_target_is_visible() {
    for step in STEPS {
        let mut fs = Failing::new(step);
        let err = publish_steps(&mut fs, Path::new("/t/db")).unwrap_err();
        match &err {
            PublishError::NotVisible { step: at, .. } => {
                assert!(
                    !fs.linked.get(),
                    "{step:?}: linked, yet reported not visible"
                );
                assert_eq!(*at, step);
            }
            PublishError::VisibleNotDurable { step: at, .. } => {
                assert!(
                    fs.linked.get(),
                    "{step:?}: not linked, yet reported visible"
                );
                assert_eq!(*at, step);
            }
            PublishError::Integrity(_) => panic!("{step:?}: the steps run no check"),
        }
        assert_eq!(step.after_link(), fs.linked.get(), "{step:?}");
        assert!(err.to_string().contains("/t/db"), "{err}");
    }
}

#[test]
fn a_publish_error_says_what_it_left() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("db");
    let mut sink = FileSink::create(&target).unwrap();
    let size = PageSize::new(512).unwrap();
    let page = [1_u8; 512];
    sink.write_page(PageRef::new(PageNo::new(1, size).unwrap(), size, &page).unwrap())
        .unwrap();
    fs::write(&target, b"someone else's").unwrap();
    let err = sink.publish().unwrap_err();
    assert_eq!(err.left(), PublishLeft::Nothing);
    assert!(matches!(
        &err,
        PublishError::NotVisible {
            step: PublishStep::CheckAbsent,
            source,
            ..
        } if source.kind() == io::ErrorKind::AlreadyExists
    ));
}
