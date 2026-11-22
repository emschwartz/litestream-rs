//! The durability steps of this store's writes, recorded in test builds for a checker that
//! crashes the store at every step.
//!
//! Each step that changes what a power loss can leave (a file created, written, synced,
//! renamed or removed; a directory created or synced; a commit returned to the caller) is
//! noted where it happens. In a release build [`note`] is empty and its closure never runs. In
//! a test build the steps go to a thread-local list while [`capture`] runs, and the crash
//! checker in the unit tests replays them under the crash law: unsynced file bytes may be
//! lost, and every directory operation since its directory's last sync may or may not have
//! happened.

use std::path::PathBuf;

/// One step a power loss cares about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// A new file appeared in a directory.
    Create(PathBuf),
    /// Bytes were written to an open file.
    Write(PathBuf),
    /// A file's bytes were synced.
    SyncFile(PathBuf),
    /// A file was renamed over another name in the same directory.
    Rename {
        /// The old name.
        from: PathBuf,
        /// The new name.
        to: PathBuf,
    },
    /// A second name for a file appeared in the same directory (a create-only commit links
    /// its scratch file to the object's name, which fails if the name is taken).
    Link {
        /// The existing name.
        from: PathBuf,
        /// The new name.
        to: PathBuf,
    },
    /// A name was removed.
    Remove(PathBuf),
    /// A directory was created.
    CreateDir(PathBuf),
    /// A directory's entries were synced.
    SyncDir(PathBuf),
    /// A commit returned success for this name.
    Committed(PathBuf),
    /// A delete returned success for this name.
    Deleted(PathBuf),
}

#[cfg(test)]
thread_local! {
    static STEPS: std::cell::RefCell<Option<Vec<Step>>> = const { std::cell::RefCell::new(None) };
}

/// Notes the step `step` builds, when a test is capturing.
#[cfg(test)]
pub(crate) fn note(step: impl FnOnce() -> Step) {
    STEPS.with(|steps| {
        if let Some(steps) = steps.borrow_mut().as_mut() {
            steps.push(step());
        }
    });
}

/// Notes nothing: release builds keep no journal.
#[cfg(not(test))]
#[inline]
pub(crate) fn note(_step: impl FnOnce() -> Step) {}

/// Runs `f` and returns what it returned with every step it took on this thread.
#[cfg(test)]
pub(crate) fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<Step>) {
    STEPS.with(|steps| *steps.borrow_mut() = Some(Vec::new()));
    let out = f();
    let taken = STEPS.with(|steps| steps.borrow_mut().take().unwrap_or_default());
    (out, taken)
}

#[cfg(test)]
mod tests;
