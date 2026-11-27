//! No compaction leaves a hole in its level that the level below could have closed.
//!
//! Go's compaction reads every file of the level below that starts above its destination's
//! newest transaction and refuses the whole set when one does not start where the one
//! before ended (`non-contiguous transaction ids in input files`, ltx `compactor.go:125`).
//! A level with a hole therefore stops Go's compaction of the level above for as long as
//! the files on both sides stay, which is what a rollback to Go met after an engine
//! planned level 2 from a listing that had lost one of its own level-1 files
//! (`docs/decisions/D228.md`).
//!
//! The judge restates the rule from what the store took, never from the planner: when a
//! file is put at a level 1 to 8 and the level's files before it end short of where it
//! starts, no file of the level below may lie wholly inside the gap. A file there is one
//! the compaction jumped over. A gap with no such file is a range the level below never
//! held or no longer holds, and is allowed; so is a gap where the replica lost one of the
//! level's own files from outside (a key the engine put and never deleted, which the store
//! no longer holds), since the engine extends a level from the newest file it knows.

use lsr_core::{FileInfo, FileKey};

use super::Violation;
use crate::store::RecorderEvent;

/// What a judged put turned out to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LevelPut {
    /// Not a put at level 1 to 8, or the replica no longer holds the file.
    Skipped,
    /// The first file of its level, or one that starts no later than the transaction after
    /// the files before it end (it extends the level or closes a hole from its start).
    Follows,
    /// [`Self::Follows`], and two or more files of the level below lie inside it: the
    /// compaction had more than one input to take, which is where one that takes only the
    /// newest would leave a hole.
    Joins,
    /// It leaves a gap that no file of the level below lies inside.
    NothingSkipped,
    /// It leaves a gap where the replica lost a file of this level from outside.
    LostOutside,
}

fn name(key: &FileKey) -> String {
    format!(
        "{} {}-{}",
        key.level().get(),
        key.range().min().get(),
        key.range().max().get()
    )
}

/// Judges the replica put `put` against the files the replica holds now (`files`, after the
/// tick that put it) and every replica event so far (`events`).
///
/// # Errors
/// [`Violation::LevelHole`] when the put leaves a gap in its level with a file of the level
/// below inside it.
pub fn judge_level_put(
    events: &[RecorderEvent],
    files: &[FileInfo],
    put: &RecorderEvent,
) -> Result<LevelPut, Violation> {
    let RecorderEvent::Put { key, .. } = put else {
        return Ok(LevelPut::Skipped);
    };
    let level = key.level().get();
    if !(1..=8).contains(&level) || !files.iter().any(|file| file.key == *key) {
        return Ok(LevelPut::Skipped);
    }
    let (min, max) = (key.range().min().get(), key.range().max().get());
    let below = |file: &&FileInfo| file.key.level().get() == level.saturating_sub(1);
    let before = files
        .iter()
        .filter(|file| file.key.level().get() == level && file.key.range().min().get() < min)
        .map(|file| file.key.range().max().get())
        .max();
    let end = match before {
        Some(end) if end.saturating_add(1) < min => end,
        Some(_) | None => {
            let inputs = files.iter().filter(below).filter(|file| {
                file.key.range().min().get() >= min && file.key.range().max().get() <= max
            });
            return Ok(if inputs.count() > 1 {
                LevelPut::Joins
            } else {
                LevelPut::Follows
            });
        }
    };
    let inside =
        |file: &&FileInfo| file.key.range().min().get() > end && file.key.range().max().get() < min;
    let Some(skipped) = files.iter().filter(below).find(inside) else {
        return Ok(LevelPut::NothingSkipped);
    };
    let deleted = |gone: &FileKey| {
        events
            .iter()
            .any(|event| matches!(event, RecorderEvent::Delete { keys, .. } if keys.contains(gone)))
    };
    let lost_outside = events.iter().any(|event| {
        matches!(event, RecorderEvent::Put { key: was, .. }
            if was.level().get() == level
                && was.range().max().get() > end
                && was.range().min().get() < min
                && !files.iter().any(|file| file.key == *was)
                && !deleted(was))
    });
    if lost_outside {
        return Ok(LevelPut::LostOutside);
    }
    Err(Violation::LevelHole {
        key: name(key),
        skipped: name(&skipped.key),
    })
}
