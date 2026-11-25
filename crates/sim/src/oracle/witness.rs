//! Every level-0 file the replica takes is bytes an engine staged.
//!
//! The engine may upload a staged file only when it staged those bytes
//! itself, or when a boot read them whole from the staging directory and
//! checked them (see the module doc of `crates/core/src/witness.rs`). The
//! judge restates the part ground truth can check: a level-0 put is
//! byte-equal to a file some engine incarnation staged (the staging
//! recorder's digest, over its whole history). A staging backup put back with
//! other bytes, or a staged file replaced by another while the engine runs,
//! is bytes no engine staged. A file replaced while the engine is down passes
//! the boot's checks by design (`docs/decisions/D175.md`), and the draws never
//! make one: a flipped byte that would leave another encoding of the same file
//! is moved to the next byte (`World::flip_newest_staged`).

use lsr_core::{ObjectStore, ReadLen, conformance::block_on};

use super::Violation;
use crate::store::{RecorderEvent, content_digest};

/// What a judged put turned out to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judged {
    /// Not a level-0 put, or the replica no longer holds what it took.
    Skipped,
    /// An engine staged those bytes.
    Staged,
}

/// Judges the replica put `put` against the staging recorder's `staging`
/// events and the replica as `remote` holds it now.
///
/// # Errors
/// [`Violation::UnstagedPut`] when no engine staged those bytes; a
/// [`Violation::BugFault`] when the replica cannot be read.
pub fn judge_put(
    staging: &[RecorderEvent],
    remote: &crate::remote::Bucket,
    put: &RecorderEvent,
) -> Result<Judged, Violation> {
    let RecorderEvent::Put {
        key, digest, size, ..
    } = put
    else {
        return Ok(Judged::Skipped);
    };
    if key.level().get() != 0 {
        return Ok(Judged::Skipped);
    }
    let staged = staging.iter().any(|event| {
        matches!(event, RecorderEvent::Put { key: k, digest: d, size: s, .. }
            if k == key && d == digest && s == size)
    });
    if staged {
        return Ok(Judged::Staged);
    }
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap_or(ReadLen::CHUNK);
    let bytes = match block_on(remote.read(key, 0, all)) {
        Ok(bytes) => bytes,
        Err(lsr_core::StoreError::NotFound { .. }) => return Ok(Judged::Skipped),
        Err(err) => return Err(super::bug_fault(format!("reading {key:?}: {err}"))),
    };
    if content_digest(&bytes) != *digest {
        return Ok(Judged::Skipped);
    }
    Err(Violation::UnstagedPut {
        key: format!(
            "{} {}-{}",
            key.level().get(),
            key.range().min().get(),
            key.range().max().get()
        ),
    })
}
