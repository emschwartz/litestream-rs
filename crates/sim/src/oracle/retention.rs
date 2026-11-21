//! Retention never deletes inside its window.
//!
//! Retention measures ages from an anchor that is never later than the
//! engine's clock, and the engine's clock is never later than the latest time
//! it was told, which the recorder notes with every delete. So a file whose
//! created-at time is within a window of that latest time is within the
//! window of the anchor too, and retention keeps it: a snapshot created less
//! than the snapshot retention before it (every snapshot from the first one
//! not created before `anchor - retention` is kept), and a level-0 file
//! created after `latest - l0 retention` (a level-0 file goes only once it is
//! that old). See the module doc of `crates/core/src/maintain/retain.rs`.
//!
//! The engine also deletes a file it put and could not trust (the store
//! reported the commit failed or the bytes wrong); such keys are passed in
//! and exempt.

use std::collections::BTreeMap;
use std::time::Duration;

use lsr_core::FileKey;
use lsr_ltx::Timestamp;

use super::Violation;
use crate::store::{RecorderEvent, RecorderLog};

/// How long retention keeps each kind of file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    /// How long a snapshot is kept.
    pub snapshots: Duration,
    /// How long a level-0 file is kept, when retention deletes level 0.
    pub level_zero: Option<Duration>,
}

fn before(high: Timestamp, window: Duration) -> Timestamp {
    let millis = i64::try_from(window.as_millis()).unwrap_or(i64::MAX);
    Timestamp::from_unix_millis(high.unix_millis().saturating_sub(millis))
}

/// Judges every delete the replica took. Returns how many deleted keys were
/// judged.
///
/// # Errors
/// [`Violation::RetentionTooEager`] for the first key deleted inside its
/// window.
pub fn deletes(
    remote: &RecorderLog,
    windows: Windows,
    exempt: &[FileKey],
) -> Result<u32, Violation> {
    let mut created: BTreeMap<FileKey, Timestamp> = BTreeMap::new();
    let mut judged = 0_u32;
    for event in remote.events() {
        match event {
            RecorderEvent::Put {
                key, created_at, ..
            } => {
                created.insert(key, created_at);
            }
            RecorderEvent::Delete { keys, high, .. } => {
                for key in keys {
                    let Some(created_at) = created.remove(&key) else {
                        continue;
                    };
                    if exempt.contains(&key) {
                        continue;
                    }
                    let kept_after = if key.level().is_snapshot() {
                        Some(before(high, windows.snapshots))
                    } else if key.level().get() == 0 {
                        windows.level_zero.map(|window| before(high, window))
                    } else {
                        None
                    };
                    if let Some(kept_after) = kept_after {
                        judged = judged.saturating_add(1);
                        if created_at > kept_after {
                            return Err(Violation::RetentionTooEager {
                                key,
                                created_at,
                                latest: high,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(judged)
}
