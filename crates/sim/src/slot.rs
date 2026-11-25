//! The staging directory's events as the checks read them, with a local course's slot folded in.
//!
//! A process that starts while the store is down stages from local state and keeps those
//! files apart, in the directory `ltx/8` of the meta directory, until a boot has listed the
//! replica; that boot moves each one up to level 0, or deletes it
//! (`docs/decisions/D220.md`). The checks that read the staging recorder ask what was staged
//! from the WAL and when, and which staged files were given up, and they read level 0. Read
//! raw, the slot misleads them three ways, which is what the rotating seeds of 2026-11-25
//! showed (4 of 930 failed): a file staged in the slot is never judged or noted; its move up
//! is taken for a staging from the WAL as it stands at the move, which may have been damaged
//! or regrown since; and a slot deleted whole gives up restore points nobody counts.
//!
//! [`as_staged`] rewrites the events one for one, so an index into the recorder's events is
//! an index into these:
//!
//! - A put in the slot is the staging of that file: it becomes a put at its level-0 key.
//! - A level-0 put of the bytes the slot holds for the same transactions is the move up,
//!   which stages nothing: it becomes a delete of no keys.
//! - A delete in the slot of a file that moved up ends the move, and is dropped. Of a file
//!   that did not, it is the give-up of that staged file: it becomes its level-0 key.
//!
//! Where the slot is on disk is stated here and not taken from the engine.

use std::collections::BTreeMap;

use lsr_core::{FileKey, Level};
use lsr_ltx::Txid;

use crate::store::RecorderEvent;

/// The staging level of the slot: the directory `ltx/8`.
pub const LEVEL: u8 = 8;

/// What the slot holds for one range of transactions.
#[derive(Clone, Copy, Debug)]
struct Held {
    /// The digest of the bytes put there.
    digest: u64,
    /// Whether level 0 has taken the same bytes since.
    moved: bool,
}

/// What became of the slot's files over a run of events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Files staged in the slot.
    pub staged: u64,
    /// Files moved up to level 0.
    pub moved_up: u64,
    /// Files deleted from the slot that never moved up.
    pub given_up: u64,
}

/// The recorder's `raw` staging events with the slot folded in (see the module docs), and
/// what became of the slot's files in the events from `from` on.
#[must_use]
pub fn as_staged(raw: &[RecorderEvent], from: usize) -> (Vec<RecorderEvent>, Counts) {
    let mut held: BTreeMap<(Txid, Txid), Held> = BTreeMap::new();
    let mut counts = Counts::default();
    let mut out = Vec::with_capacity(raw.len());
    for (index, event) in raw.iter().enumerate() {
        let mut seen = Counts::default();
        out.push(fold(event, &mut held, &mut seen));
        if index >= from {
            counts.staged = counts.staged.saturating_add(seen.staged);
            counts.moved_up = counts.moved_up.saturating_add(seen.moved_up);
            counts.given_up = counts.given_up.saturating_add(seen.given_up);
        }
    }
    (out, counts)
}

fn span(key: &FileKey) -> (Txid, Txid) {
    (key.range().min(), key.range().max())
}

fn level_zero(key: &FileKey) -> FileKey {
    FileKey::new(Level::ZERO, key.range())
}

fn fold(
    event: &RecorderEvent,
    held: &mut BTreeMap<(Txid, Txid), Held>,
    seen: &mut Counts,
) -> RecorderEvent {
    match event {
        RecorderEvent::Put { key, digest, .. } if key.level().get() == LEVEL => {
            held.insert(
                span(key),
                Held {
                    digest: *digest,
                    moved: false,
                },
            );
            seen.staged = 1;
            let mut staged = event.clone();
            if let RecorderEvent::Put { key, .. } = &mut staged {
                *key = level_zero(key);
            }
            staged
        }
        RecorderEvent::Put {
            key,
            digest,
            at,
            high,
            ..
        } if key.level().get() == 0 => match held.get_mut(&span(key)) {
            Some(slot) if slot.digest == *digest => {
                slot.moved = true;
                seen.moved_up = 1;
                RecorderEvent::Delete {
                    keys: Vec::new(),
                    at: *at,
                    high: *high,
                }
            }
            Some(_) | None => event.clone(),
        },
        RecorderEvent::Put { .. } => event.clone(),
        RecorderEvent::Delete { keys, at, high } => {
            let keys = keys
                .iter()
                .filter_map(|key| {
                    if key.level().get() != LEVEL {
                        return Some(*key);
                    }
                    match held.remove(&span(key)) {
                        Some(slot) if slot.moved => None,
                        Some(_) | None => {
                            seen.given_up = seen.given_up.saturating_add(1);
                            Some(level_zero(key))
                        }
                    }
                })
                .collect();
            RecorderEvent::Delete {
                keys,
                at: *at,
                high: *high,
            }
        }
    }
}

#[cfg(test)]
mod tests;
