//! The read-back after a boot: is the newest file of each level what its key
//! says it is?
//!
//! A compaction output or a snapshot that the store acknowledged with wrong
//! bytes is deleted at once, and remembered as distrusted when that delete
//! fails (`state`). Neither survives the process: a crash between the publish
//! and the delete leaves an object the listing shows as cover, that no restore
//! can decode. Level 0 has its own read-back in the uploader (`judge_tail`);
//! this is the same check for every level above it.
//!
//! Only the newest file of a level is read. An older one was either written by
//! a commit this engine saw succeed, or was the newest at an earlier boot and
//! read then. The newest file per level is also all the window can hold: one
//! job runs at a time, and a level's next job starts above its newest file.
//!
//! One read that fails verification is evidence about the read, not the
//! object: the actor reads the file again on a later maintenance tick and acts
//! only when that read fails too.
//!
//! A file that does not verify is deleted only when [`restores_without`] says
//! the replica loses nothing by it: a restore to the file's last transaction
//! still plans from the other files. Compaction then builds the level again
//! from the same inputs, and a missing snapshot is due at once. When nothing
//! else reaches that transaction the file stays where it is and the read-back
//! fails with `RemoteCorrupt`, as any corrupt object does (a persistent fault:
//! the configured response keeps maintenance degraded or stops the engine).
//!
//! The cost is one pass over each newest file in [`ReadLen`]-sized ranged
//! reads, once per boot and never in steady state.

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::{KeyTarget, plan_without};
use crate::upload::remote_verifies;

/// What the read-back found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadBack {
    /// Every newest file verifies and names its key.
    Sound,
    /// This file does not.
    Rotten {
        /// The file.
        key: FileKey,
        /// Whether [`restores_without`] holds for it.
        covered: bool,
    },
}

/// The newest file of each level above 0 that holds one, lowest level first.
pub(crate) fn newest_per_level(inventory: &Inventory) -> impl Iterator<Item = &FileInfo> {
    Level::ALL
        .iter()
        .skip(1)
        .filter_map(|level| inventory.level(*level).last())
}

/// Whether a restore to `key`'s last transaction still plans from the
/// listing's other files, so deleting `key` shortens no history.
#[must_use]
pub(crate) fn restores_without(inventory: &Inventory, key: &FileKey) -> bool {
    plan_without(
        inventory,
        std::slice::from_ref(key),
        KeyTarget::Txid(key.range().max()),
    )
    .chain()
    .is_some()
}

/// Reads back the newest file of each level above 0 and stops at the first
/// that does not verify.
///
/// # Errors
/// The replica's fault when a read fails; nothing was judged then.
pub(crate) async fn read_back<R: Role>(
    remote: &R,
    inventory: &Inventory,
) -> Result<ReadBack, Fault> {
    for info in newest_per_level(inventory) {
        if !remote_verifies(remote, &info.key).await? {
            return Ok(ReadBack::Rotten {
                key: info.key,
                covered: restores_without(inventory, &info.key),
            });
        }
    }
    Ok(ReadBack::Sound)
}
