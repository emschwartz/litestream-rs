//! The read-back after a boot: is the newest file of each level what its key
//! says it is?
//!
//! A compaction output or a snapshot that the store acknowledged at a wrong size
//! is deleted at once, and remembered as distrusted when that delete fails
//! (`state`). Neither survives the process: a crash between the publish and
//! the delete leaves an object the listing shows as cover, that no restore can
//! decode. Level 0 has its own read-back in the uploader (`judge_tail`); this
//! is the same check for every level above it.
//!
//! The actor owes it again after a store refused an output or a snapshot against
//! its checksum (`docs/decisions/D280.md`): that refusal deletes nothing, since the
//! key may hold an earlier attempt's sound file, so the read-back is what removes
//! one a store took cut short or with a bad tail before it refused. A body damaged
//! behind a sound header and tail is not looked for here either (D272): its first
//! whole read refuses it.
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
//! The read is of each newest file's header and tail (the page index's terminator, its size
//! field and the trailer): two ranged reads of about 125 bytes a file, never its body. The
//! header must decode and name the file's key; the tail must end where the listing says the
//! object ends, with a zero terminator, an index that fits between the header and the
//! tail, and a trailer that decodes against the header. An object cut short, or one that
//! holds another file, fails. A body with wrong bytes behind a sound header and tail is not
//! looked for: a signed upload (S3's SHA-256, a part's `Content-MD5`) has the store refuse
//! one, and reading every newest file whole cost about 16 GB of egress a restart at full size
//! (the owner's decision, `docs/decisions/D272.md`, amending D6's reasoning).

use lsr_ltx::{HEADER_LEN, Header, TRAILER_LEN, Trailer};

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::{KeyTarget, plan_without};
use crate::store::ReadLen;

/// The bytes read of a file's start: its header.
const HEAD: ReadLen = ReadLen::or_chunk(100);

/// The bytes read of a file's end: the page index's zero terminator, the index size field and
/// the trailer.
const TAIL: ReadLen = ReadLen::or_chunk(25);

/// What the read-back found.
#[derive(Debug)]
pub(crate) enum ReadBack {
    /// Every newest file's header and tail are sound and name its key.
    Sound,
    /// This file's are not.
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

/// Reads the header and tail of the newest file of each level above 0 and stops at the first
/// that is not sound.
///
/// # Errors
/// The replica's fault when a read fails; nothing was judged then.
pub(crate) async fn read_back<R: Role>(
    remote: &R,
    inventory: &Inventory,
) -> Result<ReadBack, Fault> {
    for info in newest_per_level(inventory) {
        if !ends_sound(remote, info).await? {
            return Ok(ReadBack::Rotten {
                key: info.key,
                covered: restores_without(inventory, &info.key),
            });
        }
    }
    Ok(ReadBack::Sound)
}

/// Whether `info`'s object begins with a header naming its key and ends, at the listed size,
/// with a page index terminator, an index size that fits the file and a trailer that decodes
/// against the header.
///
/// # Errors
/// The replica's fault when a read fails.
async fn ends_sound<R: Role>(remote: &R, info: &FileInfo) -> Result<bool, Fault> {
    let head = remote
        .read(&info.key, 0, HEAD)
        .await
        .map_err(|err| R::fault(&err))?;
    let Some(header) = <&[u8; HEADER_LEN]>::try_from(head.as_slice())
        .ok()
        .and_then(|head| Header::decode(head).ok())
        .filter(|header| header.txids() == info.key.range())
    else {
        return Ok(false);
    };
    let Some(at) = info
        .size
        .checked_sub(TAIL.get())
        .filter(|at| *at >= HEAD.get())
    else {
        return Ok(false);
    };
    let tail = remote
        .read(&info.key, at, TAIL)
        .await
        .map_err(|err| R::fault(&err))?;
    let Some((terminator, rest)) = tail.split_first() else {
        return Ok(false);
    };
    let Some((size, trailer)) = rest.split_first_chunk::<8>() else {
        return Ok(false);
    };
    let Ok(trailer) = <&[u8; TRAILER_LEN]>::try_from(trailer) else {
        return Ok(false);
    };
    // The index ends at its terminator and starts after the header.
    let index = u64::from_be_bytes(*size);
    let fits = index
        .checked_add(HEAD.get())
        .is_some_and(|end| end <= at.saturating_add(1));
    Ok(*terminator == 0 && index > 0 && fits && Trailer::decode(trailer, &header).is_ok())
}

#[cfg(test)]
mod tests;
