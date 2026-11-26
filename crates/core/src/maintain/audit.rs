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
//! The cost is one pass over each newest file, once per boot and never in steady state. It
//! goes a slice a tick ([`ReadingBack`]): at most a tick's bytes are read before the tick
//! ends, and sync and upload take their turns between slices, as for a compaction. A level-9
//! snapshot is the whole database, and read in one tick it held the engine for minutes.
//! Each file is one stream held between ticks where the store can hold one
//! ([`crate::store::ObjectStore::hold_stream`]), so a file costs one GET however many ticks it spans.

use std::collections::VecDeque;
use std::convert::Infallible;

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::{LtxRead, Next, ReadFault};
use crate::plan::{KeyTarget, plan_without};
use crate::store::{ReadLen, ReadStream};

/// The bytes one read of the read-back asks for: a slice ends at most this far past its bytes.
const READ: ReadLen = ReadLen::or_chunk(256 * 1024);

/// What a slice of the read-back found.
#[derive(Debug)]
pub(crate) enum ReadBack {
    /// Every newest file verifies and names its key.
    Sound,
    /// This file does not.
    Rotten {
        /// The file.
        key: FileKey,
        /// Whether [`restores_without`] held for it in the listing the read-back began on.
        covered: bool,
    },
    /// The tick's bytes are spent and more is left: the next slice reads on.
    Reading(Box<ReadingBack>),
}

/// The read-back part-way: the file being read, the files after it, each with whether
/// [`restores_without`] held for it in the listing the read-back began on, and the bytes read.
pub(crate) struct ReadingBack {
    file: (FileKey, bool),
    rest: VecDeque<(FileKey, bool)>,
    read: LtxRead,
    held: Option<Box<dyn ReadStream>>,
    bytes: u64,
}

impl std::fmt::Debug for ReadingBack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadingBack")
            .field("file", &self.file)
            .field("rest", &self.rest)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
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

/// Begins the read-back of the newest file of each level above 0 in `inventory` and reads its
/// first slice of at most `budget` bytes (see [`ReadingBack::slice`]).
///
/// # Errors
/// The replica's fault when a read fails; nothing was judged then.
pub(crate) async fn read_back<R: Role>(
    remote: &R,
    inventory: &Inventory,
    budget: u64,
) -> Result<ReadBack, Fault> {
    let mut files = newest_per_level(inventory)
        .map(|info| (info.key, restores_without(inventory, &info.key)))
        .collect::<VecDeque<_>>();
    let Some(file) = files.pop_front() else {
        return Ok(ReadBack::Sound);
    };
    let reading = ReadingBack {
        file,
        rest: files,
        read: LtxRead::new(file.0, READ),
        held: None,
        bytes: 0,
    };
    Box::new(reading)
        .opened(remote)
        .await?
        .slice(remote, budget)
        .await
}

impl ReadingBack {
    /// Holds the file's one stream from its first byte, where the store can hold one.
    async fn opened<R: Role>(mut self: Box<Self>, remote: &R) -> Result<Box<Self>, Fault> {
        self.held = remote
            .hold_stream(self.file.0, 0)
            .await
            .map_err(|err| R::fault(&err))?;
        Ok(self)
    }

    /// Reads on until `budget` bytes were read this slice, the last read ending at most [`READ`]
    /// past them, and stops at the first file that does not verify or name its key.
    ///
    /// # Errors
    /// The replica's fault when a read fails; nothing was judged then.
    pub(crate) async fn slice<R: Role>(
        mut self: Box<Self>,
        remote: &R,
        budget: u64,
    ) -> Result<ReadBack, Fault> {
        let start = self.bytes;
        let mut slot: Option<Box<dyn ReadStream + '_>> = None;
        while self.bytes.saturating_sub(start) < budget {
            let (key, covered) = self.file;
            let before = self.read.bytes_read();
            let stream: &mut (dyn ReadStream + '_) = match self.held.as_mut() {
                Some(held) => &mut **held,
                None => &mut **slot.get_or_insert_with(|| remote.read_stream(key, before)),
            };
            let next = self.read.next(stream, |_| Ok::<(), Infallible>(())).await;
            let read = self.read.bytes_read().saturating_sub(before);
            self.bytes = self.bytes.saturating_add(read);
            let verified = match next {
                Ok(Next::Event(())) => continue,
                Ok(Next::End(verified)) => verified,
                Err(ReadFault::Store(err)) => return Err(R::fault(&err)),
                Err(ReadFault::Decode(_)) => return Ok(ReadBack::Rotten { key, covered }),
                Err(ReadFault::Rejected(never)) => match never {},
            };
            if verified.header().txids() != key.range() {
                return Ok(ReadBack::Rotten { key, covered });
            }
            let Some(file) = self.rest.pop_front() else {
                return Ok(ReadBack::Sound);
            };
            self.file = file;
            self.read = LtxRead::new(file.0, READ);
            slot = None;
            self = self.opened(remote).await?;
        }
        Ok(ReadBack::Reading(self))
    }
}
