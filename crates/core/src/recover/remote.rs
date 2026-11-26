//! The newest staged file read back from the replica, for a boot whose staging directory
//! holds no level-0 file (a meta directory wiped, lost or new).
//!
//! With no staged newest staged file there is nothing to compare the WAL with, so boot used
//! to take a first-sync snapshot. But the replica's newest level-0 file is the
//! newest staged file the staging directory last held: its header names the WAL range it came
//! from (offset, size, salts) and the commit size, and its pages are what the
//! WAL held there. [`NewestStagedFile::load_remote`] reads that file and runs it through
//! the same checks a staged newest file gets (the frame before its end, and the
//! whole-range compare), so `recover` continues without a snapshot
//! exactly when it would have with the file still staged.
//!
//! The read is one GET of the key the boot's listing named newest, from byte
//! 0 for up to [`NEWEST_STAGED_READ`] bytes. A level-0 file holds one transaction, so for
//! any file under 1 MiB this is the whole object (header, pages, page index and
//! trailer), and the decoder verifies its checksum on that read. The header and
//! the index alone would not do: they sit at the two ends of the file, and the
//! index names pages without their bytes, which the compare needs. A larger
//! file takes one more read per MiB, in the same bounded memory.
//!
//! There is no newest staged file, as before, when the key is gone (the replica's newest file
//! is above level 0, its level-0 files retained away) or its header names no
//! usable WAL origin. A file that does not decode, or whose header names
//! another range than its key, is a corrupt replica. Go reaches the same
//! position another way: `checkDatabaseBehindReplica` (`db.go`) downloads the
//! replica's newest level-0 file into the empty meta directory, and `verify`
//! then reads it as its local newest staged file.

use std::io::{Read, Seek};

use lsr_ltx::{HEADER_LEN, Header, Txid};

use super::{BootWal, NEWEST_STAGED_READ, NewestStagedFile, NewestStagedFileSource, Origin};
use crate::fault::Fault;
use crate::fault::role::{Replica, Role};
use crate::layout::FileKey;
use crate::store::{ObjectStore, StoreError};

impl NewestStagedFile {
    /// Reads the replica's level-0 file for `newest`, the replica's newest
    /// transaction, and checks it against `wal` as [`NewestStagedFile::load`] checks a
    /// staged newest staged file. `None` when the replica holds no such file or its header
    /// names no WAL origin: there is no position to continue from.
    ///
    /// # Errors
    /// [`Fault::RemoteCorrupt`] when the file does not decode and verify whole
    /// or its header names another range than its key; the replica's fault
    /// for any other store error; a failed WAL read.
    pub async fn load_remote<S: ObjectStore, R: Read + Seek>(
        remote: &S,
        wal: Option<BootWal<'_, R>>,
        newest: Txid,
    ) -> Result<Option<Self>, Fault> {
        Self::load_remote_in(&Replica(remote), wal, newest).await
    }

    /// [`Self::load_remote`] with each store as its role.
    pub(crate) async fn load_remote_in<S: Role, R: Read + Seek>(
        remote: &S,
        wal: Option<BootWal<'_, R>>,
        newest: Txid,
    ) -> Result<Option<Self>, Fault> {
        let key = FileKey::staged(newest);
        let len = NEWEST_STAGED_READ;
        let first = match remote.read(&key, 0, len).await {
            Ok(first) => first,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(S::fault(&err)),
        };
        let header = first
            .first_chunk::<HEADER_LEN>()
            .and_then(|raw| Header::decode(raw).ok())
            .filter(|header| header.txids() == key.range())
            .ok_or(Fault::RemoteCorrupt)?;
        let Some(origin) = Origin::of(&header) else {
            return Ok(None);
        };
        let file = NewestStagedFileSource {
            store: remote,
            key,
            fault: S::fault,
        };
        let view = Self::checked(&file, Some(first), &header, origin, wal).await?;
        view.map(Some).ok_or(Fault::RemoteCorrupt)
    }
}

#[cfg(test)]
mod tests;
