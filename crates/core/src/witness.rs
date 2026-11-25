//! Witnessed staged files: a staged file reaches the replica only with a
//! witness bound to its bytes.
//!
//! A staging directory can be put back from a backup, or a file in it
//! replaced, and a self-consistent LTX file carries nothing that shows it is
//! old: its name, header and checksum are all as valid as they were the day it
//! was written. So the uploader trusts no name. It takes a [`Witnessed`]
//! token, held in memory and never persisted, that carries the file's key,
//! its length and its trailer's file checksum, and it compares the token with
//! the bytes it reads for the upload, on that same read (`crate::upload`). A
//! file replaced after it was witnessed, at boot or while running, is refused
//! before anything becomes visible.
//!
//! A token is minted in exactly two places, both in this crate:
//! - **Made here.** [`crate::sync`] staged the file: this process wrote those
//!   bytes, and the token comes from the encoder's own trailer.
//! - **Read at boot.** [`check_pending`] reads every pending staged file
//!   (above the replica's newest transaction) whole, verifies it, and mints
//!   its token from that read, as Go uploads the files it finds in its local
//!   directory (`docs/decisions/D175.md`). A file keeps the header time it was
//!   staged with, so the age of the oldest commit not yet on the replica
//!   survives a restart.
//!
//! The boot pass checks more than Go does: each file must start in the WAL
//! where the one before it ended (same salts, next offset; for the first
//! pending file, the replica's newest file), or at the first frame (a new WAL
//! generation or a snapshot). The first file that fails is
//! refused with every file after it. It does not compare the files with the
//! WAL: recovery compares the newest staged file with it, and the live WAL is
//! not the proof that a file's bytes are the commits' bytes.

use std::collections::BTreeMap;

use lsr_ltx::{Decoder, FileChecksum, Header, Timestamp, Txid, TxidRange};
use lsr_wal::{Salts, WalOffset};

use crate::fault::role::Role;
use crate::fault::{BugSite, Fault};
use crate::layout::{FileInfo, FileKey, Level};
use crate::store::{ObjectStore, ReadLen, StoreError};
use crate::upload::staged_from;

/// The most tokens held for pending staged files; at about 40 bytes a token
/// the cap bounds the memory. A file staged without a token would be refused
/// at upload and replaced by a snapshot, so nothing is staged past it: the
/// engine holds its position while [`Witnesses::has_room`] says no (the WAL
/// keeps the frames), and the boot pass witnesses no more than this many.
pub(crate) const MAX_WITNESSED: usize = 4096;

/// How much of a file one store read asks for during the boot pass.
const READ: ReadLen = ReadLen::CHUNK;

/// The length of a byte stream and a digest of its raw bytes (FNV-1a 64), kept
/// as the stream passes. The file checksum covers an LTX file's decompressed
/// content, not its LZ4 framing, so two files can verify to the same checksum
/// and still differ in bytes; this tells them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ByteTally {
    size: u64,
    digest: u64,
}

impl ByteTally {
    /// No bytes yet.
    pub(crate) const fn new() -> Self {
        Self {
            size: 0,
            digest: 0xcbf2_9ce4_8422_2325,
        }
    }

    /// Takes in the next bytes of the stream.
    pub(crate) fn add(&mut self, bytes: &[u8]) {
        self.size = self
            .size
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        self.digest = bytes.iter().fold(self.digest, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
        });
    }
}

/// Proof that a staged file's bytes were made by this process or matched the
/// live WAL at boot: its key, its bytes (length and a digest of them) and its
/// trailer's file checksum.
///
/// The fields are private and the constructor is this crate's, so a value
/// exists only where a witness was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Witnessed {
    key: FileKey,
    bytes: ByteTally,
    checksum: FileChecksum,
    /// The file's header timestamp: when it was staged, the clock fail-stop counts the oldest
    /// pending commit from (`crate::failstop`).
    stamp: Timestamp,
}

impl Witnessed {
    /// A token for a file this process just wrote: `bytes` tallied as it
    /// wrote them, `checksum` the encoder's trailer checksum.
    pub(crate) const fn made_here(
        key: FileKey,
        bytes: ByteTally,
        checksum: FileChecksum,
        stamp: Timestamp,
    ) -> Self {
        Self {
            key,
            bytes,
            checksum,
            stamp,
        }
    }

    /// The file's header timestamp.
    #[must_use]
    pub const fn stamp(&self) -> Timestamp {
        self.stamp
    }

    /// The staged file it vouches for.
    #[must_use]
    pub const fn key(&self) -> FileKey {
        self.key
    }

    /// Whether a read of the file that verified whole, tallied as `bytes` and
    /// ending in `checksum`, read the bytes this token was minted for.
    pub(crate) fn matches(&self, bytes: ByteTally, checksum: FileChecksum) -> bool {
        self.bytes == bytes && self.checksum == checksum
    }

    /// A token for a staged file the boot read whole and verified.
    fn of_read(key: FileKey, file: &Decoded) -> Self {
        Self {
            key,
            bytes: file.bytes,
            checksum: file.checksum,
            stamp: file.header.fields().timestamp,
        }
    }

    /// A token for the staged file `bytes`, decoded here. Tests only: it
    /// takes no witness.
    #[cfg(test)]
    pub(crate) fn of_bytes(key: FileKey, bytes: &[u8]) -> Option<Self> {
        let mut decoder = lsr_ltx::Decoder::new();
        let mut rest = bytes;
        while !rest.is_empty() {
            let (used, _) = decoder.feed(rest).ok()?;
            rest = rest.get(used..)?;
        }
        let verified = decoder.finish().ok()?;
        let mut tally = ByteTally::new();
        tally.add(bytes);
        Some(Self {
            key,
            bytes: tally,
            checksum: verified.trailer().file_checksum(),
            stamp: verified.header().fields().timestamp,
        })
    }
}

/// The tokens of the pending staged files, by transaction. At most
/// [`MAX_WITNESSED`]; tokens at or below the replica's confirmed transaction
/// are let go, since nothing uploads those files again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Witnesses {
    by_txid: BTreeMap<Txid, Witnessed>,
}

impl Witnesses {
    /// No tokens.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            by_txid: BTreeMap::new(),
        }
    }

    /// Keeps `token`, replacing one for the same file. Every token is kept:
    /// the stagers ask [`Self::has_room`] first, so the count stays within
    /// [`MAX_WITNESSED`].
    pub fn insert(&mut self, token: Witnessed) {
        self.by_txid.insert(token.key.range().max(), token);
        debug_assert!(
            self.by_txid.len() <= MAX_WITNESSED,
            "a file was staged with no room for its token"
        );
    }

    /// Whether `files` more staged files can each get a token.
    #[must_use]
    pub fn has_room(&self, files: usize) -> bool {
        self.by_txid.len().saturating_add(files) <= MAX_WITNESSED
    }

    pub(crate) fn get(&self, key: &FileKey) -> Option<&Witnessed> {
        self.by_txid
            .get(&key.range().max())
            .filter(|token| token.key == *key)
    }

    /// Lets go of every token through `confirmed`.
    pub(crate) fn confirmed(&mut self, confirmed: Option<Txid>) {
        if let Some(confirmed) = confirmed {
            self.by_txid.retain(|txid, _| *txid > confirmed);
        }
    }

    /// Lets go of every token from `from` on: those files were deleted.
    pub(crate) fn drop_from(&mut self, from: Txid) {
        self.by_txid.retain(|txid, _| *txid < from);
    }

    /// The stamp of the oldest pending staged file not in `but`, if any is held.
    #[must_use]
    pub fn oldest_stamp_but(&self, but: &std::collections::BTreeSet<Txid>) -> Option<Timestamp> {
        self.by_txid
            .iter()
            .find(|(txid, _)| !but.contains(*txid))
            .map(|(_, token)| token.stamp())
    }

    /// How many tokens are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_txid.len()
    }

    /// Whether none is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_txid.is_empty()
    }

    /// Tokens for every staged level-0 file in `staging`, decoded here.
    /// Tests only: it takes no witness.
    #[cfg(test)]
    pub(crate) fn trust_all<S: ObjectStore>(staging: &S) -> Self {
        let mut all = Self::new();
        let files = crate::conformance::block_on(staged_from(
            &crate::fault::role::Staging(staging),
            None,
            usize::MAX,
        ))
        .unwrap_or_default();
        for info in files {
            let bytes = crate::conformance::block_on(staging.read(
                &info.key,
                0,
                ReadLen::new(ReadLen::MAX_RAW).unwrap_or(ReadLen::CHUNK),
            ))
            .unwrap_or_default();
            if let Some(token) = Witnessed::of_bytes(info.key, &bytes) {
                all.insert(token);
            }
        }
        all
    }
}

impl FromIterator<Witnessed> for Witnesses {
    fn from_iter<I: IntoIterator<Item = Witnessed>>(tokens: I) -> Self {
        let mut all = Self::new();
        for token in tokens {
            all.insert(token);
        }
        all
    }
}

/// What the boot pass found among the pending staged files.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Pending {
    /// Tokens for the files that read whole and chain, oldest first: a run
    /// from the first pending transaction with no gap.
    pub(crate) tokens: Vec<Witnessed>,
    /// The first pending file that failed: it and every one after it must go.
    pub(crate) refused: Option<Txid>,
}

/// A whole file, verified on this read: its header, its bytes and its trailer
/// checksum.
struct Decoded {
    header: Header,
    bytes: ByteTally,
    checksum: FileChecksum,
}

/// Reads the staged level-0 files above `confirmed`, the replica's newest
/// transaction, and mints a token for each from the read (see the module
/// docs). The first file must follow the replica's newest file, read from
/// staging when it is still there, else from the replica; when that file does
/// not decode, only a file at the first frame can start the run.
///
/// # Errors
/// A store read that failed; that says nothing about the bytes.
pub(crate) async fn check_pending<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    confirmed: Option<Txid>,
) -> Result<Pending, Fault> {
    let mut expect = confirmed.map_or(Some(Txid::MIN), Txid::next);
    let pending: Vec<FileInfo> = match expect {
        // One past the cap: the first file no token could be minted for.
        Some(from) => staged_from(staging, Some(from), MAX_WITNESSED.saturating_add(1)).await?,
        None => Vec::new(),
    };
    let mut tokens = Vec::new();
    if pending.is_empty() {
        return Ok(Pending::default());
    }
    let mut prev = match confirmed {
        Some(tail) => tail_link(staging, remote, tail).await?,
        None => None,
    };
    for info in &pending {
        // Past the cap a file could get no token: it goes as refused files do,
        // and the WAL stages it again once uploads make room.
        let wanted = expect.map(|txid| FileKey::new(Level::ZERO, TxidRange::single(txid)));
        let read = if tokens.len() < MAX_WITNESSED && Some(info.key) == wanted {
            decode(staging, info.key, L::fault).await?
        } else {
            None
        };
        let chained = read.and_then(|file| {
            let link = link_of(&file.header)?;
            let first_frame = WalOffset::start(file.header.page_size()).get();
            // A file starts where the one before it ended, or starts a new
            // generation or a snapshot at the first frame.
            let starts = link.start == first_frame
                || prev.is_some_and(|prev| prev.salts == link.salts && prev.end == link.start);
            starts.then_some((file, link))
        });
        let Some((file, next)) = chained else {
            return Ok(Pending {
                tokens,
                refused: Some(info.key.range().min()),
            });
        };
        tokens.push(Witnessed::of_read(info.key, &file));
        prev = Some(next);
        expect = info.key.range().max().next();
    }
    Ok(Pending {
        tokens,
        refused: None,
    })
}

/// Where the replica's newest file sits in its WAL, when it reads whole.
async fn tail_link<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    tail: Txid,
) -> Result<Option<Link>, Fault> {
    let key = FileKey::new(Level::ZERO, TxidRange::single(tail));
    let file = match decode(staging, key, L::fault).await? {
        Some(file) => Some(file),
        None => decode(remote, key, S::fault).await?,
    };
    Ok(file.and_then(|file| link_of(&file.header)))
}

/// Where a file sits in its WAL: its generation and the offsets it spans.
#[derive(Clone, Copy)]
struct Link {
    salts: Salts,
    start: u64,
    end: u64,
}

/// The file's generation and WAL range, when its header names a range that
/// ends on a frame boundary.
fn link_of(header: &Header) -> Option<Link> {
    let fields = header.fields();
    let origin = fields.wal?;
    let (salt1, salt2) = origin.salts();
    let end = WalOffset::new(
        origin.offset().checked_add(origin.size())?,
        fields.page_size,
    )?;
    Some(Link {
        salts: Salts { salt1, salt2 },
        start: origin.offset(),
        end: end.get(),
    })
}

/// Whether the staging store still holds, under the token's key, exactly the
/// bytes the token was minted for: read whole, verified, same length, digest
/// and trailer checksum.
///
/// # Errors
/// The staging store's fault when a read fails.
pub(crate) async fn still_holds<L: Role>(staging: &L, token: &Witnessed) -> Result<bool, Fault> {
    let decoded = decode(staging, token.key(), L::fault).await?;
    Ok(decoded.is_some_and(|file| token.matches(file.bytes, file.checksum)))
}

/// Reads `key` whole through the decoder, tallying its raw bytes; `None` when
/// it is missing or does not verify and name exactly `key`.
async fn decode<S: ObjectStore>(
    store: &S,
    key: FileKey,
    fault: fn(&StoreError) -> Fault,
) -> Result<Option<Decoded>, Fault> {
    let mut decoder = Decoder::new();
    let mut bytes = ByteTally::new();
    let mut offset = 0_u64;
    loop {
        let chunk = match store.read(&key, offset, READ).await {
            Ok(chunk) => chunk,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(fault(&err)),
        };
        bytes.add(&chunk);
        let mut rest = chunk.as_slice();
        while !rest.is_empty() {
            let Ok((used, _)) = decoder.feed(rest) else {
                return Ok(None);
            };
            crate::invariant!(used != 0, BugSite::DecoderStalled);
            rest = rest.get(used..).unwrap_or_default();
        }
        let read = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        offset = offset.saturating_add(read);
        if read < READ.get() {
            break;
        }
    }
    match decoder.finish() {
        Ok(verified) if verified.header().txids() == key.range() => Ok(Some(Decoded {
            header: *verified.header(),
            bytes,
            checksum: verified.trailer().file_checksum(),
        })),
        Ok(_) | Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests;
