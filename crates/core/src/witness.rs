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
//! - **Read at boot.** [`scan`] reads every pending staged file
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
//! refused with every file after it, once the next boot's pass finds the same
//! file again: one read never condemns a file.
//!
//! That pass is the only read a boot makes of a pending file
//! (`docs/decisions/D219.md`). As the decoder yields each page, the same read
//! compares it with what the live WAL holds in the file's range (recovery then
//! judges the newest staged file from that, `crate::recover`) and records it
//! for the page checksum table (`crate::pages`). Nothing is taken from a file
//! that did not verify whole on that read. The live WAL is not the proof that
//! a file's bytes are the commits' bytes: the token is.
//!
//! A token outlives its upload in [`Sent`]: once the replica has acknowledged this
//! process's put of a file, the token says what the replica holds under that key, and the
//! level-1 compaction that reads the staged copy compares it with the token where it once
//! read the replica's copy back (`crate::maintain`, `docs/decisions/D236.md`).

use std::collections::BTreeMap;
use std::io::{Read, Seek};

use lsr_ltx::{Decoder, Event, FileChecksum, Header, PageChecksum, PageNo, Timestamp, Txid};
use lsr_wal::{Salts, WalOffset};

use crate::fault::role::Role;
use crate::fault::{BugSite, Fault};
use crate::layout::FileKey;
use crate::pages::Seeding;
use crate::recover::{BootWal, Compare, NewestStagedFile, Origin, Scanned};
use crate::store::{ObjectStore, ReadLen, StoreError};
use crate::upload::StagedPages;

/// The most tokens held for pending staged files; at about 40 bytes a token
/// the cap bounds the memory. A file staged without a token would be refused
/// at upload and replaced by a snapshot, so nothing is staged past it: the
/// engine holds its position while [`Witnesses::has_room`] says no (the WAL
/// keeps the frames), and the boot pass witnesses no more than this many.
pub(crate) const MAX_WITNESSED: usize = 4096;

/// How much of a file one store read asks for during the boot pass.
const READ: ReadLen = ReadLen::CHUNK;

/// The length of a byte stream and a digest of its raw bytes, kept as the stream passes.
/// The file checksum covers an LTX file's decompressed content, not its LZ4 framing, so two
/// files can verify to the same checksum and still differ in bytes; this tells them apart.
///
/// The digest reads the stream as little-endian words dealt round-robin to four independent
/// multiply-and-rotate lanes, so a core runs about eight bytes a cycle where a byte-at-a-time
/// FNV-1a runs at its multiply latency, one byte in four or five cycles (measured at
/// 0.75 GB/s against more than 5 GB/s, `docs/decisions/D210.md`). The words are laid out by
/// position in the stream, so any split of the same bytes into `add` calls gives the same
/// digest; up to 31 bytes wait in `pending` for the next call. It is an equality check
/// between two reads of one file, not a defence against an adversary, and it is never stored.
///
/// It is public for one use outside the engine: a store that lets go of bytes it was handed
/// keeps a tally of them, and [`crate::Reread`] gives the bytes back only when a second read
/// of them tallies the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteTally {
    size: u64,
    lanes: [u64; LANES],
    pending: [u8; BLOCK],
}

/// Independent hash lanes; one block is a word for each.
const LANES: usize = 4;
/// Bytes of one word.
const WORD: usize = 8;
/// Bytes one round consumes.
const BLOCK: usize = LANES * WORD;
/// The stream length's low bits that count the bytes held back: a block is 32 bytes.
const HELD_MASK: u64 = 0x1f;
const _: () = assert!(BLOCK == 32);
/// The lanes' start values: the first digits of pi, as in several well-known hashes.
const SEEDS: [u64; LANES] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];
/// An odd multiplier with well-spread bits.
const MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;

impl Default for ByteTally {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteTally {
    /// No bytes yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            size: 0,
            lanes: SEEDS,
            pending: [0; BLOCK],
        }
    }

    /// The bytes held back since the last full block: the stream's length modulo a block.
    fn held(&self) -> usize {
        usize::try_from(self.size & HELD_MASK).unwrap_or(0)
    }

    /// How many bytes were taken in.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Takes in the next bytes of the stream.
    pub fn add(&mut self, bytes: &[u8]) {
        let held = self.held();
        self.size = self
            .size
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let mut rest = bytes;
        if held != 0 {
            let room = BLOCK.saturating_sub(held);
            let (fill, after) = rest.split_at(room.min(rest.len()));
            for (slot, byte) in self.pending.iter_mut().skip(held).zip(fill) {
                *slot = *byte;
            }
            if fill.len() < room {
                return;
            }
            self.lanes = round(self.lanes, &self.pending);
            self.pending = [0; BLOCK];
            rest = after;
        }
        let (blocks, tail) = rest.as_chunks::<BLOCK>();
        for block in blocks {
            self.lanes = round(self.lanes, block);
        }
        for (slot, byte) in self.pending.iter_mut().zip(tail) {
            *slot = *byte;
        }
    }

    /// The length and the digest, with the held bytes and the length folded in: what two reads
    /// of one file are compared by, in eight bytes a token instead of the lanes and the buffer.
    pub(crate) fn seal(&self) -> Sealed {
        let mut digest = self.size;
        for lane in self.lanes {
            digest = (digest ^ lane).wrapping_mul(MULTIPLIER).rotate_left(29);
        }
        for byte in self.pending.iter().take(self.held()) {
            digest = (digest ^ u64::from(*byte))
                .wrapping_mul(MULTIPLIER)
                .rotate_left(29);
        }
        Sealed {
            size: self.size,
            digest,
        }
    }
}

/// One round: a word into each lane.
fn round(mut lanes: [u64; LANES], block: &[u8; BLOCK]) -> [u64; LANES] {
    let (words, _) = block.as_chunks::<WORD>();
    for (lane, word) in lanes.iter_mut().zip(words) {
        *lane = (*lane ^ u64::from_le_bytes(*word))
            .wrapping_mul(MULTIPLIER)
            .rotate_left(31);
    }
    lanes
}

/// A [`ByteTally`] as the token keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sealed {
    size: u64,
    digest: u64,
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
    bytes: Sealed,
    checksum: FileChecksum,
    /// The file's header timestamp: when it was staged, the clock fail-stop counts the oldest
    /// pending commit from (`crate::failstop`).
    stamp: Timestamp,
}

impl Witnessed {
    /// A token for a file this process just wrote: `bytes` tallied as it
    /// wrote them, `checksum` the encoder's trailer checksum.
    pub(crate) fn made_here(
        key: FileKey,
        bytes: ByteTally,
        checksum: FileChecksum,
        stamp: Timestamp,
    ) -> Self {
        Self {
            key,
            bytes: bytes.seal(),
            checksum,
            stamp,
        }
    }

    /// The file's header timestamp.
    #[must_use]
    pub const fn stamp(&self) -> Timestamp {
        self.stamp
    }

    /// The staged file's length in bytes, as it was when witnessed.
    #[must_use]
    pub(crate) const fn len(&self) -> u64 {
        self.bytes.size
    }

    /// The staged file it vouches for.
    #[must_use]
    pub const fn key(&self) -> FileKey {
        self.key
    }

    /// Whether a file `size` bytes long whose last bytes are `trailer` ends as the file this
    /// token was minted for does: the same length, and the same file checksum, which a
    /// trailer holds in its last eight bytes and which covers the file's whole content.
    pub(crate) fn ends_as(&self, size: u64, trailer: &[u8]) -> bool {
        self.len() == size
            && trailer
                .last_chunk::<8>()
                .is_some_and(|raw| u64::from_be_bytes(*raw) == self.checksum.get())
    }

    /// Whether a read of the file that verified whole, tallied as `bytes` and
    /// ending in `checksum`, read the bytes this token was minted for.
    pub(crate) fn matches(&self, bytes: ByteTally, checksum: FileChecksum) -> bool {
        self.matches_sealed(bytes.seal(), checksum)
    }

    /// [`Self::matches`] for a tally already sealed.
    fn matches_sealed(&self, bytes: Sealed, checksum: FileChecksum) -> bool {
        self.bytes == bytes && self.checksum == checksum
    }

    /// A token for a staged file the boot read whole and verified.
    fn of_read(key: FileKey, file: &Decoded) -> Self {
        Self {
            key,
            bytes: file.bytes.seal(),
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
            bytes: tally.seal(),
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
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.by_txid.len()
    }

    /// Whether none is.
    #[must_use]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.by_txid.is_empty()
    }

    /// Tokens for every staged level-0 file in `staging`, decoded here.
    /// Tests only: it takes no witness.
    #[cfg(test)]
    pub(crate) fn trust_all<S: ObjectStore>(staging: &S) -> Self {
        let mut all = Self::new();
        let files = crate::conformance::block_on(crate::upload::staged_from(
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

/// The most tokens [`Sent`] holds, at about 40 bytes each. Staged files are pruned once
/// level 1 covers them, so a healthy engine holds a compaction window's worth; past the cap
/// (level 1 has not compacted for that many files) the oldest token goes, and its file is
/// compared with the replica's object by a read instead.
pub(crate) const MAX_SENT: usize = MAX_WITNESSED;

/// The tokens of the staged files this process put on the replica itself and still holds in
/// staging, by transaction. The store acknowledged each put with the size sent, and the send
/// was of bytes that matched the token on the read that produced them (`crate::upload`), so
/// a token here says what the replica holds under its key: a compaction that reads the staged
/// copy compares it with the token and asks the replica nothing (`crate::maintain`). In
/// memory only: a new engine holds none, and compares by reading the replica.
///
/// A token whose staged file was deleted and staged again (a divergence drops the staged
/// files from an id on) is left to the next put under its id or the next prune: it does not
/// match the new file unless that file is the old one, and a file that does not match is
/// compared by a read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Sent {
    by_txid: BTreeMap<Txid, Witnessed>,
}

impl Sent {
    /// Keeps the token of a file the replica just took, replacing one for the same
    /// transaction; past [`MAX_SENT`] the oldest goes.
    pub(crate) fn put(&mut self, token: Witnessed) {
        self.by_txid.insert(token.key.range().max(), token);
        while self.by_txid.len() > MAX_SENT {
            self.by_txid.pop_first();
        }
    }

    /// The token of the file put under `key`, if this process put it and still holds it.
    pub(crate) fn get(&self, key: &FileKey) -> Option<&Witnessed> {
        self.by_txid
            .get(&key.range().max())
            .filter(|token| token.key == *key)
    }

    /// Lets go of every token through `through`: those staged files were pruned.
    pub(crate) fn pruned_through(&mut self, through: Txid) {
        self.by_txid.retain(|txid, _| *txid > through);
    }

    /// How many tokens are held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_txid.len()
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

/// Why a staged file cannot stay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Bad {
    /// It did not verify whole on the pass's read.
    Unreadable,
    /// It does not follow the file before it: a gap in the transactions, a WAL range that
    /// does not chain, or a file past the cap, which is not read.
    Unchained,
}

/// What a boot's one pass over the staged files found.
#[derive(Debug)]
pub(crate) struct Scan {
    /// Tokens for the pending files that read whole and chain, oldest first: a run
    /// from the first pending transaction with no gap.
    pub(crate) tokens: Vec<Witnessed>,
    /// The first file that cannot stay, and why: it and every one after it must go.
    /// Nothing past it was read.
    pub(crate) bad: Option<(FileKey, Bad)>,
    /// Every file read, as recovery sees it against the WAL.
    pub(crate) scanned: Scanned,
    /// The page checksum table and the quiet files those reads give.
    pub(crate) seeding: Seeding,
}

/// One staged file, verified on the read that produced everything here.
struct StagedRead {
    file: Decoded,
    pages: Vec<(PageNo, PageChecksum)>,
    view: NewestStagedFile,
}

/// A whole file, verified on this read: its header, its bytes and its trailer
/// checksum.
struct Decoded {
    header: Header,
    bytes: ByteTally,
    checksum: FileChecksum,
}

/// Where the boot's pass starts, which says which staged files are pending.
#[derive(Debug)]
pub(crate) enum Base<'a, S> {
    /// The replica, and its newest transaction (`None`: it holds nothing): the staged files
    /// above that are pending, and the first of them must follow the replica's newest file.
    Replica(&'a S, Option<Txid>),
    /// The oldest staged file, with no replica to ask (`docs/decisions/D220.md`): it stands
    /// where the replica's newest file would, and every file after it is pending.
    Oldest,
}

/// The boot's one pass over the staged level-0 files the replica may still need: from the
/// replica's newest transaction on (every one when the replica is empty, or was not asked),
/// oldest first. Each file is read whole once, and that read verifies it, mints the token of
/// a pending file (one above `confirmed`), compares its pages with what `wal` holds in its
/// range, and feeds the page checksum table (see the module docs). The first pending file must
/// follow the replica's newest file, read from staging when it is still there, else from the
/// replica; when that file does not decode, only a file at the first frame can start the run.
/// The pass ends at the first file that cannot stay.
///
/// # Errors
/// A store read or a WAL read that failed; that says nothing about the bytes.
pub(crate) async fn scan<L: Role, S: Role, R: Read + Seek>(
    staging: &L,
    base: Base<'_, S>,
    mut wal: Option<BootWal<'_, R>>,
) -> Result<Scan, Fault> {
    let (remote, mut confirmed) = match base {
        Base::Replica(remote, newest) => (Some(remote), newest),
        Base::Oldest => (None, None),
    };
    let mut scan = Scan {
        tokens: Vec::new(),
        bad: None,
        scanned: Scanned::new(),
        seeding: Seeding::from(confirmed),
    };
    let mut expect = confirmed.map_or(Some(Txid::MIN), Txid::next);
    // Where the file before sits in its WAL. The replica's newest file is asked of the
    // replica only when staging no longer holds it and a pending file needs it.
    let mut prev = None;
    let mut linked = confirmed.is_none();
    let mut listing = StagedPages::from(confirmed);
    while let Some(page) = listing.next(staging).await? {
        for info in page {
            if remote.is_none() && confirmed.is_none() {
                // With no replica asked, the oldest staged file is the base.
                confirmed = Some(info.key.range().max());
                expect = info.key.range().max().next();
            }
            let tail = confirmed.map(FileKey::staged);
            let pending = confirmed.is_none_or(|tip| info.key.range().max() > tip);
            // Past the cap a file could get no token: it goes as a file that leaves a gap
            // does, unread, and the WAL stages it again once uploads make room.
            let wanted = expect.map(FileKey::staged);
            if pending && (scan.tokens.len() >= MAX_WITNESSED || Some(info.key) != wanted) {
                scan.bad = Some((info.key, Bad::Unchained));
                return Ok(scan);
            }
            if let (true, false, Some(tail), Some(remote)) = (pending, linked, tail, remote) {
                prev = walk(remote, tail, S::fault, |_| Ok(()))
                    .await?
                    .and_then(|file| link_of(&file.header));
                linked = true;
            }
            let borrowed = wal.as_mut().map(|wal| BootWal {
                reader: &mut *wal.reader,
                committed: wal.committed,
            });
            let Some(read) = read_staged(staging, info.key, borrowed).await? else {
                scan.bad = Some((info.key, Bad::Unreadable));
                return Ok(scan);
            };
            let header = read.file.header;
            let link = link_of(&header);
            if pending {
                let first_frame = WalOffset::start(header.page_size()).get();
                // A file starts where the one before it ended, or starts a new
                // generation or a snapshot at the first frame.
                let chained = link.is_some_and(|link| {
                    link.start == first_frame
                        || prev.is_some_and(|prev: Link| {
                            prev.salts == link.salts && prev.end == link.start
                        })
                });
                if !chained {
                    scan.bad = Some((info.key, Bad::Unchained));
                    return Ok(scan);
                }
                scan.tokens.push(Witnessed::of_read(info.key, &read.file));
                expect = info.key.range().max().next();
            }
            linked |= Some(info.key) == tail;
            prev = link;
            scan.seeding.add(&info.key, &header, &read.pages);
            scan.scanned
                .insert(info.key.range().max(), (header, read.view));
        }
    }
    Ok(scan)
}

/// Reads the staged file `key` whole, once: verified, with its pages and what it is as a
/// position against `wal`. `None` when it does not verify and name exactly `key`.
async fn read_staged<L: Role, R: Read + Seek>(
    staging: &L,
    key: FileKey,
    mut wal: Option<BootWal<'_, R>>,
) -> Result<Option<StagedRead>, Fault> {
    let mut compare = None;
    let mut pages = Vec::new();
    let file = walk(staging, key, L::fault, |event| {
        match event {
            Event::Header(header) => {
                if let Some(at) = Origin::of(&header) {
                    compare = Some(Compare::begin(at, wal.take())?);
                }
            }
            Event::Page(page) => {
                pages.push((page.pgno(), page.checksum()));
                if let Some(compare) = &mut compare {
                    compare.page(page.pgno(), page.checksum());
                }
            }
        }
        Ok(())
    })
    .await?;
    let Some(file) = file else {
        return Ok(None);
    };
    let view = match compare {
        Some(compare) => compare.end(&file.header)?,
        None => NewestStagedFile::Unreadable {
            txid: key.range().max(),
        },
    };
    Ok(Some(StagedRead { file, pages, view }))
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
    let decoded = walk(staging, token.key(), L::fault, |_| Ok(())).await?;
    Ok(decoded.is_some_and(|file| token.matches(file.bytes, file.checksum)))
}

/// Reads `key` whole through the decoder, tallying its raw bytes and handing every header
/// and page to `on_event` as it is decoded; `None` when it is missing or does not verify and
/// name exactly `key`, whatever `on_event` was handed before that.
async fn walk<S: ObjectStore>(
    store: &S,
    key: FileKey,
    fault: fn(&StoreError) -> Fault,
    mut on_event: impl FnMut(Event<'_>) -> Result<(), Fault>,
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
            let Ok((used, event)) = decoder.feed(rest) else {
                return Ok(None);
            };
            if let Some(event) = event {
                on_event(event)?;
            }
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
