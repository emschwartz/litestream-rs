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
//! - **The live WAL.** At boot, [`witness_pending`] re-derives every pending
//!   staged file (above the replica's newest transaction) from the WAL as it
//!   is now. The live WAL is the one thing a restored staging backup does not
//!   put back, and it answers in bytes.
//!
//! The witness ladder has one more rung that v1 cannot use: a checksum chain
//! (each file's pre-apply checksum equal to the previous file's post-apply
//! one) would vouch for files the WAL no longer holds. Every file v1 and Go
//! write is untracked (`docs/decisions.md`, D2), so there is nothing to
//! compare; [`Refused::NotInLiveWal`] marks the seam where that rung goes.
//! Below it, the file is dropped.
//!
//! The boot pass walks the WAL range by range from the replica's newest
//! level-0 file (the tail) on. The tail must lie in the live WAL with the
//! pages and commit size it holds; that anchors the run on content, not on a
//! header link. Each pending file must then start where the one before it
//! ended, in the live generation, hold exactly the pages a sync step would
//! have staged for its range (the newest version of every page the range
//! holds, plus the pages the database grew by, `crate::sync::plan_pages`) with
//! the same bytes, and end at the same commit size. A grown page has no frame
//! in its range; it is compared with the database file, and only when the WAL
//! holds no frame for it anywhere, since checkpoints write the database file
//! only from frames. A pending snapshot needs no anchor: it is read from the
//! live generation's first frame and checked whole, every page against its
//! frame or the database file by the same rule, and it must not stop short
//! of a file before it in its generation; one the read cannot prove is taken
//! again, so the replica gets one snapshot either way. A file that starts a
//! new generation without holding every page is never witnessed at boot: a
//! hop between generations is proven only by a running position that held
//! the read lock across it.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek};
use std::num::NonZeroU32;

use lsr_ltx::{
    Decoder, Event, FileChecksum, Header, PageChecksum, PageNo, Timestamp, Txid, TxidRange,
};
use lsr_wal::{Salts, ScanLimit, TxOutcome, WalError, WalOffset};

use crate::fault::role::Role;
use crate::fault::{BugSite, Fault};
use crate::layout::{FileInfo, FileKey, Level};
use crate::recover::{BootWal, Range, wal_range};
use crate::source::PageSource;
use crate::store::{ObjectStore, PageLimit, ReadLen, StoreError};
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
    pub(crate) fn insert(&mut self, token: Witnessed) {
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

    /// The token for `key`, if one was taken for exactly that key.
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
    /// Tokens for the witnessed files, oldest first: a run from the first
    /// pending transaction with no gap.
    pub(crate) tokens: Vec<Witnessed>,
    /// The first pending file the WAL does not vouch for: it and every one
    /// after it must go.
    pub(crate) refused: Option<Txid>,
}

/// Why a file got no witness. Only the first refusal of a run matters, and it
/// is not reported further; the variants name the rule for readers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refused {
    /// The file or the tail does not decode and verify, names no usable WAL
    /// range, or is not the next transaction.
    Unusable,
    /// The range is not in the live WAL: another generation or page size,
    /// or a file that hops generations. This is the seam for the checksum
    /// chain rung (see the module docs), unavailable while every file is
    /// untracked.
    NotInLiveWal,
    /// The live WAL holds that range with other pages, other bytes or another
    /// commit size, or a grown page differs from the database file.
    Contradicted,
}

/// What the next pending file follows: the previous transaction, and where
/// its file ended in the live WAL when that is known (the replica's tail is
/// in the live WAL, or a pending file was witnessed). With no such end, only
/// a snapshot can start the run.
#[derive(Clone, Copy)]
struct Before {
    txid: Option<Txid>,
    link: Option<Prev>,
}

/// Where the previous file of the run ended.
#[derive(Clone, Copy)]
struct Prev {
    txid: Txid,
    salts: Salts,
    end: u64,
    commit: NonZeroU32,
}

/// A whole file, verified on this read: its header, the checksum of each page
/// and its trailer checksum.
struct Decoded {
    header: Header,
    pages: BTreeMap<PageNo, PageChecksum>,
    bytes: ByteTally,
    checksum: FileChecksum,
}

/// The WAL and the database file the boot pass compares with.
pub(crate) struct LiveDb<'a, R, P> {
    /// The WAL as boot opened it, with the committed end of the boot's one wal-index
    /// read: no range past it is proven.
    pub(crate) wal: BootWal<'a, R>,
    /// The database file.
    pub(crate) db: &'a mut P,
}

/// Witnesses the staged level-0 files above `confirmed`, the replica's newest
/// transaction, against the live WAL and database (see the module docs).
///
/// The tail is read from staging when its staged copy is there (boot's
/// listing already found it byte-equal to the replica's), else from the
/// replica. With no WAL nothing is witnessed; with an empty replica or a
/// tail not in the live WAL, only a run that starts with a snapshot is.
///
/// # Errors
/// A store or WAL read that failed; that says nothing about the bytes.
pub(crate) async fn witness_pending<L, S, R, P>(
    staging: &L,
    remote: &S,
    live: Option<LiveDb<'_, R, P>>,
    confirmed: Option<Txid>,
) -> Result<Pending, Fault>
where
    L: Role,
    S: Role,
    R: Read + Seek,
    P: PageSource,
{
    let from = confirmed.map_or(Some(Txid::MIN), Txid::next);
    let pending: Vec<FileInfo> = match from {
        // One past the cap: the first file no token could be minted for.
        Some(from) => staged_from(staging, Some(from), MAX_WITNESSED.saturating_add(1)).await?,
        None => Vec::new(),
    };
    let Some(first) = pending.first() else {
        return Ok(Pending::default());
    };
    let refuse_all = Pending {
        tokens: Vec::new(),
        refused: Some(first.key.range().min()),
    };
    let Some(mut live) = live else {
        return Ok(refuse_all);
    };
    let mut prev = Before {
        txid: confirmed,
        link: match confirmed {
            Some(tail) => anchor(staging, remote, &mut live, tail).await?,
            None => None,
        },
    };
    let mut framed = None;
    let mut tokens = Vec::new();
    for info in &pending {
        // Past the cap a file could get no token: it goes as refused files do,
        // and the WAL stages it again once uploads make room.
        if tokens.len() >= MAX_WITNESSED {
            return Ok(Pending {
                tokens,
                refused: Some(info.key.range().min()),
            });
        }
        match witness_one(staging, &mut live, info, prev, &mut framed).await? {
            Ok((token, next)) => {
                mark!(Boot4);
                tokens.push(token);
                prev = Before {
                    txid: Some(next.txid),
                    link: Some(next),
                };
            }
            Err(_refused) => {
                return Ok(Pending {
                    tokens,
                    refused: Some(info.key.range().min()),
                });
            }
        }
    }
    Ok(Pending {
        tokens,
        refused: None,
    })
}

/// The replica's tail, when the live WAL holds its range with its pages and
/// commit size: where the first pending file must start.
async fn anchor<L: Role, S: Role, R: Read + Seek, P>(
    staging: &L,
    remote: &S,
    live: &mut LiveDb<'_, R, P>,
    tail: Txid,
) -> Result<Option<Prev>, Fault> {
    let key = FileKey::new(Level::ZERO, TxidRange::single(tail));
    let after = tail.prev().map(TxidRange::single);
    let listed = staging
        .list(Level::ZERO, after, PageLimit::ONE)
        .await
        .map_err(|err| L::fault(&err))?;
    let staged = listed.files.first().is_some_and(|info| info.key == key);
    let decoded = if staged {
        decode(staging, key, L::fault).await?
    } else {
        decode(remote, key, S::fault).await?
    };
    let Some(decoded) = decoded else {
        return Ok(None);
    };
    let Some((salts, start, end, commit)) = span(&decoded.header) else {
        return Ok(None);
    };
    let held = match wal_range(&mut live.wal, start, end, salts)? {
        Range::Pages { pages, commit: got } => {
            got.is_none_or(|got| got == commit)
                && pages
                    .iter()
                    .filter(|(pgno, _)| pgno.get() <= commit.get())
                    .all(|(pgno, digest)| decoded.pages.get(pgno) == Some(digest))
        }
        Range::Rewritten | Range::NotInThisWal => false,
    };
    Ok(held.then_some(Prev {
        txid: tail,
        salts,
        end: end.get(),
        commit,
    }))
}

/// The file's generation, WAL range and commit size, when its header names a
/// range that ends on a frame boundary. Whether that range is in the live
/// WAL is [`wal_range`]'s to say: it refuses another generation or page size.
fn span(header: &Header) -> Option<(Salts, u64, WalOffset, NonZeroU32)> {
    let fields = header.fields();
    let origin = fields.wal?;
    let (salt1, salt2) = origin.salts();
    let salts = Salts { salt1, salt2 };
    let end = WalOffset::new(
        origin.offset().checked_add(origin.size())?,
        fields.page_size,
    )?;
    Some((salts, origin.offset(), end, NonZeroU32::new(fields.commit)?))
}

/// One pending file against the live WAL: its token and where it ends, or
/// why it is refused.
///
/// A file that starts where the previous one ended is an incremental. One
/// read from the live generation's first frame is a snapshot when it holds
/// every page: it is checked page by page against the WAL range and, for
/// each page the range does not hold, the database file (only where no
/// frame exists anywhere in the WAL), and it must reach at least as far as
/// a previous file in the same generation, since one that stops short is an
/// earlier state. A snapshot the read cannot prove is refused and taken
/// again; a file that starts a generation without holding every page (a
/// hop) is refused, since only a running position can prove it lost nothing.
async fn witness_one<L: Role, R: Read + Seek, P: PageSource>(
    staging: &L,
    live: &mut LiveDb<'_, R, P>,
    info: &FileInfo,
    prev: Before,
    framed: &mut Option<Option<BTreeSet<PageNo>>>,
) -> Result<Result<(Witnessed, Prev), Refused>, Fault> {
    let txid = info.key.range().max();
    let next = prev.txid.map_or(Some(Txid::MIN), Txid::next);
    if Some(info.key) != next.map(|next| FileKey::new(Level::ZERO, TxidRange::single(next))) {
        return Ok(Err(Refused::Unusable));
    }
    let Some(decoded) = decode(staging, info.key, L::fault).await? else {
        return Ok(Err(Refused::Unusable));
    };
    let Some((salts, start, end, commit)) = span(&decoded.header) else {
        return Ok(Err(Refused::NotInLiveWal));
    };
    let follows = prev
        .link
        .filter(|link| link.salts == salts && link.end == start);
    let first_frame = WalOffset::start(decoded.header.page_size()).get();
    let prev_commit = match (follows, prev.link) {
        (Some(link), _) => link.commit.get(),
        // A snapshot that stops short of a file before it in its generation
        // describes an earlier state of that WAL. (The link is always in the
        // live generation; a file in another is refused by `wal_range`.)
        (None, Some(link)) if end.get() < link.end => {
            return Ok(Err(Refused::Contradicted));
        }
        (None, _) if start == first_frame => 0,
        (None, _) => return Ok(Err(Refused::NotInLiveWal)),
    };
    let wal_pages = match wal_range(&mut live.wal, start, end, salts)? {
        Range::Pages {
            pages,
            commit: Some(got),
        } if got == commit => pages,
        Range::Pages { .. } | Range::Rewritten => return Ok(Err(Refused::Contradicted)),
        Range::NotInThisWal => return Ok(Err(Refused::NotInLiveWal)),
    };
    if !holds_exactly(&decoded, &wal_pages, prev_commit, commit, live, framed)? {
        return Ok(Err(Refused::Contradicted));
    }
    let token = Witnessed {
        key: info.key,
        bytes: decoded.bytes,
        checksum: decoded.checksum,
        stamp: decoded.header.fields().timestamp,
    };
    let next = Prev {
        txid,
        salts,
        end: end.get(),
        commit,
    };
    Ok(Ok((token, next)))
}

/// Whether the file holds exactly the pages a sync step stages for its range:
/// every WAL page up to `commit` with the WAL's bytes, and every page grown
/// above `prev_commit` the range does not hold, with the database file's
/// bytes, and nothing else. A snapshot is the case `prev_commit` = 0: every
/// page the range does not hold comes from the database file.
fn holds_exactly<R: Read + Seek, P: PageSource>(
    decoded: &Decoded,
    wal_pages: &BTreeMap<PageNo, PageChecksum>,
    prev_commit: u32,
    commit: NonZeroU32,
    live: &mut LiveDb<'_, R, P>,
    framed: &mut Option<Option<BTreeSet<PageNo>>>,
) -> Result<bool, Fault> {
    let page_size = decoded.header.page_size();
    let from_wal = wal_pages
        .iter()
        .filter(|(pgno, _)| pgno.get() <= commit.get());
    let mut expected = 0_usize;
    for (pgno, digest) in from_wal {
        if decoded.pages.get(pgno) != Some(digest) {
            return Ok(false);
        }
        expected = expected.saturating_add(1);
    }
    let grown = (prev_commit.saturating_add(1)..=commit.get())
        .filter_map(|raw| PageNo::new(raw, page_size))
        .filter(|pgno| !wal_pages.contains_key(pgno));
    let mut buf = vec![0_u8; page_size.bytes()];
    for pgno in grown {
        let Some(digest) = decoded.pages.get(&pgno) else {
            return Ok(false);
        };
        let pages = match framed {
            Some(pages) => pages,
            None => framed.insert(framed_pages(&mut live.wal)?),
        };
        // A frame anywhere may have reached the database file since: the
        // file's copy cannot be told from the database's, so no witness.
        if pages.as_ref().is_none_or(|pages| pages.contains(&pgno)) {
            return Ok(false);
        }
        if live.db.read_page(pgno, &mut buf).is_err() || PageChecksum::of(pgno, &buf) != *digest {
            return Ok(false);
        }
        expected = expected.saturating_add(1);
    }
    Ok(decoded.pages.len() == expected)
}

/// Every page any committed transaction of the live WAL writes, up to the boot's
/// committed end; `None` when the WAL cannot be read that far.
fn framed_pages<R: Read + Seek>(
    boot: &mut BootWal<'_, R>,
) -> Result<Option<BTreeSet<PageNo>>, Fault> {
    let wal = &mut *boot.reader;
    let mut cursor = wal.cursor_at_start();
    let mut pages = BTreeSet::new();
    loop {
        match wal.next_tx(&cursor, ScanLimit::Committed(boot.committed)) {
            Ok(TxOutcome::Tx(tx)) => {
                pages.extend(tx.frames().iter().map(lsr_wal::FrameRef::pgno));
                cursor = *tx.end();
            }
            Ok(TxOutcome::End { .. }) => return Ok(Some(pages)),
            Err(WalError::Io(_)) => return Err(Fault::LocalIo),
            Err(
                WalError::PageSizeMismatch { .. }
                | WalError::Restarted { .. }
                | WalError::CursorPastEnd { .. }
                | WalError::CorruptBelowCommitted { .. }
                | WalError::TruncatedBelowCommitted { .. }
                | WalError::EndNotAtCommit { .. }
                | WalError::TooManyFrames,
            ) => return Ok(None),
        }
    }
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
    let mut pages = BTreeMap::new();
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
            if let Some(Event::Page(page)) = event {
                pages.insert(page.pgno(), page.checksum());
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
            pages,
            bytes,
            checksum: verified.trailer().file_checksum(),
        })),
        Ok(_) | Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests;
