//! Reading one LTX object from a store, in bounded chunks, through one decoder.
//!
//! Every reader of whole files goes through [`LtxRead`]: restore, compaction,
//! verify, the recovery check of the staged newest staged file and the page table's seeding.
//! It owns the three rules each of them used to repeat by hand:
//! - The listed size is advisory: only the decoder knows where a file ends.
//! - A chunk shorter than asked is the end of the object (the read contract
//!   of [`ObjectStore::read`]), so no empty read follows to learn it.
//! - Nothing counts until the decoder finished: the [`Verified`] summary
//!   exists only in [`Next::End`], returned after the last byte was fed and
//!   [`lsr_ltx::Decoder::finish`] passed. A caller that stages pages commits
//!   them on `End` and never before; [`LtxRead::read_verified`] returns the
//!   summary only then, so "every page was staged and the file verified" is
//!   one value.
//!
//! Memory is one chunk: the old chunk is dropped before the next read. A read
//! told the object's listed size ([`LtxRead::sized`]) on a store whose
//! [`ObjectStore::batch_width`] for [`BatchKind::Reads`] is above one also reads ahead: when it runs
//! out it asks for up to that many chunks in one [`ObjectStore::read_many`],
//! never more than the listed size still needs, and feeds them to the decoder
//! in order. Memory is then at most that many chunks. The chunks are held
//! as fetched and reach the decoder exactly as sequential reads would, so the
//! verification rule above is unchanged; a failed read in a batch is held in
//! its place and reported only when the decoder gets there, so a fault is the
//! one a sequential read would meet first. The listed size only bounds the
//! batch: a file longer than listed is still read to its end.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::mem;

use lsr_ltx::{DbChecksum, DecodeError, Decoder, Event, Header, Txid, Verified};

use crate::fault::Fault;
use crate::layout::FileKey;
use crate::store::{BatchKind, ObjectStore, ReadAt, ReadLen, StoreError};

/// Where a verified file ends, for the file applied after it to be checked
/// against: its last transaction and, when it tracks one, the database
/// checksum after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LinkEnd {
    through: Txid,
    post_apply: Option<DbChecksum>,
}

impl LinkEnd {
    pub(crate) fn of(verified: &Verified) -> Self {
        Self {
            through: verified.header().txids().max(),
            post_apply: verified.trailer().post_apply(),
        }
    }

    /// Whether `header`'s file starts at the transaction right after.
    pub(crate) fn followed_by(&self, header: &Header) -> bool {
        self.through.next() == Some(header.txids().min())
    }
}

/// Two neighbouring files that do not chain: the first ends at a database
/// checksum that is not the one the second says it starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BrokenLink {
    pub(crate) expected: DbChecksum,
    pub(crate) found: DbChecksum,
}

/// The checksum-chain rule, stated once for restore, verify and compaction.
///
/// A file's pre-apply checksum is the database before its own first
/// transaction, so it can be compared with the file before only when it
/// starts right after that file ends: an overlapping file's predecessor has
/// already moved past that state. Files that track no checksum (every file Go
/// and this port write today) have nothing to compare and pass.
pub(crate) fn check_link(before: LinkEnd, header: &Header) -> Result<(), BrokenLink> {
    match (before.post_apply, header.fields().pre_apply) {
        (Some(expected), Some(found)) if before.followed_by(header) && expected != found => {
            Err(BrokenLink { expected, found })
        }
        (Some(_) | None, Some(_) | None) => Ok(()),
    }
}

#[cfg(test)]
mod tests;

/// Reads `key` through the decoder, `len` bytes at a time, and says whether it
/// verifies whole and names exactly `key`. `store_fault` names a store error,
/// which is never a verdict on the bytes.
///
/// # Errors
/// The store's fault when a read fails.
pub(crate) async fn verifies<S: ObjectStore>(
    store: &S,
    key: &FileKey,
    len: ReadLen,
    store_fault: fn(&StoreError) -> Fault,
) -> Result<bool, Fault> {
    match LtxRead::new(*key, len)
        .read_verified(store, |_| Ok::<(), Infallible>(()))
        .await
    {
        Ok(verified) => Ok(verified.header().txids() == key.range()),
        Err(ReadFault::Store(err)) => Err(store_fault(&err)),
        Err(ReadFault::Decode(_undecodable)) => Ok(false),
        Err(ReadFault::Rejected(never)) => match never {},
    }
}

/// Why an object could not be read to a verified end.
#[derive(Debug)]
pub(crate) enum ReadFault<E = Infallible> {
    /// The store failed a read.
    Store(StoreError),
    /// The bytes are not a valid LTX file.
    Decode(DecodeError),
    /// The caller's handler refused an event.
    Rejected(E),
}

/// What one step of a read found.
#[derive(Debug)]
pub(crate) enum Next<R> {
    /// The handler's answer to the next header or page.
    Event(R),
    /// Every byte of the object was fed and the file verified.
    End(Verified),
}

/// One object being read and decoded.
#[derive(Debug)]
pub(crate) struct LtxRead {
    key: FileKey,
    len: ReadLen,
    decoder: Decoder,
    chunk: Vec<u8>,
    pos: usize,
    /// Bytes read from the store so far.
    offset: u64,
    /// The store returned a short chunk: no more reads.
    ended: bool,
    header: Option<Header>,
    /// The listed size, when the caller allows reading ahead up to it.
    listed: Option<u64>,
    /// Chunks fetched ahead, in order from `offset`; an error ends them.
    ahead: VecDeque<Result<Vec<u8>, StoreError>>,
}

impl LtxRead {
    /// A read of `key` from its first byte, `len` bytes at a time.
    pub(crate) const fn new(key: FileKey, len: ReadLen) -> Self {
        Self {
            key,
            len,
            decoder: Decoder::new(),
            chunk: Vec::new(),
            pos: 0,
            offset: 0,
            ended: false,
            header: None,
            listed: None,
            ahead: VecDeque::new(),
        }
    }

    /// The same read, allowed to fetch ahead up to the object's listed `size`
    /// (see the module docs).
    #[must_use]
    pub(crate) const fn sized(mut self, size: u64) -> Self {
        self.listed = Some(size);
        self
    }

    /// A read whose first chunk, from offset 0 and asked as `len` bytes, the
    /// caller already holds (a read that also returned the object's created-at time, or
    /// a header probe).
    pub(crate) fn with_first(key: FileKey, len: ReadLen, first: Vec<u8>) -> Self {
        let mut read = Self::new(key, len);
        read.take(first);
        read
    }

    /// The object read.
    pub(crate) const fn key(&self) -> FileKey {
        self.key
    }

    /// Bytes read from the store so far, counted whether or not they decoded.
    pub(crate) const fn bytes_read(&self) -> u64 {
        self.offset
    }

    /// The header, once decoded. `None` again after the bytes failed to
    /// decode, as the decoder itself forgets it; a file that only ended early
    /// keeps it.
    pub(crate) const fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Bytes held: the current chunk and any fetched ahead.
    pub(crate) fn held(&self) -> usize {
        self.ahead
            .iter()
            .map(|chunk| chunk.as_ref().map_or(0, Vec::len))
            .fold(self.chunk.len(), usize::saturating_add)
    }

    /// Feeds bytes until the next event, which `on_event` turns into the
    /// step's answer, or until the object ends and the file verifies.
    ///
    /// # Errors
    /// [`ReadFault`]; after one, the read is spent.
    pub(crate) async fn next<S: ObjectStore, R, E>(
        &mut self,
        store: &S,
        on_event: impl FnOnce(Event<'_>) -> Result<R, E>,
    ) -> Result<Next<R>, ReadFault<E>> {
        loop {
            if self.pos >= self.chunk.len() {
                if self.ended {
                    self.chunk = Vec::new();
                    let decoder = mem::take(&mut self.decoder);
                    return decoder.finish().map(Next::End).map_err(ReadFault::Decode);
                }
                self.refill(store).await.map_err(ReadFault::Store)?;
                continue;
            }
            let rest = self.chunk.get(self.pos..).unwrap_or_default();
            let (used, event) = match self.decoder.feed(rest) {
                Ok(fed) => fed,
                Err(err) => {
                    self.header = None;
                    return Err(ReadFault::Decode(err));
                }
            };
            self.pos = self.pos.saturating_add(used);
            if let Some(event) = event {
                if let Event::Header(header) = event {
                    self.header = Some(header);
                }
                return on_event(event)
                    .map(Next::Event)
                    .map_err(ReadFault::Rejected);
            }
        }
    }

    /// Reads to the end, handing every event to `on_event` in file order.
    /// The summary is returned only once the whole file verified.
    ///
    /// # Errors
    /// [`ReadFault`], the first one met.
    pub(crate) async fn read_verified<S: ObjectStore, E>(
        &mut self,
        store: &S,
        mut on_event: impl FnMut(Event<'_>) -> Result<(), E>,
    ) -> Result<Verified, ReadFault<E>> {
        loop {
            match self.next(store, &mut on_event).await? {
                Next::Event(()) => {}
                Next::End(verified) => return Ok(verified),
            }
        }
    }

    async fn refill<S: ObjectStore>(&mut self, store: &S) -> Result<(), StoreError> {
        // The old chunk goes first so a read never holds two.
        self.chunk = Vec::new();
        self.pos = 0;
        if self.ahead.is_empty() {
            let count = self.batch(store.batch_width(BatchKind::Reads).get());
            if count <= 1 {
                let chunk = store.read(&self.key, self.offset, self.len).await?;
                self.take(chunk);
                return Ok(());
            }
            self.fetch_ahead(store, count).await;
        }
        let chunk = self.ahead.pop_front().unwrap_or_else(|| Ok(Vec::new()))?;
        self.take(chunk);
        Ok(())
    }

    /// How many chunks the next fetch asks for: what the listed size still
    /// needs, at least one, at most `width`.
    fn batch(&self, width: usize) -> usize {
        let Some(size) = self.listed else { return 1 };
        let left = size.saturating_sub(self.offset);
        let chunks = left.div_ceil(self.len.get());
        usize::try_from(chunks)
            .unwrap_or(usize::MAX)
            .clamp(1, width)
    }

    /// Fetches `count` chunks from `offset` in one batch and keeps them up to
    /// the first failure. Chunks after a short one (the end) are kept too: they
    /// are empty reads past the end, and the read has ended before reaching them.
    async fn fetch_ahead<S: ObjectStore>(&mut self, store: &S, count: usize) {
        let mut reads = Vec::with_capacity(count);
        let mut at = self.offset;
        for _ in 0..count {
            reads.push(ReadAt {
                key: self.key,
                offset: at,
                len: self.len,
            });
            at = at.saturating_add(self.len.get());
        }
        for result in store.read_many(&reads).await {
            let failed = result.is_err();
            self.ahead.push_back(result);
            if failed {
                break;
            }
        }
    }

    fn take(&mut self, chunk: Vec<u8>) {
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.offset = self.offset.saturating_add(len);
        self.ended = len < self.len.get();
        self.chunk = chunk;
        self.pos = 0;
    }
}
