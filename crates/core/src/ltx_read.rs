//! Reading one LTX object from a store, in bounded chunks, through one decoder.
//!
//! Every reader of whole files goes through [`LtxRead`]: restore, compaction,
//! verify, the recovery check of the staged tip and the page table's seeding.
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
//! Memory is one chunk: the old chunk is dropped before the next read.

use std::convert::Infallible;
use std::mem;

use lsr_ltx::{DecodeError, Decoder, Event, Header, Verified};

use crate::layout::FileKey;
use crate::store::{ObjectStore, ReadLen, StoreError};

#[cfg(test)]
mod tests;

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
        }
    }

    /// A read whose first chunk, from offset 0 and asked as `len` bytes, the
    /// caller already holds (a read that also returned the object's stamp, or
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

    /// Bytes of the current chunk held.
    pub(crate) fn held(&self) -> usize {
        self.chunk.len()
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
        let chunk = store.read(&self.key, self.offset, self.len).await?;
        self.take(chunk);
        Ok(())
    }

    fn take(&mut self, chunk: Vec<u8>) {
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.offset = self.offset.saturating_add(len);
        self.ended = len < self.len.get();
        self.chunk = chunk;
        self.pos = 0;
    }
}
