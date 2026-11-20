//! The push decoder: bytes in, header and pages out, the whole file verified.
//!
//! [`Decoder::feed`] takes any slice of the input and returns how much it
//! consumed and at most one event, so the caller decides how to read: one byte
//! at a time and the whole file at once produce the same events. Internally the
//! file is a sequence of units (the header, each part of each page frame, each
//! index byte, the index size, the trailer); a unit's bytes are collected in a
//! buffer whose length was bounded before the first byte arrived, see
//! [`crate::FrameError::FrameTooLong`]. Memory is one page plus one frame.
//!
//! Nothing is trusted until [`Decoder::finish`] returns [`Verified`]. The file
//! checksum is a CRC-64 over: the header; per page its page header, its 4-byte
//! block length when it has one, and its decompressed bytes (never the LZ4
//! bytes); the end-of-pages marker; the index varints, terminator and size
//! field; and the trailer's post-apply checksum. Besides that checksum the
//! decoder checks every header and trailer rule, the page order
//! ([`crate::OrderError`]), that the index lists exactly the frames read (a
//! CRC over each frame's page number, offset and length, compared at the
//! terminator), and for a tracked snapshot that its pages' checksum is the
//! post-apply checksum. Go's decoder checks the last one too.

use std::io::{self, Read};
use std::mem;

use crate::checksum::Crc64;
use crate::conv::{Cursor, u64_from};
use crate::frame::{FrameParser, Progress};
use crate::index::Varint;
use crate::order::PageSequence;
use crate::{
    Checksum, DecodeError, HEADER_LEN, Header, IndexError, PageNo, Section, TRAILER_LEN, Trailer,
};

/// One thing the decoder found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event<'a> {
    /// The header, always first.
    Header(Header),
    /// One page, decompressed. Not yet verified: see [`Decoder::finish`].
    Page {
        /// The page number.
        pgno: PageNo,
        /// Exactly one page of bytes.
        data: &'a [u8],
    },
}

/// A whole file that passed every check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Verified {
    header: Header,
    trailer: Trailer,
    page_count: u64,
}

impl Verified {
    /// The file's header.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// The file's trailer.
    #[must_use]
    pub const fn trailer(&self) -> &Trailer {
        &self.trailer
    }

    /// How many pages the file held.
    #[must_use]
    pub const fn page_count(&self) -> u64 {
        self.page_count
    }
}

/// An event before it borrows the decoder.
#[derive(Clone, Copy, Debug)]
enum Pending {
    Header(Header),
    Page(PageNo),
}

#[derive(Debug)]
enum Inner {
    Header,
    Body(Box<Body>),
    Done(Verified),
    Failed,
}

/// A push decoder for one LTX file.
#[derive(Debug)]
pub struct Decoder {
    inner: Inner,
    unit: Vec<u8>,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    /// A decoder expecting the first byte of a file.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: Inner::Header,
            unit: Vec::new(),
        }
    }

    /// The header, once decoded.
    #[must_use]
    pub fn header(&self) -> Option<&Header> {
        match &self.inner {
            Inner::Body(body) => Some(&body.header),
            Inner::Done(verified) => Some(&verified.header),
            Inner::Header | Inner::Failed => None,
        }
    }

    /// Consumes a prefix of `input` and returns its length and the event it
    /// completed, if any. Without an event, all of `input` was consumed.
    ///
    /// # Errors
    ///
    /// The first fault found; after one, every call returns
    /// [`DecodeError::Poisoned`]. Bytes after the trailer are
    /// [`DecodeError::TrailingBytes`].
    pub fn feed(&mut self, input: &[u8]) -> Result<(usize, Option<Event<'_>>), DecodeError> {
        let (consumed, pending) = self.feed_raw(input)?;
        Ok((consumed, pending.and_then(|pending| self.event(pending))))
    }

    /// Ends the input.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] unless the whole file, trailer included, was fed.
    pub fn finish(self) -> Result<Verified, DecodeError> {
        match self.inner {
            Inner::Done(verified) => Ok(verified),
            Inner::Header => Err(DecodeError::Truncated {
                section: Section::Header,
            }),
            Inner::Body(body) => Err(DecodeError::Truncated {
                section: body.section(),
            }),
            Inner::Failed => Err(DecodeError::Poisoned),
        }
    }

    fn event(&self, pending: Pending) -> Option<Event<'_>> {
        match (pending, &self.inner) {
            (Pending::Header(header), _) => Some(Event::Header(header)),
            (Pending::Page(pgno), Inner::Body(body)) => Some(Event::Page {
                pgno,
                data: body.frame.page(),
            }),
            (Pending::Page(_), Inner::Header | Inner::Done(_) | Inner::Failed) => None,
        }
    }

    fn feed_raw(&mut self, input: &[u8]) -> Result<(usize, Option<Pending>), DecodeError> {
        let mut consumed = 0_usize;
        loop {
            let rest = input.get(consumed..).unwrap_or_default();
            match self.inner {
                Inner::Failed => return Err(DecodeError::Poisoned),
                Inner::Done(_) if !rest.is_empty() => {
                    self.inner = Inner::Failed;
                    return Err(DecodeError::TrailingBytes);
                }
                Inner::Header | Inner::Body(_) | Inner::Done(_) => {}
            }
            if rest.is_empty() {
                return Ok((consumed, None));
            }
            let need = self.need();
            let take = need.saturating_sub(self.unit.len()).min(rest.len());
            self.unit
                .extend_from_slice(rest.get(..take).unwrap_or_default());
            consumed = consumed.saturating_add(take);
            if self.unit.len() < need {
                return Ok((consumed, None));
            }
            let mut unit = mem::take(&mut self.unit);
            let step = self.process(&unit);
            unit.clear();
            self.unit = unit;
            match step {
                Err(err) => {
                    self.inner = Inner::Failed;
                    return Err(err);
                }
                Ok(Some(pending)) => return Ok((consumed, Some(pending))),
                Ok(None) => {}
            }
        }
    }

    fn need(&self) -> usize {
        match &self.inner {
            Inner::Header => HEADER_LEN,
            Inner::Body(body) => body.need(),
            Inner::Done(_) | Inner::Failed => 0,
        }
    }

    fn process(&mut self, unit: &[u8]) -> Result<Option<Pending>, DecodeError> {
        match &mut self.inner {
            Inner::Header => {
                let bytes = unit
                    .first_chunk::<HEADER_LEN>()
                    .ok_or(DecodeError::Truncated {
                        section: Section::Header,
                    })?;
                let header = Header::decode(bytes)?;
                self.inner = Inner::Body(Box::new(Body::new(header, bytes)));
                Ok(Some(Pending::Header(header)))
            }
            Inner::Body(body) => match body.step(unit)? {
                Step::Continue => Ok(None),
                Step::Page(pgno) => Ok(Some(Pending::Page(pgno))),
                Step::Done(verified) => {
                    self.inner = Inner::Done(verified);
                    Ok(None)
                }
            },
            Inner::Done(_) | Inner::Failed => Err(DecodeError::Poisoned),
        }
    }
}

enum Step {
    Continue,
    Page(PageNo),
    Done(Verified),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Frames,
    Index,
    IndexSize,
    Trailer,
}

/// Everything after the header.
#[derive(Debug)]
struct Body {
    header: Header,
    stage: Stage,
    frame: FrameParser,
    seq: PageSequence,
    file: Crc64,
    /// Bytes of the file consumed.
    offset: u64,
    frame_start: u64,
    current: Option<PageNo>,
    /// The database checksum so far, for tracked snapshots only.
    db: Option<Checksum>,
    frames: Crc64,
    frame_count: u64,
    listed: Crc64,
    listed_count: u64,
    varint: Varint,
    entry: [u64; 3],
    field: usize,
    index_bytes: u64,
}

impl Body {
    fn new(header: Header, bytes: &[u8]) -> Self {
        let mut file = Crc64::new();
        file.update(bytes);
        Self {
            header,
            stage: Stage::Frames,
            frame: FrameParser::new(header.page_size()),
            seq: PageSequence::new(&header),
            file,
            offset: u64_from(HEADER_LEN),
            frame_start: 0,
            current: None,
            db: (header.tracked() && header.is_snapshot()).then_some(Checksum::EMPTY),
            frames: Crc64::new(),
            frame_count: 0,
            listed: Crc64::new(),
            listed_count: 0,
            varint: Varint::default(),
            entry: [0; 3],
            field: 0,
            index_bytes: 0,
        }
    }

    const fn need(&self) -> usize {
        match self.stage {
            Stage::Frames => self.frame.need(),
            Stage::Index => 1,
            Stage::IndexSize => 8,
            Stage::Trailer => TRAILER_LEN,
        }
    }

    const fn section(&self) -> Section {
        match self.stage {
            Stage::Frames => Section::Page,
            Stage::Index | Stage::IndexSize => Section::Index,
            Stage::Trailer => Section::Trailer,
        }
    }

    fn step(&mut self, unit: &[u8]) -> Result<Step, DecodeError> {
        let start = self.offset;
        self.offset = self.offset.saturating_add(u64_from(unit.len()));
        match self.stage {
            Stage::Frames => self.frame_unit(unit, start),
            Stage::Index => self.index_byte(unit),
            Stage::IndexSize => {
                self.file.update(unit);
                let found = Cursor::new(unit).u64();
                if found != self.index_bytes {
                    return Err(IndexError::SizeField {
                        found,
                        expected: self.index_bytes,
                    }
                    .into());
                }
                self.stage = Stage::Trailer;
                Ok(Step::Continue)
            }
            Stage::Trailer => self.trailer(unit).map(Step::Done),
        }
    }

    fn frame_unit(&mut self, unit: &[u8], start: u64) -> Result<Step, DecodeError> {
        if self.frame.hashes_unit() {
            self.file.update(unit);
        }
        match self.frame.consume(unit)? {
            Progress::More => Ok(Step::Continue),
            Progress::PageStart(pgno) => {
                self.frame_start = start;
                self.current = Some(self.seq.push(pgno)?);
                Ok(Step::Continue)
            }
            Progress::Page(_) => {
                let pgno = self.current.ok_or(DecodeError::Poisoned)?;
                let page = self.frame.page();
                self.file.update(page);
                self.db = self.db.map(|db| db.toggle(Checksum::of_page(pgno, page)));
                hash_entry(
                    &mut self.frames,
                    [
                        u64::from(pgno.get()),
                        self.frame_start,
                        u64_from(self.frame.frame_len()),
                    ],
                );
                self.frame_count = self.frame_count.saturating_add(1);
                Ok(Step::Page(pgno))
            }
            Progress::EndMarker => {
                self.seq.finish()?;
                self.stage = Stage::Index;
                Ok(Step::Continue)
            }
        }
    }

    fn index_byte(&mut self, unit: &[u8]) -> Result<Step, DecodeError> {
        self.file.update(unit);
        self.index_bytes = self.index_bytes.saturating_add(1);
        let Some(value) = self
            .varint
            .push(unit.first().copied().unwrap_or_default())?
        else {
            return Ok(Step::Continue);
        };
        if self.field == 0 && value == 0 {
            let frames = mem::replace(&mut self.frames, Crc64::new()).finish();
            let listed = mem::replace(&mut self.listed, Crc64::new()).finish();
            if self.listed_count != self.frame_count || frames != listed {
                return Err(DecodeError::IndexMismatch);
            }
            self.stage = Stage::IndexSize;
            return Ok(Step::Continue);
        }
        if self.field == 0 && u32::try_from(value).is_err() {
            return Err(IndexError::PageNumber { value }.into());
        }
        if let Some(slot) = self.entry.get_mut(self.field) {
            *slot = value;
        }
        self.field = self.field.saturating_add(1);
        if self.field == self.entry.len() {
            self.field = 0;
            hash_entry(&mut self.listed, self.entry);
            self.listed_count = self.listed_count.saturating_add(1);
            if self.listed_count > self.frame_count {
                return Err(DecodeError::IndexMismatch);
            }
        }
        Ok(Step::Continue)
    }

    fn trailer(&mut self, unit: &[u8]) -> Result<Verified, DecodeError> {
        let bytes = unit
            .first_chunk::<TRAILER_LEN>()
            .ok_or(DecodeError::Truncated {
                section: Section::Trailer,
            })?;
        self.file.update(bytes.get(..8).unwrap_or_default());
        let computed = mem::replace(&mut self.file, Crc64::new()).finish();
        let trailer = Trailer::decode(bytes, &self.header)?;
        if trailer.file_checksum() != computed {
            return Err(DecodeError::FileChecksum {
                computed,
                stored: trailer.file_checksum(),
            });
        }
        if let Some(computed) = self.db
            && trailer.post_apply() != Some(computed)
        {
            return Err(DecodeError::PostApplyMismatch {
                computed,
                stored: trailer.post_apply(),
            });
        }
        Ok(Verified {
            header: self.header,
            trailer,
            page_count: self.frame_count,
        })
    }
}

fn hash_entry(crc: &mut Crc64, entry: [u64; 3]) {
    for value in entry {
        crc.update(&value.to_be_bytes());
    }
}

/// A [`Decoder`] pulling from a reader.
#[derive(Debug)]
pub struct ReadDecoder<R> {
    reader: R,
    decoder: Decoder,
    buf: Box<[u8]>,
    pos: usize,
    len: usize,
}

impl<R: Read> ReadDecoder<R> {
    /// Decodes the file `reader` yields; it need not be buffered.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoder: Decoder::new(),
            buf: vec![0; 64 * 1024].into_boxed_slice(),
            pos: 0,
            len: 0,
        }
    }

    /// The next event, or `None` at the end of the input: then call
    /// [`ReadDecoder::finish`].
    ///
    /// # Errors
    ///
    /// As [`Decoder::feed`], plus [`DecodeError::Io`] from the reader.
    pub fn next_event(&mut self) -> Result<Option<Event<'_>>, DecodeError> {
        loop {
            if self.pos == self.len {
                self.len = read_retrying(&mut self.reader, &mut self.buf)?;
                self.pos = 0;
                if self.len == 0 {
                    return Ok(None);
                }
            }
            let (consumed, pending) = self
                .decoder
                .feed_raw(self.buf.get(self.pos..self.len).unwrap_or_default())?;
            self.pos = self.pos.saturating_add(consumed);
            if let Some(pending) = pending {
                return Ok(self.decoder.event(pending));
            }
        }
    }

    /// Ends the input; see [`Decoder::finish`].
    ///
    /// # Errors
    ///
    /// As [`Decoder::finish`].
    pub fn finish(self) -> Result<Verified, DecodeError> {
        self.decoder.finish()
    }
}

fn read_retrying(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buf) {
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            other => return other,
        }
    }
}
