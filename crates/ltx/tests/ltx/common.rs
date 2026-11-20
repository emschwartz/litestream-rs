//! Helpers shared by the tests: decoding, fixtures, hand-built files.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crc::{CRC_64_GO_ISO, Crc};
use lsr_ltx::{DecodeError, Decoder, Event, Header, Verified};

/// What a decode produced: the header, every page in order, the verdict.
#[derive(Debug, PartialEq, Eq)]
pub struct Decoded {
    pub header: Header,
    pub pages: Vec<(u32, Vec<u8>)>,
    pub verified: Verified,
}

/// Decodes `bytes` feeding chunks whose lengths cycle through `chunks`.
pub fn decode_chunked(bytes: &[u8], chunks: &[usize]) -> Result<Decoded, DecodeError> {
    let mut decoder = Decoder::new();
    let mut header = None;
    let mut pages = Vec::new();
    let mut rest = bytes;
    let mut sizes = chunks.iter().copied().cycle();
    while !rest.is_empty() {
        let take = sizes.next().unwrap_or(rest.len()).clamp(1, rest.len());
        let mut chunk = &rest[..take];
        rest = &rest[take..];
        while !chunk.is_empty() {
            let (used, event) = decoder.feed(chunk)?;
            match event {
                Some(Event::Header(h)) => header = Some(h),
                Some(Event::Page { pgno, data }) => pages.push((pgno.get(), data.to_vec())),
                None if used == chunk.len() => {}
                None => return Err(DecodeError::Poisoned), // unreachable: no event means all input was used
            }
            chunk = &chunk[used..];
        }
    }
    let verified = decoder.finish()?;
    let header = header.unwrap_or(*verified.header());
    Ok(Decoded {
        header,
        pages,
        verified,
    })
}

pub fn decode(bytes: &[u8]) -> Result<Decoded, DecodeError> {
    decode_chunked(bytes, &[usize::MAX])
}

pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Every Go fixture: (ltx version, file path).
pub fn fixtures() -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    for version in ["ltx-v0.5.1", "ltx-v0.5.3"] {
        for size in ["512", "4096", "16384", "65536"] {
            let dir = fixtures_dir().join(version).join(size);
            let mut files: Vec<PathBuf> = fs::read_dir(&dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "ltx"))
                .collect();
            files.sort();
            assert_eq!(files.len(), 4, "{}", dir.display());
            out.extend(files.into_iter().map(|path| (version.to_owned(), path)));
        }
    }
    out
}

/// A fixture's sidecar: the fields Go decoded, and each page's SHA-256.
#[derive(Debug, Default)]
pub struct Expect {
    pub fields: BTreeMap<String, u64>,
    pub pages: Vec<(u32, String)>,
}

pub fn expect(ltx: &Path) -> Expect {
    let mut path = ltx.as_os_str().to_owned();
    path.push(".expect");
    let mut out = Expect::default();
    for line in fs::read_to_string(path).unwrap().lines() {
        let parts: Vec<&str> = line.split(' ').collect();
        match parts.as_slice() {
            ["page", pgno, sha] => out.pages.push((pgno.parse().unwrap(), (*sha).to_owned())),
            [key, value] => {
                // Go prints signed fields signed; every fixture value is non-negative.
                out.fields.insert((*key).to_owned(), value.parse().unwrap());
            }
            _ => panic!("bad sidecar line {line:?}"),
        }
    }
    out
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

// ---- Hand-built files, for faults Go never writes. ----

pub const NO_CHECKSUM: u32 = 2;

/// A raw header with every field settable, valid as given by default.
#[derive(Clone, Copy, Debug)]
pub struct RawHeader {
    pub flags: u32,
    pub page_size: u32,
    pub commit: u32,
    pub min: u64,
    pub max: u64,
    pub pre_apply: u64,
    pub node_id: u64,
}

impl RawHeader {
    pub fn untracked(page_size: u32, commit: u32, min: u64) -> Self {
        Self {
            flags: NO_CHECKSUM,
            page_size,
            commit,
            min,
            max: min,
            pre_apply: 0,
            node_id: 0,
        }
    }

    pub fn bytes(&self) -> [u8; 100] {
        let mut b = [0; 100];
        b[0..4].copy_from_slice(b"LTX1");
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min.to_be_bytes());
        b[24..32].copy_from_slice(&self.max.to_be_bytes());
        b[40..48].copy_from_slice(&self.pre_apply.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        b
    }
}

/// One page frame as bytes, and the bytes the file checksum covers for it.
#[derive(Clone, Debug)]
pub struct Frame {
    pub pgno: u32,
    pub bytes: Vec<u8>,
    pub hashed: Vec<u8>,
}

pub fn page_header(pgno: u32, flags: u16) -> Vec<u8> {
    let mut out = pgno.to_be_bytes().to_vec();
    out.extend_from_slice(&flags.to_be_bytes());
    out
}

/// A block-encoded frame holding `block` as the LZ4 data, `page` as what it should decode to.
pub fn block_frame_raw(pgno: u32, block: &[u8], page: &[u8]) -> Frame {
    let mut bytes = page_header(pgno, 1);
    bytes.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
    let mut hashed = bytes.clone();
    hashed.extend_from_slice(page);
    bytes.extend_from_slice(block);
    Frame {
        pgno,
        bytes,
        hashed,
    }
}

pub fn block_frame(pgno: u32, page: &[u8]) -> Frame {
    block_frame_raw(pgno, &lz4_flex::block::compress(page), page)
}

pub const STORED: u32 = 0x8000_0000;

/// A legacy LZ4 frame, every part settable.
#[derive(Clone, Debug)]
pub struct Legacy {
    pub magic: [u8; 4],
    pub flg: u8,
    pub bd: u8,
    pub content_size: Option<u64>,
    /// (size field, block bytes) per block.
    pub blocks: Vec<(u32, Vec<u8>)>,
}

impl Legacy {
    /// What Go's pierrec writer emits for one page: one block, stored raw.
    pub fn stored(page: &[u8]) -> Self {
        Self {
            magic: 0x184d_2204_u32.to_le_bytes(),
            flg: 0x64,
            bd: 0x40,
            content_size: None,
            blocks: vec![(STORED | u32::try_from(page.len()).unwrap(), page.to_vec())],
        }
    }

    pub fn compressed(page: &[u8]) -> Self {
        let block = lz4_flex::block::compress(page);
        Self {
            blocks: vec![(u32::try_from(block.len()).unwrap(), block)],
            ..Self::stored(page)
        }
    }

    pub fn frame(&self, pgno: u32, page: &[u8]) -> Frame {
        let mut bytes = page_header(pgno, 0);
        let mut hashed = bytes.clone();
        hashed.extend_from_slice(page);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&[self.flg, self.bd]);
        if let Some(size) = self.content_size {
            bytes.extend_from_slice(&size.to_le_bytes());
        }
        bytes.push(0); // header checksum, not checked
        for (size, data) in &self.blocks {
            bytes.extend_from_slice(&size.to_le_bytes());
            bytes.extend_from_slice(data);
            if self.flg & 0x10 != 0 {
                bytes.extend_from_slice(&[0; 4]);
            }
        }
        bytes.extend_from_slice(&[0; 4]);
        if self.flg & 0x04 != 0 {
            bytes.extend_from_slice(&[0; 4]);
        }
        Frame {
            pgno,
            bytes,
            hashed,
        }
    }
}

pub fn uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).unwrap());
}

/// The index Go writes for these frames, placed after `header`.
pub fn index_for(frames: &[Frame]) -> Vec<u8> {
    let mut index = Vec::new();
    let mut offset = 100_u64;
    for frame in frames {
        let size = u64::try_from(frame.bytes.len()).unwrap();
        uvarint(&mut index, u64::from(frame.pgno));
        uvarint(&mut index, offset);
        uvarint(&mut index, size);
        offset += size;
    }
    uvarint(&mut index, 0);
    index
}

/// A whole file with a correct index and file checksum.
pub fn assemble(header: &RawHeader, frames: &[Frame], post_apply: u64) -> Vec<u8> {
    assemble_with_index(header, frames, &index_for(frames), post_apply)
}

/// A whole file with the given index bytes (terminator included) and a correct
/// size field and file checksum.
pub fn assemble_with_index(
    header: &RawHeader,
    frames: &[Frame],
    index: &[u8],
    post_apply: u64,
) -> Vec<u8> {
    let crc = Crc::<u64>::new(&CRC_64_GO_ISO);
    let mut digest = crc.digest();
    let mut out = header.bytes().to_vec();
    digest.update(&out);
    for frame in frames {
        out.extend_from_slice(&frame.bytes);
        digest.update(&frame.hashed);
    }
    let mut tail = vec![0; 6];
    tail.extend_from_slice(index);
    tail.extend_from_slice(&u64::try_from(index.len()).unwrap().to_be_bytes());
    tail.extend_from_slice(&post_apply.to_be_bytes());
    digest.update(&tail);
    tail.extend_from_slice(&((1 << 63) | digest.finalize()).to_be_bytes());
    out.extend_from_slice(&tail);
    out
}

/// A page of `size` bytes that LZ4 compresses well.
pub fn page(size: u32, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from(i % 7).unwrap().wrapping_add(seed))
        .collect()
}
