//! Files Go wrote, at ltx v0.5.1 (LZ4 frames) and v0.5.3 (LZ4 blocks), decode
//! to exactly what Go's own decoder read from them.

use std::fs;
use std::io::Cursor;

use lsr_ltx::{Checksum, Encoder, Event, INDEX_TAIL_LEN, PageIndex, ReadDecoder, decode_frame};

use crate::common::{Decoded, decode, decode_chunked, expect, fixtures, sha256_hex};

fn check_against_sidecar(name: &str, decoded: &Decoded) {
    let want = expect(std::path::Path::new(name));
    let f = decoded.header.fields();
    let wal = f.wal.map_or((0, 0, 0, 0), |w| {
        (
            w.offset(),
            w.size(),
            u64::from(w.salts().0),
            u64::from(w.salts().1),
        )
    });
    let got = [
        ("flags", if f.tracked { 0 } else { 2 }),
        ("page_size", u64::from(f.page_size.get())),
        ("commit", u64::from(f.commit)),
        ("min_txid", f.txids.min().get()),
        ("max_txid", f.txids.max().get()),
        (
            "timestamp",
            u64::try_from(f.timestamp.unix_millis()).unwrap(),
        ),
        ("pre_apply", f.pre_apply.map_or(0, Checksum::get)),
        ("wal_offset", wal.0),
        ("wal_size", wal.1),
        ("wal_salt1", wal.2),
        ("wal_salt2", wal.3),
        ("node_id", f.node_id),
        (
            "post_apply",
            decoded
                .verified
                .trailer()
                .post_apply()
                .map_or(0, Checksum::get),
        ),
        (
            "file_checksum",
            decoded.verified.trailer().file_checksum().get(),
        ),
    ];
    for (key, value) in got {
        assert_eq!(want.fields[key], value, "{name}: {key}");
    }
    let pages: Vec<(u32, String)> = decoded
        .pages
        .iter()
        .map(|(pgno, data)| (*pgno, sha256_hex(data)))
        .collect();
    assert_eq!(pages, want.pages, "{name}");
    assert_eq!(
        decoded.verified.page_count(),
        u64::try_from(pages.len()).unwrap()
    );
}

#[test]
fn go_fixtures_decode_to_what_go_read() {
    let all = fixtures();
    assert_eq!(all.len(), 32);
    for (_, path) in all {
        let bytes = fs::read(&path).unwrap();
        let decoded = decode(&bytes).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
        check_against_sidecar(path.to_str().unwrap(), &decoded);
    }
}

#[test]
fn fixtures_cover_both_page_encodings() {
    for (version, path) in fixtures() {
        let bytes = fs::read(&path).unwrap();
        // The first page header's flags: 0 for an LZ4 frame, 1 for a sized block.
        let flags = u16::from_be_bytes([bytes[104], bytes[105]]);
        assert_eq!(
            flags,
            if version == "ltx-v0.5.1" { 0 } else { 1 },
            "{}",
            path.display()
        );
    }
}

#[test]
fn go_fixtures_decode_the_same_in_any_chunking() {
    for (_, path) in fixtures() {
        let bytes = fs::read(&path).unwrap();
        let whole = decode(&bytes).unwrap();
        for chunks in [&[1_usize][..], &[7, 1, 4093], &[99, 101, 6, 4, 1000]] {
            if chunks == [1] && bytes.len() > 40_000 {
                continue;
            }
            assert_eq!(
                decode_chunked(&bytes, chunks).unwrap(),
                whole,
                "{} {chunks:?}",
                path.display()
            );
        }
    }
}

#[test]
fn read_decoder_matches_the_push_decoder() {
    for (_, path) in fixtures() {
        let bytes = fs::read(&path).unwrap();
        let whole = decode(&bytes).unwrap();
        let mut reader = ReadDecoder::new(Cursor::new(&bytes));
        let mut pages = Vec::new();
        while let Some(event) = reader.next_event().unwrap() {
            if let Event::Page { pgno, data } = event {
                pages.push((pgno.get(), data.to_vec()));
            }
        }
        assert_eq!(reader.finish().unwrap(), whole.verified);
        assert_eq!(pages, whole.pages);
    }
}

#[test]
fn page_index_reads_every_page_out_of_order() {
    for (_, path) in fixtures() {
        let bytes = fs::read(&path).unwrap();
        let whole = decode(&bytes).unwrap();
        let tail: &[u8; INDEX_TAIL_LEN] = bytes[bytes.len() - INDEX_TAIL_LEN..].try_into().unwrap();
        let region_len = usize::try_from(PageIndex::region_len(tail)).unwrap();
        let region_end = bytes.len() - lsr_ltx::TRAILER_LEN;
        let index =
            PageIndex::decode(&bytes[region_end - region_len..region_end], &whole.header).unwrap();
        assert_eq!(index.entries().len(), whole.pages.len());
        let mut out = Vec::new();
        for (entry, (pgno, data)) in index.entries().iter().zip(&whole.pages).rev() {
            assert_eq!(index.get(entry.pgno()), Some(entry));
            let start = usize::try_from(entry.offset()).unwrap();
            let end = start + usize::try_from(entry.size()).unwrap();
            assert_eq!(
                decode_frame(&bytes[start..end], &whole.header, entry, &mut out).unwrap(),
                entry.pgno()
            );
            assert_eq!((entry.pgno().get(), &out), (*pgno, data));
        }
    }
}

#[test]
fn re_encoded_fixtures_decode_to_the_same_pages() {
    for (_, path) in fixtures() {
        let whole = decode(&fs::read(&path).unwrap()).unwrap();
        let mut encoder = Encoder::new(Vec::new(), whole.header).unwrap();
        for (pgno, data) in &whole.pages {
            encoder
                .write_page(
                    lsr_ltx::PageNo::new(*pgno, whole.header.page_size()).unwrap(),
                    data,
                )
                .unwrap();
        }
        let post = whole.verified.trailer().post_apply();
        let (bytes, trailer) = encoder.finish(post).unwrap();
        let again = decode(&bytes).unwrap();
        assert_eq!((again.header, &again.pages), (whole.header, &whole.pages));
        assert_eq!(again.verified.trailer(), &trailer);
        assert_eq!(trailer.post_apply(), post);
    }
}
