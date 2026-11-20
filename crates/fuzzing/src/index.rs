//! The `index` target: one page located and decoded out of order.

use lsr_ltx::{
    Checksum, Header, HeaderFields, INDEX_TAIL_LEN, PageIndex, PageSize, Timestamp, Txid,
    TxidRange, decode_frame,
};

use crate::bytes::{Reader, violation};

/// Bytes of the compact header description the input starts with.
pub const HEADER_SPEC_LEN: usize = 14;

/// Builds a valid header from a compact description: page-size exponent,
/// flags (bit 0 tracked, bit 1 snapshot), commit, first transaction, span,
/// timestamp. `None` when the fields do not make a header.
fn build_header(spec: [u8; HEADER_SPEC_LEN]) -> Option<Header> {
    let mut reader = Reader::new(&spec);
    let exponent = reader.u8()?;
    let flags = reader.u8()?;
    let commit = reader.u32()?;
    let first = reader.u16()?;
    let span = reader.u16()?;
    let millis = reader.u32()?;
    let page_size = PageSize::new(512_u32.checked_shl(u32::from(exponent & 7))?)?;
    let min = if flags & 2 == 0 {
        Txid::new(u64::from(first).max(1))?
    } else {
        Txid::MIN
    };
    let max = Txid::new(min.get().saturating_add(u64::from(span)))?;
    let tracked = flags & 1 != 0;
    Header::new(HeaderFields {
        tracked,
        page_size,
        commit,
        txids: TxidRange::new(min, max)?,
        timestamp: Timestamp::from_unix_millis(i64::from(millis)),
        pre_apply: (tracked && min != Txid::MIN).then_some(Checksum::EMPTY),
        wal: None,
        node_id: 0,
    })
    .ok()
}

/// The compact description of `header`, for seeding: the inverse of the
/// builder where the header fits its ranges (the timestamp is kept modulo
/// 2^32; the decoders never read it).
#[must_use]
pub fn header_spec(header: &Header) -> Option<[u8; HEADER_SPEC_LEN]> {
    let fields = header.fields();
    let exponent = u8::try_from(fields.page_size.get().trailing_zeros().checked_sub(9)?).ok()?;
    let snapshot = fields.txids.min() == Txid::MIN;
    let flags = u8::from(fields.tracked) | (u8::from(snapshot) << 1);
    let first = u16::try_from(fields.txids.min().get()).ok()?;
    let span = u16::try_from(
        fields
            .txids
            .max()
            .get()
            .checked_sub(fields.txids.min().get())?,
    )
    .ok()?;
    let millis = u32::try_from(fields.timestamp.unix_millis() & 0xffff_ffff).ok()?;
    let mut spec = Vec::with_capacity(HEADER_SPEC_LEN);
    spec.push(exponent);
    spec.push(flags);
    spec.extend(fields.commit.to_be_bytes());
    spec.extend(first.to_be_bytes());
    spec.extend(span.to_be_bytes());
    spec.extend(millis.to_be_bytes());
    spec.try_into().ok()
}

fn check_index(index: &PageIndex, region: &[u8], header: &Header) {
    let entries = index.entries();
    // Every entry costs at least a few varint bytes, so more entries than
    // bytes means the decoder allocated for something the input did not hold.
    if entries.len() > region.len() {
        violation("more index entries than region bytes");
    }
    let mut previous = 0;
    for entry in entries {
        let pgno = entry.pgno().get();
        if pgno <= previous || pgno > header.commit() {
            violation("index entries are not ascending within the commit size");
        }
        previous = pgno;
        if index.get(entry.pgno()) != Some(entry) {
            violation("get does not find a listed entry");
        }
        if entry.offset().checked_add(entry.size()).is_none() {
            violation("an entry's end overflows u64");
        }
    }
}

fn check_frame(frame: &[u8], header: &Header, index: &PageIndex) {
    let mut out = Vec::new();
    for entry in index.entries().iter().take(8) {
        match decode_frame(frame, header, entry, &mut out) {
            Ok(pgno) => {
                if out.len() != header.page_size().bytes()
                    || pgno.get() > header.commit()
                    || pgno != entry.pgno()
                {
                    violation("decode_frame returned something other than the entry's page");
                }
            }
            Err(_) => {
                if out.iter().any(|byte| *byte != 0) {
                    violation("a rejected frame left page bytes in the output");
                }
            }
        }
    }
    // One page of output, with room for amortised growth and nothing more.
    if out.capacity() > header.page_size().bytes().saturating_mul(2) {
        violation("decode_frame allocated beyond one page");
    }
}

/// Decodes an index region and one frame against a header, in any state.
///
/// Input: [`HEADER_SPEC_LEN`] header bytes (see [`header_spec`]), a
/// big-endian `u16` region length, the region, then the frame bytes. Neither
/// decoder may panic, and what they accept must be consistent with the header.
pub fn index(data: &[u8]) {
    let mut reader = Reader::new(data);
    let Some(spec) = reader.array::<HEADER_SPEC_LEN>() else {
        return;
    };
    let Some(header) = build_header(spec) else {
        return;
    };
    let Some(region_len) = reader.u16() else {
        return;
    };
    let region = reader
        .take(usize::from(region_len))
        .unwrap_or_else(|| reader.rest());
    let frame = reader.rest();
    if let Some(tail) = region.last_chunk::<INDEX_TAIL_LEN>() {
        std::hint::black_box(PageIndex::region_len(tail));
    }
    if let Ok(index) = PageIndex::decode(region, &header) {
        check_index(&index, region, &header);
        check_frame(frame, &header, &index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_round_trips_through_the_builder() {
        let spec = [3, 3, 0, 0, 0, 9, 0, 1, 0, 4, 0, 0, 0, 7];
        let header = build_header(spec).expect("a header");
        assert_eq!(header_spec(&header), Some(spec));
        assert_eq!(header.page_size().get(), 4096);
        assert_eq!(header.commit(), 9);
    }

    #[test]
    fn short_and_garbage_input_do_not_panic() {
        index(&[]);
        index(&[0; HEADER_SPEC_LEN]);
        index(&[0xff; 64]);
    }

    #[test]
    fn an_empty_region_is_an_error_not_a_panic() {
        let mut data = vec![3, 3, 0, 0, 0, 9, 0, 1, 0, 4, 0, 0, 0, 7];
        data.extend([0, 0]);
        index(&data);
    }
}
