//! The `decode` target: the push decoder under any chunking.

use lsr_ltx::{Decoder, Encoder, Event, Header, PageNo, Verified};

use crate::bytes::{Reader, violation};

/// One event, owned so runs can be compared after the decoder is gone.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Header(Header),
    Page { pgno: PageNo, data: Vec<u8> },
}

impl From<Event<'_>> for Seen {
    fn from(event: Event<'_>) -> Self {
        match event {
            Event::Header(header) => Self::Header(header),
            Event::Page { pgno, data } => Self::Page {
                pgno,
                data: data.to_vec(),
            },
        }
    }
}

/// How a run ended. Errors are compared by their `Debug` text: `DecodeError`
/// is not `PartialEq`, and its variants carry everything that distinguishes them.
#[derive(Debug, PartialEq, Eq)]
enum End {
    Verified(Verified),
    Failed(String),
}

#[derive(Debug, PartialEq, Eq)]
struct Run {
    events: Vec<Seen>,
    end: End,
}

/// Feeds one chunk to the decoder, recording events. `Some` is the failure.
fn feed_chunk(decoder: &mut Decoder, chunk: &[u8], events: &mut Vec<Seen>) -> Option<End> {
    let mut rest = chunk;
    let mut budget = chunk.len().saturating_mul(2).saturating_add(8);
    while !rest.is_empty() {
        budget = budget
            .checked_sub(1)
            .unwrap_or_else(|| violation("feed keeps returning without consuming input"));
        let used = match decoder.feed(rest) {
            Ok((used, event)) => {
                if let Some(event) = event {
                    events.push(Seen::from(event));
                }
                used
            }
            Err(error) => return Some(End::Failed(format!("{error:?}"))),
        };
        rest = rest
            .get(used..)
            .unwrap_or_else(|| violation("feed consumed more than it was given"));
    }
    None
}

fn run(chunks: &[&[u8]]) -> Run {
    let mut decoder = Decoder::new();
    let mut events = Vec::new();
    for chunk in chunks {
        if let Some(end) = feed_chunk(&mut decoder, chunk, &mut events) {
            return Run { events, end };
        }
    }
    let end = match decoder.finish() {
        Ok(verified) => End::Verified(verified),
        Err(error) => End::Failed(format!("{error:?}")),
    };
    Run { events, end }
}

/// Cuts `file` at `points` (each reduced modulo the length, order and
/// repeats irrelevant); the chunks always concatenate to `file`.
fn chunks<'a>(file: &'a [u8], points: &[u16]) -> Vec<&'a [u8]> {
    let modulus = file.len().saturating_add(1);
    let mut cuts: Vec<usize> = points
        .iter()
        .map(|point| usize::from(*point).checked_rem(modulus).unwrap_or(0))
        .collect();
    cuts.sort_unstable();
    let mut out = Vec::new();
    let mut from = 0;
    for cut in cuts {
        out.extend(file.get(from..cut));
        from = cut;
    }
    out.extend(file.get(from..));
    out
}

/// A verified file, written again with the strict encoder, must verify again
/// with the same header and pages.
fn reencode(events: &[Seen], verified: &Verified) {
    let Some(Seen::Header(header)) = events.first() else {
        violation("a verified file produced no header event");
    };
    // The strict encoder never writes an empty database; the decoder accepts one.
    if header.commit() == 0 {
        return;
    }
    let mut encoder = Encoder::new(Vec::new(), *header)
        .unwrap_or_else(|error| violation(&format!("encoder refused a decoded header: {error:?}")));
    for event in events {
        if let Seen::Page { pgno, data } = event {
            encoder.write_page(*pgno, data).unwrap_or_else(|error| {
                violation(&format!("encoder refused a decoded page: {error:?}"))
            });
        }
    }
    let (bytes, _) = encoder
        .finish(verified.trailer().post_apply())
        .unwrap_or_else(|error| violation(&format!("encoder refused to finish: {error:?}")));
    let again = run(&[&bytes]);
    let End::Verified(second) = &again.end else {
        violation(&format!("re-encoded file does not verify: {:?}", again.end));
    };
    if again.events != events || second.page_count() != verified.page_count() {
        violation("re-encoded file decodes to different pages");
    }
}

/// Feeds one file to the decoder whole and in fuzz-chosen chunks.
///
/// Input: a mode byte (bit 7: feed one byte at a time; bits 0 to 2: how many
/// cut points follow), that many big-endian `u16` cut points, then the file.
/// Both feeds must produce the same events and the same ending; a file that
/// verifies must survive a re-encode.
pub fn decode(data: &[u8]) {
    let mut reader = Reader::new(data);
    let Some(mode) = reader.u8() else {
        return;
    };
    let cut_count = usize::from(mode & 0x07);
    let points: Vec<u16> = (0..cut_count).map_while(|_| reader.u16()).collect();
    let file = reader.rest();
    let whole = run(&[file]);
    let pieces: Vec<&[u8]> = if mode & 0x80 == 0 {
        chunks(file, &points)
    } else {
        file.chunks(1).collect()
    };
    let split = run(&pieces);
    if whole != split {
        violation(&format!(
            "chunking changed the result: whole {:?} ending {:?}, split {:?} ending {:?}",
            whole.events.len(),
            whole.end,
            split.events.len(),
            split.end
        ));
    }
    if let End::Verified(verified) = &whole.end {
        reencode(&whole.events, verified);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_concatenate_to_the_file() {
        let file: Vec<u8> = (0..=9).collect();
        for points in [&[][..], &[0], &[3, 3, 20], &[10, 1]] {
            let joined: Vec<u8> = chunks(&file, points).concat();
            assert_eq!(joined, file);
        }
    }

    #[test]
    fn garbage_and_empty_input_do_not_panic() {
        decode(&[]);
        decode(&[0x00]);
        decode(&[0x83, 0, 1, 0, 2, 0, 3, b'L', b'T', b'X', b'1']);
        decode(&[0x80; 300]);
    }
}
