//! What a stop for another writer leaves for the next process.
//!
//! The engine stops by default when it finds another writer's files on the
//! replica (`OtherWriter`). That stop lived in memory only, so a supervisor's
//! restart undid it: boot cannot tell another writer's files above the newest
//! staged file from its own staging directory put back from a backup, takes
//! them as `BehindRemote`, and snapshots above them, so the replica's one
//! history interleaves two databases.
//!
//! A [`StopRecord`] is the stop made durable. The actor sets it when it stops
//! for another writer ([`crate::Actor::stop_record`]); the driver stores it in
//! the meta directory and hands it back to the next actor
//! ([`crate::Actor::remember_stop`]). It names `ours`, the newest transaction
//! this engine had staged or confirmed on the replica when it stopped: the
//! position the stop judged the other writer's files against. A replica whose
//! newest transaction is above it still holds them, and the boot stops again
//! ([`StopRecord::still_stopped`]). Ids acknowledged before a bucket rollback
//! took them are left out on purpose: another writer's file may hold one now,
//! and ids alone cannot tell it from this engine's own. The cost is a stop
//! that holds when this engine's own lost files reappear above its staging,
//! which an operator clears. Once an operator
//! has removed them the replica ends at or below `ours`, the boot clears the
//! record and replicates as before. This works whatever happened to the
//! staging directory in between, which the boot's own judgement cannot.
//!
//! This is the first fact the engine keeps across processes. The engine
//! journal (`docs/notes/2026-11-22/journal-design-v4.md` on the analysis
//! branch) is designed to hold such facts in one file with one write
//! discipline; when it lands, the record becomes one of its fields and this
//! codec its migration source.
//!
//! The stored form is one line of text, parsed strictly, so a torn or edited
//! file is an error and never a missing stop:
//! `lsr-stop 1 other-writer <ours>\n`, with `<ours>` the transaction id as 16
//! lower-case hex digits, or `none`.

use lsr_ltx::Txid;
use thiserror::Error;

/// The stored form's first word.
const MAGIC: &str = "lsr-stop";
/// The one version this code writes and reads.
const VERSION: &str = "1";
/// The one cause a record names.
const OTHER_WRITER: &str = "other-writer";
/// The longest stored form, exactly: the words, three spaces, 16 digits and a
/// newline.
const MAX_LEN: usize = 41;

/// A stop for another writer, as the next process must see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopRecord {
    /// The newest transaction this engine had when it stopped.
    ours: Option<Txid>,
}

/// Why stored bytes are not a [`StopRecord`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum StopRecordError {
    /// Longer than any record, or not UTF-8.
    #[error("the stop record is not a line of text of at most {MAX_LEN} bytes")]
    NotText,
    /// The line does not end in a newline: a write cut short.
    #[error("the stop record is not one whole line")]
    Unterminated,
    /// The first word is not the record's.
    #[error("the stop record does not start with {MAGIC}")]
    Magic,
    /// A version this code does not read.
    #[error("the stop record has a version this code does not read")]
    Version,
    /// A cause this code does not know.
    #[error("the stop record names a cause this code does not know")]
    Cause,
    /// The transaction is neither 16 hex digits nor `none`.
    #[error("the stop record's transaction does not parse")]
    Txid,
    /// Words missing or left over.
    #[error("the stop record has the wrong number of words")]
    Words,
}

impl StopRecord {
    /// The record of a stop for another writer, taken when this engine's
    /// newest transaction was `ours`.
    #[must_use]
    pub const fn other_writer(ours: Option<Txid>) -> Self {
        Self { ours }
    }

    /// The newest transaction this engine had when it stopped.
    #[must_use]
    pub const fn ours(self) -> Option<Txid> {
        self.ours
    }

    /// Whether a boot that finds the replica's newest transaction at
    /// `replica` must stop again: the replica holds a transaction this
    /// engine never had, so the other writer's files are still there.
    #[must_use]
    pub fn still_stopped(self, replica: Option<Txid>) -> bool {
        replica > self.ours
    }

    /// The stored form.
    #[must_use]
    pub fn encode(self) -> String {
        let ours = self
            .ours
            .map_or_else(|| "none".to_owned(), |txid| format!("{:016x}", txid.get()));
        format!("{MAGIC} {VERSION} {OTHER_WRITER} {ours}\n")
    }

    /// Parses the stored form; nothing else is a record.
    ///
    /// # Errors
    /// [`StopRecordError`], naming the first thing that is wrong.
    pub fn decode(bytes: &[u8]) -> Result<Self, StopRecordError> {
        if bytes.len() > MAX_LEN {
            return Err(StopRecordError::NotText);
        }
        let text = std::str::from_utf8(bytes).map_err(|_utf8| StopRecordError::NotText)?;
        let line = text
            .strip_suffix('\n')
            .ok_or(StopRecordError::Unterminated)?;
        let mut words = line.split(' ');
        let mut next = || words.next().ok_or(StopRecordError::Words);
        if next()? != MAGIC {
            return Err(StopRecordError::Magic);
        }
        if next()? != VERSION {
            return Err(StopRecordError::Version);
        }
        if next()? != OTHER_WRITER {
            return Err(StopRecordError::Cause);
        }
        let ours = parse_ours(next()?)?;
        if words.next().is_some() {
            return Err(StopRecordError::Words);
        }
        Ok(Self { ours })
    }
}

fn parse_ours(word: &str) -> Result<Option<Txid>, StopRecordError> {
    if word == "none" {
        return Ok(None);
    }
    txid_word(word).map(Some).ok_or(StopRecordError::Txid)
}

/// A transaction id as the records store it: exactly 16 lower-case hex
/// digits, not zero.
pub(crate) fn txid_word(word: &str) -> Option<Txid> {
    let lower_hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    if word.len() != 16 || !word.chars().all(lower_hex) {
        return None;
    }
    Txid::new(u64::from_str_radix(word, 16).ok()?)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn txid(raw: u64) -> Option<Txid> {
        Txid::new(raw)
    }

    proptest! {
        #[test]
        fn every_record_round_trips(raw in proptest::option::of(1..=u64::MAX)) {
            let record = StopRecord::other_writer(raw.and_then(Txid::new));
            prop_assert_eq!(StopRecord::decode(record.encode().as_bytes()), Ok(record));
        }

        #[test]
        fn a_cut_record_never_decodes(raw in proptest::option::of(1..=u64::MAX), cut in 0usize..48) {
            let stored = StopRecord::other_writer(raw.and_then(Txid::new)).encode();
            let cut = cut.min(stored.len().saturating_sub(1));
            prop_assert!(StopRecord::decode(stored.as_bytes().get(..cut).unwrap_or_default()).is_err());
        }

        #[test]
        fn arbitrary_bytes_decode_only_to_what_encodes_back(bytes in proptest::collection::vec(any::<u8>(), 0..48)) {
            if let Ok(record) = StopRecord::decode(&bytes) {
                let stored = record.encode();
                prop_assert_eq!(stored.as_bytes(), bytes.as_slice());
            }
        }
    }

    #[test]
    fn every_error_has_bytes_that_produce_it() {
        let cases: [(&[u8], StopRecordError); 8] = [
            (&[b'x'; 42], StopRecordError::NotText),
            (
                b"lsr-stop 1 other-writer none\xff\n",
                StopRecordError::NotText,
            ),
            (
                b"lsr-stop 1 other-writer none",
                StopRecordError::Unterminated,
            ),
            (b"lsr-stap 1 other-writer none\n", StopRecordError::Magic),
            (b"lsr-stop 2 other-writer none\n", StopRecordError::Version),
            (b"lsr-stop 1 bug none\n", StopRecordError::Cause),
            (
                b"lsr-stop 1 other-writer 000000000000000A\n",
                StopRecordError::Txid,
            ),
            (
                b"lsr-stop 1 other-writer none extra\n",
                StopRecordError::Words,
            ),
        ];
        for (bytes, want) in cases {
            assert_eq!(StopRecord::decode(bytes), Err(want), "{bytes:?}");
        }
        assert_eq!(
            StopRecord::decode(b"lsr-stop 1 other-writer 0000000000000000\n"),
            Err(StopRecordError::Txid)
        );
        assert_eq!(
            StopRecord::decode(b"lsr-stop 1\n"),
            Err(StopRecordError::Words)
        );
    }

    #[test]
    fn the_stored_form_is_pinned() {
        assert_eq!(
            StopRecord::other_writer(txid(0x2a)).encode(),
            "lsr-stop 1 other-writer 000000000000002a\n"
        );
        assert_eq!(
            StopRecord::other_writer(None).encode(),
            "lsr-stop 1 other-writer none\n"
        );
    }

    /// The length bound is exact: the longest record decodes, one byte more never reaches
    /// the parser.
    #[test]
    fn the_longest_record_is_the_length_bound() {
        let longest = StopRecord::other_writer(txid(u64::MAX)).encode();
        assert_eq!(longest.len(), MAX_LEN);
        assert_eq!(
            StopRecord::decode(longest.as_bytes()),
            Ok(StopRecord::other_writer(txid(u64::MAX)))
        );
        let mut longer = longest.into_bytes();
        longer.insert(0, b' ');
        assert_eq!(StopRecord::decode(&longer), Err(StopRecordError::NotText));
    }

    #[test]
    fn a_record_names_the_transaction_it_was_taken_at() {
        assert_eq!(StopRecord::other_writer(txid(5)).ours(), txid(5));
        assert_eq!(StopRecord::other_writer(None).ours(), None);
    }

    #[test]
    fn a_replica_past_ours_is_still_another_writer() {
        let record = StopRecord::other_writer(txid(5));
        assert!(record.still_stopped(txid(6)));
        assert!(!record.still_stopped(txid(5)));
        assert!(!record.still_stopped(txid(1)));
        assert!(!record.still_stopped(None));
        let empty = StopRecord::other_writer(None);
        assert!(empty.still_stopped(txid(1)));
        assert!(!empty.still_stopped(None));
    }
}
