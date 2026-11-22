//! A takeover record: an operator's word that this database may replicate
//! over what the replica holds, through one transaction.
//!
//! At boot, every replica file above the newest staged file must be vouched
//! for before the engine builds on it (`Actor::boot`). The live WAL vouches
//! when the replica's newest level-0 file is a range it still holds with the
//! same pages: the engine continues from it. Nothing else local can tell this
//! database's own files from another replicator's: a staging directory put
//! back from a backup, a VM restored to an older image, a wiped meta
//! directory after the WAL restarted, and a second writer on the prefix all
//! look alike. So without the WAL's word the engine stops with
//! `OtherWriter`, and only a takeover record covering the replica's newest
//! transaction lets it go on as it used to: a snapshot above those files.
//!
//! `lsr take-over <db>` writes one through the replica's newest transaction,
//! and a restore writes one through the transaction it restored, so a
//! container that restores at start never stops. A record covers only what
//! the replica held when it was written: another writer's file above it
//! stops the engine again.
//!
//! The record is local state Go never reads, beside the stop record
//! (`crate::stop_record`); both are fields-in-waiting of the engine journal.
//! The stored form is one line, parsed strictly:
//! `lsr-takeover 1 <through>\n`, `<through>` as 16 lower-case hex digits.

use lsr_ltx::Txid;
use thiserror::Error;

use crate::stop_record::txid_word;

const MAGIC: &str = "lsr-takeover";
const VERSION: &str = "1";
/// The longest stored form: the words, two spaces, 16 digits and a newline.
const MAX_LEN: usize = 48;

/// An operator's word that this database may replicate over the replica's
/// files through `through`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TakeoverRecord {
    through: Txid,
}

/// Why stored bytes are not a [`TakeoverRecord`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum TakeoverRecordError {
    /// Longer than any record, or not UTF-8.
    #[error("the takeover record is not a line of text of at most {MAX_LEN} bytes")]
    NotText,
    /// The line does not end in a newline: a write cut short.
    #[error("the takeover record is not one whole line")]
    Unterminated,
    /// The first word is not the record's.
    #[error("the takeover record does not start with {MAGIC}")]
    Magic,
    /// A version this code does not read.
    #[error("the takeover record has a version this code does not read")]
    Version,
    /// The transaction is not 16 lower-case hex digits naming a transaction.
    #[error("the takeover record's transaction does not parse")]
    Txid,
    /// Words missing or left over.
    #[error("the takeover record has the wrong number of words")]
    Words,
}

impl TakeoverRecord {
    /// The record of a takeover through `through`.
    #[must_use]
    pub const fn through(through: Txid) -> Self {
        Self { through }
    }

    /// The newest transaction the takeover covers.
    #[must_use]
    pub const fn txid(self) -> Txid {
        self.through
    }

    /// Whether the takeover lets a boot build above a replica whose newest
    /// transaction is `tip`.
    #[must_use]
    pub fn covers(self, tip: Txid) -> bool {
        tip <= self.through
    }

    /// The stored form.
    #[must_use]
    pub fn encode(self) -> String {
        format!("{MAGIC} {VERSION} {:016x}\n", self.through.get())
    }

    /// Parses the stored form; nothing else is a record.
    ///
    /// # Errors
    /// [`TakeoverRecordError`], naming the first thing that is wrong.
    pub fn decode(bytes: &[u8]) -> Result<Self, TakeoverRecordError> {
        if bytes.len() > MAX_LEN {
            return Err(TakeoverRecordError::NotText);
        }
        let text = std::str::from_utf8(bytes).map_err(|_utf8| TakeoverRecordError::NotText)?;
        let line = text
            .strip_suffix('\n')
            .ok_or(TakeoverRecordError::Unterminated)?;
        let mut words = line.split(' ');
        let mut next = || words.next().ok_or(TakeoverRecordError::Words);
        if next()? != MAGIC {
            return Err(TakeoverRecordError::Magic);
        }
        if next()? != VERSION {
            return Err(TakeoverRecordError::Version);
        }
        let through = txid_word(next()?).ok_or(TakeoverRecordError::Txid)?;
        if words.next().is_some() {
            return Err(TakeoverRecordError::Words);
        }
        Ok(Self { through })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn every_record_round_trips(raw in 1..=u64::MAX) {
            let record = TakeoverRecord::through(Txid::new(raw).unwrap());
            prop_assert_eq!(TakeoverRecord::decode(record.encode().as_bytes()), Ok(record));
        }

        #[test]
        fn a_cut_record_never_decodes(raw in 1..=u64::MAX, cut in 0usize..48) {
            let stored = TakeoverRecord::through(Txid::new(raw).unwrap()).encode();
            let cut = cut.min(stored.len().saturating_sub(1));
            prop_assert!(TakeoverRecord::decode(stored.as_bytes().get(..cut).unwrap_or_default()).is_err());
        }

        #[test]
        fn arbitrary_bytes_decode_only_to_what_encodes_back(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Ok(record) = TakeoverRecord::decode(&bytes) {
                let stored = record.encode();
                prop_assert_eq!(stored.as_bytes(), bytes.as_slice());
            }
        }
    }

    #[test]
    fn every_error_has_bytes_that_produce_it() {
        let cases: [(&[u8], TakeoverRecordError); 8] = [
            (&[b'x'; 49], TakeoverRecordError::NotText),
            (
                b"lsr-takeover 1 000000000000000\xff\n",
                TakeoverRecordError::NotText,
            ),
            (
                b"lsr-takeover 1 000000000000002a",
                TakeoverRecordError::Unterminated,
            ),
            (
                b"lsr-takeovr 1 000000000000002a\n",
                TakeoverRecordError::Magic,
            ),
            (
                b"lsr-takeover 2 000000000000002a\n",
                TakeoverRecordError::Version,
            ),
            (
                b"lsr-takeover 1 0000000000000000\n",
                TakeoverRecordError::Txid,
            ),
            (
                b"lsr-takeover 1 000000000000002a x\n",
                TakeoverRecordError::Words,
            ),
            (b"lsr-takeover 1\n", TakeoverRecordError::Words),
        ];
        for (bytes, want) in cases {
            assert_eq!(TakeoverRecord::decode(bytes), Err(want), "{bytes:?}");
        }
    }

    #[test]
    fn the_stored_form_is_pinned_and_covers_up_to_its_transaction() {
        let record = TakeoverRecord::through(Txid::new(0x2a).unwrap());
        assert_eq!(record.encode(), "lsr-takeover 1 000000000000002a\n");
        assert!(record.covers(Txid::new(0x2a).unwrap()));
        assert!(record.covers(Txid::MIN));
        assert!(!record.covers(Txid::new(0x2b).unwrap()));
    }
}
