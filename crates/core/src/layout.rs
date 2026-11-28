//! Where a replica's files live: levels, object keys and listing entries.
//!
//! A key is the object's name relative to the replica prefix, exactly as
//! Litestream's S3 client writes it: four lowercase hex digits of level, a
//! slash, then the sixteen-digit range name (`0009/0000000000000001-0000000000000005.ltx`).
//! Go's file replica spells the level in decimal under an `ltx/` directory
//! instead (`ltx/9/…`); that mapping belongs to the directory store, not to the
//! key, so the key stays one thing for every backend.
//!
//! Go's own parser accepts a zero transaction id and a reversed range; this
//! one does not, because [`Txid`] cannot hold zero and [`TxidRange`] cannot be
//! reversed. A name Go would list but this cannot represent is an unrecognized name
//! to the stores: skipped and counted, never half-understood. A store's own
//! scratch file ([`is_scratch_name`]) is neither an object nor unrecognized.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use lsr_ltx::{ParseTxidError, Timestamp, Txid, TxidRange};
use thiserror::Error;

/// A compaction level, 0 to 9. Level 9 holds snapshots.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Level(u8);

impl Level {
    /// The level that holds full snapshots.
    pub const SNAPSHOT: Self = Self(9);
    /// The level every sync writes first.
    pub const ZERO: Self = Self(0);
    /// The level whose slot in the staging directory holds the files a local course staged
    /// before any boot listed the replica (`crate::actor`'s `local` module). Staging holds
    /// level 0 otherwise, and on a replica this is an ordinary compaction level.
    pub(crate) const LOCAL_COURSE: Self = Self(8);
    /// The first compaction level.
    pub(crate) const ONE: Self = Self(1);
    /// The compaction levels, 1 to 8, lowest first.
    pub(crate) const COMPACTION: [Self; 8] = [
        Self(1),
        Self(2),
        Self(3),
        Self(4),
        Self(5),
        Self(6),
        Self(7),
        Self(8),
    ];
    /// Every level, lowest first.
    pub const ALL: [Self; 10] = [
        Self(0),
        Self(1),
        Self(2),
        Self(3),
        Self(4),
        Self(5),
        Self(6),
        Self(7),
        Self(8),
        Self(9),
    ];

    /// Returns `None` above 9, Go's highest level.
    #[must_use]
    pub const fn new(raw: u8) -> Option<Self> {
        if raw <= Self::SNAPSHOT.0 {
            Some(Self(raw))
        } else {
            None
        }
    }

    /// The level as a number.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Whether this is the snapshot level.
    #[must_use]
    pub const fn is_snapshot(self) -> bool {
        self.0 == Self::SNAPSHOT.0
    }
}

/// Decimal, the form Go's file replica uses for a directory name.
impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Orders ranges by first transaction, then last: the order of their names.
#[must_use]
pub fn cmp_range(a: TxidRange, b: TxidRange) -> Ordering {
    a.min().cmp(&b.min()).then_with(|| a.max().cmp(&b.max()))
}

/// Which end of a range a parse error is about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeSide {
    /// The first transaction.
    Min,
    /// The last transaction.
    Max,
}

impl fmt::Display for RangeSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Min => "first",
            Self::Max => "last",
        })
    }
}

/// Why a string is not an LTX object key.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ParseKeyError {
    /// There is no slash between the level and the file name.
    #[error("key has no level separator")]
    MissingLevelSeparator,
    /// The level is not exactly four characters.
    #[error("level is {found} bytes, want 4")]
    LevelWidth {
        /// The length found, in bytes.
        found: usize,
    },
    /// A level character is not one of `0-9` or `a-f`.
    #[error("level has non-hex byte {byte:#04x}")]
    LevelDigit {
        /// The offending byte.
        byte: u8,
    },
    /// The level is above 9.
    #[error("level {found} is above 9")]
    LevelRange {
        /// The level the digits spell.
        found: u16,
    },
    /// The name does not end in `.ltx`.
    #[error("file name does not end in .ltx")]
    MissingSuffix,
    /// The name has no dash between its two transaction ids.
    #[error("file name has no dash between transaction ids")]
    MissingTxidSeparator,
    /// One end of the range is not a transaction id.
    #[error("{side} transaction id: {source}")]
    Txid {
        /// Which end failed.
        side: RangeSide,
        /// What was wrong with it.
        source: ParseTxidError,
    },
    /// The first transaction is above the last.
    #[error("range runs backwards")]
    Reversed,
}

/// Parses `<min>-<max>.ltx`, the name of one file inside a level.
///
/// # Errors
/// Anything Go's filename pattern rejects, plus zero ids and reversed ranges.
pub fn parse_file_name(name: &str) -> Result<TxidRange, ParseKeyError> {
    let stem = name
        .strip_suffix(".ltx")
        .ok_or(ParseKeyError::MissingSuffix)?;
    let (min, max) = stem
        .split_once('-')
        .ok_or(ParseKeyError::MissingTxidSeparator)?;
    let min = Txid::from_str(min).map_err(|source| ParseKeyError::Txid {
        side: RangeSide::Min,
        source,
    })?;
    let max = Txid::from_str(max).map_err(|source| ParseKeyError::Txid {
        side: RangeSide::Max,
        source,
    })?;
    TxidRange::new(min, max).ok_or(ParseKeyError::Reversed)
}

/// The length of every file name: two sixteen-digit ids, a dash and `.ltx`.
const FILE_NAME_LEN: usize = 37;

/// Whether a name is a store's scratch file for an upload: a key's file name
/// followed by nothing (Go writes `<name>.ltx.tmp`) or by a dot and anything
/// (`<name>.ltx.<unique>.tmp`), then `.tmp`. A scratch file is a store's own
/// bookkeeping, not an unrecognized name, and never an object.
#[must_use]
pub fn is_scratch_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".tmp") else {
        return false;
    };
    let (Some(key), Some(rest)) = (stem.get(..FILE_NAME_LEN), stem.get(FILE_NAME_LEN..)) else {
        return false;
    };
    parse_file_name(key).is_ok() && (rest.is_empty() || rest.starts_with('.'))
}

/// The file name for a range: `<min>-<max>.ltx`.
#[must_use]
pub fn file_name(range: TxidRange) -> String {
    format!("{range}.ltx")
}

/// The name of one LTX object: a level and the range it covers.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileKey {
    level: Level,
    range: TxidRange,
}

impl FileKey {
    /// Pairs a level with a range.
    #[must_use]
    pub const fn new(level: Level, range: TxidRange) -> Self {
        Self { level, range }
    }

    /// The level-0 file of the one transaction `txid`.
    #[must_use]
    pub const fn staged(txid: Txid) -> Self {
        Self::new(Level::ZERO, TxidRange::single(txid))
    }

    /// The snapshot of the database at `txid`: level 9, transactions 1
    /// through `txid`.
    #[must_use]
    pub const fn snapshot(txid: Txid) -> Self {
        Self::new(Level::SNAPSHOT, TxidRange::from_start(txid))
    }

    /// The level.
    #[must_use]
    pub const fn level(&self) -> Level {
        self.level
    }

    /// The transactions the file covers.
    #[must_use]
    pub const fn range(&self) -> TxidRange {
        self.range
    }

    /// The name inside the level: `<min>-<max>.ltx`.
    #[must_use]
    pub fn file_name(&self) -> String {
        file_name(self.range)
    }
}

impl Ord for FileKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.level
            .cmp(&other.level)
            .then_with(|| cmp_range(self.range, other.range))
    }
}

impl PartialOrd for FileKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Go's S3 key relative to the prefix: `%04x/<min>-<max>.ltx`.
impl fmt::Display for FileKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04x}/{}.ltx", self.level.get(), self.range)
    }
}

impl fmt::Debug for FileKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileKey({self})")
    }
}

impl FromStr for FileKey {
    type Err = ParseKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (level, name) = s
            .split_once('/')
            .ok_or(ParseKeyError::MissingLevelSeparator)?;
        if level.len() != 4 {
            return Err(ParseKeyError::LevelWidth { found: level.len() });
        }
        let mut raw: u16 = 0;
        for byte in level.bytes() {
            let digit = match byte {
                b'0'..=b'9' => byte.wrapping_sub(b'0'),
                b'a'..=b'f' => byte.wrapping_sub(b'a').wrapping_add(10),
                _ => return Err(ParseKeyError::LevelDigit { byte }),
            };
            raw = (raw << 4) | u16::from(digit);
        }
        let level = u8::try_from(raw)
            .ok()
            .and_then(Level::new)
            .ok_or(ParseKeyError::LevelRange { found: raw })?;
        Ok(Self::new(level, parse_file_name(name)?))
    }
}

/// What a store knows about one committed object.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FileInfo {
    /// The object's name.
    pub key: FileKey,
    /// Its length in bytes.
    pub size: u64,
    /// What the listing says about when the file was created: exact, or only
    /// the backend's own modification time. Readers that decide by time take
    /// [`FileInfo::created_at`] or ask the store for [`CreatedAt`].
    pub listed: ListedTime,
}

impl FileInfo {
    /// The exact stored created-at time the listing carried, when it did.
    #[must_use]
    pub const fn created_at(&self) -> Option<CreatedAt> {
        match self.listed {
            ListedTime::Created(created_at) => Some(created_at),
            ListedTime::LastModified(_) => None,
        }
    }
}

/// The time a listing attaches to a file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ListedTime {
    /// Exact: what the writer recorded as created, read with the listing at no extra
    /// request.
    Created(CreatedAt),
    /// The backend's last-modified time. It is not a created-at time: it can differ from
    /// the stored one by any writer clock lead, so no reader decides by it.
    LastModified(Timestamp),
}

/// The time a writer recorded as a file's creation, with where it came from. Only stores make
/// one, so a reader cannot pass a `LastModified` off as exact.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CreatedAt {
    at: Timestamp,
    origin: CreatedAtSource,
}

impl CreatedAt {
    /// A created-at time read from the object's metadata.
    #[must_use]
    pub const fn metadata(at: Timestamp) -> Self {
        Self {
            at,
            origin: CreatedAtSource::Metadata,
        }
    }

    /// A created-at time that is the file's modification time, as a file store keeps it.
    #[must_use]
    pub const fn mtime(at: Timestamp) -> Self {
        Self {
            at,
            origin: CreatedAtSource::Mtime,
        }
    }

    /// The listed last-modified time standing in for metadata that is absent.
    #[must_use]
    pub const fn fallback(last_modified: Timestamp) -> Self {
        Self {
            at: last_modified,
            origin: CreatedAtSource::Fallback,
        }
    }

    /// The instant as recorded. What it says about the database's history
    /// depends on the file's level and on [`Self::origin`]: it may be the
    /// newest commit, a snapshot's write or an object's upload. A decision
    /// about when a transaction committed reads [`Stamp::of`] instead (a
    /// source guard holds the callers of this to an allowlist).
    #[must_use]
    pub const fn at(self) -> Timestamp {
        self.at
    }

    /// Where it was read from.
    #[must_use]
    pub const fn origin(self) -> CreatedAtSource {
        self.origin
    }

    /// The created-at time as an instant a writer set, or `None` for one at or before
    /// the Unix epoch: that is a writer that recorded no created-at time (Go leaves the
    /// field zero), never a file created in 1970, so nothing may age it.
    #[must_use]
    pub const fn non_zero(self) -> Option<NonZeroCreatedAt> {
        if self.at.unix_millis() > 0 {
            Some(NonZeroCreatedAt(self.at))
        } else {
            None
        }
    }
}

/// A created-at time that is a non-zero creation time: after the Unix epoch. Only
/// [`CreatedAt::non_zero`] makes one, so code that ages a file by its created-at time
/// cannot be handed the zero a writer leaves when it records no created-at time.
///
/// ```compile_fail
/// # use lsr_core::NonZeroCreatedAt;
/// # use lsr_ltx::Timestamp;
/// let _ = NonZeroCreatedAt(Timestamp::from_unix_millis(0));
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct NonZeroCreatedAt(Timestamp);

impl NonZeroCreatedAt {
    /// The instant.
    #[must_use]
    pub const fn at(self) -> Timestamp {
        self.0
    }
}

/// Where a [`CreatedAt`] came from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CreatedAtSource {
    /// The object's stored metadata.
    Metadata,
    /// The file's modification time.
    Mtime,
    /// The last-modified time, because no metadata was stored.
    Fallback,
}

/// What a file's created-at time says about when its newest transaction first
/// reached level 0 (its commit, below). The answer depends on two facts the
/// time alone does not carry, the file's level and where the time was read,
/// so only [`Stamp::of`] makes one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dating {
    /// The commit as the engine first saw it: a level 0-8 file with its stored created-at
    /// time. A level-0 file is stamped by a clock read after the committed read of its sync
    /// (`Env::clock`), and compaction gives its output its newest input's time (here and in
    /// Go), so the time is at or after every commit the file carries (no `AsOf` applies a
    /// commit made after its instant). It is when the transaction first reached staging, not
    /// when SQLite committed it: a commit made while the engine was down is dated when the
    /// engine came back, so `AsOf(t)` is the newest state the engine had staged before `t`.
    Commit,
    /// At or after the commit, by an unknown margin: a snapshot (stamped when
    /// written), or any file whose store lost the created-at time and reports
    /// the object's last-modified time instead ([`CreatedAtSource::Fallback`]:
    /// a writer before Go v0.5.1, or a bucket copy that dropped metadata).
    NotBeforeCommit,
    /// The writer recorded no time (zero or before the epoch).
    Unset,
}

/// A created-at time together with what it dates. Planner decisions read
/// times only through this, so a write time or an upload time cannot answer
/// a question about when a transaction committed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stamp {
    at: Timestamp,
    dating: Dating,
}

impl Stamp {
    /// The provenance table: what `created_at`, stored for a file at `level`,
    /// dates. A file store's modification time counts as stored, because it
    /// is how such a store keeps the created-at time (Go sets it the same
    /// way); a copy that did not preserve it cannot be told apart.
    #[must_use]
    pub const fn of(level: Level, created_at: CreatedAt) -> Self {
        let dating = if created_at.at.unix_millis() <= 0 {
            Dating::Unset
        } else if level.is_snapshot() {
            Dating::NotBeforeCommit
        } else {
            match created_at.origin {
                CreatedAtSource::Metadata | CreatedAtSource::Mtime => Dating::Commit,
                CreatedAtSource::Fallback => Dating::NotBeforeCommit,
            }
        };
        Self {
            at: created_at.at,
            dating,
        }
    }

    /// What the time dates.
    #[must_use]
    pub const fn dating(self) -> Dating {
        self.dating
    }

    /// Whether the file was recorded as created strictly before `instant`:
    /// `AsOf` eligibility, the comparison Go makes. Sound for every dating,
    /// since no time precedes the commit it bounds; an unset time counts as
    /// before every later instant, as Go's zero time does.
    #[must_use]
    pub const fn before(self, instant: Timestamp) -> bool {
        self.at.unix_millis() < instant.unix_millis()
    }

    /// Whether the file's newest transaction committed at or after `instant`,
    /// when the time can tell: only a [`Dating::Commit`] answers.
    #[must_use]
    pub const fn commit_at_or_after(self, instant: Timestamp) -> Option<bool> {
        match self.dating {
            Dating::Commit => Some(self.at.unix_millis() >= instant.unix_millis()),
            Dating::NotBeforeCommit | Dating::Unset => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn only_a_created_at_after_the_epoch_is_real() {
        let ms = Timestamp::from_unix_millis;
        for origin in [CreatedAt::metadata, CreatedAt::mtime, CreatedAt::fallback] {
            for unset in [i64::MIN, -1, 0] {
                assert_eq!(origin(ms(unset)).non_zero(), None, "{unset}");
            }
            for set in [1, 1_700_000_000_000, i64::MAX] {
                assert_eq!(
                    origin(ms(set)).non_zero().map(NonZeroCreatedAt::at),
                    Some(ms(set))
                );
            }
        }
        let (early, late) = (
            CreatedAt::mtime(ms(5)).non_zero(),
            CreatedAt::metadata(ms(6)).non_zero(),
        );
        assert!(
            early < late,
            "non-zero created-at times order by instant, whatever their origin"
        );
    }

    #[test]
    fn a_snapshot_key_is_level_nine_from_the_first_transaction() {
        for max in [1, 2, u64::MAX] {
            let key = FileKey::snapshot(Txid::new(max).unwrap());
            assert_eq!(key, FileKey::new(Level::SNAPSHOT, range(1, max)));
        }
        assert_eq!(
            FileKey::snapshot(Txid::new(0x2a).unwrap()).to_string(),
            "0009/0000000000000001-000000000000002a.ltx"
        );
    }

    fn range(min: u64, max: u64) -> TxidRange {
        TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap()
    }

    #[test]
    fn scratch_names_are_a_key_name_plus_tmp() {
        let name = file_name(range(3, 9));
        assert_eq!(name.len(), FILE_NAME_LEN);
        for scratch in [
            format!("{name}.tmp"),
            format!("{name}.1-0.tmp"),
            format!("{name}.4242-17.tmp"),
        ] {
            assert!(is_scratch_name(&scratch), "{scratch}");
            assert!(parse_file_name(&scratch).is_err(), "{scratch}");
        }
        for other in [
            name.clone(),
            format!("{name}.bak"),
            format!("{name}x.tmp"),
            ".tmp".to_owned(),
            "README.tmp".to_owned(),
            "0000000000000002-0000000000000001.ltx.tmp".to_owned(),
        ] {
            assert!(!is_scratch_name(&other), "{other}");
        }
    }

    #[test]
    fn debug_and_the_range_side_print_what_a_log_line_needs() {
        let key = FileKey::new(Level::SNAPSHOT, range(1, 0xab));
        assert_eq!(
            format!("{key:?}"),
            "FileKey(0009/0000000000000001-00000000000000ab.ltx)"
        );
        assert_eq!(RangeSide::Min.to_string(), "first");
        assert_eq!(RangeSide::Max.to_string(), "last");
    }

    #[test]
    fn display_is_the_s3_key() {
        let key = FileKey::new(Level::SNAPSHOT, range(1, 0xab));
        assert_eq!(
            key.to_string(),
            "0009/0000000000000001-00000000000000ab.ltx"
        );
        assert_eq!(key.file_name(), "0000000000000001-00000000000000ab.ltx");
    }

    #[test]
    fn parse_accepts_every_level_and_round_trips() {
        for level in Level::ALL {
            let key = FileKey::new(level, range(3, 9));
            assert_eq!(key.to_string().parse::<FileKey>(), Ok(key));
        }
    }

    #[test]
    fn parse_rejection_table() {
        let min = "0000000000000001";
        let max = "0000000000000002";
        let cases: Vec<(String, ParseKeyError)> = vec![
            (
                format!("0001{min}-{max}.ltx"),
                ParseKeyError::MissingLevelSeparator,
            ),
            (
                format!("001/{min}-{max}.ltx"),
                ParseKeyError::LevelWidth { found: 3 },
            ),
            (
                format!("00001/{min}-{max}.ltx"),
                ParseKeyError::LevelWidth { found: 5 },
            ),
            (
                format!("000A/{min}-{max}.ltx"),
                ParseKeyError::LevelDigit { byte: b'A' },
            ),
            (
                format!("000g/{min}-{max}.ltx"),
                ParseKeyError::LevelDigit { byte: b'g' },
            ),
            (
                format!("000a/{min}-{max}.ltx"),
                ParseKeyError::LevelRange { found: 10 },
            ),
            (
                format!("0100/{min}-{max}.ltx"),
                ParseKeyError::LevelRange { found: 256 },
            ),
            (
                format!("ffff/{min}-{max}.ltx"),
                ParseKeyError::LevelRange { found: 0xffff },
            ),
            (format!("0001/{min}-{max}"), ParseKeyError::MissingSuffix),
            (
                format!("0001/{min}-{max}.ltx.tmp"),
                ParseKeyError::MissingSuffix,
            ),
            (
                format!("0001/{min}{max}.ltx"),
                ParseKeyError::MissingTxidSeparator,
            ),
            (format!("0001/{max}-{min}.ltx"), ParseKeyError::Reversed),
            (
                "0001/0000000000000000-0000000000000002.ltx".to_owned(),
                ParseKeyError::Txid {
                    side: RangeSide::Min,
                    source: ParseTxidError::Zero,
                },
            ),
            (
                "0001/000000000000000A-0000000000000002.ltx".to_owned(),
                ParseKeyError::Txid {
                    side: RangeSide::Min,
                    source: ParseTxidError::Digit { byte: b'A' },
                },
            ),
            (
                "0001/0000000000000001-2.ltx".to_owned(),
                ParseKeyError::Txid {
                    side: RangeSide::Max,
                    source: ParseTxidError::Length { found: 1 },
                },
            ),
        ];
        for (input, want) in cases {
            assert_eq!(input.parse::<FileKey>(), Err(want), "{input}");
        }
    }

    #[test]
    fn keys_order_by_level_then_range() {
        let a = FileKey::new(Level::ZERO, range(5, 5));
        let b = FileKey::new(Level::ZERO, range(5, 7));
        let c = FileKey::new(Level::ZERO, range(6, 6));
        let d = FileKey::new(Level::SNAPSHOT, range(1, 1));
        assert!(a < b && b < c && c < d);
    }

    #[test]
    fn level_stops_at_nine() {
        assert_eq!(Level::new(9), Some(Level::SNAPSHOT));
        assert_eq!(Level::new(10), None);
        assert!(Level::SNAPSHOT.is_snapshot() && !Level::ZERO.is_snapshot());
        assert_eq!(Level::SNAPSHOT.to_string(), "9");
    }

    proptest! {
        #[test]
        fn key_text_round_trips(level in 0u8..=9, a in 1u64.., b in 1u64..) {
            let (min, max) = if a <= b { (a, b) } else { (b, a) };
            let key = FileKey::new(Level::new(level).unwrap(), range(min, max));
            prop_assert_eq!(key.to_string().parse::<FileKey>(), Ok(key));
        }

        #[test]
        fn arbitrary_text_never_panics(s in ".{0,60}") {
            if let Ok(key) = s.parse::<FileKey>() {
                prop_assert_eq!(key.to_string(), s);
            }
        }
    }
}
