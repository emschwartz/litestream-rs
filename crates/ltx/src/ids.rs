//! Identifiers that every layer shares.
//!
//! A transaction id is never zero on the wire: Go uses zero for "no
//! transaction", which `Option<Txid>` says in the type instead.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

/// A transaction id: a position in a replica's history, starting at 1.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Txid(NonZeroU64);

impl Txid {
    /// The first transaction id of every replica.
    pub const MIN: Self = Self(NonZeroU64::MIN);
    /// The largest representable transaction id.
    pub const MAX: Self = Self(NonZeroU64::MAX);

    /// Returns `None` for zero, which is not a transaction id.
    #[must_use]
    pub const fn new(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(nonzero) => Some(Self(nonzero)),
            None => None,
        }
    }

    /// The id as the integer stored in an LTX header.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The id that directly follows this one, or `None` at `Txid::MAX`.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(nonzero) => Some(Self(nonzero)),
            None => None,
        }
    }

    /// The id that directly precedes this one, or `None` at `Txid::MIN`.
    #[must_use]
    pub const fn prev(self) -> Option<Self> {
        match self.0.get().checked_sub(1) {
            Some(raw) => Self::new(raw),
            None => None,
        }
    }
}

/// Sixteen lowercase hex digits, the form used in object keys.
impl fmt::Display for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.get())
    }
}

impl fmt::Debug for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txid({self})")
    }
}

/// Why a string is not a transaction id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseTxidError {
    /// The string is not exactly sixteen bytes long.
    Length {
        /// The length found, in bytes.
        found: usize,
    },
    /// A byte is not one of `0-9` or `a-f`.
    Digit {
        /// The offending byte.
        byte: u8,
    },
    /// The digits spell zero.
    Zero,
}

impl fmt::Display for ParseTxidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Length { found } => write!(f, "transaction id is {found} bytes, want 16"),
            Self::Digit { byte } => write!(f, "transaction id has non-hex byte {byte:#04x}"),
            Self::Zero => f.write_str("transaction id is zero"),
        }
    }
}

impl std::error::Error for ParseTxidError {}

/// Parses the object-key form only: sixteen lowercase hex digits. Go writes
/// nothing else, and accepting uppercase would let two keys name one id.
impl FromStr for Txid {
    type Err = ParseTxidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 16 {
            return Err(ParseTxidError::Length { found: s.len() });
        }
        let mut raw: u64 = 0;
        for byte in s.bytes() {
            let digit = match byte {
                b'0'..=b'9' => byte.wrapping_sub(b'0'),
                b'a'..=b'f' => byte.wrapping_sub(b'a').wrapping_add(10),
                _ => return Err(ParseTxidError::Digit { byte }),
            };
            raw = (raw << 4) | u64::from(digit);
        }
        Self::new(raw).ok_or(ParseTxidError::Zero)
    }
}

/// A point in time as LTX stores it: milliseconds since the Unix epoch.
///
/// Signed because the header field is; nothing here reads a clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Wraps a millisecond count taken from a header or a store listing.
    #[must_use]
    pub const fn from_unix_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Milliseconds since the Unix epoch.
    #[must_use]
    pub const fn unix_millis(self) -> i64 {
        self.0
    }
}

/// Milliseconds in a day.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// A field of an RFC 3339 instant, named by a parse error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeField {
    /// Month, 01 to 12.
    Month,
    /// Day of the month, valid for that month and year.
    Day,
    /// Hour, 00 to 23.
    Hour,
    /// Minute, 00 to 59.
    Minute,
    /// Second, 00 to 59; a leap second 60 is not representable.
    Second,
    /// Hour of a numeric offset, 00 to 23.
    OffsetHour,
    /// Minute of a numeric offset, 00 to 59.
    OffsetMinute,
}

/// Why a string is not an RFC 3339 instant this crate reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampParseError {
    /// The string ends before the field that was expected next.
    Truncated {
        /// Byte position where more input was needed.
        at: usize,
    },
    /// A byte that is not an ASCII digit where a digit belongs.
    Digit {
        /// Byte position.
        at: usize,
    },
    /// A byte other than the fixed separator (`-`, `T`, `:`) at its position.
    Separator {
        /// Byte position.
        at: usize,
    },
    /// A field outside its range.
    Range {
        /// The field.
        field: TimeField,
    },
    /// A `.` with no digit after it.
    EmptyFraction,
    /// More than nine fractional digits.
    FractionTooLong,
    /// No `Z` and no numeric offset after the time.
    MissingZone,
    /// A zone that is neither `Z` nor `+HH:MM` or `-HH:MM`.
    Zone {
        /// Byte position.
        at: usize,
    },
    /// Bytes after the zone.
    Trailing {
        /// Byte position of the first extra byte.
        at: usize,
    },
}

impl fmt::Display for TimestampParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Truncated { at } => write!(f, "timestamp ends early at byte {at}"),
            Self::Digit { at } => write!(f, "timestamp has a non-digit at byte {at}"),
            Self::Separator { at } => write!(f, "timestamp has a wrong separator at byte {at}"),
            Self::Range { field } => write!(f, "timestamp {field:?} is out of range"),
            Self::EmptyFraction => f.write_str("timestamp has a '.' with no fractional digits"),
            Self::FractionTooLong => f.write_str("timestamp has more than 9 fractional digits"),
            Self::MissingZone => f.write_str("timestamp has no zone; end it with Z or +HH:MM"),
            Self::Zone { at } => write!(f, "timestamp zone at byte {at} is not Z or +HH:MM"),
            Self::Trailing { at } => write!(f, "timestamp has extra bytes from byte {at}"),
        }
    }
}

impl std::error::Error for TimestampParseError {}

/// Reads fixed-width ASCII fields from a string.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn bump(&mut self) {
        self.at = self.at.saturating_add(1);
    }

    fn separator(&mut self, want: u8) -> Result<(), TimestampParseError> {
        match self.peek() {
            Some(byte) if byte == want => {
                self.bump();
                Ok(())
            }
            Some(_) => Err(TimestampParseError::Separator { at: self.at }),
            None => Err(TimestampParseError::Truncated { at: self.at }),
        }
    }

    fn digit(&mut self) -> Result<i64, TimestampParseError> {
        let byte = self
            .peek()
            .ok_or(TimestampParseError::Truncated { at: self.at })?;
        if !byte.is_ascii_digit() {
            return Err(TimestampParseError::Digit { at: self.at });
        }
        self.bump();
        Ok(i64::from(byte.wrapping_sub(b'0')))
    }

    fn number(&mut self, width: usize) -> Result<i64, TimestampParseError> {
        let mut value: i64 = 0;
        for _ in 0..width {
            value = value.saturating_mul(10).saturating_add(self.digit()?);
        }
        Ok(value)
    }

    fn bounded(&mut self, field: TimeField, max: i64) -> Result<i64, TimestampParseError> {
        let value = self.number(2)?;
        (value <= max)
            .then_some(value)
            .ok_or(TimestampParseError::Range { field })
    }

    /// Up to nine digits after a `.`, as whole milliseconds (truncated).
    fn fraction_millis(&mut self) -> Result<i64, TimestampParseError> {
        let mut millis: i64 = 0;
        let mut seen: u32 = 0;
        while let Some(byte) = self.peek().filter(u8::is_ascii_digit) {
            if seen >= 9 {
                return Err(TimestampParseError::FractionTooLong);
            }
            if seen < 3 {
                millis = millis
                    .saturating_mul(10)
                    .saturating_add(i64::from(byte.wrapping_sub(b'0')));
            }
            seen = seen.saturating_add(1);
            self.bump();
        }
        if seen == 0 {
            return Err(TimestampParseError::EmptyFraction);
        }
        for _ in seen..3 {
            millis = millis.saturating_mul(10);
        }
        Ok(millis)
    }

    /// The zone as seconds east of UTC.
    fn zone_seconds(&mut self) -> Result<i64, TimestampParseError> {
        let sign: i64 = match self.peek() {
            None => return Err(TimestampParseError::MissingZone),
            Some(b'Z') => {
                self.bump();
                return Ok(0);
            }
            Some(b'+') => 1,
            Some(b'-') => -1,
            Some(_) => return Err(TimestampParseError::Zone { at: self.at }),
        };
        self.bump();
        let hours = self.bounded(TimeField::OffsetHour, 23)?;
        self.zone_colon()?;
        let minutes = self.bounded(TimeField::OffsetMinute, 59)?;
        Ok(sign.saturating_mul(
            hours
                .saturating_mul(3600)
                .saturating_add(minutes.saturating_mul(60)),
        ))
    }

    fn zone_colon(&mut self) -> Result<(), TimestampParseError> {
        match self.separator(b':') {
            Err(TimestampParseError::Separator { at }) => Err(TimestampParseError::Zone { at }),
            other => other,
        }
    }
}

const fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

const fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Hinnant's algorithm).
#[expect(
    clippy::arithmetic_side_effects,
    reason = "year is at most four digits here, so every intermediate is far below i64::MAX"
)]
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`]: (year, month, day).
#[expect(
    clippy::arithmetic_side_effects,
    reason = "days comes from an i64 of milliseconds, so it is below 1.1e11 and every intermediate is far below i64::MAX"
)]
const fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

impl Timestamp {
    /// Parses `YYYY-MM-DDTHH:MM:SS[.f{1,9}]` followed by `Z` or `+HH:MM` /
    /// `-HH:MM`, the shape Go's `time.RFC3339` reads. Fractions are truncated
    /// to milliseconds; years are 0000 to 9999; a leap second is refused.
    ///
    /// # Errors
    /// [`TimestampParseError`] naming the first thing that is wrong.
    pub fn parse_rfc3339(text: &str) -> Result<Self, TimestampParseError> {
        let mut cur = Cursor {
            bytes: text.as_bytes(),
            at: 0,
        };
        let year = cur.number(4)?;
        cur.separator(b'-')?;
        let month = cur.bounded(TimeField::Month, 12)?;
        if month == 0 {
            return Err(TimestampParseError::Range {
                field: TimeField::Month,
            });
        }
        cur.separator(b'-')?;
        let day = cur.number(2)?;
        if day == 0 || day > days_in_month(year, month) {
            return Err(TimestampParseError::Range {
                field: TimeField::Day,
            });
        }
        cur.separator(b'T')?;
        let hour = cur.bounded(TimeField::Hour, 23)?;
        cur.separator(b':')?;
        let minute = cur.bounded(TimeField::Minute, 59)?;
        cur.separator(b':')?;
        let second = cur.bounded(TimeField::Second, 59)?;
        let fraction = if cur.peek() == Some(b'.') {
            cur.bump();
            cur.fraction_millis()?
        } else {
            0
        };
        let offset = cur.zone_seconds()?;
        if cur.at < text.len() {
            return Err(TimestampParseError::Trailing { at: cur.at });
        }
        Ok(Self::from_parts(
            year, month, day, hour, minute, second, fraction, offset,
        ))
    }

    /// Fields already checked by the caller; years are four digits, so nothing
    /// here can overflow.
    #[expect(
        clippy::too_many_arguments,
        reason = "private constructor over the seven validated fields of one instant"
    )]
    fn from_parts(
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        minute: i64,
        second: i64,
        fraction: i64,
        offset: i64,
    ) -> Self {
        let seconds = days_from_civil(year, month, day)
            .saturating_mul(86_400)
            .saturating_add(hour.saturating_mul(3600))
            .saturating_add(minute.saturating_mul(60))
            .saturating_add(second)
            .saturating_sub(offset);
        Self(seconds.saturating_mul(1000).saturating_add(fraction))
    }
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`, always UTC with three fractional digits. A year
/// outside 0000 to 9999 prints with a sign and at least four digits
/// (`+10000`, `-0001`); [`Timestamp::parse_rfc3339`] does not read those back.
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.0.div_euclid(MILLIS_PER_DAY);
        let of_day = self.0.rem_euclid(MILLIS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        let (secs, millis) = (of_day / 1000, of_day % 1000);
        let sign = if year < 0 {
            "-"
        } else if year > 9999 {
            "+"
        } else {
            ""
        };
        write!(
            f,
            "{sign}{:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
            year.unsigned_abs(),
            secs / 3600,
            secs % 3600 / 60,
            secs % 60,
        )
    }
}

/// An inclusive, non-empty range of transaction ids: what one LTX file covers.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxidRange {
    min: Txid,
    max: Txid,
}

impl TxidRange {
    /// Returns `None` when `min` is above `max`.
    #[must_use]
    pub fn new(min: Txid, max: Txid) -> Option<Self> {
        (min <= max).then_some(Self { min, max })
    }

    /// The range holding exactly one transaction.
    #[must_use]
    pub const fn single(txid: Txid) -> Self {
        Self {
            min: txid,
            max: txid,
        }
    }

    /// The first transaction covered.
    #[must_use]
    pub const fn min(self) -> Txid {
        self.min
    }

    /// The last transaction covered.
    #[must_use]
    pub const fn max(self) -> Txid {
        self.max
    }

    /// Whether `txid` falls inside the range.
    #[must_use]
    pub fn contains(self, txid: Txid) -> bool {
        self.min <= txid && txid <= self.max
    }
}

/// `<min>-<max>`, the stem of an LTX object key.
impl fmt::Display for TxidRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.min, self.max)
    }
}

impl fmt::Debug for TxidRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxidRange({self})")
    }
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test models do calendar arithmetic on small bounded values"
)]
mod tests {
    use super::*;

    fn txid(raw: u64) -> Txid {
        Txid::new(raw).unwrap()
    }

    #[test]
    fn zero_is_not_a_txid() {
        assert_eq!(Txid::new(0), None);
        assert_eq!(
            "0000000000000000".parse::<Txid>(),
            Err(ParseTxidError::Zero)
        );
    }

    #[test]
    fn display_is_the_object_key_form() {
        assert_eq!(txid(1).to_string(), "0000000000000001");
        assert_eq!(Txid::MAX.to_string(), "ffffffffffffffff");
        assert_eq!(format!("{:?}", txid(0xab)), "Txid(00000000000000ab)");
    }

    #[test]
    fn parse_round_trips_the_bounds() {
        for id in [Txid::MIN, txid(0x1234_5678_9abc_def0), Txid::MAX] {
            assert_eq!(id.to_string().parse::<Txid>(), Ok(id));
        }
    }

    #[test]
    fn parse_rejects_everything_go_never_writes() {
        assert_eq!(
            "1".parse::<Txid>(),
            Err(ParseTxidError::Length { found: 1 })
        );
        assert_eq!(
            "00000000000000001".parse::<Txid>(),
            Err(ParseTxidError::Length { found: 17 })
        );
        assert_eq!(
            "000000000000000A".parse::<Txid>(),
            Err(ParseTxidError::Digit { byte: b'A' })
        );
        assert_eq!(
            "+000000000000001".parse::<Txid>(),
            Err(ParseTxidError::Digit { byte: b'+' })
        );
        assert_eq!(
            "00000000000000é".parse::<Txid>(),
            Err(ParseTxidError::Digit { byte: 0xc3 })
        );
    }

    #[test]
    fn timestamps_order_by_millis() {
        let early = Timestamp::from_unix_millis(-1);
        let late = Timestamp::from_unix_millis(1_795_000_000_000);
        assert!(early < late);
        assert_eq!(late.unix_millis(), 1_795_000_000_000);
    }

    /// Days since the epoch by walking the calendar one year and one month at
    /// a time: no formula shared with the code under test.
    fn model_days(year: i64, month: i64, day: i64) -> i64 {
        let leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let mut days = 0_i64;
        let mut y = 1970;
        while y < year {
            days += if leap(y) { 366 } else { 365 };
            y += 1;
        }
        while y > year {
            y -= 1;
            days -= if leap(y) { 366 } else { 365 };
        }
        let lengths = [
            31,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        days += lengths
            .iter()
            .take(usize::try_from(month - 1).unwrap())
            .sum::<i64>();
        days + day - 1
    }

    fn ts(text: &str) -> Timestamp {
        Timestamp::parse_rfc3339(text).unwrap()
    }

    fn err(text: &str) -> TimestampParseError {
        Timestamp::parse_rfc3339(text).unwrap_err()
    }

    #[test]
    fn known_instants_parse_and_print() {
        assert_eq!(ts("1970-01-01T00:00:00Z").unix_millis(), 0);
        // The stored time of a checked-in fixture file.
        let fixture = Timestamp::from_unix_millis(1_795_182_585_566);
        assert_eq!(fixture.to_string(), "2026-11-20T13:49:45.566Z");
        assert_eq!(ts("2026-11-20T13:49:45.566Z"), fixture);
        assert_eq!(
            ts("0000-01-01T00:00:00Z").to_string(),
            "0000-01-01T00:00:00.000Z"
        );
        assert_eq!(
            ts("9999-12-31T23:59:59.999Z").to_string(),
            "9999-12-31T23:59:59.999Z"
        );
        assert_eq!(
            Timestamp::from_unix_millis(-1).to_string(),
            "1969-12-31T23:59:59.999Z"
        );
    }

    #[test]
    fn offsets_fractions_and_truncation() {
        assert_eq!(ts("2024-03-01T01:30:00+01:30"), ts("2024-03-01T00:00:00Z"));
        assert_eq!(ts("2024-03-01T00:00:00-05:00"), ts("2024-03-01T05:00:00Z"));
        assert_eq!(ts("2024-03-01T00:00:00-00:00"), ts("2024-03-01T00:00:00Z"));
        assert_eq!(ts("2024-03-01T00:00:00.5Z").unix_millis() % 1000, 500);
        assert_eq!(ts("2024-03-01T00:00:00.12Z").unix_millis() % 1000, 120);
        assert_eq!(
            ts("2024-03-01T00:00:00.123999999Z").unix_millis() % 1000,
            123
        );
        assert_eq!(ts("2024-03-01T00:00:00.0009Z"), ts("2024-03-01T00:00:00Z"));
    }

    #[test]
    fn leap_days_by_rule() {
        ts("2000-02-29T00:00:00Z");
        ts("2024-02-29T00:00:00Z");
        for bad in [
            "1900-02-29T00:00:00Z",
            "2100-02-29T00:00:00Z",
            "2023-02-29T00:00:00Z",
        ] {
            assert_eq!(
                err(bad),
                TimestampParseError::Range {
                    field: TimeField::Day
                }
            );
        }
    }

    #[test]
    fn every_parse_error_is_produced() {
        use TimestampParseError as E;
        assert_eq!(err(""), E::Truncated { at: 0 });
        assert_eq!(err("2024-01-01T00:00"), E::Truncated { at: 16 });
        assert_eq!(err("20x4-01-01T00:00:00Z"), E::Digit { at: 2 });
        assert_eq!(err("2024/01/01T00:00:00Z"), E::Separator { at: 4 });
        assert_eq!(err("2024-01-01 00:00:00Z"), E::Separator { at: 10 });
        assert_eq!(err("2024-01-01t00:00:00Z"), E::Separator { at: 10 });
        assert_eq!(
            err("2024-13-01T00:00:00Z"),
            E::Range {
                field: TimeField::Month
            }
        );
        assert_eq!(
            err("2024-00-01T00:00:00Z"),
            E::Range {
                field: TimeField::Month
            }
        );
        assert_eq!(
            err("2024-01-00T00:00:00Z"),
            E::Range {
                field: TimeField::Day
            }
        );
        assert_eq!(
            err("2024-04-31T00:00:00Z"),
            E::Range {
                field: TimeField::Day
            }
        );
        assert_eq!(
            err("2024-01-01T24:00:00Z"),
            E::Range {
                field: TimeField::Hour
            }
        );
        assert_eq!(
            err("2024-01-01T00:60:00Z"),
            E::Range {
                field: TimeField::Minute
            }
        );
        assert_eq!(
            err("2024-01-01T00:00:60Z"),
            E::Range {
                field: TimeField::Second
            }
        );
        assert_eq!(
            err("2024-01-01T00:00:00+24:00"),
            E::Range {
                field: TimeField::OffsetHour
            }
        );
        assert_eq!(
            err("2024-01-01T00:00:00+00:60"),
            E::Range {
                field: TimeField::OffsetMinute
            }
        );
        assert_eq!(err("2024-01-01T00:00:00.Z"), E::EmptyFraction);
        assert_eq!(err("2024-01-01T00:00:00.1234567890Z"), E::FractionTooLong);
        assert_eq!(err("2024-01-01T00:00:00"), E::MissingZone);
        assert_eq!(err("2024-01-01T00:00:00z"), E::Zone { at: 19 });
        assert_eq!(err("2024-01-01T00:00:00+0100"), E::Zone { at: 22 });
        assert_eq!(err("2024-01-01T00:00:00Zjunk"), E::Trailing { at: 20 });
        assert_eq!(err("2024-01-01T00:00:00+01:00 "), E::Trailing { at: 25 });
    }

    #[test]
    fn display_of_years_outside_four_digits_is_signed() {
        let far = Timestamp::from_unix_millis(model_days(10_000, 1, 1) * MILLIS_PER_DAY);
        assert_eq!(far.to_string(), "+10000-01-01T00:00:00.000Z");
        let before = Timestamp::from_unix_millis(-62_198_755_200_000);
        assert_eq!(before.to_string(), "-0001-01-01T00:00:00.000Z");
        err(&far.to_string());
        // Extremes of the integer print without panicking.
        let _ = Timestamp::from_unix_millis(i64::MIN).to_string();
        let _ = Timestamp::from_unix_millis(i64::MAX).to_string();
    }

    proptest::proptest! {
        #[test]
        fn every_millisecond_round_trips(millis in -62_167_219_200_000_i64..=253_402_300_799_999) {
            let stamp = Timestamp::from_unix_millis(millis);
            proptest::prop_assert_eq!(Timestamp::parse_rfc3339(&stamp.to_string()), Ok(stamp));
        }

        #[test]
        fn parsing_matches_the_calendar_walk(
            year in 0_i64..=9999,
            month in 1_i64..=12,
            day in 1_i64..=28,
            secs in 0_i64..86_400,
            frac in 0_i64..1000,
        ) {
            let text = format!(
                "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{frac:03}Z",
                secs / 3600, secs % 3600 / 60, secs % 60
            );
            let want = (model_days(year, month, day) * 86_400 + secs) * 1000 + frac;
            proptest::prop_assert_eq!(Timestamp::parse_rfc3339(&text).unwrap().unix_millis(), want);
        }

        #[test]
        fn month_ends_match_the_calendar_walk(year in 0_i64..=9999) {
            for (month, last) in [(1, 31), (2, if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 { 29 } else { 28 }), (12, 31)] {
                let at = |d: i64| format!("{year:04}-{month:02}-{d:02}T00:00:00Z");
                proptest::prop_assert_eq!(
                    Timestamp::parse_rfc3339(&at(last)).unwrap().unix_millis(),
                    model_days(year, month, last) * MILLIS_PER_DAY
                );
                proptest::prop_assert_eq!(
                    Timestamp::parse_rfc3339(&at(last + 1)),
                    Err(TimestampParseError::Range { field: TimeField::Day })
                );
            }
        }
    }

    #[test]
    fn next_and_prev_stop_at_the_bounds() {
        assert_eq!(Txid::MIN.prev(), None);
        assert_eq!(Txid::MAX.next(), None);
        assert_eq!(txid(7).next(), Some(txid(8)));
        assert_eq!(txid(8).prev(), Some(txid(7)));
    }

    #[test]
    fn range_requires_min_at_most_max() {
        assert_eq!(TxidRange::new(txid(2), txid(1)), None);
        let range = TxidRange::new(txid(2), txid(4)).unwrap();
        assert_eq!(range.to_string(), "0000000000000002-0000000000000004");
        assert!(range.contains(txid(2)) && range.contains(txid(4)));
        assert!(!range.contains(txid(1)) && !range.contains(txid(5)));
        assert_eq!(
            TxidRange::single(txid(3)).min(),
            TxidRange::single(txid(3)).max()
        );
    }
}
