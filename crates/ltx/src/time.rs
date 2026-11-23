//! The instant LTX stores: [`Timestamp`], its RFC 3339 reader and writer. The calendar
//! arithmetic is `jiff`'s; what stays here is the shape Go reads and writes. Nothing here
//! reads a clock.

use std::fmt;
use std::time::Duration;

use jiff::civil::{DateTime, date};
use jiff::{Span, Unit};

/// Milliseconds in a day.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// A point in time as LTX stores it: milliseconds since the Unix epoch.
///
/// Signed because the header field is; nothing here reads a clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The last instant a timestamp holds.
    pub const MAX: Self = Self(i64::MAX);

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

    /// The instant `by` later, to the whole millisecond, or `None` past the
    /// last instant a timestamp holds.
    #[must_use]
    pub fn checked_add(self, by: Duration) -> Option<Self> {
        let millis = i64::try_from(by.as_millis()).ok()?;
        self.0.checked_add(millis).map(Self)
    }

    /// The instant `by` later, to the whole millisecond, stopping at
    /// [`Self::MAX`]: a wait too long to hold never ends.
    #[must_use]
    pub fn saturating_add(self, by: Duration) -> Self {
        let millis = i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_add(millis))
    }

    /// The instant `by` earlier, to the whole millisecond, or `None` before
    /// the first instant a timestamp holds.
    #[must_use]
    pub fn checked_sub(self, by: Duration) -> Option<Self> {
        let millis = i64::try_from(by.as_millis()).ok()?;
        self.0.checked_sub(millis).map(Self)
    }

    /// The time from `earlier` to `self`, zero when `earlier` is not before it.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        u64::try_from(self.0.saturating_sub(earlier.0))
            .map_or(Duration::ZERO, Duration::from_millis)
    }
}

/// Why a string is not an RFC 3339 instant this crate reads.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TimestampParseError {
    /// Not `YYYY-MM-DDTHH:MM:SS[.f{1,9}]` followed by `Z` or `+HH:MM` / `-HH:MM`: a lowercase
    /// `t` or `z`, a space, an offset without its colon, a leap second, a zone name, an
    /// offset hour past 23, or a year that is not four digits.
    #[error("timestamp is not YYYY-MM-DDTHH:MM:SS[.f]Z or with +HH:MM")]
    Shape,
    /// The calendar rejects it: a month, day, hour, minute, second or fraction out of range.
    #[error("timestamp is not a valid instant: {reason}")]
    Calendar {
        /// What `jiff` says is wrong.
        reason: String,
    },
}

/// Whether `text` has the shape Go's `time.RFC3339` reads, which is narrower than what
/// `jiff` accepts: an uppercase `T` and `Z`, an offset as `+HH:MM` with the hour below 24,
/// a four-digit year, and no leap second.
fn has_go_shape(text: &str) -> bool {
    let bytes = text.as_bytes();
    let digits = |at: usize, n: usize| {
        bytes
            .get(at..at.saturating_add(n))
            .is_some_and(|run| run.iter().all(u8::is_ascii_digit))
    };
    let zone_ok = match bytes.split_last() {
        Some((b'Z', _)) => true,
        _ => bytes.len().checked_sub(6).is_some_and(|at| {
            let hour = bytes.get(at.saturating_add(1)..at.saturating_add(3));
            matches!(bytes.get(at), Some(b'+' | b'-'))
                && digits(at.saturating_add(1), 2)
                && hour <= Some(b"23".as_slice())
                && bytes.get(at.saturating_add(3)) == Some(&b':')
                && digits(at.saturating_add(4), 2)
        }),
    };
    digits(0, 4)
        && bytes.get(10) == Some(&b'T')
        && bytes.get(17..19) != Some(b"60".as_slice())
        && zone_ok
}

impl Timestamp {
    /// Parses `YYYY-MM-DDTHH:MM:SS[.f{1,9}]` followed by `Z` or `+HH:MM` /
    /// `-HH:MM`, the shape Go's `time.RFC3339` reads. Fractions are truncated
    /// to milliseconds; years are 0000 to 9999; a leap second is refused.
    ///
    /// # Errors
    /// [`TimestampParseError`] saying whether the shape or the calendar is wrong.
    pub fn parse_rfc3339(text: &str) -> Result<Self, TimestampParseError> {
        if !has_go_shape(text) {
            return Err(TimestampParseError::Shape);
        }
        let calendar = |error: jiff::Error| TimestampParseError::Calendar {
            reason: error.to_string(),
        };
        // `jiff::Timestamp` stops at 9999-12-30, a day short of Go's last instant, so the
        // civil date and time are read on their own and the offset applied here.
        let (civil_text, offset) = match text.strip_suffix('Z') {
            Some(rest) => (rest, 0_i64),
            None => {
                let (rest, zone) = text.split_at(text.len().saturating_sub(6));
                let number = |at: std::ops::Range<usize>| {
                    zone.get(at).and_then(|digits| digits.parse::<i64>().ok())
                };
                let (hours, minutes) = number(1..3)
                    .zip(number(4..6))
                    .ok_or(TimestampParseError::Shape)?;
                if minutes > 59 {
                    return Err(TimestampParseError::Calendar {
                        reason: "offset minute out of range".to_owned(),
                    });
                }
                let sign = if zone.starts_with('-') { -1 } else { 1 };
                (
                    rest,
                    hours
                        .saturating_mul(3600)
                        .saturating_add(minutes.saturating_mul(60))
                        .saturating_mul(sign),
                )
            }
        };
        let datetime: DateTime = civil_text.parse().map_err(calendar)?;
        let days = datetime
            .date()
            .since((Unit::Day, date(1970, 1, 1)))
            .map_err(calendar)?
            .get_days();
        let of_day = i64::from(datetime.hour())
            .saturating_mul(3600)
            .saturating_add(i64::from(datetime.minute()).saturating_mul(60))
            .saturating_add(i64::from(datetime.second()))
            .saturating_sub(offset)
            .saturating_mul(1000)
            .saturating_add(i64::from(datetime.subsec_nanosecond()).saturating_div(1_000_000));
        Ok(Self(
            i64::from(days)
                .saturating_mul(MILLIS_PER_DAY)
                .saturating_add(of_day),
        ))
    }
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`, always UTC with three fractional digits. A year before 0 prints
/// with a sign (`-0001`). An instant outside years -9999 to 9999, which `jiff` cannot hold,
/// prints as its millisecond count and `ms` (`300000000000000ms`);
/// [`Timestamp::parse_rfc3339`] does not read that.
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.0.div_euclid(MILLIS_PER_DAY);
        let of_day = self.0.rem_euclid(MILLIS_PER_DAY);
        let day = Span::new()
            .try_days(days)
            .ok()
            .and_then(|span| date(1970, 1, 1).checked_add(span).ok());
        let Some(day) = day else {
            return write!(f, "{}ms", self.0);
        };
        let secs = of_day / 1000;
        let sign = if day.year() < 0 { "-" } else { "" };
        write!(
            f,
            "{sign}{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            day.year().unsigned_abs(),
            day.month(),
            day.day(),
            secs / 3600,
            secs % 3600 / 60,
            secs % 60,
            of_day % 1000,
        )
    }
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test models do calendar arithmetic on small bounded values"
)]
mod tests {
    use super::*;

    #[test]
    fn century_and_cycle_boundaries_format_correctly() {
        // The century terms of the day-of-era formula only matter from the
        // first day of a century (March 1st) and on the last day of a cycle
        // (2000-02-29, where `doe / 146_096` is 1).
        for text in [
            "1900-03-01T00:00:00.000Z",
            "2100-02-28T00:00:00.000Z",
            "2100-03-01T00:00:00.000Z",
            "2200-03-01T00:00:00.000Z",
            "1600-02-29T00:00:00.000Z",
            "2000-02-29T12:34:56.789Z",
            "2400-02-29T23:59:59.999Z",
        ] {
            let parsed = Timestamp::parse_rfc3339(text).unwrap();
            assert_eq!(parsed.to_string(), text);
        }
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
            assert!(
                matches!(err(bad), TimestampParseError::Calendar { .. }),
                "{bad}"
            );
        }
    }

    /// What Go's `time.RFC3339` refuses and `jiff` alone would read.
    #[test]
    fn only_go_s_shape_is_read() {
        for bad in [
            "",
            "2024-01-01T00:00",
            "20x4-01-01T00:00:00Z",
            "2024/01/01T00:00:00Z",
            "2024-01-01 00:00:00Z",
            "2024-01-01t00:00:00Z",
            "2024-01-01T00:00:00z",
            "2024-01-01T00:00:00",
            "2024-01-01T00:00:00+0100",
            "2024-01-01T00:00:00+01",
            "2024-01-01T00:00:00+24:00",
            "2024-01-01T00:00:00+01:00:30",
            "2024-01-01T00:00:00Z[UTC]",
            "2024-01-01T00:00:00Zjunk",
            "2024-01-01T00:00:00+01:00 ",
            "2024-01-01T00:00:60Z",
            "+10000-01-01T00:00:00Z",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00.1234567890Z",
        ] {
            assert!(Timestamp::parse_rfc3339(bad).is_err(), "{bad:?}");
        }
        for bad in [
            "2024-13-01T00:00:00Z",
            "2024-00-01T00:00:00Z",
            "2024-01-00T00:00:00Z",
            "2024-04-31T00:00:00Z",
            "2024-01-01T24:00:00Z",
            "2024-01-01T00:60:00Z",
            "2024-01-01T00:00:00+00:60",
        ] {
            assert!(
                matches!(err(bad), TimestampParseError::Calendar { .. }),
                "{bad}"
            );
        }
    }

    /// A fraction below a millisecond is dropped toward the earlier millisecond, also
    /// before the epoch, where the instant is negative.
    #[test]
    fn sub_millisecond_fractions_floor() {
        assert_eq!(ts("1969-12-31T23:59:59.9999Z").unix_millis(), -1);
        assert_eq!(ts("1969-12-31T23:59:59.999Z").unix_millis(), -1);
        assert_eq!(ts("1969-12-31T23:59:59.0001Z").unix_millis(), -1000);
    }

    /// Years `jiff` cannot hold print as the millisecond count, never panic.
    #[test]
    fn display_outside_jiffs_range_is_the_millisecond_count() {
        assert_eq!(
            Timestamp::from_unix_millis(-62_198_755_200_000).to_string(),
            "-0001-01-01T00:00:00.000Z"
        );
        assert_eq!(
            Timestamp::from_unix_millis(model_days(10_000, 1, 1) * MILLIS_PER_DAY).to_string(),
            "253402300800000ms"
        );
        assert_eq!(
            Timestamp::from_unix_millis(i64::MIN).to_string(),
            format!("{}ms", i64::MIN)
        );
        assert_eq!(Timestamp::MAX.to_string(), format!("{}ms", i64::MAX));
    }

    proptest::proptest! {
        #[test]
        fn every_millisecond_round_trips(millis in -62_167_219_200_000_i64..=253_402_300_799_999) {
            let created_at = Timestamp::from_unix_millis(millis);
            proptest::prop_assert_eq!(Timestamp::parse_rfc3339(&created_at.to_string()), Ok(created_at));
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
                let past = Timestamp::parse_rfc3339(&at(last + 1));
                proptest::prop_assert_eq!(
                    matches!(past, Err(TimestampParseError::Calendar { .. })),
                    true
                );
            }
        }
    }

    #[test]
    fn instant_arithmetic_is_whole_milliseconds_and_never_wraps() {
        let t = Timestamp::from_unix_millis(10_000);
        let ms = Duration::from_millis;
        assert_eq!(
            t.checked_sub(ms(4_000)),
            Some(Timestamp::from_unix_millis(6_000))
        );
        assert_eq!(
            t.checked_add(ms(4_000)),
            Some(Timestamp::from_unix_millis(14_000))
        );
        assert_eq!(
            t.checked_sub(Duration::from_micros(1_999)),
            Some(Timestamp::from_unix_millis(9_999)),
            "a part millisecond is dropped, as the stored created-at times are whole"
        );
        assert_eq!(
            t.checked_sub(ms(20_000)).map(Timestamp::unix_millis),
            Some(-10_000)
        );
        let first = Timestamp::from_unix_millis(i64::MIN);
        let last = Timestamp::MAX;
        assert_eq!(first.checked_sub(ms(1)), None);
        assert_eq!(last.checked_add(ms(1)), None);
        assert_eq!(t.checked_add(Duration::MAX), None);
        assert_eq!(t.checked_sub(Duration::MAX), None);
        assert_eq!(first.checked_add(Duration::ZERO), Some(first));
        assert_eq!(
            t.saturating_add(ms(4_000)),
            Timestamp::from_unix_millis(14_000)
        );
        assert_eq!(last.saturating_add(ms(1)), last);
        assert_eq!(t.saturating_add(Duration::MAX), last);
    }

    #[test]
    fn the_time_since_an_instant_is_zero_when_it_is_not_in_the_past() {
        let ms = Duration::from_millis;
        let at = Timestamp::from_unix_millis;
        assert_eq!(at(10_000).saturating_duration_since(at(4_000)), ms(6_000));
        assert_eq!(
            at(4_000).saturating_duration_since(at(10_000)),
            Duration::ZERO
        );
        assert_eq!(at(7).saturating_duration_since(at(7)), Duration::ZERO);
        assert_eq!(at(-5).saturating_duration_since(at(-9)), ms(4));
        assert_eq!(
            Timestamp::MAX.saturating_duration_since(at(i64::MIN)),
            ms(u64::MAX >> 1)
        );
    }
}
