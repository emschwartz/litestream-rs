//! Go's `time.Duration`: signed nanoseconds, parsed by `time.ParseDuration`
//! and printed by `Duration.String`, both ported line for line from Go 1.24's
//! `time/format.go` and `time/time.go`.
//!
//! The grammar is `[-+]?([0-9]*(\.[0-9]*)?[a-z]+)+` with the units `ns`, `us`,
//! `µs` (U+00B5), `μs` (U+03BC), `ms`, `s`, `m` and `h`; a lone `0` (signed or
//! not) is zero. Fractions keep their first 19 significant digits and convert
//! through `f64`, as Go does, so `1.5h` is exact and `0.1ns` is zero.

use std::fmt;
use std::time::Duration;

/// A Go duration: nanoseconds, possibly negative (Go's validation decides
/// which keys may be).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GoDuration(i64);

/// Why a duration string is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DurationError {
    /// Not the grammar, or out of range (Go's "invalid duration").
    #[error("invalid duration")]
    Invalid,
    /// A number with no unit after it.
    #[error("missing unit in duration")]
    MissingUnit,
    /// A unit Go does not know.
    #[error("unknown unit in duration")]
    UnknownUnit,
}

const NANOSECOND: u64 = 1;
const MICROSECOND: u64 = 1_000;
const MILLISECOND: u64 = 1_000_000;
const SECOND: u64 = 1_000_000_000;
const MINUTE: u64 = 60_000_000_000;
const HOUR: u64 = 3_600_000_000_000;
const LIMIT: u64 = 1 << 63;

impl GoDuration {
    /// Zero.
    pub const ZERO: Self = Self(0);

    /// From nanoseconds.
    #[must_use]
    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Whole seconds, for defaults; saturates.
    #[must_use]
    pub const fn from_secs(secs: i64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// The nanoseconds.
    #[must_use]
    pub const fn nanos(self) -> i64 {
        self.0
    }

    /// As a `std` duration, when it is not negative.
    #[must_use]
    pub fn to_std(self) -> Option<Duration> {
        u64::try_from(self.0).ok().map(Duration::from_nanos)
    }

    /// Parses as Go's `time.ParseDuration`.
    ///
    /// # Errors
    /// A [`DurationError`] where Go returns an error.
    pub fn parse(text: &str) -> Result<Self, DurationError> {
        let (neg, mut rest) = match text.as_bytes().first() {
            Some(b'-') => (true, text.get(1..).unwrap_or("")),
            Some(b'+') => (false, text.get(1..).unwrap_or("")),
            _ => (false, text),
        };
        if rest == "0" {
            return Ok(Self::ZERO);
        }
        if rest.is_empty() {
            return Err(DurationError::Invalid);
        }
        let mut total: u64 = 0;
        while !rest.is_empty() {
            let (value, after) = component(rest)?;
            total = total.checked_add(value).ok_or(DurationError::Invalid)?;
            if total > LIMIT {
                return Err(DurationError::Invalid);
            }
            rest = after;
        }
        if neg {
            // Go's `-Duration(d)` with d at most 1<<63: wraps to i64::MIN at the bound.
            return Ok(Self(0_i64.wrapping_sub_unsigned(total)));
        }
        i64::try_from(total)
            .map(Self)
            .map_err(|_overflow| DurationError::Invalid)
    }
}

/// One `[0-9]*(\.[0-9]*)?unit` component: its nanoseconds and the rest.
fn component(s: &str) -> Result<(u64, &str), DurationError> {
    if !s.starts_with(|c: char| c == '.' || c.is_ascii_digit()) {
        return Err(DurationError::Invalid);
    }
    let (whole, rest) = leading_int(s)?;
    let pre = rest.len() != s.len();
    let (frac, scale, rest, post) = match rest.strip_prefix('.') {
        Some(after) => {
            let (frac, scale, tail) = leading_fraction(after);
            (frac, scale, tail, tail.len() != after.len())
        }
        None => (0, 1.0, rest, false),
    };
    if !pre && !post {
        return Err(DurationError::Invalid);
    }
    let unit_len = rest
        .find(|c: char| c == '.' || c.is_ascii_digit())
        .unwrap_or(rest.len());
    if unit_len == 0 {
        return Err(DurationError::MissingUnit);
    }
    let (unit, rest) = rest.split_at(unit_len);
    let unit = unit_nanos(unit).ok_or(DurationError::UnknownUnit)?;
    if whole > LIMIT.checked_div(unit).unwrap_or(0) {
        return Err(DurationError::Invalid);
    }
    let mut value = whole.saturating_mul(unit);
    if frac > 0 {
        value = value.saturating_add(fraction_nanos(frac, unit, scale));
        if value > LIMIT {
            return Err(DurationError::Invalid);
        }
    }
    Ok((value, rest))
}

fn unit_nanos(unit: &str) -> Option<u64> {
    match unit {
        "ns" => Some(NANOSECOND),
        "us" | "\u{b5}s" | "\u{3bc}s" => Some(MICROSECOND),
        "ms" => Some(MILLISECOND),
        "s" => Some(SECOND),
        "m" => Some(MINUTE),
        "h" => Some(HOUR),
        _ => None,
    }
}

/// Go's `leadingInt`: digits, refusing a value above 1<<63.
fn leading_int(s: &str) -> Result<(u64, &str), DurationError> {
    let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (number, rest) = s.split_at(digits);
    let mut x: u64 = 0;
    for digit in number.bytes() {
        if x > LIMIT / 10 {
            return Err(DurationError::Invalid);
        }
        x = x
            .saturating_mul(10)
            .saturating_add(u64::from(digit.saturating_sub(b'0')));
        if x > LIMIT {
            return Err(DurationError::Invalid);
        }
    }
    Ok((x, rest))
}

/// Go's `leadingFraction`: digits after the point, dropping those past what fits.
fn leading_fraction(s: &str) -> (u64, f64, &str) {
    let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (number, rest) = s.split_at(digits);
    let mut x: u64 = 0;
    let mut scale = 1.0_f64;
    let mut overflow = false;
    for digit in number.bytes() {
        if overflow {
            continue;
        }
        if x > (LIMIT - 1) / 10 {
            overflow = true;
            continue;
        }
        let y = x
            .saturating_mul(10)
            .saturating_add(u64::from(digit.saturating_sub(b'0')));
        if y > LIMIT {
            overflow = true;
            continue;
        }
        x = y;
        scale *= 10.0;
    }
    (x, scale, rest)
}

/// Go's `uint64(float64(f) * (float64(unit) / scale))`.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Go converts through float64 here; the product is at most one hour of nanoseconds, so it fits"
)]
fn fraction_nanos(frac: u64, unit: u64, scale: f64) -> u64 {
    ((frac as f64) * ((unit as f64) / scale)) as u64
}

impl fmt::Display for GoDuration {
    /// Go's `Duration.String`: `1h30m0s`, `300ms`, `1.5µs`, `0s`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let neg = self.0 < 0;
        let u = self.0.unsigned_abs();
        if neg {
            f.write_str("-")?;
        }
        if u == 0 {
            return f.write_str("0s");
        }
        if u < MICROSECOND {
            return write!(f, "{u}ns");
        }
        if u < MILLISECOND {
            return write_frac(f, u, MICROSECOND, "\u{b5}s");
        }
        if u < SECOND {
            return write_frac(f, u, MILLISECOND, "ms");
        }
        let secs = u / SECOND;
        let (hours, minutes) = (secs / 3600, (secs / 60) % 60);
        if hours > 0 {
            write!(f, "{hours}h")?;
        }
        if hours > 0 || minutes > 0 {
            write!(f, "{minutes}m")?;
        }
        write_frac(f, u % MINUTE, SECOND, "s")
    }
}

/// `value / per` with its fraction, trailing zeros dropped, then `unit`.
fn write_frac(f: &mut fmt::Formatter<'_>, value: u64, per: u64, unit: &str) -> fmt::Result {
    let (whole, frac) = (
        value.checked_div(per).unwrap_or(0),
        value.checked_rem(per).unwrap_or(0),
    );
    if frac == 0 {
        return write!(f, "{whole}{unit}");
    }
    let width = usize::try_from(per.ilog10()).unwrap_or(9);
    let digits = format!("{frac:0width$}");
    write!(f, "{whole}.{}{unit}", digits.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn go_examples_parse() {
        let cases = [
            ("0", 0),
            ("-0", 0),
            ("+0", 0),
            ("10s", 10 * 1_000_000_000),
            ("1h30m", 5_400_000_000_000),
            ("1.5h", 5_400_000_000_000),
            ("300ms", 300_000_000),
            ("3\u{b5}s", 3000),
            ("3\u{3bc}s", 3000),
            ("3us", 3000),
            ("-1h", -3_600_000_000_000),
            ("+5s", 5_000_000_000),
            (".5s", 500_000_000),
            ("1.s", 1_000_000_000),
            ("1h1m1s1ms1us1ns", 3_661_001_001_001),
            ("9223372036854775807ns", i64::MAX),
            ("-9223372036854775808ns", i64::MIN),
        ];
        for (text, nanos) in cases {
            assert_eq!(GoDuration::parse(text), Ok(GoDuration(nanos)), "{text}");
        }
    }

    #[test]
    fn go_refusals() {
        let cases = [
            ("", DurationError::Invalid),
            ("-", DurationError::Invalid),
            ("5", DurationError::MissingUnit),
            ("10 s", DurationError::UnknownUnit),
            ("10S", DurationError::UnknownUnit),
            ("1d", DurationError::UnknownUnit),
            (".s", DurationError::Invalid),
            ("s", DurationError::Invalid),
            ("9223372036854775808ns", DurationError::Invalid),
            ("3000000h", DurationError::Invalid),
        ];
        for (text, error) in cases {
            assert_eq!(GoDuration::parse(text), Err(error), "{text}");
        }
    }

    #[test]
    fn prints_as_go() {
        let cases = [
            (0, "0s"),
            (1, "1ns"),
            (1_500, "1.5\u{b5}s"),
            (300_000_000, "300ms"),
            (1_000_000_000, "1s"),
            (90_000_000_000, "1m30s"),
            (3_600_000_000_000, "1h0m0s"),
            (5_400_000_000_000, "1h30m0s"),
            (1_500_000_000, "1.5s"),
            (-3_600_000_000_000, "-1h0m0s"),
            (i64::MIN, "-2562047h47m16.854775808s"),
        ];
        for (nanos, text) in cases {
            assert_eq!(GoDuration(nanos).to_string(), text, "{nanos}");
        }
    }

    proptest! {
        #[test]
        fn what_go_prints_parses_back(nanos in any::<i64>()) {
            let d = GoDuration(nanos);
            prop_assert_eq!(GoDuration::parse(&d.to_string()), Ok(d));
        }

        #[test]
        fn a_number_and_unit_is_the_product(n in 0_u64..1_000_000, unit in 0_usize..8) {
            let units = [("ns", 1), ("us", 1_000), ("\u{b5}s", 1_000), ("\u{3bc}s", 1_000),
                ("ms", 1_000_000), ("s", 1_000_000_000), ("m", 60_000_000_000), ("h", 3_600_000_000_000_u64)];
            let (name, per) = units[unit];
            let expected = n.checked_mul(per).and_then(|v| i64::try_from(v).ok());
            prop_assert_eq!(GoDuration::parse(&format!("{n}{name}")).ok().map(GoDuration::nanos), expected);
        }
    }
}
