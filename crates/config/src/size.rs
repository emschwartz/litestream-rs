//! Go's `ParseByteSize` (`cmd/litestream/main.go`), which trims the text and
//! hands it to go-humanize v1.0.1's `ParseBytes`, ported line for line.
//!
//! The number is the leading run of digits, `.` and `,` (commas dropped), read
//! as a float; the rest, trimmed and lower-cased, is a unit from the table
//! below (empty is bytes). The product is truncated to whole bytes and must
//! fit an `i64`, as Litestream checks after go-humanize.

/// Why a byte size is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SizeError {
    /// Nothing but space.
    #[error("empty size")]
    Empty,
    /// No number go-humanize reads at the front.
    #[error("no number at the start")]
    Number,
    /// A unit go-humanize does not know.
    #[error("unknown unit")]
    Unit,
    /// More than an `i64` of bytes.
    #[error("size exceeds maximum allowed value")]
    TooLarge,
}

const KI: u64 = 1 << 10;
const MI: u64 = 1 << 20;
const GI: u64 = 1 << 30;
const TI: u64 = 1 << 40;
const PI: u64 = 1 << 50;
const EI: u64 = 1 << 60;

fn unit_bytes(unit: &str) -> Option<u64> {
    Some(match unit {
        "" | "b" => 1,
        "kib" | "ki" => KI,
        "kb" | "k" => 1_000,
        "mib" | "mi" => MI,
        "mb" | "m" => 1_000_000,
        "gib" | "gi" => GI,
        "gb" | "g" => 1_000_000_000,
        "tib" | "ti" => TI,
        "tb" | "t" => 1_000_000_000_000,
        "pib" | "pi" => PI,
        "pb" | "p" => 1_000_000_000_000_000,
        "eib" | "ei" => EI,
        "eb" | "e" => 1_000_000_000_000_000_000,
        _ => return None,
    })
}

/// Parses a byte size as Go's `ParseByteSize`.
///
/// # Errors
/// A [`SizeError`] where Go returns an error.
pub fn parse_byte_size(text: &str) -> Result<u64, SizeError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(SizeError::Empty);
    }
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == ','))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: String = number.chars().filter(|&c| c != ',').collect();
    let value: f64 = number.parse().map_err(|_syntax| SizeError::Number)?;
    if !value.is_finite() {
        return Err(SizeError::Number);
    }
    let unit = unit_bytes(&unit.trim().to_lowercase()).ok_or(SizeError::Unit)?;
    let bytes = scale(value, unit).ok_or(SizeError::TooLarge)?;
    if bytes > u64::try_from(i64::MAX).unwrap_or(u64::MAX) {
        return Err(SizeError::TooLarge);
    }
    Ok(bytes)
}

/// go-humanize's `f *= float64(m)`, refused at `math.MaxUint64`, then `uint64(f)`.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "go-humanize multiplies in float64 and truncates; the bound is checked first and the value is not negative"
)]
fn scale(value: f64, unit: u64) -> Option<u64> {
    let product = value * (unit as f64);
    if product >= u64::MAX as f64 {
        return None;
    }
    Some(product as u64)
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "expected sizes are small products"
)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Go's `TestParseByteSize` table (`cmd/litestream/main_test.go`).
    #[test]
    fn go_parse_byte_size_table() {
        let ok = [
            ("1MiB", MI),
            ("5MiB", 5 * MI),
            ("1GiB", GI),
            ("1TiB", TI),
            ("1024KiB", MI),
            ("1MB", 1_000_000),
            ("5MB", 5_000_000),
            ("1GB", 1_000_000_000),
            ("1TB", 1_000_000_000_000),
            ("1000KB", 1_000_000),
            ("1M", 1_000_000),
            ("1K", 1_000),
            ("1G", 1_000_000_000),
            ("1T", 1_000_000_000_000),
            ("1.5MB", 1_500_000),
            ("1.5MiB", 1_572_864),
            ("0.5MB", 500_000),
            ("2.5GiB", 2_684_354_560),
            ("100.5KB", 100_500),
            ("100B", 100),
            ("100", 100),
            ("1mib", MI),
            ("5MIB", 5 * MI),
            ("1gib", GI),
            ("1 MiB", MI),
            ("5 MB", 5_000_000),
            ("10 GiB", 10 * GI),
            ("100MB", 100_000_000),
            ("1,000KB", 1_000_000),
        ];
        for (text, bytes) in ok {
            assert_eq!(parse_byte_size(text), Ok(bytes), "{text}");
        }
        for text in ["", "MB", "invalid", "1XB", "notanumber"] {
            assert!(parse_byte_size(text).is_err(), "{text}");
        }
    }

    /// Go's `TestParseByteSizeOverflow`.
    #[test]
    fn ten_exabytes_is_too_large() {
        assert_eq!(parse_byte_size("10EB"), Err(SizeError::TooLarge));
    }

    proptest! {
        #[test]
        // Products stay below 2^53, where Go's float64 arithmetic is exact.
        fn a_whole_number_and_unit_is_the_product(n in 0_u64..1_000_000, unit in 0_usize..6) {
            let units = [("b", 1), ("KiB", KI), ("kb", 1_000), ("MiB", MI), ("M", 1_000_000), ("g", 1_000_000_000)];
            let (name, per) = units[unit];
            prop_assert_eq!(parse_byte_size(&format!(" {n} {name} ")), Ok(n * per));
        }
    }
}
