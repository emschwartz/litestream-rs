//! yaml.v2 v2.4.0's reading of a scalar into a Go field, ported from its
//! `resolve.go` and `decode.go` (`scalar`).
//!
//! A quoted or block scalar is always a string. A plain one resolves by its
//! first character, as yaml.v2's table does:
//! - YAML 1.1 booleans: `y Y yes Yes YES true True TRUE on On ON` and
//!   `n N no No NO false False FALSE off Off OFF`; `tRue` is a string.
//! - Null: nothing, `~`, `null`, `Null`, `NULL`.
//! - Numbers, when it starts with a digit or sign: underscores removed, then
//!   Go's `ParseInt(s, 0, 64)` (so `0600` is octal 384, `0x10` is 16, `0b101`
//!   is 5, `08` is not octal and falls through to the float 8), then
//!   `ParseUint`, then a YAML-style float (`1e3`, `2.7`).
//!
//! Then the field's Go type decides, as `decode.go` does:
//! - a string takes the scalar's text whatever it resolved to (`yes`, `0600`
//!   and `1.0` stay as written);
//! - a bool takes only a boolean (a quoted `"true"` is a type error);
//! - an integer takes an integer, or a float Go truncates; litestream-rs refuses a
//!   fraction instead of dropping it;
//! - a `time.Duration` takes a string through `time.ParseDuration`, or an
//!   integer as nanoseconds (`sync-interval: 5` is 5ns in Go: read the same,
//!   with a warning);
//! - null leaves the field at Go's zero (or its pointer nil).

use crate::duration::GoDuration;
use crate::error::{Expected, Found, ProblemKind};
use crate::size::parse_byte_size;
use crate::yaml::{Node, NodeKind};

/// What a plain scalar resolves to.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Resolved<'a> {
    Null,
    Bool(bool),
    Int(i64),
    /// An integer above `i64::MAX` that fits `u64`.
    BigUint(u64),
    Float(f64),
    Str(&'a str),
}

/// yaml.v2's `resolve` for an untagged scalar.
pub(crate) fn resolve(text: &str, plain: bool) -> Resolved<'_> {
    if !plain {
        return Resolved::Str(text);
    }
    let Some(first) = text.bytes().next() else {
        return Resolved::Null;
    };
    let hinted = matches!(first, b'+' | b'-' | b'0'..=b'9' | b'.')
        || matches!(
            first,
            b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~'
        );
    if !hinted {
        return Resolved::Str(text);
    }
    if let Some(known) = table(text) {
        return known;
    }
    match first {
        b'.' => text.parse().map_or(Resolved::Str(text), Resolved::Float),
        b'+' | b'-' | b'0'..=b'9' => number(text),
        _ => Resolved::Str(text),
    }
}

fn table(text: &str) -> Option<Resolved<'_>> {
    Some(match text {
        "y" | "Y" | "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" => {
            Resolved::Bool(true)
        }
        "n" | "N" | "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => {
            Resolved::Bool(false)
        }
        "~" | "null" | "Null" | "NULL" => Resolved::Null,
        ".nan" | ".NaN" | ".NAN" => Resolved::Float(f64::NAN),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => Resolved::Float(f64::INFINITY),
        "-.inf" | "-.Inf" | "-.INF" => Resolved::Float(f64::NEG_INFINITY),
        _ => return None,
    })
}

fn number(text: &str) -> Resolved<'_> {
    let plain: String = text.chars().filter(|&c| c != '_').collect();
    if let Some(value) = go_parse_int(&plain) {
        return Resolved::Int(value);
    }
    if let Some(value) = go_parse_uint(&plain) {
        return Resolved::BigUint(value);
    }
    if yaml_style_float(&plain)
        && let Ok(value) = plain.parse()
    {
        return Resolved::Float(value);
    }
    Resolved::Str(text)
}

/// Go's `strconv.ParseUint(s, 0, 64)` once underscores are gone.
fn go_parse_uint(s: &str) -> Option<u64> {
    let (base, digits) = match s.as_bytes() {
        [] => return None,
        [b'0', b'b' | b'B', _, ..] => (2, s.get(2..)?),
        [b'0', b'o' | b'O', _, ..] => (8, s.get(2..)?),
        [b'0', b'x' | b'X', _, ..] => (16, s.get(2..)?),
        [b'0', ..] => (8, s.get(1..)?),
        _ => (10, s),
    };
    digits.chars().try_fold(0_u64, |n, c| {
        let digit = c.to_digit(36).filter(|&d| d < base)?;
        n.checked_mul(u64::from(base))?
            .checked_add(u64::from(digit))
    })
}

/// Go's `strconv.ParseInt(s, 0, 64)`.
fn go_parse_int(s: &str) -> Option<i64> {
    let (neg, rest) = match s.as_bytes().first() {
        Some(b'+') => (false, s.get(1..)?),
        Some(b'-') => (true, s.get(1..)?),
        _ => (false, s),
    };
    let magnitude = go_parse_uint(rest)?;
    if neg {
        0_i64.checked_sub_unsigned(magnitude)
    } else {
        i64::try_from(magnitude).ok()
    }
}

/// yaml.v2's `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`.
fn yaml_style_float(s: &str) -> bool {
    let s = s.strip_prefix(['-', '+']).unwrap_or(s);
    let digits = |t: &str| t.bytes().take_while(u8::is_ascii_digit).count();
    let (mantissa_ok, rest) = if let Some(after) = s.strip_prefix('.') {
        let n = digits(after);
        (n > 0, after.get(n..).unwrap_or(""))
    } else {
        let n = digits(s);
        let after = s.get(n..).unwrap_or("");
        match after.strip_prefix('.') {
            Some(frac) => (n > 0, frac.get(digits(frac)..).unwrap_or("")),
            None => (n > 0, after),
        }
    };
    if !mantissa_ok {
        return false;
    }
    match rest.strip_prefix(['e', 'E']) {
        None => rest.is_empty(),
        Some(exp) => {
            let exp = exp.strip_prefix(['-', '+']).unwrap_or(exp);
            !exp.is_empty() && digits(exp) == exp.len()
        }
    }
}

fn found(node: &Node, secret: bool) -> Found {
    match &node.kind {
        NodeKind::Scalar { text, .. } => Found::Scalar((!secret).then(|| text.clone())),
        NodeKind::Map(_) => Found::Mapping,
        NodeKind::Seq(_) => Found::List,
    }
}

pub(crate) fn wrong(expected: Expected, node: &Node, secret: bool) -> ProblemKind {
    ProblemKind::WrongType {
        expected,
        found: found(node, secret),
    }
}

fn scalar(
    node: &Node,
    expected: Expected,
    secret: bool,
) -> Result<(&str, Resolved<'_>), ProblemKind> {
    match &node.kind {
        NodeKind::Scalar { text, plain } => Ok((text, resolve(text, *plain))),
        NodeKind::Map(_) | NodeKind::Seq(_) => Err(wrong(expected, node, secret)),
    }
}

/// A Go `*string`: any scalar's text, empty included; `None` for null only, which
/// leaves yaml.v2's pointer nil.
pub(crate) fn read_str_ptr(node: &Node, secret: bool) -> Result<Option<String>, ProblemKind> {
    let (text, resolved) = scalar(node, Expected::String, secret)?;
    Ok(match resolved {
        Resolved::Null => None,
        Resolved::Bool(_)
        | Resolved::Int(_)
        | Resolved::BigUint(_)
        | Resolved::Float(_)
        | Resolved::Str(_) => Some(text.to_owned()),
    })
}

/// A Go `string`: any scalar's text; `None` for null and for empty, which Go
/// cannot tell apart.
pub(crate) fn read_str(node: &Node, secret: bool) -> Result<Option<String>, ProblemKind> {
    read_str_ptr(node, secret).map(|text| text.filter(|text| !text.is_empty()))
}

/// A Go `bool` or `*bool`.
pub(crate) fn read_bool(node: &Node, secret: bool) -> Result<Option<bool>, ProblemKind> {
    match scalar(node, Expected::Boolean, secret)?.1 {
        Resolved::Null => Ok(None),
        Resolved::Bool(value) => Ok(Some(value)),
        Resolved::Int(_) | Resolved::BigUint(_) | Resolved::Float(_) | Resolved::Str(_) => {
            Err(wrong(Expected::Boolean, node, secret))
        }
    }
}

/// A Go `int` or `int64`.
pub(crate) fn read_int(node: &Node, secret: bool) -> Result<Option<i64>, ProblemKind> {
    match scalar(node, Expected::Integer, secret)?.1 {
        Resolved::Null => Ok(None),
        Resolved::Int(value) => Ok(Some(value)),
        Resolved::BigUint(_) => Err(ProblemKind::OutOfRange {
            expected: Expected::Integer,
        }),
        Resolved::Float(value) => whole(value, Expected::Integer).map(Some),
        Resolved::Bool(_) | Resolved::Str(_) => Err(wrong(Expected::Integer, node, secret)),
    }
}

/// A Go `uint32`.
pub(crate) fn read_u32(node: &Node, secret: bool) -> Result<Option<u32>, ProblemKind> {
    let range = ProblemKind::OutOfRange {
        expected: Expected::Unsigned32,
    };
    match scalar(node, Expected::Unsigned32, secret)?.1 {
        Resolved::Null => Ok(None),
        Resolved::Int(value) => u32::try_from(value).map(Some).map_err(|_too_big| range),
        Resolved::BigUint(_) => Err(range),
        Resolved::Float(value) => {
            let value = whole(value, Expected::Unsigned32)?;
            u32::try_from(value).map(Some).map_err(|_too_big| range)
        }
        Resolved::Bool(_) | Resolved::Str(_) => Err(wrong(Expected::Unsigned32, node, secret)),
    }
}

/// A Go `time.Duration`, and whether it was a bare number (nanoseconds).
pub(crate) fn read_duration(
    node: &Node,
    secret: bool,
) -> Result<Option<(GoDuration, bool)>, ProblemKind> {
    match scalar(node, Expected::Duration, secret)?.1 {
        Resolved::Null => Ok(None),
        Resolved::Int(nanos) => Ok(Some((GoDuration::from_nanos(nanos), true))),
        Resolved::BigUint(_) => Err(ProblemKind::OutOfRange {
            expected: Expected::Duration,
        }),
        Resolved::Float(value) => {
            let nanos = whole(value, Expected::Duration)?;
            Ok(Some((GoDuration::from_nanos(nanos), true)))
        }
        Resolved::Str(text) => GoDuration::parse(text)
            .map(|d| Some((d, false)))
            .map_err(|_syntax| wrong(Expected::Duration, node, secret)),
        Resolved::Bool(_) => Err(wrong(Expected::Duration, node, secret)),
    }
}

/// Litestream's `ByteSize`, which reads the scalar's text whatever it resolves to.
pub(crate) fn read_bytes(node: &Node, secret: bool) -> Result<Option<u64>, ProblemKind> {
    match read_str(node, secret)? {
        None => Ok(None),
        Some(text) => parse_byte_size(&text)
            .map(Some)
            .map_err(ProblemKind::ByteSize),
    }
}

/// A Go `[]string`; a null item is an empty string, as in Go.
pub(crate) fn read_list(node: &Node, secret: bool) -> Result<Option<Vec<String>>, ProblemKind> {
    if node.is_null() {
        return Ok(None);
    }
    match &node.kind {
        NodeKind::Seq(items) => items
            .iter()
            .map(|item| read_str(item, secret).map(Option::unwrap_or_default))
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        NodeKind::Scalar { .. } | NodeKind::Map(_) => Err(wrong(Expected::List, node, secret)),
    }
}

/// A float that is a whole number in `i64`'s range, which Go would truncate.
fn whole(value: f64, expected: Expected) -> Result<i64, ProblemKind> {
    if !value.is_finite() {
        return Err(ProblemKind::OutOfRange { expected });
    }
    if value.fract().abs().to_bits() != 0 {
        return Err(ProblemKind::Fractional);
    }
    float_to_i64(value).ok_or(ProblemKind::OutOfRange { expected })
}

/// A whole, finite float as `i64`, when it fits.
#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "the range is checked first; the value is whole, so nothing is lost"
)]
fn float_to_i64(value: f64) -> Option<i64> {
    const BOUND: f64 = 9.223_372_036_854_776e18; // 2^63, exactly
    (-BOUND..BOUND).contains(&value).then_some(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(text: &str) -> Node {
        Node {
            line: 1,
            kind: NodeKind::Scalar {
                text: text.to_owned(),
                plain: true,
            },
        }
    }

    #[test]
    fn resolves_as_yaml_v2() {
        let cases = [
            ("yes", Resolved::Bool(true)),
            ("Y", Resolved::Bool(true)),
            ("ON", Resolved::Bool(true)),
            ("off", Resolved::Bool(false)),
            ("tRue", Resolved::Str("tRue")),
            ("", Resolved::Null),
            ("~", Resolved::Null),
            ("0600", Resolved::Int(384)),
            ("0o600", Resolved::Int(384)),
            ("08", Resolved::Float(8.0)),
            ("0x10", Resolved::Int(16)),
            ("0b101", Resolved::Int(5)),
            ("-0x10", Resolved::Int(-16)),
            ("1_000", Resolved::Int(1000)),
            ("+5", Resolved::Int(5)),
            ("1e3", Resolved::Float(1000.0)),
            ("2.7", Resolved::Float(2.7)),
            (".5", Resolved::Float(0.5)),
            ("18446744073709551615", Resolved::BigUint(u64::MAX)),
            ("99999999999999999999", Resolved::Float(1e20)),
            ("10s", Resolved::Str("10s")),
            ("2001-01-01", Resolved::Str("2001-01-01")),
            ("abc", Resolved::Str("abc")),
        ];
        for (text, want) in cases {
            assert_eq!(resolve(text, true), want, "{text}");
        }
        assert_eq!(resolve("true", false), Resolved::Str("true"));
    }

    #[test]
    fn integers_refuse_what_go_would_truncate() {
        assert_eq!(read_int(&plain("2.7"), false), Err(ProblemKind::Fractional));
        assert_eq!(read_int(&plain("1e3"), false), Ok(Some(1000)));
        assert_eq!(read_u32(&plain("0600"), false), Ok(Some(384)));
        assert_eq!(
            read_u32(&plain("-1"), false),
            Err(ProblemKind::OutOfRange {
                expected: Expected::Unsigned32
            })
        );
    }

    #[test]
    fn secrets_are_not_shown_in_type_errors() {
        let error = read_bool(&plain("hunter2"), true).unwrap_err();
        assert!(!error.to_string().contains("hunter2"), "{error}");
        let error = read_bool(&plain("hunter2"), false).unwrap_err();
        assert!(error.to_string().contains("hunter2"), "{error}");
    }
}
