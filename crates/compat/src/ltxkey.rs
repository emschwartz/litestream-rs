//! Go's LTX object keys. TEMPORARY: replace with the key type of `lsr-core` (task 2).
//!
//! Two layouts exist and they differ, read from the Go source at 041a8c3:
//! - the file replica (`file/replica_client.go` via `litestream.LTXFilePath`) writes
//!   `ltx/<level in decimal>/<min:016x>-<max:016x>.ltx`, for example `ltx/0/...`;
//! - the S3 client (`s3/replica_client.go`, `fmt.Sprintf("%04x/%s", level, filename)`) writes
//!   `<%04x level>/<min:016x>-<max:016x>.ltx` directly under the prefix.
//!
//! The plan's single `<%04x level>/...` shape is the S3 one; a directory replica has the `ltx/`
//! segment and an unpadded decimal level.

use crate::error::CompatError;

/// The snapshot level.
pub const SNAPSHOT_LEVEL: u8 = 9;

/// Which of Go's two key layouts a key is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyLayout {
    /// `ltx/<level>/<min>-<max>.ltx`, as the file replica writes it.
    File,
    /// `<%04x level>/<min>-<max>.ltx`, as the S3 client writes it.
    S3,
}

/// A parsed LTX key: level 0 to 9 and a TXID range with both ends at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalKey {
    /// The level, 0 to 9.
    pub level: u8,
    /// First TXID.
    pub min: u64,
    /// Last TXID, at least `min`.
    pub max: u64,
}

impl LocalKey {
    /// Parses a path relative to the replica root in the given layout.
    pub fn parse(rel: &str, layout: KeyLayout) -> Result<Self, CompatError> {
        Self::try_parse(rel, layout).ok_or_else(|| CompatError::BadKey(rel.to_owned()))
    }

    fn try_parse(rel: &str, layout: KeyLayout) -> Option<Self> {
        let (level_part, name) = match layout {
            KeyLayout::File => {
                let rest = rel.strip_prefix("ltx/")?;
                rest.split_once('/')?
            }
            KeyLayout::S3 => rel.split_once('/')?,
        };
        let level = match layout {
            KeyLayout::File => parse_decimal_level(level_part)?,
            KeyLayout::S3 => parse_padded_hex_level(level_part)?,
        };
        let stem = name.strip_suffix(".ltx")?;
        let (min, max) = stem.split_once('-')?;
        let (min, max) = (parse_txid(min)?, parse_txid(max)?);
        (min >= 1 && min <= max).then_some(Self { level, min, max })
    }

    /// The key in the given layout.
    pub fn render(self, layout: KeyLayout) -> String {
        let name = format!("{:016x}-{:016x}.ltx", self.min, self.max);
        match layout {
            KeyLayout::File => format!("ltx/{}/{name}", self.level),
            KeyLayout::S3 => format!("{:04x}/{name}", self.level),
        }
    }

    /// Whether this key is a snapshot (level 9).
    pub const fn is_snapshot(self) -> bool {
        self.level == SNAPSHOT_LEVEL
    }
}

fn parse_decimal_level(s: &str) -> Option<u8> {
    let single_digit = s.len() == 1 && s.bytes().all(|b| b.is_ascii_digit());
    let level: u8 = s.parse().ok().filter(|_| single_digit)?;
    (level <= SNAPSHOT_LEVEL).then_some(level)
}

fn parse_padded_hex_level(s: &str) -> Option<u8> {
    let lower_hex = s
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    let level = u8::from_str_radix(s, 16)
        .ok()
        .filter(|_| s.len() == 4 && lower_hex)?;
    (level <= SNAPSHOT_LEVEL).then_some(level)
}

fn parse_txid(s: &str) -> Option<u64> {
    let lower_hex = s
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if s.len() == 16 && lower_hex {
        u64::from_str_radix(s, 16).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "0000000000000001-0000000000000a0b.ltx";

    #[test]
    fn both_layouts_round_trip() {
        let key = LocalKey {
            level: 9,
            min: 1,
            max: 0xa0b,
        };
        assert_eq!(key.render(KeyLayout::File), format!("ltx/9/{GOOD}"));
        assert_eq!(key.render(KeyLayout::S3), format!("0009/{GOOD}"));
        for layout in [KeyLayout::File, KeyLayout::S3] {
            assert_eq!(LocalKey::parse(&key.render(layout), layout).ok(), Some(key));
        }
    }

    #[test]
    fn rejection_table() {
        let bad_file = [
            format!("ltx/10/{GOOD}"),
            format!("ltx/00/{GOOD}"),
            format!("ltx/x/{GOOD}"),
            format!("0/{GOOD}"),
            "ltx/0/0000000000000001-0000000000000002.tmp".to_owned(),
            "ltx/0/000000000000001-0000000000000002.ltx".to_owned(),
            "ltx/0/000000000000000A-0000000000000002.ltx".to_owned(),
            "ltx/0/0000000000000003-0000000000000002.ltx".to_owned(),
            "ltx/0/0000000000000000-0000000000000002.ltx".to_owned(),
            "ltx/0/0000000000000001.ltx".to_owned(),
        ];
        for k in &bad_file {
            assert!(LocalKey::parse(k, KeyLayout::File).is_err(), "{k}");
        }
        let bad_s3 = [
            format!("9/{GOOD}"),
            format!("000a/{GOOD}"),
            format!("0009/x/{GOOD}"),
            format!("00G9/{GOOD}"),
            format!("0F09/{GOOD}"),
            format!("ltx/0/{GOOD}"),
        ];
        for k in &bad_s3 {
            assert!(LocalKey::parse(k, KeyLayout::S3).is_err(), "{k}");
        }
    }
}
