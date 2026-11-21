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

    /// The range from the first transaction through `max`: what a snapshot of
    /// the database at `max` covers. Never empty, since no id is below
    /// [`Txid::MIN`].
    #[must_use]
    pub const fn from_start(max: Txid) -> Self {
        Self {
            min: Txid::MIN,
            max,
        }
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
mod tests {
    use super::*;

    fn txid(raw: u64) -> Txid {
        Txid::new(raw).unwrap()
    }

    #[test]
    fn error_messages_say_what_is_wrong() {
        assert_eq!(
            "abc".parse::<Txid>().unwrap_err().to_string(),
            "transaction id is 3 bytes, want 16"
        );
        assert_eq!(
            format!("{:?}", TxidRange::single(txid(3))),
            "TxidRange(0000000000000003-0000000000000003)"
        );
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

    #[test]
    fn a_range_from_the_start_is_every_transaction_through_its_end() {
        for max in [Txid::MIN, txid(9), Txid::MAX] {
            let range = TxidRange::from_start(max);
            assert_eq!(Some(range), TxidRange::new(Txid::MIN, max));
            assert_eq!((range.min(), range.max()), (Txid::MIN, max));
        }
    }
}
