//! The read rule: what boot makes of the file's two slots (docs/journal.md, "Reading").

use std::num::NonZeroU64;

use crate::codec::{Decoded, Invalid, SLOT, decode};
use crate::facts::Journal;

/// A record's sequence number: never zero.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Seq(NonZeroU64);

impl Seq {
    /// The first record of a file.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// `None` for zero.
    #[must_use]
    pub const fn new(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(raw) => Some(Self(raw)),
            None => None,
        }
    }

    /// The number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The next number, or `None` at the largest.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(raw) => Some(Self(raw)),
            None => None,
        }
    }

    /// Whether this number proves a creation returned: only the first record may come from a
    /// creation that did not finish.
    #[must_use]
    pub const fn proves_created(self) -> bool {
        self.0.get() >= 2
    }
}

/// One of the two slots.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Slot {
    /// Bytes 0 to 4096.
    S0,
    /// Bytes 4096 to 8192.
    S1,
}

impl Slot {
    /// The other slot.
    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::S0 => Self::S1,
            Self::S1 => Self::S0,
        }
    }

    /// The slot's first byte in the file.
    #[must_use]
    pub const fn offset(self) -> u64 {
        match self {
            Self::S0 => 0,
            Self::S1 => 4096,
        }
    }
}

/// What reading one slot returned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotRead<'a> {
    /// All 4096 bytes.
    Bytes(&'a [u8; SLOT]),
    /// End of file first.
    Short,
    /// An I/O error.
    Io,
}

/// What opening the file and reading each slot returned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Opened<'a> {
    /// ENOENT.
    Absent,
    /// Any other error opening it.
    Io,
    /// Each slot's read, slot 0 first.
    Slots(SlotRead<'a>, SlotRead<'a>),
}

/// Why a journal that exists holds no record.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Why {
    /// A read failed and no slot is valid.
    Io,
    /// Every slot is short or holds no valid record.
    Invalid,
    /// Two valid slots share a sequence number.
    Tie,
}

/// The read rule's verdict.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// No file.
    Missing,
    /// A file, and no record.
    Unreadable(Why),
    /// The winning record is from a newer version.
    Newer {
        /// Its slot.
        slot: Slot,
        /// Its sequence number.
        seq: Seq,
        /// Its version.
        version: u16,
    },
    /// The winning record.
    Read {
        /// Its slot.
        slot: Slot,
        /// Its sequence number.
        seq: Seq,
        /// What it says.
        journal: Journal,
    },
}

/// The verdict, and whether a slot's read failed with an I/O error (a logged event: the slot
/// that failed may hold the newest record).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Parsed {
    /// The verdict.
    pub verdict: Verdict,
    /// Whether either slot's read failed.
    pub slot_failed: bool,
    /// Why each slot that holds no record holds none, slot 0 first (`None` for a valid slot or
    /// an unopened file).
    pub invalid: [Option<SlotFault>; 2],
}

/// Why one slot holds no record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotFault {
    /// The read was short.
    Short,
    /// The read failed.
    Io,
    /// The bytes hold no valid record.
    Invalid(Invalid),
}

/// One slot's record, or why there is none.
fn judge_slot(read: SlotRead<'_>) -> Result<Decoded, SlotFault> {
    match read {
        SlotRead::Bytes(bytes) => decode(bytes).map_err(SlotFault::Invalid),
        SlotRead::Short => Err(SlotFault::Short),
        SlotRead::Io => Err(SlotFault::Io),
    }
}

/// The read rule: pure and total.
#[must_use]
pub fn parse(opened: Opened<'_>) -> Parsed {
    let (s0, s1) = match opened {
        Opened::Absent => return unopened(Verdict::Missing),
        Opened::Io => return unopened(Verdict::Unreadable(Why::Io)),
        Opened::Slots(s0, s1) => (judge_slot(s0), judge_slot(s1)),
    };
    let slot_failed = matches!(s0, Err(SlotFault::Io)) || matches!(s1, Err(SlotFault::Io));
    let invalid = [s0.err(), s1.err()];
    let seq = |decoded: &Decoded| match decoded {
        Decoded::Ours { seq, .. } | Decoded::Newer { seq, .. } => *seq,
    };
    let winner = match (s0, s1) {
        (Ok(a), Ok(b)) if seq(&a) == seq(&b) => None,
        (Ok(a), Ok(b)) if seq(&b) > seq(&a) => Some((Slot::S1, b)),
        (Ok(a), Ok(_) | Err(_)) => Some((Slot::S0, a)),
        (Err(_), Ok(b)) => Some((Slot::S1, b)),
        (Err(_), Err(_)) => {
            let why = if slot_failed { Why::Io } else { Why::Invalid };
            return Parsed {
                verdict: Verdict::Unreadable(why),
                slot_failed,
                invalid,
            };
        }
    };
    let verdict = match winner {
        None => Verdict::Unreadable(Why::Tie),
        Some((slot, Decoded::Ours { seq, journal })) => Verdict::Read { slot, seq, journal },
        Some((slot, Decoded::Newer { seq, version })) => Verdict::Newer { slot, seq, version },
    };
    Parsed {
        verdict,
        slot_failed,
        invalid,
    }
}

fn unopened(verdict: Verdict) -> Parsed {
    Parsed {
        verdict,
        slot_failed: false,
        invalid: [None, None],
    }
}
