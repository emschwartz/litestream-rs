//! The five facts. Each is a lower bound, a debt, a count, a self-validating proof or a dated
//! verdict, so an older record is always safe to read (docs/journal.md).

use std::num::NonZeroU32;

use lsr_ltx::{FileChecksum, Timestamp, Txid};

/// An 8-byte digest of the configured destination, computed by the embedding.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ReplicaId(pub [u8; 8]);

impl ReplicaId {
    /// The digest of a destination's canonical name (FNV-1a 64, little-endian): two names
    /// that differ in any byte are, for this purpose, two destinations.
    #[must_use]
    pub fn of(name: &[u8]) -> Self {
        let digest = name.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
        });
        Self(digest.to_le_bytes())
    }
}

/// A staged file's bytes: its length, the FNV-1a 64 digest of its raw bytes, and its LTX file
/// checksum.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Triple {
    /// The length in bytes.
    pub len: u64,
    /// The raw-byte digest.
    pub digest: u64,
    /// The trailer's file checksum.
    pub checksum: FileChecksum,
}

/// The floor: every transaction id at or below `bound` is taken on `replica`, and, with an
/// anchor, the replica held those bytes at `bound` when this engine confirmed them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Floor {
    replica: ReplicaId,
    bound: Txid,
    anchor: Option<Triple>,
}

/// A floor would fall for the same destination: an engine bug.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("the floor for this destination would fall from {from} to {to}")]
pub struct WouldLower {
    /// The bound held.
    pub from: Txid,
    /// The bound asked for.
    pub to: Txid,
}

impl Floor {
    /// A first floor.
    #[must_use]
    pub const fn new(replica: ReplicaId, bound: Txid, anchor: Option<Triple>) -> Self {
        Self {
            replica,
            bound,
            anchor,
        }
    }

    /// The floor after a confirmed upload at `bound` on `replica`. Another destination starts a
    /// new floor; the same one never falls.
    ///
    /// # Errors
    ///
    /// [`WouldLower`] when `bound` is below this floor's bound for the same destination.
    pub fn advance(
        self,
        replica: ReplicaId,
        bound: Txid,
        anchor: Option<Triple>,
    ) -> Result<Self, WouldLower> {
        if replica == self.replica && bound < self.bound {
            return Err(WouldLower {
                from: self.bound,
                to: bound,
            });
        }
        Ok(Self::new(replica, bound, anchor))
    }

    /// The same bound, claiming nothing about the replica's bytes.
    #[must_use]
    pub const fn withdraw_anchor(self) -> Self {
        Self {
            anchor: None,
            ..self
        }
    }

    /// The destination.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// The bound.
    #[must_use]
    pub const fn bound(&self) -> Txid {
        self.bound
    }

    /// The anchor, if any.
    #[must_use]
    pub const fn anchor(&self) -> Option<Triple> {
        self.anchor
    }
}

/// The unproven-WAL debt: a WAL fsync failed and its cure is at this step.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unproven {
    /// No debt.
    No,
    /// A TRUNCATE checkpoint is owed.
    Truncate,
    /// The TRUNCATE ran; a snapshot is owed.
    Snapshot,
}

impl Unproven {
    /// Whether a record holding `self` may follow one holding `old`: a debt is never forgiven.
    /// It is paid only through its cure (a TRUNCATE, then a snapshot), and a new WAL fsync
    /// failure may raise it again at any step.
    #[must_use]
    pub const fn may_follow(self, old: Self) -> bool {
        matches!(
            (old, self),
            (_, Self::Truncate)
                | (Self::No, Self::No)
                | (Self::Truncate | Self::Snapshot, Self::Snapshot)
                | (Self::Snapshot, Self::No)
        )
    }
}

/// The snapshot budget's stored numbers; the scheduling rule gives them meaning.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct StoredBudget {
    /// Tokens left.
    pub tokens: u8,
    /// When they were counted.
    pub as_of: Timestamp,
}

/// The WAL's cumulative frame checksum at a staged file's end offset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WalChecksum(pub u32, pub u32);

/// The proof for the first pending staged file, when it starts a WAL generation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Proof {
    /// The file's transaction.
    pub txid: Txid,
    /// Its bytes.
    pub triple: Triple,
    /// The WAL's chain value at its end.
    pub wal: WalChecksum,
}

/// A boot found the database file ahead of its WAL: pages no recovered WAL frame covers differ
/// from their images at the newest staged transaction, so a checkpoint copied frames a failed WAL
/// sync covered and the WAL then lost them (`docs/decisions.md` D103). The engine keeps
/// `restore_point` restorable until a fixed window after `made` has passed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Ahead {
    /// The newest staged transaction at the check: the last state before the damage.
    pub restore_point: Txid,
    /// How many pages differed.
    pub pages: NonZeroU32,
    /// The lowest page that differed.
    pub first: NonZeroU32,
    /// When the verdict was made, on the engine's clock.
    pub made: Timestamp,
}

/// The five facts.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Journal {
    /// The floor, if any.
    pub floor: Option<Floor>,
    /// The unproven-WAL debt.
    pub unproven: Unproven,
    /// The budget; `None` is a full one.
    pub budget: Option<StoredBudget>,
    /// The pending proof, if any.
    pub pending: Option<Proof>,
    /// A database file found ahead of its WAL, while its restore point is kept.
    pub ahead: Option<Ahead>,
}

/// Why a record may not follow another (docs/journal.md, "Writing", rule 5).
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum Lowered {
    /// The same destination's bound would fall.
    #[error(transparent)]
    Floor(#[from] WouldLower),
    /// A floor would be dropped.
    #[error("the floor would be dropped")]
    FloorDropped,
    /// The debt would move other than forward through its cure.
    #[error("the unproven-WAL debt would move from {from:?} to {to:?}")]
    Debt {
        /// The debt held.
        from: Unproven,
        /// The debt asked for.
        to: Unproven,
    },
    /// A later verdict would move the kept restore point forward, onto a state the earlier
    /// verdict found damaged.
    #[error("the kept restore point would move from {from} to {to}")]
    AheadRestorePoint {
        /// The restore point held.
        from: Txid,
        /// The restore point asked for.
        to: Txid,
    },
}

impl Journal {
    /// What an absent journal means: no floor, no debt known, a full budget, no proof, no
    /// verdict.
    pub const ABSENT: Self = Self {
        floor: None,
        unproven: Unproven::No,
        budget: None,
        pending: None,
        ahead: None,
    };

    /// Whether `new` may follow `self` in consecutive records.
    ///
    /// # Errors
    ///
    /// The first rule `new` breaks.
    pub fn monotonic(&self, new: &Self) -> Result<(), Lowered> {
        match (self.floor, new.floor) {
            (Some(old), Some(floor)) => {
                old.advance(floor.replica, floor.bound, floor.anchor)?;
            }
            (Some(_), None) => return Err(Lowered::FloorDropped),
            (None, _) => {}
        }
        if !new.unproven.may_follow(self.unproven) {
            return Err(Lowered::Debt {
                from: self.unproven,
                to: new.unproven,
            });
        }
        // A verdict ends when its window passes, which this record cannot see; while two stand,
        // the older restore point is the one before any damage.
        match (self.ahead, new.ahead) {
            (Some(old), Some(ahead)) if ahead.restore_point > old.restore_point => {
                Err(Lowered::AheadRestorePoint {
                    from: old.restore_point,
                    to: ahead.restore_point,
                })
            }
            (Some(_) | None, Some(_) | None) => Ok(()),
        }
    }
}
