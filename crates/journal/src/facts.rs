//! The four facts. Each is a lower bound, a debt, a count or a self-validating proof, so an older
//! record is always safe to read (docs/journal.md).

use lsr_ltx::{FileChecksum, Timestamp, Txid};

/// An 8-byte digest of the configured destination, computed by the embedding.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ReplicaId(pub [u8; 8]);

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
    /// Whether a record holding `self` may follow one holding `old`: the debt moves only forward
    /// through its cure.
    #[must_use]
    pub const fn may_follow(self, old: Self) -> bool {
        matches!(
            (old, self),
            (Self::No, Self::No | Self::Truncate)
                | (Self::Truncate, Self::Truncate | Self::Snapshot)
                | (Self::Snapshot, Self::Snapshot | Self::No)
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

/// The four facts.
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
}

impl Journal {
    /// What an absent journal means: no floor, no debt known, a full budget, no proof.
    pub const ABSENT: Self = Self {
        floor: None,
        unproven: Unproven::No,
        budget: None,
        pending: None,
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
        if new.unproven.may_follow(self.unproven) {
            Ok(())
        } else {
            Err(Lowered::Debt {
                from: self.unproven,
                to: new.unproven,
            })
        }
    }
}
