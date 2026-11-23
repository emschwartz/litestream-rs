//! The confirmed floor: the never-lowered mark of what this engine confirmed on its replica,
//! held in the engine journal (`lsr_journal::Floor`, `docs/journal.md`; issue #171, item 4).
//!
//! The floor says every transaction id at or below its bound is taken on one destination, and,
//! with an anchor, which bytes that destination held at the bound when this engine confirmed
//! them. It rises with the uploader's confirmed transaction, anchored on the confirmed file's
//! witness (its length, raw-byte digest and file checksum) when this process holds one, and it
//! is written at the end of the tick, as every journalled fact is. A boot reads it back for the
//! same destination only: the configured one (`ActorConfig::destination`), so a replica moved
//! to another bucket starts with no floor. With no destination named the engine neither adopts
//! nor writes one, and a record's floor passes through untouched.
//!
//! It never falls. A bucket rollback takes files from the replica but frees no id: the anchor
//! is withdrawn, since the replica no longer holds those bytes, and the bound stays. A boot whose
//! listing ends below the floor keeps the floor; what the boot does about it is the boot rule's.

use lsr_journal::Floor;
use lsr_ltx::Txid;

use super::Actor;

impl Actor {
    /// The floor after the confirmed transaction became `confirmed`: raised to it, anchored on
    /// its witness when this process holds one. Called before the tokens through it are let go.
    pub(super) fn raise_floor(&mut self, confirmed: Option<Txid>) {
        let (Some(destination), Some(bound)) = (self.kept.cfg.destination, confirmed) else {
            return;
        };
        if self.kept.floor.is_some_and(|floor| floor.bound() >= bound) {
            return;
        }
        let anchor = self
            .engine
            .witnesses
            .ending_at(bound)
            .map(crate::witness::Witnessed::triple);
        self.kept.floor = Some(Floor::new(destination, bound, anchor));
    }

    /// Takes the floor a journal recorded when it is the configured destination's.
    pub(super) fn adopt_floor(&mut self, recorded: Option<Floor>) {
        let ours = recorded.filter(|floor| Some(floor.replica()) == self.kept.cfg.destination);
        self.kept.floor = match (self.kept.floor, ours) {
            (Some(held), Some(read)) if read.bound() > held.bound() => Some(read),
            (Some(held), _) => Some(held),
            (None, read) => read,
        };
    }

    /// The replica lost files it held (a bucket rollback): the floor claims nothing about its
    /// bytes any more, and its bound stays.
    pub(super) fn floor_regressed(&mut self) {
        self.kept.floor = self.kept.floor.map(Floor::withdraw_anchor);
    }

    /// The confirmed floor for the configured destination: no transaction id at or below its
    /// bound may be taken again, and its anchor, when present, is the bytes the replica held
    /// at the bound when this engine confirmed them.
    #[must_use]
    pub const fn floor(&self) -> Option<Floor> {
        self.kept.floor
    }
}
