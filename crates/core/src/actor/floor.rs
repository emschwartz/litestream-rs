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

    /// Takes the floor a journal recorded when it is the configured destination's. A floor held
    /// already (a rebuilt engine boots beside its `Kept`) stands: the record holds only what this
    /// process wrote, so it is never ahead of it, and it lags when the journal cannot be written.
    pub(super) fn adopt_floor(&mut self, recorded: Option<Floor>) {
        let ours = recorded.filter(|floor| Some(floor.replica()) == self.kept.cfg.destination);
        self.kept.floor = self.kept.floor.or(ours);
    }

    /// The floor's bound a boot keeps before it adopts the journal: the floor held, or the one
    /// the journal it just read records for the configured destination, whichever is higher.
    /// Recovery takes it as the replica's tip, so a staged position below it is behind the
    /// replica and the boot snapshots above it, whatever a listing or a read found short.
    pub(super) fn boot_floor(&self, journal: Option<&lsr_journal::Parsed>) -> Option<Txid> {
        let read = journal.and_then(|parsed| match parsed.verdict {
            lsr_journal::Verdict::Read { journal, .. } => journal.floor,
            lsr_journal::Verdict::Newer { .. }
            | lsr_journal::Verdict::Missing
            | lsr_journal::Verdict::Unreadable(_) => None,
        });
        let ours = read.filter(|floor| Some(floor.replica()) == self.kept.cfg.destination);
        let bound = |floor: Floor| floor.bound();
        self.kept.floor.map(bound).max(ours.map(bound))
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
