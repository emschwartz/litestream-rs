//! A process starts: it opens the journal, reads each slot with its own call, and judges.

use super::super::fs::{Content, Device, Pair, SlotRead};
use super::{
    Base, Config, Facts, Label, Plant, Proc, Property, ReadFault, Rid, Slot, State, Step, Verdict,
    Why,
};

/// What `open` and the slot reads returned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Opened {
    /// ENOENT.
    Absent,
    /// An I/O error on `open`.
    Eio,
    /// Each slot's read.
    Slots(Pair<SlotRead>),
}

impl State {
    /// A boot, with each transient read error the budget allows.
    pub(super) fn boots(&self, config: &Config) -> Vec<Step> {
        let mut faults = vec![None];
        if self.left.read_faults > 0 && self.fs.exists() {
            faults.push(Some(ReadFault::Open));
            faults.extend(Slot::BOTH.map(|slot| Some(ReadFault::Slot(slot))));
        }
        faults
            .into_iter()
            .map(|fault| self.boot(config, fault))
            .collect()
    }

    /// The reads as the file answers them, with `fault` injected: the ground truth.
    fn opened(&self, fault: Option<ReadFault>) -> Opened {
        if !self.fs.exists() {
            return Opened::Absent;
        }
        let read = |slot: Slot| match fault {
            Some(ReadFault::Slot(at)) if at == slot => SlotRead::Eio,
            Some(ReadFault::Slot(_) | ReadFault::Open) | None => self.fs.read(slot),
        };
        match fault {
            Some(ReadFault::Open) => Opened::Eio,
            Some(ReadFault::Slot(_)) | None => {
                Opened::Slots(Pair::new(read(Slot::S0), read(Slot::S1)))
            }
        }
    }

    /// The valid slots in `reads`, with their records' sequence numbers.
    fn valid(&self, reads: &Pair<SlotRead>) -> Vec<(Slot, Rid, u8)> {
        Slot::BOTH
            .into_iter()
            .filter_map(|slot| match reads.get(slot) {
                SlotRead::Got(Content::Rec(rid)) => Some((slot, *rid, self.rec(*rid)?.seq)),
                SlotRead::Got(Content::Zero | Content::Garbage)
                | SlotRead::Short
                | SlotRead::Eio => None,
            })
            .collect()
    }

    /// The reader's verdict and the winning slot, as the writer under `config` computes it.
    fn verdict(&self, config: &Config, opened: Opened) -> (Verdict, Option<Slot>) {
        let reads = match opened {
            Opened::Absent => return (Verdict::Missing, None),
            Opened::Eio if config.has(Plant::EioIsMissing) => return (Verdict::Missing, None),
            Opened::Eio => return (Verdict::Unreadable(Why::Io), None),
            Opened::Slots(reads) => reads,
        };
        let any_eio = Slot::BOTH
            .iter()
            .any(|slot| *reads.get(*slot) == SlotRead::Eio);
        let reads = if config.has(Plant::OneRead) && any_eio {
            Pair::new(SlotRead::Eio, SlotRead::Eio)
        } else {
            reads
        };
        let valid = self.valid(&reads);
        let pick = match valid.as_slice() {
            [] if any_eio && config.has(Plant::EioIsMissing) => return (Verdict::Missing, None),
            [] if any_eio => return (Verdict::Unreadable(Why::Io), None),
            [] => return (Verdict::Unreadable(Why::Invalid), None),
            [one] => *one,
            [a, b] if a.2 == b.2 => return (Verdict::Unreadable(Why::Tie), None),
            [a, b] if (a.2 > b.2) != config.has(Plant::LowerSeqWins) => *a,
            [_, b, ..] => *b,
        };
        let (slot, rid, _) = pick;
        let newer = self.rec(rid).is_some_and(|rec| rec.newer);
        let verdict = if newer {
            Verdict::Newer(rid)
        } else {
            Verdict::Read(rid)
        };
        (verdict, Some(slot))
    }

    /// The boot-time properties.
    fn judge_boot(
        &self,
        config: &Config,
        fault: Option<ReadFault>,
        verdict: Verdict,
    ) -> Result<(), Property> {
        let missing = matches!(verdict, Verdict::Missing);
        if missing && self.fs.exists() {
            return Err(Property::EioNotMissing);
        }
        if missing && self.ghost.floor.is_some() {
            return Err(Property::MissingAfterAck);
        }
        if let Opened::Slots(truth) = self.opened(fault) {
            let valid = self.valid(&truth);
            if let [a, b] = valid.as_slice()
                && a.2 == b.2
            {
                return Err(Property::NoTie);
            }
            let lost = matches!(
                verdict,
                Verdict::Missing | Verdict::Unreadable(Why::Io | Why::Invalid)
            );
            if lost && !valid.is_empty() {
                return Err(Property::SlotSurvives);
            }
        }
        let tears = matches!(config.device, Device::Tears { .. });
        let strict = fault.is_none() && !self.ghost.media && !(tears && self.ghost.relaxed);
        if strict {
            self.judge_strict(verdict)?;
        }
        Ok(())
    }

    /// The strict form: nothing older than the newest record acknowledged or claimed durable,
    /// and its floor not lower unless a reset was logged since.
    fn judge_strict(&self, verdict: Verdict) -> Result<(), Property> {
        let Some(floor) = self.ghost.floor else {
            return Ok(());
        };
        let read = match verdict {
            Verdict::Read(rid) | Verdict::Newer(rid) if rid >= floor => rid,
            Verdict::Read(_) | Verdict::Newer(_) | Verdict::Missing | Verdict::Unreadable(_) => {
                return Err(Property::Recovered);
            }
        };
        if let (Verdict::Read(_), Some(got), Some(acked)) =
            (verdict, self.rec(read), self.rec(floor))
            && !self.ghost.reset
            && got.facts.floor_below(&acked.facts)
        {
            return Err(Property::FloorMonotone);
        }
        Ok(())
    }

    /// One boot with `fault` injected.
    fn boot(&self, config: &Config, fault: Option<ReadFault>) -> Step {
        let (verdict, winner) = self.verdict(config, self.opened(fault));
        let label = Label::Boot(fault, verdict);
        if let Err(property) = self.judge_boot(config, fault, verdict) {
            return (label, Err(property));
        }
        let mut next = self.clone();
        if fault.is_some() {
            next.left.read_faults = next.left.read_faults.saturating_sub(1);
        }
        // Every verdict but a clean read is a logged event; a slot that failed to read is one
        // too (its record may be the newest, and the floor may now fall back to the other's).
        let slot_failed = matches!(self.opened(fault), Opened::Slots(reads)
            if Slot::BOTH.iter().any(|slot| *reads.get(*slot) == SlotRead::Eio));
        next.ghost.reset |= slot_failed || !matches!(verdict, Verdict::Read(_));
        if slot_failed || matches!(verdict, Verdict::Unreadable(_)) {
            // A logged reset: the engine starts again from what it could read (main's rules
            // when nothing), and the next write may overwrite the slot it could not read, so
            // nothing acknowledged before is owed any more.
            next.ghost.floor = None;
        }
        let (rid, read) = match verdict {
            Verdict::Read(rid) => (Some(rid), self.rec(rid).map(|rec| rec.facts)),
            Verdict::Newer(rid) => (Some(rid), None),
            Verdict::Missing | Verdict::Unreadable(_) => (None, None),
        };
        let seq = rid.and_then(|rid| self.rec(rid)).map_or(0, |rec| rec.seq);
        let durable = rid.filter(|_| config.has(Plant::ClaimWinner));
        if let Some(claimed) = durable {
            next.ghost.floor = next.ghost.floor.max(Some(claimed));
        }
        let first = if config.has(Plant::Seq1Created) { 1 } else { 2 };
        next.proc = Some(Proc {
            // A slot that could not be read may hold a record numbered above the winner's (an
            // earlier process's failed fdatasync lets the slots drift apart), so a writer that
            // continued from the winner could number its records below it. It re-establishes
            // instead, carrying what it read.
            base: if rid.is_some() && (!slot_failed || config.has(Plant::ContinueAfterSlotError)) {
                Base::Winner
            } else {
                Base::Establish
            },
            seq,
            winner: winner.unwrap_or(Slot::S0),
            next: match winner {
                Some(slot) if config.has(Plant::WrongSlot) => slot,
                Some(slot) => slot.other(),
                None => Slot::S0,
            },
            created: seq >= first,
            synced: false,
            durable,
            last: read,
            facts: read.unwrap_or(Facts::ABSENT),
            job: None,
            retry: None,
        });
        (label, Ok(next))
    }
}
