//! The reference writer: the engine's decisions and each system call of one write.
//!
//! - **Establish** (no file, or no record): `open(O_CREAT)`, one write of 8192 bytes (the record
//!   with `seq = 1` in slot 0, zeros in slot 1), `fsync`, `fsync` of the directory. It never
//!   unlinks: a file that exists and holds no record is first zeroed whole and synced, then
//!   established in place. The zeroing is what lets sequence numbers start again at 1: without
//!   it a create whose `fsync` failed leaves clean pages showing `seq = 1` over a disk that may
//!   still hold the old file's records with higher numbers, and a later record could lose to
//!   one of them.
//! - **Finish** (the winner has `seq = 1`, so creation may not have returned): zeros into the
//!   other slot (which also extends a short file), `fsync`, `fsync` of the directory. Then the
//!   write proper. The winner's slot is not touched.
//! - **Boot sync** (the first write of a lifetime): `fdatasync`, so a slot an earlier process
//!   wrote and did not sync is on disk before the other slot is touched.
//! - **Write**: the record with the next `seq` into the slot that does not hold the newest
//!   record, then `fdatasync`. Success acknowledges it and moves to the other slot. Failure
//!   moves nothing: the next attempt rewrites the same slot, never a bare `fdatasync`.

use super::super::fs::{Content, Len, Partial, Rid, Slot};
use super::{
    Base, Call, Config, Facts, Floor, Label, Op, Outcome, Pc, Plant, Proc, Property, Rec, State,
    Step,
};

/// A system call's possible results: its outcome, the file after it, and whether it spent a
/// failure from the budget.
type Results = Vec<(Outcome, super::Fs, bool)>;

impl State {
    /// The engine's possible requests when no write is in flight.
    pub(super) fn decisions(&self, config: &Config, proc: Proc) -> Vec<Step> {
        let Some(left) = self.left.writes.checked_sub(1) else {
            return Vec::new();
        };
        if let (true, Some(rid)) = (config.has(Plant::BareRetry), proc.retry) {
            let mut next = self.clone();
            next.left.writes = left;
            next.proc = Some(Proc {
                job: Some((rid, Pc::RecSync)),
                retry: None,
                ..proc
            });
            return vec![(Label::Engine(Op::Advance), Ok(next))];
        }
        let mut ops = vec![Op::Advance];
        if self.left.resets > 0 {
            ops.push(Op::Reset);
        }
        if config.lower {
            ops.push(Op::Lower);
        }
        ops.into_iter()
            .map(|op| self.decide(config, proc, op, left))
            .collect()
    }

    /// The facts the engine asks for with `op`.
    fn intent(next: &mut Self, proc: &Proc, op: Op) -> Facts {
        let facts = proc.facts;
        match op {
            Op::Advance => {
                let bound = match facts.floor {
                    Some(floor) if floor.replica == next.dest => floor.bound.saturating_add(1),
                    Some(_) | None => 0,
                };
                Facts {
                    floor: Some(Floor {
                        replica: next.dest,
                        bound,
                    }),
                    unproven: facts.unproven.next(),
                    tokens: u8::from(facts.tokens == 0),
                    pending: !facts.pending,
                }
            }
            Op::Reset => {
                next.left.resets = next.left.resets.saturating_sub(1);
                next.dest = next.dest.saturating_add(1);
                next.ghost.reset = true;
                Facts {
                    floor: Some(Floor {
                        replica: next.dest,
                        bound: 0,
                    }),
                    ..facts
                }
            }
            Op::Lower => Facts {
                floor: facts.floor.map(|floor| Floor {
                    bound: floor.bound.saturating_sub(1),
                    ..floor
                }),
                ..facts
            },
        }
    }

    /// One engine request: refused if not monotonic, else issued and its first call queued.
    fn decide(&self, config: &Config, proc: Proc, op: Op, left: u8) -> Step {
        let mut next = self.clone();
        next.left.writes = left;
        let intent = Self::intent(&mut next, &proc, op);
        if !config.has(Plant::NoMonotonic) && !Facts::monotonic(proc.last.as_ref(), &intent) {
            return (Label::Refused(op), Ok(next));
        }
        let acked = next
            .ghost
            .floor
            .and_then(|rid| next.rec(rid))
            .map(|rec| rec.facts);
        if let Some(acked) = acked
            && !next.ghost.reset
            && intent.floor_below(&acked)
        {
            return (Label::Engine(op), Err(Property::FloorMonotone));
        }
        let pc = self.first_pc(config, &proc);
        let seq = match pc {
            Pc::EstCreate | Pc::EstZero => 1,
            Pc::EstZeroSync
            | Pc::EstWrite
            | Pc::EstSync
            | Pc::EstDir
            | Pc::FinWrite
            | Pc::FinSync
            | Pc::FinDir
            | Pc::BootSync
            | Pc::RecWrite
            | Pc::RecSync => proc.seq.saturating_add(1),
        };
        let issued = next.issue(Rec {
            seq,
            facts: intent,
            newer: false,
        });
        next.proc = Some(Proc {
            facts: intent,
            last: Some(intent),
            job: issued.map(|rid| (rid, pc)),
            ..proc
        });
        (Label::Engine(op), Ok(next))
    }

    /// Where a new write starts.
    fn first_pc(&self, config: &Config, proc: &Proc) -> Pc {
        match proc.base {
            Base::Establish if self.fs.exists() && !config.has(Plant::RecreateWithoutZeroing) => {
                Pc::EstZero
            }
            Base::Establish => Pc::EstCreate,
            Base::Winner if !proc.created => Pc::FinWrite,
            Base::Winner if !proc.synced && !config.has(Plant::NoBootSync) => Pc::BootSync,
            Base::Winner => Pc::RecWrite,
        }
    }

    /// The results of writing all of `slot`.
    fn slot_writes(&self, config: &Config, slot: Slot, content: Content) -> Results {
        if self.full && self.fs.allocates(slot) {
            return vec![(Outcome::Enospc, self.fs, false)];
        }
        let block = config.block;
        let mut out = vec![(
            Outcome::Ok,
            self.fs.write(slot, content, block, None),
            false,
        )];
        let partials: &[Partial] = if config.torn_writes {
            &[Partial::Untouched, Partial::Torn, Partial::Head]
        } else {
            &[Partial::Untouched]
        };
        if self.left.failures > 0 {
            for &partial in partials {
                let fs = self.fs.write(slot, content, block, Some(partial));
                out.push((Outcome::Failed(partial), fs, true));
            }
        }
        out
    }

    /// The results of one write of 8192 bytes: `first` in slot 0, zeros in slot 1.
    fn whole_writes(&self, config: &Config, first_slot: Content) -> Results {
        let block = config.block;
        let mut out = Vec::new();
        for (outcome, first, failed) in self.slot_writes(config, Slot::S0, first_slot) {
            if outcome != Outcome::Ok {
                out.push((outcome, first, failed));
                continue;
            }
            let record = matches!(first_slot, Content::Rec(_));
            let whole = if record && config.has(Plant::SetLenCreate) {
                first.set_full_len()
            } else if self.full && first.allocates(Slot::S1) {
                out.push((Outcome::Enospc, first, false));
                continue;
            } else {
                first.write(Slot::S1, Content::Zero, block, None)
            };
            out.push((Outcome::Ok, whole, false));
            if self.left.failures > 0 {
                out.push((Outcome::Failed(Partial::FirstSlot), first, true));
            }
        }
        out
    }

    /// The results of an `fsync` of the file.
    fn syncs(&self, config: &Config) -> Results {
        let mut out = vec![(Outcome::Ok, self.fs.synced(), false)];
        if self.left.failures > 0 {
            for (fs, reaches) in self.fs.sync_failures(config.block, config.device) {
                out.push((Outcome::SyncFailed(reaches), fs, true));
            }
        }
        out
    }

    /// The results of an `fsync` of the directory.
    fn dir_syncs(&self) -> Results {
        let mut out = vec![(Outcome::Ok, self.fs.dir_synced(), false)];
        if self.left.failures > 0 {
            out.push((Outcome::DirFailed(false), self.fs, true));
            out.push((Outcome::DirFailed(true), self.fs.dir_synced(), true));
        }
        out
    }

    /// The results of `open(O_CREAT)`.
    fn creates(&self) -> Results {
        let mut out = vec![(Outcome::Ok, self.fs.create(), false)];
        if self.left.failures > 0 {
            out.push((Outcome::Failed(Partial::Untouched), self.fs, true));
        }
        out
    }

    /// Every result of the call at `pc`, each carried to the writer's next state.
    pub(super) fn syscall(&self, config: &Config, proc: Proc, (rid, pc): (Rid, Pc)) -> Vec<Step> {
        let (call, results) = match pc {
            Pc::EstCreate => (Call::Create, self.creates()),
            Pc::EstZero => (Call::WriteZeros, self.whole_writes(config, Content::Zero)),
            Pc::EstWrite => (
                Call::WriteWhole,
                self.whole_writes(config, Content::Rec(rid)),
            ),
            Pc::FinWrite => {
                let other = proc.winner.other();
                (
                    Call::WriteSlot(other),
                    self.slot_writes(config, other, Content::Zero),
                )
            }
            Pc::RecWrite => {
                let seq = self.rec(rid).map_or(0, |rec| rec.seq);
                if seq >= 2 && self.fs.allocates(proc.next) {
                    let label = Label::Sys(Call::WriteSlot(proc.next), Outcome::Ok);
                    return vec![(label, Err(Property::NoAllocation))];
                }
                let record = Content::Rec(rid);
                (
                    Call::WriteSlot(proc.next),
                    self.slot_writes(config, proc.next, record),
                )
            }
            Pc::EstZeroSync | Pc::EstSync | Pc::FinSync | Pc::BootSync | Pc::RecSync => {
                (Call::Fsync, self.syncs(config))
            }
            Pc::EstDir | Pc::FinDir => (Call::DirFsync, self.dir_syncs()),
        };
        results
            .into_iter()
            .map(|(outcome, fs, failed)| {
                let mut next = self.clone();
                next.fs = fs;
                if failed {
                    next.left.failures = next.left.failures.saturating_sub(1);
                }
                let label = Label::Sys(call, outcome);
                let ok = outcome == Outcome::Ok;
                (
                    label,
                    next.advance(config, proc, rid, pc, ok).map(|()| next),
                )
            })
            .collect()
    }

    /// The writer after the call at `pc` returned (`ok` or not).
    fn advance(
        &mut self,
        config: &Config,
        proc: Proc,
        rid: Rid,
        pc: Pc,
        ok: bool,
    ) -> Result<(), Property> {
        let goto = |pc: Pc| {
            Some(Proc {
                job: Some((rid, pc)),
                ..proc
            })
        };
        let sync = matches!(
            pc,
            Pc::EstZeroSync | Pc::EstSync | Pc::FinSync | Pc::BootSync | Pc::RecSync
        );
        if !ok && sync {
            self.ghost.outstanding = true;
            self.ghost.relaxed |= matches!(pc, Pc::BootSync);
        }
        if !ok && pc == Pc::RecSync && config.has(Plant::FailedSyncIsSuccess) {
            return self.ack(config, proc, rid, Some(proc.next));
        }
        if !ok {
            let retry = (pc == Pc::RecSync && config.has(Plant::BareRetry)).then_some(rid);
            self.proc = Some(Proc {
                job: None,
                retry,
                ..proc
            });
            return Ok(());
        }
        self.proc = match pc {
            Pc::EstCreate | Pc::EstZeroSync => goto(Pc::EstWrite),
            Pc::EstZero => goto(Pc::EstZeroSync),
            Pc::EstWrite => goto(Pc::EstSync),
            Pc::EstSync if config.has(Plant::NoDirFsync) => {
                return self.ack(config, proc, rid, None);
            }
            Pc::EstSync => goto(Pc::EstDir),
            Pc::EstDir => return self.ack(config, proc, rid, None),
            Pc::FinWrite => goto(Pc::FinSync),
            Pc::FinSync => goto(Pc::FinDir),
            Pc::FinDir | Pc::BootSync => Some(Proc {
                created: true,
                synced: true,
                job: Some((rid, Pc::RecWrite)),
                ..proc
            }),
            Pc::RecWrite if config.has(Plant::NoSync) => {
                return self.ack(config, proc, rid, Some(proc.next));
            }
            Pc::RecWrite => goto(Pc::RecSync),
            Pc::RecSync => return self.ack(config, proc, rid, Some(proc.next)),
        };
        Ok(())
    }

    /// `rid` is acknowledged: it was written to `written` (to slot 0 by a create when `None`).
    fn ack(
        &mut self,
        config: &Config,
        proc: Proc,
        rid: Rid,
        written: Option<Slot>,
    ) -> Result<(), Property> {
        if !(self.fs.entry_durable() && self.fs.len_durable() == Len::Full) {
            return Err(Property::CreateFinished);
        }
        self.ghost.floor = self.ghost.floor.max(Some(rid));
        self.ghost.reset = false;
        self.ghost.outstanding = false;
        self.ghost.relaxed = false;
        let written = written.unwrap_or(Slot::S0);
        let next = if config.has(Plant::WrongSlot) {
            written
        } else {
            written.other()
        };
        self.proc = Some(Proc {
            base: Base::Winner,
            seq: self.rec(rid).map_or(proc.seq, |rec| rec.seq),
            winner: written,
            next,
            created: true,
            synced: true,
            durable: Some(rid),
            job: None,
            retry: None,
            ..proc
        });
        Ok(())
    }
}
