//! The replica on the S3 store ([`crate::remote`]): the run's switch, and the provider faults a
//! history draws for it ([`crate::Op::HttpFault`]).

use super::{Opts, Runner};

/// The refusal a drawn fault must come back as, when it is one.
const fn expected(fault: HttpFault) -> Option<Denied> {
    match fault {
        HttpFault::Denied(_, code) => Some(crate::remote::expected_denial(code)),
        HttpFault::DenyDeletes => Some(Denied::Permission),
        HttpFault::SlowDown(_)
        | HttpFault::TooManyRequests(_)
        | HttpFault::Internal(_)
        | HttpFault::RequestTimeout(_)
        | HttpFault::AfterPublish(_)
        | HttpFault::TruncateBody(_)
        | HttpFault::WrongEtag(_)
        | HttpFault::ConnectionReset(_) => None,
    }
}
use lsr_store_s3::{Fault, Shape};

use lsr_core::{Degraded, Denied, Event, PersistentCause, StopCause, Task, Tick};

use crate::failstop::{Fired, fired};
use crate::generate::HttpFault;
use crate::oracle::Violation;
use crate::world::TickEnd;

impl Runner {
    /// Puts the replica on S3 when `opts` asks, before the first boot.
    pub(super) fn with_remote_s3(mut self, opts: &Opts) -> Self {
        if opts.remote_s3 {
            self.w.use_s3_remote();
            self.report.reach.bump("remote_s3.runs");
        }
        self
    }

    /// Pushes `fault` onto the S3 store's fault layer; a step with nothing to act on on
    /// `MemStore`.
    pub(super) fn http_fault(&mut self, fault: HttpFault) -> Result<(), Violation> {
        let pushed = match self.w.remote_raw.wire() {
            Some(wire) => {
                match crate::remote::provider_fault(fault) {
                    Some(provider) => wire.push(provider),
                    None => self.w.remote_raw.deny_deletes(),
                }
                if let Some(why) = expected(fault) {
                    self.expect(StopCause::StoreDenied(why));
                }
                true
            }
            None => false,
        };
        self.scenario_bool("HttpFault", pushed)
    }

    /// The provider faults a listing, a HEAD or a GET has met so far: each cut a read short,
    /// as an object-level read fault does.
    pub(super) fn http_read_faults(&self) -> u32 {
        let fired = self
            .w
            .remote_raw
            .wire()
            .map_or_else(Vec::new, |wire| wire.fired());
        let reads = fired
            .iter()
            .filter(|fault| matches!(fault.on(), Shape::List | Shape::Head | Shape::Get))
            .count();
        u32::try_from(reads).unwrap_or(u32::MAX)
    }

    /// The replica's fault fires for the fail-stop judge (`crate::failstop::fired`): the
    /// object-level layer's, and the provider faults the S3 store's requests met. A provider
    /// fault is doubtful when the bucket took the write before the answer went wrong, and a
    /// refusal when it denied the request (or a policy refused a key inside a delete).
    pub(super) fn remote_fired(&self) -> Fired {
        let object = fired(&self.w.remote_faults);
        let provider = self
            .w
            .remote_raw
            .wire()
            .map_or_else(Vec::new, |wire| wire.fired());
        let refused_deletes = self.w.remote_raw.refused_deletes();
        let object = Fired {
            all: object.all.saturating_add(refused_deletes),
            refused: object.refused.saturating_add(refused_deletes),
            doubtful: object.doubtful,
        };
        provider.iter().fold(object, |was, fault| {
            let refused = crate::remote::denial_of(fault).is_some();
            let doubtful = matches!(
                fault,
                Fault::AfterPublish { .. }
                    | Fault::WrongEtag { on: Shape::Put }
                    | Fault::TruncateBody { on: Shape::Put, .. }
            );
            Fired {
                all: was.all.saturating_add(1),
                refused: was.refused.saturating_add(u64::from(refused)),
                doubtful: was.doubtful.saturating_add(u64::from(doubtful)),
            }
        })
    }

    /// A refusal the S3 replica answered in the tick must come back as the engine's report of
    /// it: a task degraded with it, or a stop for it (the engine takes any store refusal as
    /// persistent); of several in one tick, at least one. A refusal reported as another, or as a fault to retry, tells the
    /// operator to fix the wrong thing.
    ///
    /// # Errors
    /// [`Violation::DenialMisreported`].
    /// Where the refusal judge starts counting, taken just before a tick so that a refusal met
    /// by the simulator's own reads between ticks is not charged to the engine.
    pub(super) fn denials_baseline(&mut self) {
        self.degraded_before = Task::ALL
            .iter()
            .filter_map(|task| self.actor.degraded(*task).map(|degraded| (*task, degraded)))
            .collect();
        if let Some(wire) = self.w.remote_raw.wire() {
            let (fired_now, refused, others) = self.denial_counts(wire);
            self.denials_seen = (fired_now.len(), refused, others);
        }
    }

    fn denial_counts(&self, wire: &crate::remote::Wire) -> (Vec<Fault>, u64, u64) {
        let others = crate::failstop::fired(&self.w.remote_faults)
            .all
            .saturating_add(crate::failstop::fired(&self.w.staging_faults).all);
        (wire.fired(), self.w.remote_raw.refused_deletes(), others)
    }

    pub(super) fn judge_denials(&mut self, ended: &TickEnd<Tick>) -> Result<(), Violation> {
        let Some(owed) = self.owed_denials() else {
            return Ok(());
        };
        if owed.masked || matches!(ended, TickEnd::Crashed { .. }) {
            self.report.reach.bump("http_fault.denial_masked");
            return Ok(());
        }
        let mut reported: Vec<PersistentCause> = Task::ALL
            .iter()
            .filter_map(|task| self.actor.degraded(*task).map(|degraded| degraded.cause))
            .collect();
        if let TickEnd::Done(Tick::Stopped(cause)) = ended {
            reported.extend(PersistentCause::of(*cause));
        }
        // A task standing degraded on the WAL or the database says nothing of a request the
        // store refused in this tick: a read-ahead HEAD (maintenance's created-at reads) the
        // engine drops by design is not a failure the engine owed a report of.
        //
        // A degradation that stood unchanged since before the tick (the same cause, the same
        // count of failures) is no failure of this tick's requests: a later tick that only reads
        // ahead must still be allowed to drop its refused HEAD while an earlier, unrelated
        // refusal keeps a task degraded.
        let failed = fresh_causes(&self.degraded_before, |task| self.actor.degraded(task))
            .into_iter()
            .any(|cause| !says_nothing_of_requests(cause))
            || matches!(
                ended,
                TickEnd::Done(Tick::Ran {
                    event: Event::Retrying { .. },
                    ..
                })
            )
            || matches!(
                ended,
                TickEnd::Done(Tick::Ran {
                    event: Event::Degraded { cause, .. },
                    ..
                }) if !says_nothing_of_requests(*cause)
            );
        if owed.may_drop && !failed {
            self.report.reach.bump("http_fault.denial_read_ahead");
            return Ok(());
        }
        // Requests sent as one batch can meet several refusals; the engine reports the first.
        if owed
            .kinds
            .iter()
            .any(|why| reported.contains(&PersistentCause::StoreDenied(*why)))
        {
            self.report.reach.bump("http_fault.denial_reported");
            return Ok(());
        }
        Err(Violation::DenialMisreported {
            expected: owed.first,
            got: format!("{reported:?}"),
        })
    }

    /// The refusals met since [`Self::denials_baseline`], and moves the baseline past them.
    fn owed_denials(&mut self) -> Option<Owed> {
        let wire = self.w.remote_raw.wire()?;
        let (fired_now, refused, others) = self.denial_counts(wire);
        let new = fired_now.get(self.denials_seen.0..).unwrap_or_default();
        let deletes_refused = refused > self.denials_seen.1;
        let denials: Vec<&Fault> = new
            .iter()
            .filter(|fault| crate::remote::denial_of(fault).is_some())
            .collect();
        let mut kinds: Vec<Denied> = denials
            .iter()
            .filter_map(|fault| crate::remote::denial_of(fault))
            .collect();
        if deletes_refused {
            kinds.push(Denied::Permission);
        }
        // A HEAD may be a created-at time read ahead, whose failure is dropped by design (see
        // `lsr_core::created_at`): it may go unreported, never misreported.
        let may_drop = !deletes_refused
            && denials.iter().all(|fault| {
                matches!(
                    fault,
                    Fault::Status {
                        on: Shape::Head,
                        ..
                    }
                )
            });
        // Another fault met in the same tick may be the one the engine reports: it reports the
        // first failure a step meets, and which request fails first is not the judge's to pin.
        let masked = others > self.denials_seen.2 || new.len() > denials.len();
        self.denials_seen = (fired_now.len(), refused, others);
        Some(Owed {
            first: kinds.first().copied()?,
            kinds,
            may_drop,
            masked,
        })
    }

    /// The provider faults pushed and not yet met by a request, at the end of a run.
    pub(super) fn http_faults_pending(&self) -> usize {
        self.w.remote_raw.wire().map_or(0, |wire| wire.pending())
    }
}

/// The causes tasks stand degraded with now that were not already standing, unchanged, when the
/// tick began: a task that failed again (another cause, another failure count) or began failing
/// in the tick counts, one that kept the state it had does not.
fn fresh_causes(
    before: &[(Task, Degraded)],
    now: impl Fn(Task) -> Option<Degraded>,
) -> Vec<PersistentCause> {
    Task::ALL
        .iter()
        .filter_map(|task| {
            let degraded = now(*task)?;
            let stood = before
                .iter()
                .any(|(was, state)| was == task && *state == degraded);
            (!stood).then_some(degraded.cause)
        })
        .collect()
}

/// Whether a degraded cause cannot come from a refused request, so that a task standing on it is
/// no failure of one: the local WAL or database, and a store with no conditional writes (a 501 on
/// a create-only write, which stands for the rest of the process whatever else the store later
/// refuses).
const fn says_nothing_of_requests(cause: PersistentCause) -> bool {
    match cause {
        PersistentCause::WalCorrupt
        | PersistentCause::WalTruncated
        | PersistentCause::WalUnlinked
        | PersistentCause::WalSyncFailed
        | PersistentCause::PageSizeChanged { .. }
        | PersistentCause::DatabasePageMismatch { .. }
        | PersistentCause::ConditionalWritesUnsupported => true,
        PersistentCause::StoreDenied(_)
        | PersistentCause::StoreRefused
        | PersistentCause::RemoteCorrupt
        | PersistentCause::OtherWriter
        | PersistentCause::RemoteRegressed
        | PersistentCause::Bug
        | PersistentCause::Stalled(_) => false,
    }
}

/// The refusals one tick met, as [`Runner::judge_denials`] weighs them.
struct Owed {
    first: Denied,
    kinds: Vec<Denied>,
    may_drop: bool,
    masked: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store without conditional writes stands degraded for the whole process; a refused
    /// read-ahead HEAD in a later tick must not be judged against that standing cause.
    #[test]
    fn a_standing_unsupported_create_only_is_no_failure_of_a_refused_request() {
        assert!(says_nothing_of_requests(
            PersistentCause::ConditionalWritesUnsupported
        ));
        assert!(!says_nothing_of_requests(PersistentCause::StoreDenied(
            Denied::Permission
        )));
        assert!(!says_nothing_of_requests(PersistentCause::StoreRefused));
    }

    fn degraded(cause: PersistentCause, retries: u32) -> Degraded {
        Degraded {
            cause,
            since: lsr_ltx::Timestamp::from_unix_millis(1_700_000_057_000),
            retries,
        }
    }

    /// A maintenance step that failed on a refused upload stands degraded through the next
    /// ticks. A tick that only reads ahead and meets a refused HEAD owes no report of it, and
    /// the standing state is no failure of that tick; a task that failed again in the tick is.
    #[test]
    fn a_standing_degradation_is_no_failure_of_a_later_tick() {
        let standing = degraded(PersistentCause::StoreDenied(Denied::BucketMissing), 1);
        let before = [(Task::Maintenance, standing)];
        let unchanged = |task| (task == Task::Maintenance).then_some(standing);
        assert!(fresh_causes(&before, unchanged).is_empty());

        let again = degraded(PersistentCause::StoreDenied(Denied::BucketMissing), 2);
        assert_eq!(
            fresh_causes(&before, |task| (task == Task::Maintenance).then_some(again)),
            [PersistentCause::StoreDenied(Denied::BucketMissing)]
        );
        let other = degraded(PersistentCause::StoreDenied(Denied::Permission), 1);
        assert_eq!(
            fresh_causes(&before, |task| match task {
                Task::Maintenance => Some(standing),
                Task::Upload => Some(other),
                Task::Sync | Task::Checkpoint | Task::Prune => None,
            }),
            [PersistentCause::StoreDenied(Denied::Permission)]
        );
    }
}
