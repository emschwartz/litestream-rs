//! The scheduled verify ([`Op::Verify`](crate::Op::Verify)), run as the replica thread runs it
//! and judged against what the database really was, and an operator's `lsr verify` beside it.
//!
//! `lsr verify` walks every file and dry-runs the restore of the latest state; its report must
//! be unrestorable exactly when that state does not restore from the raw replica.
//!
//! The step asks the engine for the live pages, as the replica thread does when a run is due;
//! the history goes on, and after the tick that hands them over the runner runs
//! `lsr_core::verify_against` over the raw replica: the listing checks at names depth,
//! then the restore of the live transaction compared with the live pages. The verdict is then
//! judged with nothing verify computed: the raw replica's restore of the same transaction,
//! page for page against the model's database for the commit the replica's file for it holds
//! (the oracle's map, by each file's WAL origin).
//!
//! - verify said the same, and the replica restores another database: verify is blind, the
//!   failure the release bar leans on it to catch;
//! - verify said it differs, or that the restore fails, and the replica restores the right
//!   database: a false alarm, which degrades a healthy engine and pins retention;
//! - verify said compaction merged the transaction away, and it plans: the same.
//!
//! A listing or read that fails says nothing about the replica, but the raw store takes no
//! fault, so on it one is a bug. An ask the engine never answers (a crash drops it, or a fault
//! holds its uploads back until the run ends) is not judged, as a real run that never starts.

use lsr_core::conformance::block_on;
use lsr_core::{
    Compared, Depth, LivePages, MemSink, PlanOutcome, RestoreError, RestoreTarget, Severity,
    restore, verify, verify_against,
};

use crate::oracle::{Violation, bug_fault};

use super::Runner;

/// What the raw replica restores for the live transaction, against the model.
#[derive(Debug)]
enum Truth {
    /// The database the model holds for the commit.
    Right,
    /// Another database, from this page on.
    Wrong(u32),
    /// The restore failed.
    Fails(RestoreError),
}

impl Runner {
    /// An operator's `lsr verify` over the raw replica, judged now; then the scheduled verify
    /// asks the engine for the live pages, judged after the tick that hands them over.
    pub(super) fn verify(&mut self) -> Result<(), Violation> {
        let report = block_on(verify(&self.w.remote_raw, Depth::Contents))
            .map_err(|err| bug_fault(format!("verify on the raw replica: {err}")))?;
        let unrestorable = report.worst() == Some(Severity::Unrestorable);
        let restores = block_on(restore(
            &self.w.remote_raw,
            RestoreTarget::Latest,
            MemSink::new(),
        ))
        .is_ok();
        let verdict = if unrestorable {
            "unrestorable"
        } else {
            "restorable"
        };
        self.report.reach.bump(format!("verify.contents.{verdict}"));
        for finding in report.findings() {
            let kind = crate::reach::variant(finding);
            self.report
                .reach
                .bump(format!("verify.contents.finding.{kind}"));
        }
        if unrestorable == restores {
            return Err(bug_fault(format!(
                "verify called the replica {verdict}, and its latest state restores: {restores}"
            )));
        }
        self.actor.read_live_pages();
        Ok(())
    }

    /// Judges a scheduled verify when the tick that just ran handed the live pages over.
    pub(super) fn verify_ready(&mut self) -> Result<(), Violation> {
        let Some(live) = self.actor.take_live_pages() else {
            return Ok(());
        };
        let verified = block_on(verify_against(&self.w.remote_raw, &live))
            .map_err(|err| bug_fault(format!("verify on the raw replica: {err}")))?;
        let compared = verified.compared();
        self.report
            .reach
            .bump(format!("verify.{}", crate::reach::variant(compared)));
        if let Compared::RestoreFails { error, .. } = compared {
            let why = crate::reach::variant(error);
            self.report.reach.bump(format!("verify.RestoreFails.{why}"));
            if let RestoreError::NoPlan(no) = error {
                let outcome = crate::reach::variant(no.outcome());
                self.report
                    .reach
                    .bump(format!("verify.RestoreFails.NoPlan.{outcome}"));
            }
        }
        match self.truth(&live)? {
            Some(truth) => judge(compared, &truth),
            None => {
                self.report.reach.bump("verify.unjudged");
                Ok(())
            }
        }
    }

    /// The raw replica's restore of the live transaction, judged against the model's database
    /// for the commit the replica's file for it holds; `None` when that is not known.
    fn truth(&self, live: &LivePages) -> Result<Option<Truth>, Violation> {
        let ground = crate::oracle::Ground {
            lost: &self.w.lost,
            foreign: &self.w.foreign,
            foreign_removed: &self.w.foreign_removed,
            foreign_put: &self.w.foreign_put,
            ..crate::oracle::Ground::default()
        };
        // The oracle's own map from the replica's files to the commits they hold, by each
        // file's WAL origin: an id a rewind or a rollback reused maps to what the replica has.
        // A map the oracle would refuse is the oracle's to report, at the next restore.
        let Ok(cover) = crate::oracle::covers(&self.w.model, &self.w.remote_recorder, ground)
        else {
            return Ok(None);
        };
        let Some(&commit) = cover.get(&live.txid()) else {
            return Ok(None);
        };
        let image = self
            .w
            .model
            .rec(commit)
            .map(|rec| std::rc::Rc::clone(&rec.image))
            .ok_or_else(|| bug_fault("a staged commit the model has no record of"))?;
        let sink = MemSink::new();
        let target = RestoreTarget::Txid(live.txid());
        if let Err(err) = block_on(restore(&self.w.remote_raw, target, sink.clone())) {
            return Ok(Some(Truth::Fails(err)));
        }
        let db = sink
            .published()
            .ok_or_else(|| bug_fault("restore returned without publishing"))?;
        Ok(Some(match crate::oracle::first_diff(&image, &db) {
            None => Truth::Right,
            Some(page) => Truth::Wrong(page),
        }))
    }
}

fn judge(compared: &Compared, truth: &Truth) -> Result<(), Violation> {
    let agrees = match (compared, truth) {
        (Compared::Same { .. }, Truth::Right)
        | (Compared::Differs { .. }, Truth::Wrong(_))
        | (Compared::RestoreFails { .. }, Truth::Fails(_)) => true,
        (Compared::Merged { .. }, Truth::Fails(RestoreError::NoPlan(no))) => {
            matches!(no.outcome(), PlanOutcome::InsideFile { .. })
        }
        (Compared::Same { .. } | Compared::Merged { .. }, Truth::Wrong(_) | Truth::Fails(_))
        | (Compared::Merged { .. } | Compared::Differs { .. }, Truth::Right)
        | (Compared::Differs { .. }, Truth::Fails(_))
        | (Compared::RestoreFails { .. }, Truth::Right | Truth::Wrong(_)) => false,
    };
    if agrees {
        Ok(())
    } else {
        let truth = match truth {
            Truth::Right => "the database it was".to_owned(),
            Truth::Wrong(page) => format!("another database from page {page}"),
            Truth::Fails(err) => format!("a failure: {err}"),
        };
        Err(bug_fault(format!(
            "the scheduled verify said {compared:?}, and the replica's restore of that \
             transaction gives {truth}"
        )))
    }
}
