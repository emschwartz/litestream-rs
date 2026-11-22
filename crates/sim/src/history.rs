//! The history judge (`lsr-history`) inside the simulator, beside the oracle.
//!
//! The oracle maps each transaction id to a commit through the WAL origin the engine wrote in
//! the file's header, and keeps exemptions for runs where that mapping is ambiguous. This judge
//! never reads a header. The log is fed from the model as the app commits (each commit's image,
//! digested) and from every power loss (the commit the database holds afterwards), so it is the
//! tree of everything the database was. A restore of the raw replica is digested by its pages
//! and judged by content alone ([`lsr_history::Judge`]):
//!
//! - at every oracle check and at random moments (one step in [`ONE_IN`], from a seed stream
//!   nothing else draws from, so a seed's history is unchanged): `Latest`, and `AsOf` at
//!   [`INSTANTS`] random instants;
//! - at every oracle check, every restore point from 1 to the newest, in order (the sweep);
//! - after a drain that finished, `Latest` is the local head (lag bound 0).
//!
//! What it may not judge: a restore that stops on an object the store damaged, or that the
//! planner refuses, is counted and left to the oracle. A bucket rollback or an operator's
//! delete of the newest file is a regression the run caused itself, so it is excused once.

use lsr_core::conformance::block_on;
use lsr_core::{
    AsOfInside, Database, Entropy, FileKey, Inventory, MemSink, MemStore, RestoreError,
    RestoreOptions, RestoreTarget, SeededEntropy, restore_with,
};
use lsr_history::{AheadMode, Counts, Digest, Judge, Log, NodeId};
use lsr_ltx::{Timestamp, Txid};

use crate::model::{Model, PageImage};
use crate::oracle::{Violation, bug_fault};
use crate::reach::Reach;

/// One step in this many judges the replica at random.
pub const ONE_IN: u64 = 8;
/// `AsOf` instants per judgment.
pub const INSTANTS: u32 = 3;

/// The digest of a model image.
pub(crate) fn image_digest(image: &PageImage) -> Digest {
    let pages: Vec<_> = (1..=image.db_size())
        .filter_map(|pgno| image.page(pgno))
        .collect();
    let size = u32::try_from(image.page_size().bytes()).unwrap_or(u32::MAX);
    Digest::of_pages(size, pages.iter().map(|p| p.as_slice()))
}

/// The digest of a restored database.
fn database_digest(db: &Database) -> Digest {
    let pages: Vec<Vec<u8>> = (1..=db.page_count())
        .filter_map(|pgno| db.page(pgno))
        .collect();
    let size = u32::try_from(db.page_size().bytes()).unwrap_or(u32::MAX);
    Digest::of_pages(size, pages.iter().map(Vec::as_slice))
}

/// Restores `target` from `store` and digests what it built, as the judge digests it, with the
/// transaction the restore reached.
///
/// # Errors
/// The restore's own error, untouched: the caller decides what the fault it put in place
/// explains.
pub(crate) fn restored_digest<S: lsr_core::ObjectStore>(
    store: &S,
    target: RestoreTarget,
) -> Result<Result<(Digest, Txid), RestoreError>, Violation> {
    let sink = MemSink::new();
    let options = RestoreOptions {
        as_of_inside: AsOfInside::TakeEarlier,
    };
    let restored = match block_on(restore_with(store, target, options, sink.clone())) {
        Ok(restored) => restored,
        Err(err) => return Ok(Err(err)),
    };
    let db = sink
        .published()
        .ok_or_else(|| bug_fault("restore returned without publishing"))?;
    Ok(Ok((database_digest(&db), restored.reached())))
}

/// What a restore came to, by content.
enum Restored {
    Content(Digest),
    /// The planner refused, or the restore stopped on a damaged object: the oracle's to judge.
    Unjudged,
}

fn restore(
    remote: &MemStore,
    target: RestoreTarget,
    damaged: &[FileKey],
) -> Result<Restored, Violation> {
    let sink = MemSink::new();
    let options = RestoreOptions {
        as_of_inside: AsOfInside::TakeEarlier,
    };
    match block_on(restore_with(remote, target, options, sink.clone())) {
        Ok(_) => {}
        Err(RestoreError::NoPlan(_)) => return Ok(Restored::Unjudged),
        Err(RestoreError::File { key, .. }) if damaged.contains(&key) => {
            return Ok(Restored::Unjudged);
        }
        Err(other) => return Err(bug_fault(format!("restore {target:?}: {other}"))),
    }
    let db = sink
        .published()
        .ok_or_else(|| bug_fault("restore returned without publishing"))?;
    Ok(Restored::Content(database_digest(&db)))
}

/// What one judgment covers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ask {
    /// Every restore point from 1 to the newest, in order.
    pub sweep: bool,
    /// A drain just finished: `Latest` must be the local head.
    pub drained: bool,
}

/// The log, the judge, and which node each model commit is.
#[derive(Debug)]
pub struct History {
    log: Log,
    judge: Judge,
    nodes: Vec<NodeId>,
    rng: SeededEntropy,
    first: i64,
    unjudged: u32,
}

impl History {
    /// A history whose database starts as the model's genesis at `now`.
    #[must_use]
    pub fn new(model: &Model, now: Timestamp, seed: u64, mode: AheadMode) -> Self {
        let root = model
            .commits()
            .first()
            .map_or_else(Digest::genesis, |rec| image_digest(&rec.image));
        Self {
            log: Log::new(root, now.unix_millis()),
            judge: Judge::new(mode),
            nodes: vec![NodeId::ROOT],
            rng: SeededEntropy::new(seed),
            first: now.unix_millis(),
            unjudged: 0,
        }
    }

    /// Logs every commit the model took since the last call, on the local head, at `now`.
    pub fn log_commits(&mut self, model: &Model, now: Timestamp) {
        for rec in model.commits().iter().skip(self.nodes.len()) {
            let id = self.log.commit(image_digest(&rec.image), now.unix_millis());
            self.nodes.push(id);
        }
    }

    /// After a power loss: the database holds the model's newest commit.
    pub fn power_loss(&mut self, model: &Model, now: Timestamp) {
        self.log_commits(model, now);
        let node = model
            .newest_commit()
            .and_then(|id| self.nodes.get(id.index()).copied())
            .unwrap_or(NodeId::ROOT);
        self.log.recover(node, now.unix_millis());
    }

    /// The clock reads `now` (after a jump, say).
    pub fn note_time(&mut self, now: Timestamp) {
        self.log.note_time(now.unix_millis());
    }

    /// The run took files from the replica itself.
    pub const fn excuse_regression(&mut self) {
        self.judge.excuse_regression();
    }

    /// Whether this step judges at random.
    pub fn draw(&self) -> bool {
        self.rng.below(ONE_IN) == 0
    }

    /// The log.
    #[must_use]
    pub const fn log(&self) -> &Log {
        &self.log
    }

    /// Judges the raw replica now.
    ///
    /// # Errors
    /// [`Violation::History`] for what the judge does not accept; a bug fault for a restore
    /// that failed in a way the oracle would also call a bug.
    pub fn judge(
        &mut self,
        remote: &MemStore,
        now: Timestamp,
        damaged: &[FileKey],
        ask: Ask,
    ) -> Result<(), Violation> {
        self.log.note_time(now.unix_millis());
        let newest = block_on(Inventory::load(remote))
            .map_err(|err| bug_fault(format!("listing the replica: {err}")))?
            .newest();
        let Some(newest) = newest else {
            return Ok(());
        };
        if ask.sweep {
            self.sweep(remote, newest, damaged)?;
        }
        if let Restored::Content(digest) = self.restored(remote, RestoreTarget::Latest, damaged)? {
            self.judge
                .latest(&self.log, digest)
                .map_err(Violation::History)?;
            if ask.drained {
                let now = now.unix_millis();
                self.judge
                    .lag(&self.log, digest, now, 0)
                    .map_err(Violation::History)?;
            }
        }
        for _ in 0..INSTANTS {
            let span = u64::try_from(now.unix_millis().saturating_sub(self.first)).unwrap_or(0);
            let offset = i64::try_from(self.rng.below(span.saturating_add(2))).unwrap_or(0);
            let instant = self.first.saturating_add(offset);
            let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(instant));
            if let Restored::Content(digest) = self.restored(remote, target, damaged)? {
                self.judge
                    .as_of(&self.log, instant, digest, None)
                    .map_err(Violation::History)?;
            }
        }
        Ok(())
    }

    fn restored(
        &mut self,
        remote: &MemStore,
        target: RestoreTarget,
        damaged: &[FileKey],
    ) -> Result<Restored, Violation> {
        let restored = restore(remote, target, damaged)?;
        if matches!(restored, Restored::Unjudged) {
            self.unjudged = self.unjudged.saturating_add(1);
        }
        Ok(restored)
    }

    fn sweep(
        &mut self,
        remote: &MemStore,
        newest: Txid,
        damaged: &[FileKey],
    ) -> Result<(), Violation> {
        let mut points = Vec::new();
        for txid in (1..=newest.get()).filter_map(Txid::new) {
            if let Restored::Content(digest) =
                self.restored(remote, RestoreTarget::Txid(txid), damaged)?
            {
                points.push((txid.get(), digest, false));
            }
        }
        self.judge
            .sweep(&self.log, &points)
            .map_err(Violation::History)
    }

    /// What the judge counted, and the restores it left unjudged.
    #[must_use]
    pub const fn counts(&self) -> (Counts, u32) {
        (self.judge.counts(), self.unjudged)
    }

    /// Adds what the judge counted to `reach`.
    pub fn tally(&self, reach: &mut Reach) {
        let counts = self.judge.counts();
        reach.add("history.latest", u64::from(counts.latest));
        reach.add("history.points", u64::from(counts.points));
        reach.add("history.asof", u64::from(counts.as_of));
        reach.add("history.asof.untimed", u64::from(counts.as_of_untimed));
        reach.add("history.lag", u64::from(counts.lag));
        reach.add("history.ahead.latest", u64::from(counts.ahead_latest));
        reach.add("history.ahead.asof", u64::from(counts.ahead_as_of));
        reach.add("history.rewound", u64::from(counts.rewound));
        reach.add("history.excused", u64::from(counts.excused));
        reach.add("history.unjudged", u64::from(self.unjudged));
        reach.add(
            "history.rewinds",
            u64::try_from(self.log.rewinds().len()).unwrap_or(u64::MAX),
        );
    }
}
