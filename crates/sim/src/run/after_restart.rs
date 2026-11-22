//! What the application does right after a restart, before the new engine's
//! first tick ([`AfterRestart`]).
//!
//! The app keeps its own log of the commits it made: the pages with their
//! bytes and the database size each left, the last [`REDO_MAX`] of them. A
//! redo replays the newest `K` of those verbatim; a retry replays them too,
//! each one verbatim or restamped as a stream of its own draws (so a retry
//! that restamps nothing is the redo of the same seed); a busy app commits new
//! work until the WAL's committed end passes the end it had before the
//! restart. All run from random streams of their own, so a run with the
//! behaviour off is the run it was before the behaviour existed.

use std::collections::VecDeque;

use lsr_core::{Entropy, SeededEntropy};
use lsr_ltx::{PageNo, PageSize};

use crate::CommitId;
use crate::generate::{AfterRestart, BUSY_MAX, REDO_MAX};
use crate::seed::Seed;

use super::Runner;

/// One commit in the app's own log.
pub(super) struct Logged {
    id: CommitId,
    pages: Vec<(PageNo, Vec<u8>)>,
    db_size: u32,
    page_size: PageSize,
}

/// The behaviour, its stream, the app's log, and the restart waiting to be
/// answered.
pub(super) struct App {
    behaviour: AfterRestart,
    rng: SeededEntropy,
    /// Which of a retry's commits are restamped.
    restamp: SeededEntropy,
    log: VecDeque<Logged>,
    /// The WAL's committed end, in frames, as the operation that restarted
    /// the process began.
    restarted: Option<u32>,
}

impl App {
    pub(super) fn new(seed: Seed) -> Self {
        Self {
            behaviour: AfterRestart::Nothing,
            rng: SeededEntropy::new(seed.stream(18)),
            restamp: SeededEntropy::new(seed.stream(19)),
            log: VecDeque::new(),
            restarted: None,
        }
    }

    pub(super) fn set(&mut self, behaviour: AfterRestart) {
        self.behaviour = behaviour;
    }

    /// The app committed `pages`, leaving `db_size` pages of `page_size`.
    pub(super) fn logged(
        &mut self,
        id: CommitId,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
        page_size: PageSize,
    ) {
        if !matches!(self.behaviour, AfterRestart::Redo | AfterRestart::Retry) {
            return;
        }
        if u64::try_from(self.log.len()).unwrap_or(u64::MAX) >= REDO_MAX {
            self.log.pop_front();
        }
        self.log.push_back(Logged {
            id,
            pages: pages.to_vec(),
            db_size,
            page_size,
        });
    }

    /// The process restarted during an operation that began with the WAL's
    /// committed end at `end` frames.
    pub(super) fn restarted(&mut self, end: u32) {
        if self.behaviour != AfterRestart::Nothing && self.restarted.is_none() {
            self.restarted = Some(end);
        }
    }
}

impl Runner {
    /// Answers a restart the last operation made, as the app's behaviour says.
    pub(super) fn after_restart(&mut self) {
        let Some(end) = self.app.restarted.take() else {
            return;
        };
        match self.app.behaviour {
            AfterRestart::Nothing => {}
            AfterRestart::Redo => self.redo(false),
            AfterRestart::Retry => self.redo(true),
            AfterRestart::Busy => self.busy(end),
        }
    }

    /// Replays the newest `K` logged commits, oldest first, with their own
    /// bytes. A commit of another page size than the database's now (a
    /// vacuum resized it since) is skipped: the app's write would not lay
    /// out the same pages. With `retry`, each commit is drawn verbatim or
    /// restamped: the same pages, every one with the restart's own stamp
    /// over its first bytes.
    fn redo(&mut self, retry: bool) {
        let k = self.app.rng.below(REDO_MAX).saturating_add(1);
        let newest = self.w.model.newest_commit();
        let lost = self
            .app
            .log
            .iter()
            .filter(|logged| Some(logged.id) > newest)
            .count();
        let take = usize::try_from(k)
            .unwrap_or(usize::MAX)
            .min(self.app.log.len());
        let from = self.app.log.len().saturating_sub(take);
        let replay: Vec<_> = self
            .app
            .log
            .iter()
            .skip(from)
            .filter(|logged| logged.page_size == self.w.sqlite.page_size())
            .map(|logged| (logged.pages.clone(), logged.db_size))
            .collect();
        self.report.reach.bump("app.redo");
        if lost > 0 {
            self.report.reach.bump("app.redo_after_loss");
            if take > lost {
                self.report.reach.bump("app.redo_past_lost_end");
            }
        }
        if self.trace {
            let what: Vec<_> = replay
                .iter()
                .map(|(pages, db_size)| (pages.len(), *db_size))
                .collect();
            self.report.trace.push(format!(
                "    app redoes {} of {} logged commits ({lost} lost): (pages, size) {what:?}",
                replay.len(),
                self.app.log.len()
            ));
        }
        if retry {
            self.report.reach.bump("app.retry");
        }
        for (mut pages, db_size) in replay {
            if retry && self.app.restamp.below(2) == 0 {
                self.report.reach.bump("app.retry_restamped");
                let stamp = (1_u64 << 63) | u64::from(self.report.steps) << 16;
                for (page, data) in &mut pages {
                    let stamp = stamp | u64::from(page.get() & 0xffff);
                    for (slot, byte) in data.iter_mut().zip(stamp.to_be_bytes()) {
                        *slot = byte;
                    }
                }
            }
            // SQLite writes every page a commit adds. A replay on a smaller
            // database than the original commit's (a shrink since) adds pages
            // the original did not, with bytes of their own.
            let size = self.w.sqlite.db_size();
            if db_size > size {
                let added: Vec<_> = self
                    .all_pages(db_size)
                    .into_iter()
                    .filter(|(page, _)| page.get() > size && !pages.iter().any(|(p, _)| p == page))
                    .collect();
                pages.extend(added);
            }
            self.app_commit(pages, db_size);
        }
    }

    /// Commits new work until the WAL's committed end passes `end`.
    fn busy(&mut self, end: u32) {
        self.report.reach.bump("app.busy");
        let first = self.w.model.newest_commit();
        for _ in 0..BUSY_MAX {
            let before = self.w.model.newest_commit();
            let pages = u8::try_from(self.app.rng.below(4).saturating_add(1)).unwrap_or(1);
            self.commit(pages, false);
            if self.w.model.newest_commit() == before {
                break;
            }
            if self.w.sqlite.mx_frame() > end {
                self.report.reach.bump("app.busy_past_end");
                break;
            }
        }
        if self.trace {
            self.report.trace.push(format!(
                "    app commits from {first:?} to {:?}, past the end of {end} frames: {}",
                self.w.model.newest_commit(),
                self.w.sqlite.mx_frame()
            ));
        }
    }
}
