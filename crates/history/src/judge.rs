//! The judge: a restored state against the history tree.
//!
//! It knows nothing about WALs, transaction ids, salts or LTX files. A restore is named by its
//! content ([`Digest`]); the judge finds the nodes of the [`Log`] with that content and asks:
//!
//! 1. **A node of the tree.** Some commit (or the root) left the database exactly so. A state no
//!    commit made is [`Anomaly::NotInHistory`], always fatal.
//! 2. **Never backwards on a branch.** Each `Latest` restore extends the previous one, unless a
//!    rewind the log recorded after the previous node cut it off, and the new node descends from
//!    that rewind's target ([`Anomaly::WentBackwards`]). A driver that took files from the
//!    replica itself says so with [`Judge::excuse_regression`]. Under
//!    [`Signals::Required`] neither excuse is the driver's alone: a rewind explains a step only
//!    when the engine itself reported a loss ([`Judge::signal`]) after it, or the step is
//!    [`Anomaly::Unsignaled`]; the driver's excuse is a debt the engine pays by reporting a
//!    loss later (it cannot have seen a replica changed under it before it looks again), and
//!    [`Judge::unreported`] counts the excused steps no report followed.
//!    The replica's restore points, in its own order, obey the same rule ([`Judge::sweep`]).
//! 3. **As of an instant.** An `AsOf(t)` restore is a node committed before `t`
//!    ([`Anomaly::AsOfTooNew`]) and, given a staleness bound, one that was the local head at some
//!    instant within the bound before `t` ([`Anomaly::AsOfTooOld`]).
//! 4. **Bounded lag.** With no fault active the driver may ask that `Latest` was the local head
//!    within a bound of now ([`Anomaly::Lagging`]).
//! 5. **Replica ahead of the database.** A restore whose every node is off the local branch (a
//!    rewind cut it) holds commits the database lost. That is its own class: counted, and fatal
//!    only under [`AheadMode::Fatal`], so the owner decides whether it is allowed.

use thiserror::Error;

use crate::{Digest, Log, NodeId};

/// What was restored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The newest state.
    Latest,
    /// The newest state created strictly before this instant (the driver's clock, ms).
    AsOf(i64),
    /// One of the replica's restore points, named by its place in the replica's own order.
    Point(u64),
}

/// Whether a replica holding commits the database lost fails the judgment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AheadMode {
    /// Counted in [`Counts`], not failed.
    #[default]
    Count,
    /// [`Anomaly::Ahead`].
    Fatal,
}

/// Whether an excused regression needs the engine's own report of a loss.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Signals {
    /// The driver's record alone excuses a regression.
    #[default]
    Ignored,
    /// A regression is excused only when [`Judge::signal`] came after what excuses it.
    Required,
}

/// A restore the history does not explain.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Anomaly {
    /// No commit ever left the database in the restored state.
    #[error("restoring {target:?} gave a state no commit made ({digest})")]
    NotInHistory {
        /// What was restored.
        target: Target,
        /// The restored content.
        digest: Digest,
    },
    /// The restored file is not a sound database, or its chain does not verify.
    #[error("restoring {target:?} gave a broken database: {why}")]
    Broken {
        /// What was restored.
        target: Target,
        /// What the reader found.
        why: String,
    },
    /// `Latest` moved off the branch it was on with no rewind to explain it.
    #[error("latest went back from node {from} to node {to}, and no rewind cut node {from} off")]
    WentBackwards {
        /// A node the previous `Latest` restored.
        from: u32,
        /// A node this one restored.
        to: u32,
    },
    /// An `AsOf` restore reached a state whose commit began at or after the instant.
    #[error("restoring as of {instant} gave node {node}, begun at {begun}")]
    AsOfTooNew {
        /// The instant.
        instant: i64,
        /// The node reached.
        node: u32,
        /// When its commit began.
        begun: i64,
    },
    /// An `AsOf` restore reached a state that was not live within the bound before the instant.
    #[error("restoring as of {instant} gave node {node}, not live within {bound} ms before it")]
    AsOfTooOld {
        /// The instant.
        instant: i64,
        /// The bound.
        bound: i64,
        /// The node reached.
        node: u32,
    },
    /// With no fault active, `Latest` was not the local head within the bound of now.
    #[error("latest gave node {node} at {now}; the head is node {head}, bound {bound} ms")]
    Lagging {
        /// The driver's clock.
        now: i64,
        /// The bound.
        bound: i64,
        /// The node reached.
        node: u32,
        /// The local head.
        head: u32,
    },
    /// `Latest` went back over a rewind or an excuse the driver recorded, and the engine
    /// reported no loss after it ([`Signals::Required`] only).
    #[error("latest went back from node {from} to node {to}, and the engine reported no loss")]
    Unsignaled {
        /// A node the previous `Latest` restored.
        from: u32,
        /// A node this one restored.
        to: u32,
    },
    /// The replica holds commits the database lost ([`AheadMode::Fatal`] only).
    #[error("restoring {target:?} gave node {node}, which a rewind cut from the database")]
    Ahead {
        /// What was restored.
        target: Target,
        /// The node reached.
        node: u32,
    },
}

/// What the judge looked at, and the anomalies it counted instead of failing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// `Latest` restores judged.
    pub latest: u32,
    /// Restore points judged in sweeps.
    pub points: u32,
    /// `AsOf` restores judged.
    pub as_of: u32,
    /// `AsOf` restores whose instant could not be compared: the clock went back.
    pub as_of_untimed: u32,
    /// Lag checks.
    pub lag: u32,
    /// `Latest` restores that held commits the database lost.
    pub ahead_latest: u32,
    /// `AsOf` restores that held commits the database had lost by the instant.
    pub ahead_as_of: u32,
    /// `Latest` moves off a branch that a recorded rewind explained.
    pub rewound: u32,
    /// `Latest` moves off a branch the driver excused.
    pub excused: u32,
}

/// How a restore moved from the one before it.
enum Step {
    /// Onto a descendant (or the same node), or there was nothing before.
    Forward,
    /// Off a branch that a rewind recorded after the earlier node cut off, onto the rewind's
    /// side: the place in the log's rewinds of the earliest one that explains it, so a report
    /// after any explaining rewind covers the step.
    Rewound(usize),
    /// Off a branch with nothing to explain it.
    Back { from: u32, to: u32 },
}

fn step(log: &Log, previous: &[NodeId], nodes: &[NodeId]) -> Step {
    let pairs = || {
        previous
            .iter()
            .flat_map(|p| nodes.iter().map(move |z| (*p, *z)))
    };
    if previous.is_empty() || pairs().any(|(p, z)| log.is_ancestor(p, z)) {
        return Step::Forward;
    }
    let explained = pairs()
        .flat_map(|(p, z)| {
            log.rewinds().iter().enumerate().filter(move |(_, r)| {
                p.get() < r.seq && !log.is_ancestor(p, r.to) && log.is_ancestor(r.to, z)
            })
        })
        .map(|(index, _)| index)
        .min();
    if let Some(index) = explained {
        return Step::Rewound(index);
    }
    let first = |list: &[NodeId]| list.first().map_or(0, |n| n.get());
    Step::Back {
        from: first(previous),
        to: first(nodes),
    }
}

/// Judges one replica's restores over a run.
#[derive(Clone, Debug, Default)]
pub struct Judge {
    mode: AheadMode,
    signals: Signals,
    previous: Vec<NodeId>,
    excused: bool,
    // The engine's loss reports, each by how many rewinds the log had recorded when it came.
    reported: Vec<usize>,
    // How many reports had come when the driver last excused a regression.
    excused_after: usize,
    // For each step the driver's excuse let through: how many reports had come by then.
    owed: Vec<usize>,
    counts: Counts,
}

impl Judge {
    /// A judge that has seen nothing.
    #[must_use]
    pub fn new(mode: AheadMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    /// The same judge, with `signals` deciding whether an excuse needs the engine's report.
    #[must_use]
    pub const fn with_signals(mut self, signals: Signals) -> Self {
        self.signals = signals;
        self
    }

    /// The engine reported a loss of history now (a snapshot that says the database or the
    /// replica went back, a stop on a regressed replica). `log` dates the report: it covers
    /// every rewind recorded before it.
    pub fn signal(&mut self, log: &Log) {
        self.reported.push(log.rewinds().len());
    }

    /// What it judged and counted so far.
    #[must_use]
    pub const fn counts(&self) -> Counts {
        self.counts
    }

    /// The driver took files from the replica itself (a bucket rolled back, an operator deleted
    /// the newest file, another writer put one): the next `Latest` may go back.
    pub fn excuse_regression(&mut self) {
        self.excused = true;
        self.excused_after = self.reported.len();
    }

    /// Whether the rewind at `index` in the log's rewinds may explain a step: signals are not
    /// required, or the engine reported a loss after the log recorded it.
    fn reported_since(&self, index: usize) -> bool {
        match self.signals {
            Signals::Ignored => true,
            Signals::Required => self.reported.iter().any(|seen| *seen > index),
        }
    }

    /// Under [`Signals::Required`], the steps the driver's excuse let through that no report of
    /// the engine's has followed since the excuse: a regression the engine never noticed.
    #[must_use]
    pub fn unreported(&self) -> u32 {
        let count = self
            .owed
            .iter()
            .filter(|after| self.reported.len() <= **after)
            .count();
        match self.signals {
            Signals::Ignored => 0,
            Signals::Required => u32::try_from(count).unwrap_or(u32::MAX),
        }
    }

    fn found(log: &Log, target: Target, digest: Digest) -> Result<Vec<NodeId>, Anomaly> {
        let nodes = log.find(&digest).to_vec();
        if nodes.is_empty() {
            return Err(Anomaly::NotInHistory { target, digest });
        }
        Ok(nodes)
    }

    fn ahead(&mut self, target: Target, node: NodeId) -> Result<(), Anomaly> {
        match target {
            Target::Latest => self.counts.ahead_latest = self.counts.ahead_latest.saturating_add(1),
            Target::AsOf(_) | Target::Point(_) => {
                self.counts.ahead_as_of = self.counts.ahead_as_of.saturating_add(1);
            }
        }
        match self.mode {
            AheadMode::Count => Ok(()),
            AheadMode::Fatal => Err(Anomaly::Ahead {
                target,
                node: node.get(),
            }),
        }
    }

    /// A `Latest` restore whose content is `digest`; the node it is taken for.
    ///
    /// # Errors
    /// [`Anomaly::NotInHistory`], [`Anomaly::WentBackwards`], or [`Anomaly::Ahead`].
    pub fn latest(&mut self, log: &Log, digest: Digest) -> Result<NodeId, Anomaly> {
        self.counts.latest = self.counts.latest.saturating_add(1);
        let nodes = Self::found(log, Target::Latest, digest)?;
        let head = log.head();
        let on_branch: Vec<NodeId> = nodes
            .iter()
            .copied()
            .filter(|n| log.is_ancestor(*n, head))
            .collect();
        let chosen = on_branch
            .last()
            .or(nodes.last())
            .copied()
            .unwrap_or(NodeId::ROOT);
        if on_branch.is_empty() {
            self.ahead(Target::Latest, chosen)?;
        }
        self.onward(log, &nodes)?;
        self.previous = nodes;
        self.excused = false;
        Ok(chosen)
    }

    /// Whether moving from the previous `Latest` to `nodes` stays on a branch, or is explained.
    fn onward(&mut self, log: &Log, nodes: &[NodeId]) -> Result<(), Anomaly> {
        let first = |list: &[NodeId]| list.first().map_or(0, |n| n.get());
        let unsignaled = || Anomaly::Unsignaled {
            from: first(&self.previous),
            to: first(nodes),
        };
        match step(log, &self.previous, nodes) {
            Step::Forward => Ok(()),
            Step::Rewound(index) if self.reported_since(index) => {
                self.counts.rewound = self.counts.rewound.saturating_add(1);
                Ok(())
            }
            // A rewind the engine has not reported yet, with the driver's excuse standing too
            // (a rollback and a power loss between two looks): the excuse lets it through and
            // is owed a report, as it would be alone.
            Step::Rewound(_) | Step::Back { .. } if self.excused => {
                self.counts.excused = self.counts.excused.saturating_add(1);
                self.owed.push(self.excused_after);
                Ok(())
            }
            Step::Rewound(_) => Err(unsignaled()),
            Step::Back { from, to } => Err(Anomaly::WentBackwards { from, to }),
        }
    }

    /// The replica's restore points in its own order (by transaction id, in the engine's
    /// words), each reduced to its content: every one is a node of the tree, and each extends
    /// the one before it unless a recorded rewind explains the move. `skip` names points the
    /// driver put there itself, judged for membership only.
    ///
    /// # Errors
    /// [`Anomaly::NotInHistory`] or [`Anomaly::WentBackwards`] for the first point that fails.
    pub fn sweep(&mut self, log: &Log, points: &[(u64, Digest, bool)]) -> Result<(), Anomaly> {
        let mut previous: Vec<NodeId> = Vec::new();
        for (point, digest, foreign) in points {
            self.counts.points = self.counts.points.saturating_add(1);
            let nodes = Self::found(log, Target::Point(*point), *digest)?;
            if *foreign {
                continue;
            }
            match step(log, &previous, &nodes) {
                Step::Forward => {}
                Step::Rewound(_) => self.counts.rewound = self.counts.rewound.saturating_add(1),
                Step::Back { from, to } => return Err(Anomaly::WentBackwards { from, to }),
            }
            previous = nodes;
        }
        Ok(())
    }

    /// An `AsOf(instant)` restore whose content is `digest`: a node whose commit began before
    /// the instant ([`Log::commit_between`]: a commit still in flight at the instant may be
    /// there, one begun after it may not). With `bound`, the node must also have been the local
    /// head within `bound` ms before the instant.
    ///
    /// # Errors
    /// [`Anomaly::NotInHistory`], [`Anomaly::AsOfTooNew`], [`Anomaly::AsOfTooOld`], or
    /// [`Anomaly::Ahead`].
    pub fn as_of(
        &mut self,
        log: &Log,
        instant: i64,
        digest: Digest,
        bound: Option<i64>,
    ) -> Result<NodeId, Anomaly> {
        self.counts.as_of = self.counts.as_of.saturating_add(1);
        let target = Target::AsOf(instant);
        let nodes = Self::found(log, target, digest)?;
        let newest = nodes.last().copied().unwrap_or(NodeId::ROOT);
        if log.clock_went_back() {
            self.counts.as_of_untimed = self.counts.as_of_untimed.saturating_add(1);
            return Ok(newest);
        }
        let before: Vec<NodeId> = nodes
            .iter()
            .copied()
            .filter(|n| log.begun_at(*n).is_some_and(|at| at < instant))
            .collect();
        let Some(&chosen) = before.last() else {
            return Err(Anomaly::AsOfTooNew {
                instant,
                node: newest.get(),
                begun: log.begun_at(newest).unwrap_or(i64::MAX),
            });
        };
        if let Some(bound) = bound {
            let from = instant.saturating_sub(bound);
            let to = instant.saturating_sub(1);
            if !before.iter().any(|n| log.live_within(*n, from, to)) {
                return Err(Anomaly::AsOfTooOld {
                    instant,
                    bound,
                    node: chosen.get(),
                });
            }
        }
        let then = log.head_at(instant.saturating_sub(1));
        // In flight at the instant: begun before it, acknowledged after, on the head's branch.
        let in_flight = |n: NodeId| {
            log.is_ancestor(then, n) && log.committed_at(n).is_some_and(|at| at >= instant)
        };
        if !before
            .iter()
            .any(|n| log.is_ancestor(*n, then) || in_flight(*n))
        {
            self.ahead(target, chosen)?;
        }
        Ok(chosen)
    }

    /// With no fault active: a `Latest` restore whose content is `digest` is on the local branch
    /// and was its head at some instant within `bound` ms of `now`.
    ///
    /// # Errors
    /// [`Anomaly::NotInHistory`] or [`Anomaly::Lagging`].
    pub fn lag(&mut self, log: &Log, digest: Digest, now: i64, bound: i64) -> Result<(), Anomaly> {
        self.counts.lag = self.counts.lag.saturating_add(1);
        let nodes = Self::found(log, Target::Latest, digest)?;
        let head = log.head();
        let from = now.saturating_sub(bound);
        // The head is live now by definition; any other node only by the clock, which cannot be
        // read back once it went back.
        let fresh = nodes.iter().any(|n| {
            *n == head
                || (!log.clock_went_back()
                    && log.is_ancestor(*n, head)
                    && log.live_within(*n, from, now))
        });
        if fresh {
            return Ok(());
        }
        Err(Anomaly::Lagging {
            now,
            bound,
            node: nodes.last().map_or(0, |n| n.get()),
            head: head.get(),
        })
    }
}
