//! The delta-debugging shrinker: a failing history becomes the few operations that matter.
//!
//! A failure at step `k` is cut to the first `k` + 1 operations first (the prefix property
//! of [crate::generate]: the first `k` of a history are the history of `k`). Then [minimize]
//! runs ddmin over the list: it removes chunks of the operations, halving the chunk until it is
//! one operation, keeping every cut that still fails in the same [Class]; and it tries
//! smaller values for each field of every operation that remains ([simpler]), keeping each that
//! still fails the same way, until nothing changes. A history is a list of operations and every
//! step is decided from its position, so a cut moves the draws of what follows; a candidate
//! that no longer fails the same way is simply not kept (D285).
//!
//! The decisions are pure: the candidates come in a fixed order, no clock is read, and the
//! number of runs is capped by [MAX_RUNS], so the same failure shrinks to the same list. The
//! result prints as Rust source ([scenario_source]) for an author to turn into an explicit
//! scenario test.

use std::mem::{Discriminant, discriminant};

use lsr_core::SnapshotCauseKind;

use crate::generate::{AppAt, AppOp, Op, PartsOp, WalOp, Weights, history};
use crate::oracle::{Bound, Violation};
use crate::record::{self, Unprintable};
use crate::run::{Opts, drawn_opts, run_ops};
use crate::seed::Seed;
use lsr_history::Anomaly;

/// The most runs one shrink makes, the first (which finds the failure) included. When it is
/// spent the shrink keeps what it has: the list is still a failing one, only not known to be
/// minimal.
pub const MAX_RUNS: u32 = 1_500;

/// What a shrunk history must keep failing as: the check that raised it, not the text it
/// printed. Two violations are the same class when they are the same variant of
/// [Violation] and, for the variants whose check has a named kind, the same kind: the same
/// [Bound] of a resource bound, the same [Anomaly] variant of a history judge verdict, the
/// same snapshot cause of a needless snapshot. The ids, keys, pages and counts in the text
/// change with every cut (the history is renumbered), and a failure by another check is another
/// bug, so a candidate that fails differently is not kept (D285).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Class {
    check: Discriminant<Violation>,
    bound: Option<Bound>,
    anomaly: Option<Discriminant<Anomaly>>,
    snapshot: Option<SnapshotCauseKind>,
}

impl Class {
    /// The class of a violation.
    #[must_use]
    pub fn of(violation: &Violation) -> Self {
        // Not a `match`: a variant added later is its own class by its discriminant until
        // it names a kind worth keeping apart.
        let bound = if let Violation::ResourceBound { what, .. } = violation {
            Some(*what)
        } else {
            None
        };
        let anomaly = if let Violation::History(found) = violation {
            Some(discriminant(found))
        } else {
            None
        };
        let snapshot = if let Violation::NeedlessSnapshot { cause, .. } = violation {
            Some(*cause)
        } else {
            None
        };
        Self {
            check: discriminant(violation),
            bound,
            anomaly,
            snapshot,
        }
    }
}

/// A failing history cut down.
#[derive(Clone, Debug)]
pub struct Shrunk {
    /// The operations that remain.
    pub ops: Vec<Op>,
    /// What they fail with.
    pub violation: Violation,
    /// The options they fail under (the weights' draws made explicit).
    pub opts: Opts,
    /// How many operations the failing history had.
    pub before: usize,
    /// How many of them it takes to reach the failure: the cut at the failing step.
    pub prefix: usize,
    /// Runs made, the first included.
    pub runs: u32,
    /// The budget ran out before nothing more could be removed or made smaller.
    pub capped: bool,
}

/// Shrinks the history `seed` names at `steps` operations under `weights`; `None` when it
/// passes.
#[must_use]
pub fn shrink(seed: Seed, steps: u32, weights: &Weights, opts: &Opts) -> Option<Shrunk> {
    shrink_ops(
        seed,
        &history(seed, steps, weights),
        &drawn_opts(weights, opts),
    )
}

/// Shrinks a list of operations run exactly under `opts`; `None` when it passes.
#[must_use]
pub fn shrink_ops(seed: Seed, ops: &[Op], opts: &Opts) -> Option<Shrunk> {
    let first = run_ops(seed, ops, opts).err()?;
    let class = Class::of(&first.violation);
    let mut violation = first.violation;
    let hint = usize::try_from(first.steps).unwrap_or(usize::MAX);
    let (kept, runs, capped) = minimize(ops, hint, 1, |candidate| {
        match run_ops(seed, candidate, opts) {
            Err(failure) if Class::of(&failure.violation) == class => {
                violation = failure.violation;
                true
            }
            Ok(_) | Err(_) => false,
        }
    });
    Some(Shrunk {
        ops: kept,
        violation,
        opts: opts.clone(),
        before: ops.len(),
        prefix: hint.min(ops.len()),
        runs,
        capped,
    })
}

/// The budget of one shrink: every candidate run is counted, and none is made once it is spent.
struct Budget<F> {
    runs: u32,
    fails: F,
}

impl<F: FnMut(&[Op]) -> bool> Budget<F> {
    /// Whether `candidate` still fails; `false`, without a run, when the budget is spent.
    fn fails(&mut self, candidate: &[Op]) -> bool {
        if self.runs >= MAX_RUNS {
            return false;
        }
        self.runs = self.runs.saturating_add(1);
        (self.fails)(candidate)
    }

    const fn spent(&self) -> bool {
        self.runs >= MAX_RUNS
    }
}

/// The core: `ops` fails (the caller ran it, as run number `used`); `hint` operations are known
/// to be enough. Returns the shrunk list, the runs made in all and whether the budget ended
/// the search. `fails` says whether a candidate still fails in the class that counts.
pub fn minimize(
    ops: &[Op],
    hint: usize,
    used: u32,
    fails: impl FnMut(&[Op]) -> bool,
) -> (Vec<Op>, u32, bool) {
    let mut budget = Budget { runs: used, fails };
    let mut ops: Vec<Op> = ops.to_vec();
    if hint < ops.len() {
        let cut: Vec<Op> = ops.iter().take(hint).cloned().collect();
        if budget.fails(&cut) {
            ops = cut;
        }
    }
    loop {
        remove_chunks(&mut ops, &mut budget);
        let changed = shrink_fields(&mut ops, &mut budget);
        if !changed || budget.spent() {
            break;
        }
    }
    (ops, budget.runs, budget.spent())
}

/// ddmin by complements: remove one chunk at a time, `ops` split in `n` chunks, `n` doubling
/// each pass that removes nothing, until the chunk is one operation and nothing goes.
fn remove_chunks<F: FnMut(&[Op]) -> bool>(ops: &mut Vec<Op>, budget: &mut Budget<F>) {
    let mut parts = 2_usize;
    while !ops.is_empty() && !budget.spent() {
        let chunk = ops.len().div_ceil(parts.min(ops.len()));
        let mut removed = false;
        let mut start = 0;
        while start < ops.len() {
            let end = start.saturating_add(chunk);
            let candidate: Vec<Op> = ops
                .iter()
                .take(start)
                .chain(ops.iter().skip(end))
                .cloned()
                .collect();
            if budget.fails(&candidate) {
                *ops = candidate;
                removed = true;
            } else {
                start = end;
            }
        }
        if removed {
            parts = parts.saturating_sub(1).max(2);
        } else if chunk == 1 {
            break;
        } else {
            parts = parts.saturating_mul(2);
        }
    }
}

/// Replaces each operation by the first of its [simpler] forms that still fails, again until
/// none does. Whether any was replaced.
fn shrink_fields<F: FnMut(&[Op]) -> bool>(ops: &mut Vec<Op>, budget: &mut Budget<F>) -> bool {
    let mut changed = false;
    for index in 0..ops.len() {
        while let Some(op) = ops.get(index) {
            let mut taken = false;
            for form in simpler(op) {
                let mut candidate = ops.clone();
                if let Some(slot) = candidate.get_mut(index) {
                    *slot = form;
                }
                if budget.fails(&candidate) {
                    *ops = candidate;
                    taken = true;
                    break;
                }
            }
            if !taken {
                break;
            }
            changed = true;
        }
    }
    changed
}

/// Smaller values of `n` toward zero, smallest first: zero, half, one less.
fn lower<T>(n: T) -> Vec<T>
where
    T: Copy + Into<u64> + TryFrom<u64>,
{
    let n: u64 = n.into();
    let mut out: Vec<u64> = Vec::new();
    for value in [0, n / 2, n.saturating_sub(1)] {
        if value < n && !out.contains(&value) {
            out.push(value);
        }
    }
    out.into_iter()
        .filter_map(|v| T::try_from(v).ok())
        .collect()
}

/// As [lower] for a signed value, toward zero from either side.
fn lower_signed(n: i64) -> Vec<i64> {
    let sign = n.signum();
    lower(n.unsigned_abs())
        .into_iter()
        .filter_map(|m| i64::try_from(m).ok().map(|m| m.saturating_mul(sign)))
        .collect()
}

/// Simpler forms of an operation, most reduced first: its numeric fields lowered toward their
/// minimum, a flag cleared, a variant replaced by its plainer sibling. Empty when the operation
/// is already as plain as it gets. Exhaustive over [Op], so a new operation says what simpler
/// means for it.
#[must_use]
pub fn simpler(op: &Op) -> Vec<Op> {
    match op {
        Op::Commit { pages, grow } => {
            let mut out: Vec<Op> = lower(*pages)
                .into_iter()
                .map(|pages| Op::Commit { pages, grow: *grow })
                .collect();
            if *grow {
                out.push(Op::Commit {
                    pages: *pages,
                    grow: false,
                });
            }
            out
        }
        Op::CommitInTick { pages } => lower(*pages)
            .into_iter()
            .map(|pages| Op::CommitInTick { pages })
            .collect(),
        Op::OtherConnectionCommit { pages } => lower(*pages)
            .into_iter()
            .map(|pages| Op::OtherConnectionCommit { pages })
            .collect(),
        Op::Vacuum { page_size: Some(_) } => vec![Op::Vacuum { page_size: None }],
        Op::OtherProcessCheckpoint(mode) => {
            other_modes(*mode).map(Op::OtherProcessCheckpoint).collect()
        }
        Op::AppInTick { at, op } => {
            let mut out: Vec<Op> = app_ats(*at)
                .into_iter()
                .map(|at| Op::AppInTick { at, op: *op })
                .collect();
            out.extend(
                app_ops(*op)
                    .into_iter()
                    .map(|op| Op::AppInTick { at: *at, op }),
            );
            out
        }
        Op::Crash(crate::sqlite::CrashKind::Power) => {
            vec![Op::Crash(crate::sqlite::CrashKind::Process)]
        }
        Op::StoreFault {
            target,
            op,
            ahead,
            shape,
        } => lower(*ahead)
            .into_iter()
            .map(|ahead| Op::StoreFault {
                target: *target,
                op: *op,
                ahead,
                shape: shape.clone(),
            })
            .collect(),
        Op::ClockJump(millis) => lower_signed(*millis)
            .into_iter()
            .map(Op::ClockJump)
            .collect(),
        Op::Pause { read, millis } => lower(*millis)
            .into_iter()
            .map(|millis| Op::Pause {
                read: *read,
                millis,
            })
            .collect(),
        Op::ShutdownWithSnapshot | Op::ShutdownWithRetention => vec![Op::Shutdown],
        Op::Wal(wal) => wals(*wal).into_iter().map(Op::Wal).collect(),
        Op::Structured { seq, data } => {
            let mut out: Vec<Op> = lower(*seq)
                .into_iter()
                .map(|seq| Op::Structured { seq, data: *data })
                .collect();
            out.extend(
                lower(*data)
                    .into_iter()
                    .map(|data| Op::Structured { seq: *seq, data }),
            );
            out
        }
        Op::AppAutocheckpoint(Some(frames)) => {
            let mut out = vec![Op::AppAutocheckpoint(None)];
            out.extend(
                lower(*frames)
                    .into_iter()
                    .map(|f| Op::AppAutocheckpoint(Some(f))),
            );
            out
        }
        Op::Parts(PartsOp::Land(index)) => lower(*index)
            .into_iter()
            .map(|i| Op::Parts(PartsOp::Land(i)))
            .collect(),
        Op::Shrink { pages } => lower(*pages)
            .into_iter()
            .map(|pages| Op::Shrink { pages })
            .collect(),
        Op::SecondWriter { ticks } => lower(*ticks)
            .into_iter()
            .map(|ticks| Op::SecondWriter { ticks })
            .collect(),
        Op::WalDamage { op, ticks } => {
            let mut out: Vec<Op> = lower(*ticks)
                .into_iter()
                .map(|ticks| Op::WalDamage { op: *op, ticks })
                .collect();
            out.extend(
                wals(*op)
                    .into_iter()
                    .map(|op| Op::WalDamage { op, ticks: *ticks }),
            );
            out
        }
        Op::Hold { hold, ticks } => lower(*ticks)
            .into_iter()
            .map(|ticks| Op::Hold { hold: *hold, ticks })
            .collect(),
        Op::Vacuum { page_size: None }
        | Op::Spill
        | Op::Rollback
        | Op::OtherProcessReader { .. }
        | Op::Crash(crate::sqlite::CrashKind::Process)
        | Op::EnvFault(_)
        | Op::Tick
        | Op::Settle
        | Op::UntilPublished
        | Op::Shutdown
        | Op::RequestSnapshot
        | Op::Restore
        | Op::Restart
        | Op::TrustLocal
        | Op::PowerLoss
        | Op::LyingWalSync
        | Op::Meta(_)
        | Op::SaveDatabase
        | Op::PutBackDatabase { .. }
        | Op::OurReader { .. }
        | Op::Remote(_)
        | Op::AppAutocheckpoint(None)
        | Op::HttpFault(_)
        | Op::Parts(PartsOp::ByHand | PartsOp::BySeed)
        | Op::StagingDamage(_)
        | Op::Heal
        | Op::Verify => Vec::new(),
    }
}

/// The plainer checkpoint modes than `mode`: PASSIVE takes no wait and no reset.
fn other_modes(
    mode: crate::sqlite::OtherProcessMode,
) -> impl Iterator<Item = crate::sqlite::OtherProcessMode> {
    use crate::sqlite::OtherProcessMode as M;
    let plainer: &'static [M] = match mode {
        M::Passive => &[],
        M::Restart | M::Truncate | M::Full => &[M::Passive],
    };
    plainer.iter().copied()
}

fn app_ats(at: AppAt) -> Vec<AppAt> {
    match at {
        AppAt::Call(calls) => lower(calls).into_iter().map(AppAt::Call).collect(),
        AppAt::Before(_) => vec![AppAt::Call(0)],
    }
}

fn app_ops(op: AppOp) -> Vec<AppOp> {
    match op {
        AppOp::Commit { pages, grow } => {
            let mut out: Vec<AppOp> = lower(pages)
                .into_iter()
                .map(|pages| AppOp::Commit { pages, grow })
                .collect();
            if grow {
                out.push(AppOp::Commit { pages, grow: false });
            }
            out
        }
        AppOp::Spill { pages } => lower(pages)
            .into_iter()
            .map(|pages| AppOp::Spill { pages })
            .collect(),
        AppOp::Checkpoint(mode) => other_modes(mode).map(AppOp::Checkpoint).collect(),
        AppOp::ReaderBegin | AppOp::ReaderEnd | AppOp::DeleteWal => Vec::new(),
    }
}

fn wals(wal: WalOp) -> Vec<WalOp> {
    match wal {
        WalOp::TruncateAtFrame(frame) => lower(frame)
            .into_iter()
            .map(WalOp::TruncateAtFrame)
            .collect(),
        WalOp::RewriteSameLength(n) => lower(n).into_iter().map(WalOp::RewriteSameLength).collect(),
        WalOp::RegrowSameSalts(n) => lower(n).into_iter().map(WalOp::RegrowSameSalts).collect(),
        WalOp::RegrowWithoutCommitMarks(n) => lower(n)
            .into_iter()
            .map(WalOp::RegrowWithoutCommitMarks)
            .collect(),
        WalOp::TearHeader | WalOp::FlipLastFrameBit => Vec::new(),
    }
}

/// Every field of `opts` that a replay must set, as the lines of an `Opts` literal. Destructures
/// the whole struct, so a new field must say whether a reproduction needs it.
fn opts_fields(opts: &Opts) -> Vec<String> {
    let Opts {
        plant: _,
        trace: _,
        record: _,
        allow_stop: _,
        responses: _,
        retention: _,
        switch_retention: _,
        drawn,
        block_crash_law,
        lying_wal_sync,
        after_restart,
        clock_law,
        overlap,
        error_domain,
        staging_fs,
        truncate_pages,
        slice_bytes,
        first_db_pages,
        cost: _,
        latency_tail,
        kill_after_fault,
        remote_s3,
        remote_parts,
        no_create_only,
        judges,
        sqlite: _,
        write_gate: _,
    } = opts;
    let mut out = Vec::new();
    for (name, on) in [
        ("drawn", drawn),
        ("block_crash_law", block_crash_law),
        ("lying_wal_sync", lying_wal_sync),
        ("overlap", overlap),
        ("error_domain", error_domain),
        ("staging_fs", staging_fs),
        ("kill_after_fault", kill_after_fault),
        ("remote_s3", remote_s3),
        ("no_create_only", no_create_only),
        ("latency_tail", latency_tail),
    ] {
        if *on {
            out.push(format!("{name}: true"));
        }
    }
    if *after_restart != crate::generate::AfterRestart::default() {
        out.push(format!(
            "after_restart: lsr_sim::AfterRestart::{after_restart:?}"
        ));
    }
    if *clock_law != crate::clock::ClockLaw::default() {
        out.push(format!("clock_law: lsr_sim::ClockLaw::{clock_law:?}"));
    }
    if let Some(pages) = truncate_pages {
        out.push(format!(
            "truncate_pages: std::num::NonZeroU32::new({pages})"
        ));
    }
    if let Some(bytes) = slice_bytes {
        out.push(format!("slice_bytes: std::num::NonZeroU64::new({bytes})"));
    }
    if let Some(pages) = first_db_pages {
        out.push(format!(
            "first_db_pages: std::num::NonZeroU32::new({pages})"
        ));
    }
    if let Some(parts) = remote_parts {
        out.push(format!(
            "remote_parts: lsr_sim::remote::Parts::new({}, {})",
            parts.part, parts.lanes
        ));
    }
    if *judges != crate::run::Judges::default() {
        out.push(format!(
            "judges: lsr_sim::run::Judges {{ oracle: {}, history: {} }}",
            judges.oracle, judges.history
        ));
    }
    out
}

/// The shrunk history as Rust source to paste into an explicit scenario test: the operations,
/// the options they fail under, and the call.
///
/// # Errors
/// [Unprintable] for an operation value [crate::record] cannot write yet.
pub fn scenario_source(seed: Seed, shrunk: &Shrunk) -> Result<String, Unprintable> {
    let mut out = format!(
        "// seed 0x{seed}: {} operations ({} up to the failure) shrunk to {} in {} runs{}\n// fails with: {}\nlet ops = [\n",
        shrunk.before,
        shrunk.prefix,
        shrunk.ops.len(),
        shrunk.runs,
        if shrunk.capped {
            " (budget spent, not minimal)"
        } else {
            ""
        },
        shrunk.violation,
    );
    for op in &shrunk.ops {
        out.push_str(&format!("    {},\n", record::op(op)?));
    }
    out.push_str("];\nlet opts = Opts {\n");
    for field in opts_fields(&shrunk.opts) {
        out.push_str(&format!("    {field},\n"));
    }
    out.push_str("    ..Opts::default()\n};\n");
    out.push_str(&format!("run_ops(Seed::new(0x{seed}), &ops, &opts)\n"));
    Ok(out)
}

#[cfg(test)]
mod tests;
