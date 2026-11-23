//! Coverage marks per cell of the error domain, so the soak gate fails on a cell no run reached.
//!
//! [`crate::marks`] counts fault classes: `EnvFault.Source` or `StoreFault.Error` as a whole. A
//! table of 96 source cells and 96 store cells can have most of its drawn cells never fire while
//! the class count is high, so each cell gets two keys of its own, `cell.<seam>.<Op>.<Class>`
//! followed by `.drawn` (a history held it) and `.hit` (a call returned it):
//!
//! - [`Cell::Drawn`] and [`Cell::KnownWrong`] cells must be hit. The soak draws known-wrong cells
//!   too ([`crate::Weights::known_wrong`]), so a cell whose defect has no draw left is a zero.
//! - [`Cell::Impossible`] and [`Cell::Unmodelled`] cells must stay undrawn: a draw of one means
//!   the generator stopped reading the table. The older subset that runs without
//!   [`crate::Weights::error_domain`] draw from passes each draw through the table too
//!   ([`super::possible_source`], [`super::possible_store`]).
//! - [`Cell::World`] cells are the world change their [`EnvFault`] names, counted under that
//!   fault's own class, since no call returns them as an injected error.
//!
//! A store fault counts the call that returned its error under any shape that carries one
//! ([`crate::run::shape_error`]): a plain error, an error after the object was published, or a
//! rate. A source fault counts the call that took the injection.

use std::collections::BTreeMap;

use lsr_core::{Denied, SourceError, SourceOp, StoreOp};

use super::{
    Cell, SOURCE_CLASSES, SOURCE_OPS, STORE_CLASSES, STORE_OPS, SourceClass, StoreClass, TARGETS,
    source_cell, source_class, store_cell, store_class,
};
use crate::generate::{EnvFault, Op, Target};
use crate::reach::Reach;
use crate::store::FaultShape;

pub(super) const fn source_op_name(op: SourceOp) -> &'static str {
    match op {
        SourceOp::Open => "Open",
        SourceOp::Len => "Len",
        SourceOp::Committed => "Committed",
        SourceOp::PageCount => "PageCount",
        SourceOp::ReadPage => "ReadPage",
        SourceOp::BeginBarrier => "BeginBarrier",
        SourceOp::EndBarrier => "EndBarrier",
        SourceOp::Checkpoint => "Checkpoint",
        SourceOp::BumpSeq => "BumpSeq",
        SourceOp::BeginWriteLock => "BeginWriteLock",
        SourceOp::EndWriteLock => "EndWriteLock",
        SourceOp::Sync => "Sync",
    }
}

const fn source_class_name(class: SourceClass) -> &'static str {
    match class {
        SourceClass::Io => "Io",
        SourceClass::DiskFull => "DiskFull",
        SourceClass::FsyncFailed => "FsyncFailed",
        SourceClass::Busy => "Busy",
        SourceClass::ReadersHeld => "ReadersHeld",
        SourceClass::Short => "Short",
        SourceClass::Unlinked => "Unlinked",
        SourceClass::ReadTxLost => "ReadTxLost",
    }
}

const fn store_op_name(op: StoreOp) -> &'static str {
    match op {
        StoreOp::List => "List",
        StoreOp::Read => "Read",
        StoreOp::Upload => "Upload",
        StoreOp::Delete => "Delete",
    }
}

const fn store_class_name(class: StoreClass) -> &'static str {
    match class {
        StoreClass::Transient => "Transient",
        StoreClass::Throttled => "Throttled",
        StoreClass::Denied(why) => match why {
            Denied::Credentials => "Denied.Credentials",
            Denied::Permission => "Denied.Permission",
            Denied::ClockSkew => "Denied.ClockSkew",
            Denied::BucketMissing => "Denied.BucketMissing",
            Denied::Tls => "Denied.Tls",
        },
        StoreClass::NotFound => "NotFound",
        StoreClass::AlreadyExists => "AlreadyExists",
        StoreClass::ScratchExhausted => "ScratchExhausted",
        StoreClass::Corrupt => "Corrupt",
        StoreClass::Refused => "Refused",
        StoreClass::CreateOnlyUnsupported => "CreateOnlyUnsupported",
    }
}

const fn target_name(target: Target) -> &'static str {
    match target {
        Target::Staging => "staging",
        Target::Remote => "remote",
    }
}

/// The key of a source cell, without its count.
#[must_use]
pub fn source_key(op: SourceOp, class: SourceClass) -> String {
    format!(
        "cell.source.{}.{}",
        source_op_name(op),
        source_class_name(class)
    )
}

/// The key of a store cell, without its count.
#[must_use]
pub fn store_key(target: Target, op: StoreOp, class: StoreClass) -> String {
    format!(
        "cell.{}.{}.{}",
        target_name(target),
        store_op_name(op),
        store_class_name(class)
    )
}

/// Every cell of both tables, by key.
#[must_use]
pub fn cells() -> Vec<(String, Cell)> {
    let source = SOURCE_OPS.iter().flat_map(|op| {
        SOURCE_CLASSES
            .iter()
            .map(|class| (source_key(*op, *class), source_cell(*op, *class)))
    });
    let store = TARGETS.iter().flat_map(|target| {
        STORE_OPS.iter().flat_map(move |op| {
            STORE_CLASSES.iter().map(move |class| {
                (
                    store_key(*target, *op, *class),
                    store_cell(*target, *op, *class),
                )
            })
        })
    });
    source.chain(store).collect()
}

/// The `.hit` keys the gate requires above zero: every drawn and known-wrong cell.
#[must_use]
pub fn required_keys() -> Vec<String> {
    cells()
        .into_iter()
        .filter(|(_, cell)| match cell {
            Cell::Drawn | Cell::KnownWrong(_) => true,
            Cell::World(_) | Cell::Impossible(_) | Cell::Unmodelled(_) => false,
        })
        .map(|(key, _)| format!("{key}.hit"))
        .collect()
}

/// The `.drawn` keys the gate requires at zero: every impossible and unmodelled cell.
#[must_use]
pub fn undrawn_keys() -> Vec<String> {
    cells()
        .into_iter()
        .filter(|(_, cell)| match cell {
            Cell::Impossible(_) | Cell::Unmodelled(_) => true,
            Cell::Drawn | Cell::KnownWrong(_) | Cell::World(_) => false,
        })
        .map(|(key, _)| format!("{key}.drawn"))
        .collect()
}

/// The cell `op` draws, if it draws one: a source failure injected, or a store fault whose
/// shape carries an error.
#[must_use]
pub fn drawn_key(op: &Op) -> Option<String> {
    // Only the two fault operations draw from the domain; every other operation is left out on
    // purpose, so a new one needs no arm here.
    if let Op::EnvFault(EnvFault::Source(source, err)) = op {
        return Some(source_key(*source, source_class(*err)));
    }
    if let Op::StoreFault {
        target, op, shape, ..
    } = op
    {
        return crate::run::shape_error(shape).map(|err| store_key(*target, *op, store_class(err)));
    }
    None
}

/// One run's cell counts.
#[derive(Debug, Default)]
pub struct CellMarks {
    drawn: BTreeMap<String, u64>,
}

impl CellMarks {
    /// The runner begins applying `op`.
    pub fn begin(&mut self, op: &Op) {
        if let Some(key) = drawn_key(op) {
            let slot = self.drawn.entry(key).or_insert(0);
            *slot = slot.saturating_add(1);
        }
    }

    /// Adds the run's counts to `reach`, with the calls each store fault hit (`store_hits`, the
    /// store it was scheduled on with each) and the source failures calls took (`taken`).
    pub fn tally(
        &self,
        reach: &mut Reach,
        store_hits: &[(Target, StoreOp, FaultShape, u32)],
        taken: &[(SourceOp, SourceError)],
    ) {
        for (key, n) in &self.drawn {
            reach.add(format!("{key}.drawn"), *n);
        }
        for (target, op, shape, hits) in store_hits {
            if let Some(err) = crate::run::shape_error(shape) {
                let key = store_key(*target, *op, store_class(err));
                reach.add(format!("{key}.hit"), u64::from(*hits));
            }
        }
        for (op, err) in taken {
            reach.bump(format!("{}.hit", source_key(*op, source_class(*err))));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_distinct_and_cover_both_tables() {
        let mut keys: Vec<String> = cells().into_iter().map(|(key, _)| key).collect();
        let all = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), all);
        assert_eq!(
            all,
            SOURCE_OPS.len() * SOURCE_CLASSES.len()
                + TARGETS.len() * STORE_OPS.len() * STORE_CLASSES.len()
        );
    }

    #[test]
    fn every_cell_is_gated_one_way_or_counted_under_its_world_fault() {
        let required = required_keys();
        let undrawn = undrawn_keys();
        for (key, cell) in cells() {
            let hit = required.contains(&format!("{key}.hit"));
            let quiet = undrawn.contains(&format!("{key}.drawn"));
            let world = matches!(cell, Cell::World(_));
            assert_eq!(
                [hit, quiet, world].iter().filter(|b| **b).count(),
                1,
                "{key}"
            );
        }
    }
}
