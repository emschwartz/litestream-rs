//! Seed 0x25's history of 44 operations, as the first generator (`Generator::V1`, since deleted) drew it.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{StoreError, StoreOp};
use lsr_sim::generate::{Op, Target};
use lsr_sim::{CrashKind, FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::List,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::Tick,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 1,
        shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Tick,
    Op::Tick,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::CommitInTick { pages: 4 },
    Op::Tick,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Crash(CrashKind::Process),
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Tick,
    Op::Crash(CrashKind::Power),
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::OtherProcessReader { begin: true },
    Op::Tick,
    Op::Tick,
    Op::Spill,
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Commit {
        pages: 2,
        grow: true,
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::ErrorAfterPublish(StoreError::Throttled {
            op: StoreOp::Upload,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::Tick,
    Op::Crash(CrashKind::Power),
    Op::Commit {
        pages: 1,
        grow: false,
    },
];
