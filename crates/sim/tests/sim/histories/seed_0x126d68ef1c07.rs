//! Seed 0x126d68ef1c07's history of 52 operations, as the first generator (`Generator::V1`, since deleted) drew it.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{StoreError, StoreOp};
use lsr_ltx::PageSize;
use lsr_sim::generate::{EnvFault, Op, Target};
use lsr_sim::{CrashKind, FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::Tick,
    Op::Crash(CrashKind::Power),
    Op::Tick,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::CommitInTick { pages: 1 },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Tick,
    Op::ClockJump(50954),
    Op::Tick,
    Op::RequestSnapshot,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Rollback,
    Op::Tick,
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Rollback,
    Op::Tick,
    Op::Vacuum {
        page_size: PageSize::new(1024),
    },
    Op::Restore,
    Op::Tick,
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::Tick,
    Op::EnvFault(EnvFault::WipeStaging),
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Commit {
        pages: 4,
        grow: true,
    },
    Op::Tick,
    Op::Tick,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Crash(CrashKind::Power),
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::Tick,
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Spill,
    Op::ClockJump(-43516),
    Op::CommitInTick { pages: 2 },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Tick,
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Read,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Read,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::OtherProcessReader { begin: true },
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::ClockJump(118858),
    Op::Restore,
];
