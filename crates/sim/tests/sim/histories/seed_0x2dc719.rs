//! Seed 0x2dc719's history, as the live generator under the seed's own mix with the whole error
//! domain drew it, shrunk by hand from the first 90 operations to the 9 that still fail.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Op, RemoteOp};

pub const OPS: &[Op] = &[
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Shutdown,
    Op::Shutdown,
    Op::Tick,
    Op::Remote(RemoteOp::RollBack),
    Op::Shutdown,
    Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
    Op::AppInTick {
        at: AppAt::Call(20),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Tick,
];
