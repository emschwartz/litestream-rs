//! Seed 0x8ef's history of 12 operations, as the generator under the seed's own mix drew it before the merge of #188.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{EnvFault, MetaOp, Op};
use std::io::ErrorKind;

pub const OPS: &[Op] = &[
    Op::OtherProcessReader { begin: false },
    Op::Meta(MetaOp::DeleteAll),
    Op::Tick,
    Op::Heal,
    Op::Tick,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::CommitInTick { pages: 3 },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::ClockJump(93600000),
    Op::EnvFault(EnvFault::Source(
        SourceOp::ReadPage,
        SourceError::Io(ErrorKind::Other),
    )),
    Op::OtherProcessReader { begin: true },
    Op::Tick,
];
