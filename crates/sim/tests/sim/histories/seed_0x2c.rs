//! Seed 0x2c's history of 8 operations, as the live generator under the seed's own mix drew it.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_sim::generate::{MetaOp, Op, WalOp};

pub const OPS: &[Op] = &[
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Tick,
    Op::Spill,
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(8063039911997408971),
        ticks: 2,
    },
    Op::Shutdown,
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(13577244702264426995),
        ticks: 7,
    },
    Op::Restore,
];
