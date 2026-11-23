//! Seed 0x95a's history under its mixed mix, shrunk to the operations that still fail: bucket rollbacks leave a hole wider than the probe's gap, and a boot with staging wiped behind a lagging listing took the hole as the replica's end and uploaded below what the replica held, so the latest state went back.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_sim::generate::{EnvFault, Hold, MetaOp, Op, RemoteOp};

pub const OPS: &[Op] = &[
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Hold {
        hold: Hold::LyingDisk,
        ticks: 2,
    },
    Op::OtherProcessReader { begin: true },
    Op::Shutdown,
    Op::CommitInTick { pages: 4 },
    Op::CommitInTick { pages: 2 },
    Op::Meta(MetaOp::ForgeNewest),
    Op::StagingDamage(MetaOp::ForgeNewest),
    Op::Shutdown,
    Op::Shutdown,
    Op::Remote(RemoteOp::RollBack),
    Op::Tick,
    Op::Meta(MetaOp::ForgeNewest),
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::LaggingListings {
        writes: 2,
        window: 24,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::Shutdown,
    Op::EnvFault(EnvFault::WipeStaging),
    Op::CommitInTick { pages: 1 },
    Op::Hold {
        hold: Hold::LaggingListing,
        ticks: 8,
    },
];
