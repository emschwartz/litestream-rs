//! The kernel evicts the WAL pages a failed fsync of the engine lost (`EnvFault::EvictPageCache`):
//! later reads find the disk's older bytes under frames SQLite still counts, so the WAL is corrupt
//! from then on. The engine may report it (`PersistentCause::WalCorrupt`, or a WAL cut short),
//! and the run must take that as the fault it is, never as a bug.

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{EnvFault, Hold};
use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, Op, Opts, Seed};

/// Readers hold the WAL while staging is wiped, so the engine's next boot reads the WAL from its
/// start; an eviction is armed, the engine's WAL fsync fails and the process dies right after
/// (the kill after a fault). The application redoes its commits after the restart, and the engine
/// that boots next meets the evicted frames and reports the WAL corrupt. Found by soak seed
/// 0x126d690a07b0, shrunk to these steps.
#[test]
fn a_wal_corrupted_by_an_evicted_page_cache_is_a_fault_not_a_bug() {
    let ops = [
        Op::Hold {
            hold: Hold::ReadersHeld,
            ticks: 6,
        },
        Op::EnvFault(EnvFault::WipeStaging),
        Op::EnvFault(EnvFault::EvictPageCache),
        Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
        Op::Tick,
        Op::Tick,
        Op::Restore,
    ];
    let opts = Opts {
        kill_after_fault: true,
        after_restart: AfterRestart::Redo,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(1), &ops, &opts) {
        panic!("{failure}");
    }
}
