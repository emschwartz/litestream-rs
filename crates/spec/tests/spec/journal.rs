//! The journal model, walked exhaustively: the reference writer meets every property in every
//! configuration, each planted defect is caught by the property it breaks on a configuration the
//! reference passes, and the two stated assumptions are shown to be needed.

use lsr_spec::journal::explore::{Report, explore};
use lsr_spec::journal::fs::{Block, Device};
use lsr_spec::journal::world::{Budget, Config, Plant, Property};

const BLOCKS: [Block; 3] = [Block::B512, Block::B4096, Block::Large];
const TEARS: Device = Device::Tears {
    unchanged_safe: true,
};

/// Every kind of event at once: enough writes that both slots take both roles after a create,
/// two crashes so a recovery is itself crashed, two failures so a retry can fail.
fn everything() -> Budget {
    Budget {
        writes: 4,
        crashes: 2,
        failures: 2,
        evictions: 1,
        resets: 1,
        newer: 1,
        fills: 1,
        ..Budget::default()
    }
}

/// Latent and transient read errors, with fewer of the rest so the walk stays small.
fn media() -> Budget {
    Budget {
        writes: 4,
        crashes: 2,
        failures: 1,
        evictions: 1,
        latent: 1,
        read_faults: 1,
        ..Budget::default()
    }
}

/// What the plants are judged on.
fn small() -> Budget {
    Budget {
        writes: 4,
        crashes: 2,
        failures: 1,
        evictions: 1,
        ..Budget::default()
    }
}

fn clean(config: &Config) -> Report {
    let report = explore(config);
    assert_eq!(report.counterexample, None, "{config:?}");
    assert!(
        report.states > 1000,
        "{config:?} walked only {} states",
        report.states
    );
    report
}

fn breaks(config: &Config, property: Property) {
    let report = explore(config);
    let found = report
        .counterexample
        .unwrap_or_else(|| panic!("{config:?} survived {} states", report.states));
    assert_eq!(found.property, property, "{:#?}", found.trace);
}

#[test]
fn the_reference_meets_every_property_under_the_block_law() {
    for block in BLOCKS {
        let report = clean(&Config::new(block, Device::Atomic, everything()));
        for kind in [
            "Kill",
            "Evict",
            "Fill",
            "NewerWrite",
            "Engine(Reset)",
            "Boot(None, Newer(Rid))",
            "Sys(DirFsync, DirFailed(false))",
            "Sys(WriteWhole, Failed(FirstSlot))",
            "Sys(WriteSlot(S0), Failed(Untouched))",
            "Sys(WriteSlot(S1), Enospc)",
        ] {
            assert!(
                report.reached.contains(kind),
                "{block:?} never reached {kind}"
            );
        }
        let powered = report
            .reached
            .iter()
            .filter(|kind| kind.starts_with("Power"))
            .count();
        assert!(
            powered >= 5,
            "{block:?} reached {powered} kinds of power loss"
        );
    }
}

#[test]
fn the_reference_meets_every_property_on_a_tearing_device() {
    for block in BLOCKS {
        let report = clean(&Config::new(block, TEARS, everything()));
        assert!(
            report.reached.iter().any(|kind| kind.contains("Torn")),
            "{block:?} never tore a sector"
        );
    }
}

#[test]
fn the_reference_meets_every_property_with_unreadable_sectors() {
    for device in [Device::Atomic, TEARS] {
        let report = clean(&Config::new(Block::B4096, device, media()));
        for kind in [
            "Latent(S0)",
            "Latent(S1)",
            "Boot(Some(Open), Unreadable(Io))",
            "Boot(Some(Slot(S0)), Read(Rid))",
            "Boot(None, Unreadable(Io))",
        ] {
            assert!(
                report.reached.contains(kind),
                "{device:?} never reached {kind}"
            );
        }
    }
}

#[test]
fn the_reference_refuses_every_falling_floor() {
    let mut config = Config::new(Block::B4096, Device::Atomic, small());
    config.lower = true;
    let report = clean(&config);
    assert!(report.reached.contains("Refused(Lower)"));
}

/// Review 5, J3: with both slots in one block, writing one rewrites the other's sectors with the
/// same bytes. That is safe only if the device never destroys a sector it rewrites unchanged.
#[test]
fn a_large_block_needs_a_device_that_spares_unchanged_sectors() {
    let destroys = Device::Tears {
        unchanged_safe: false,
    };
    breaks(
        &Config::new(Block::Large, destroys, small()),
        Property::Recovered,
    );
    for block in [Block::B512, Block::B4096] {
        clean(&Config::new(block, destroys, small()));
    }
}

/// A failed write that left part of a page changed in the cache would let a boot after a failed
/// fdatasync overwrite the last acknowledged record with garbage. A page-aligned 4096-byte write
/// from a resident buffer does not do that, which is why `torn_writes` is off elsewhere.
#[test]
fn the_writer_needs_page_atomic_slot_writes() {
    // A failed fdatasync, then the torn write: two failures.
    let two = Budget {
        failures: 2,
        ..small()
    };
    let mut config = Config::new(Block::B4096, Device::Atomic, two);
    config.torn_writes = true;
    breaks(&config, Property::Recovered);
}

fn planted(device: Device, budget: Budget, plant: Plant, property: Property) {
    let config = Config::new(Block::B4096, device, budget);
    clean(&config);
    breaks(&config.planted(plant), property);
}

fn reads() -> Budget {
    Budget {
        read_faults: 1,
        ..small()
    }
}

#[test]
fn planted_no_directory_fsync_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::NoDirFsync,
        Property::CreateFinished,
    );
}

#[test]
fn planted_seq_1_taken_as_created_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::Seq1Created,
        Property::CreateFinished,
    );
}

#[test]
fn planted_recreate_without_zeroing_is_caught() {
    // A stale record must outnumber the record after the reset: three writes before it, the
    // recreate, one after; a kill before each of three boots.
    let deeper = Budget {
        writes: 5,
        crashes: 3,
        ..reads()
    };
    planted(
        Device::Atomic,
        deeper,
        Plant::RecreateWithoutZeroing,
        Property::Recovered,
    );
}

#[test]
fn planted_continuing_after_a_slot_read_error_is_caught() {
    // The slots drift (a failed fdatasync, a kill, a write that lands), a boot misses the newer
    // slot, its write over that slot fails, and a newer binary continues from the cache.
    let drift = Budget {
        writes: 6,
        crashes: 3,
        failures: 2,
        evictions: 1,
        read_faults: 1,
        newer: 1,
        ..Budget::default()
    };
    planted(
        Device::Atomic,
        drift,
        Plant::ContinueAfterSlotError,
        Property::Recovered,
    );
}

#[test]
fn planted_wrong_slot_is_caught_on_a_tearing_device() {
    planted(TEARS, small(), Plant::WrongSlot, Property::Recovered);
}

/// Under the block law alone a record in one sector is never torn, so overwriting the newest
/// record in place is safe: the second slot is there for the devices that tear.
#[test]
fn planted_wrong_slot_survives_the_block_law_alone() {
    let deep = Budget {
        writes: 6,
        failures: 2,
        ..small()
    };
    clean(&Config::new(Block::B4096, Device::Atomic, deep).planted(Plant::WrongSlot));
}

#[test]
fn planted_read_error_as_missing_is_caught() {
    planted(
        Device::Atomic,
        reads(),
        Plant::EioIsMissing,
        Property::EioNotMissing,
    );
}

#[test]
fn planted_one_read_for_both_slots_is_caught() {
    planted(
        Device::Atomic,
        reads(),
        Plant::OneRead,
        Property::SlotSurvives,
    );
}

#[test]
fn planted_claiming_the_winner_durable_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::ClaimWinner,
        Property::Recovered,
    );
}

#[test]
fn planted_failed_fdatasync_taken_as_success_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::FailedSyncIsSuccess,
        Property::Recovered,
    );
}

#[test]
fn planted_bare_fdatasync_retry_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::BareRetry,
        Property::Recovered,
    );
}

#[test]
fn planted_no_fdatasync_is_caught() {
    planted(Device::Atomic, small(), Plant::NoSync, Property::Recovered);
}

#[test]
fn planted_lower_seq_wins_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::LowerSeqWins,
        Property::Recovered,
    );
}

#[test]
fn planted_create_by_set_len_is_caught() {
    planted(
        Device::Atomic,
        small(),
        Plant::SetLenCreate,
        Property::NoAllocation,
    );
}

#[test]
fn planted_no_boot_sync_is_caught_on_a_tearing_device() {
    planted(TEARS, small(), Plant::NoBootSync, Property::Recovered);
}

#[test]
fn planted_no_monotonic_check_is_caught() {
    let mut config = Config::new(Block::B4096, Device::Atomic, small());
    config.lower = true;
    clean(&config);
    breaks(&config.planted(Plant::NoMonotonic), Property::FloorMonotone);
}

/// The budgets above, doubled where it matters: `just journal-model` runs it.
#[test]
#[ignore = "minutes; run by `just journal-model`"]
fn deep() {
    let deep = Budget {
        writes: 5,
        crashes: 3,
        failures: 2,
        evictions: 1,
        latent: 1,
        read_faults: 1,
        resets: 1,
        newer: 1,
        fills: 1,
    };
    for block in BLOCKS {
        for device in [Device::Atomic, TEARS] {
            let report = clean(&Config::new(block, device, deep));
            println!("{block:?} {device:?}: {} states", report.states);
        }
    }
}
