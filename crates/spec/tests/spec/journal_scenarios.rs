//! Named histories through the journal model, one per finding of the five design reviews and
//! of the model itself (docs/journal.md, "Findings and where each is checked"). Each takes the
//! reference writer through the history and, where a plant undoes the rule, shows the same
//! history break the property the rule keeps.

use lsr_spec::journal::fs::{Block, Device, Slot};
use lsr_spec::journal::script::{Script, ScriptError};
use lsr_spec::journal::world::{Budget, Config, Plant, Property};

const CREATE: [&str; 6] = [
    "Boot(None, Missing)",
    "Engine(Advance)",
    "Sys(Create, Ok)",
    "Sys(WriteWhole, Ok)",
    "Sys(Fsync, Ok)",
    "Sys(DirFsync, Ok)",
];

const WRITE_S1: [&str; 3] = [
    "Engine(Advance)",
    "Sys(WriteSlot(S1), Ok)",
    "Sys(Fsync, Ok)",
];
const WRITE_S0: [&str; 3] = [
    "Engine(Advance)",
    "Sys(WriteSlot(S0), Ok)",
    "Sys(Fsync, Ok)",
];

fn budget() -> Budget {
    Budget {
        writes: 8,
        crashes: 4,
        failures: 3,
        evictions: 2,
        read_faults: 1,
        newer: 1,
        resets: 1,
        fills: 1,
        latent: 0,
    }
}

fn config() -> Config {
    Config::new(Block::B4096, Device::Atomic, budget())
}

fn tears() -> Config {
    let tearing = Device::Tears {
        unchanged_safe: true,
    };
    Config::new(Block::B4096, tearing, budget())
}

/// Runs `parts` in order from the initial state.
fn run(config: Config, parts: &[&[&str]]) -> Result<Script, ScriptError> {
    let steps: Vec<&str> = parts.iter().flat_map(|part| part.iter().copied()).collect();
    Script::run(config, &steps)
}

fn seq(script: &Script, slot: Slot) -> Option<u8> {
    script.state().reads(slot).map(|rec| rec.seq)
}

fn broke(result: Result<Script, ScriptError>, want: Property) {
    match result {
        Err(ScriptError::Broke { property, .. }) => assert_eq!(property, want),
        Err(other) => panic!("{other:?}"),
        Ok(script) => panic!("nothing broke: {:?}", script.trace()),
    }
}

/// Review 5, J1: a kill after the file's fsync and before the directory's leaves a journal a
/// later boot reads as `seq = 1`. It finishes the creation before writing `seq = 2`.
#[test]
fn j1_a_kill_between_the_two_create_fsyncs_is_finished_by_the_next_boot() {
    let head = [
        "Boot(None, Missing)",
        "Engine(Advance)",
        "Sys(Create, Ok)",
        "Sys(WriteWhole, Ok)",
        "Sys(Fsync, Ok)",
        "Kill",
        "Boot(None, Read",
        "Engine(Advance)",
    ];
    let finish = [
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, Ok)",
        "Sys(DirFsync, Ok)",
    ];
    let tail = [
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, Ok)",
        "Power",
        "Boot(None, Read",
    ];
    let script = run(config(), &[&head, &finish, &tail]).unwrap();
    assert_eq!(seq(&script, Slot::S1), Some(2));
    let planted = config().planted(Plant::Seq1Created);
    broke(
        run(planted, &[&head, &["Sys(Fsync, Ok)"], &tail]),
        Property::CreateFinished,
    );
}

/// Review 5, J1: a power loss during the create can keep 4096 bytes with slot 0 whole. The
/// finish extends the file before any record after the first, so that write never allocates.
#[test]
fn j1_a_short_file_is_extended_before_the_second_record() {
    let head = [
        "Boot(None, Missing)",
        "Engine(Advance)",
        "Sys(Create, Ok)",
        "Sys(WriteWhole, Ok)",
        "Power*s0: Some(New)*entry: true, len: Short1",
        "Boot(None, Read",
        "Engine(Advance)",
    ];
    let finish = [
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, Ok)",
        "Sys(DirFsync, Ok)",
    ];
    let script = run(config(), &[&head, &finish, &["Sys(WriteSlot(S1), Ok)"]]).unwrap();
    assert!(script.state().fs().len_durable() == lsr_spec::journal::fs::Len::Full);
    let planted = config().planted(Plant::Seq1Created);
    let write = ["Sys(Fsync, Ok)", "Sys(WriteSlot(S1)"];
    broke(run(planted, &[&head, &write]), Property::NoAllocation);
}

/// Review 5, J2: each slot is read with its own call, so an error in one keeps the other.
#[test]
fn j2_a_slot_that_reads_on_its_own_survives_an_error_in_the_other() {
    let tail = ["Kill", "Boot(Some(Slot(S1)), Read"];
    let script = run(config(), &[&CREATE, &WRITE_S1, &tail]).unwrap();
    assert_eq!(seq(&script, Slot::S0), Some(1));
    let planted = config().planted(Plant::OneRead);
    let either = ["Kill", "Boot(Some(Slot(S1))"];
    broke(
        run(planted, &[&CREATE, &WRITE_S1, &either]),
        Property::SlotSurvives,
    );
}

/// Review 5, J2: an I/O error is `Unreadable`, never `Missing`.
#[test]
fn j2_an_error_opening_the_journal_is_unreadable_never_missing() {
    run(
        config(),
        &[&CREATE, &["Kill", "Boot(Some(Open), Unreadable(Io))"]],
    )
    .unwrap();
    let planted = config().planted(Plant::EioIsMissing);
    broke(
        run(planted, &[&CREATE, &["Kill", "Boot(Some(Open)"]]),
        Property::EioNotMissing,
    );
}

/// Review 5, J3: under a block of 8192 bytes or more, writing one slot rewrites the other's
/// sectors unchanged. A device that may destroy such a sector loses the durable record.
#[test]
fn j3_a_large_block_loses_the_durable_record_to_a_device_that_destroys_unchanged_sectors() {
    let destroys = Device::Tears {
        unchanged_safe: false,
    };
    let failed = "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: Some(Torn) }";
    let tail = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        failed,
        "Power",
        "Boot",
    ];
    let large = Config::new(Block::Large, destroys, budget());
    broke(
        run(large, &[&CREATE, &WRITE_S1, &tail]),
        Property::Recovered,
    );
    let safe = Config::new(Block::Large, tears().device, budget());
    let refused = run(safe, &[&CREATE, &WRITE_S1, &tail]);
    assert!(matches!(refused, Err(ScriptError::NoSuchStep { step, .. }) if step == failed));
}

/// Review 5, J5: the writer refuses a record whose floor falls with no reset (a `Fault::Bug`
/// in the engine), so the falling record never reaches the disk.
#[test]
fn j5_the_writer_refuses_a_record_whose_floor_falls() {
    let mut lower = config();
    lower.lower = true;
    run(lower, &[&CREATE, &WRITE_S1, &["Refused(Lower)"]]).unwrap();
    broke(
        run(
            lower.planted(Plant::NoMonotonic),
            &[&CREATE, &WRITE_S1, &["Engine(Lower)"]],
        ),
        Property::FloorMonotone,
    );
}

/// Review 5, J5's boot half: "when both slots are valid, the pair must be adjacent" is not
/// taken. A failed fdatasync and a kill let the slots drift: here they hold 5 and 2, and both
/// were written correctly.
#[test]
fn j5_a_failed_fdatasync_and_a_kill_leave_the_slots_more_than_one_apart() {
    let drift = [
        "Engine(Advance)",
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: None, s1: Some(Old) }",
        "Kill",
        "Boot(None, Read",
        "Engine(Advance)",
        "Sys(Fsync, Ok)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, Ok)",
        "Power",
        "Boot(None, Read",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &WRITE_S0, &drift]).unwrap();
    assert_eq!(
        (seq(&script, Slot::S0), seq(&script, Slot::S1)),
        (Some(5), Some(2))
    );
}

/// Model finding: recreating an unreadable file zeroes it and syncs before the new `seq = 1`
/// record. Without that, a create whose fsync failed leaves clean pages showing `seq = 1` over
/// the old records, and once `seq = 2` is acknowledged an eviction brings back `seq = 3`.
#[test]
fn model_a_recreate_zeroes_the_file_first() {
    let reset = [
        "Kill",
        "Boot(Some(Open), Unreadable(Io))",
        "Engine(Advance)",
    ];
    let zero = ["Sys(WriteZeros, Ok)", "Sys(Fsync, Ok)"];
    let create = [
        "Sys(WriteWhole, Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: Some(Old) }",
        "Kill",
        "Boot(None, Read",
        "Engine(Advance)",
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, Ok)",
        "Sys(DirFsync, Ok)",
    ];
    let tail = [
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, Ok)",
        "Kill",
        "Evict",
        "Boot",
    ];
    let script = run(
        config(),
        &[&CREATE, &WRITE_S1, &WRITE_S0, &reset, &zero, &create, &tail],
    );
    assert_eq!(seq(&script.unwrap(), Slot::S1), Some(2));
    let planted = config().planted(Plant::RecreateWithoutZeroing);
    let open = ["Sys(Create, Ok)"];
    let history: [&[&str]; 7] = [&CREATE, &WRITE_S1, &WRITE_S0, &reset, &open, &create, &tail];
    broke(run(planted, &history), Property::Recovered);
}

/// Model finding: on a device that tears a sector it is writing, a lifetime's first write
/// syncs first, so a slot an earlier process wrote and did not sync is on disk before the
/// other slot is touched. The design's crash table needed a failed fsync and an eviction for
/// this; a kill between a write and its fdatasync is enough.
#[test]
fn model_a_lifetime_syncs_before_its_first_write_on_a_tearing_device() {
    let killed = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Kill",
        "Boot(None, Read",
        "Engine(Advance)",
    ];
    let torn = "Sys(Fsync, SyncFailed(Pair { s0: None, s1: Some(Torn) }";
    let synced = [
        "Sys(Fsync, Ok)",
        "Sys(WriteSlot(S1), Ok)",
        torn,
        "Power",
        "Boot",
    ];
    let script = run(tears(), &[&CREATE, &WRITE_S1, &killed, &synced]).unwrap();
    assert_eq!(seq(&script, Slot::S0), Some(3));
    let unsynced = [
        "Sys(WriteSlot(S1), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: Some(Torn) }",
        "Power",
        "Boot",
    ];
    let planted = tears().planted(Plant::NoBootSync);
    broke(
        run(planted, &[&CREATE, &WRITE_S1, &killed, &unsynced]),
        Property::Recovered,
    );
}

/// Model finding: a new lifetime claims no record durable until its own write is synced. Boot
/// may read a record whose fdatasync failed in an earlier process and is only in the cache.
#[test]
fn model_a_lifetime_claims_nothing_durable_before_it_writes() {
    let failed = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: None }",
        "Kill",
        "Boot(None, Read",
        "Kill",
        "Evict",
        "Boot",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &failed]).unwrap();
    assert_eq!(seq(&script, Slot::S1), Some(2));
    let planted = config().planted(Plant::ClaimWinner);
    broke(
        run(planted, &[&CREATE, &WRITE_S1, &failed]),
        Property::Recovered,
    );
}

/// Model finding, a stated assumption: a failed slot write changes nothing in the cache. If it
/// could leave a torn page, a boot after a failed fdatasync would tear the acknowledged record.
#[test]
fn model_a_torn_page_cache_write_would_lose_the_acknowledged_record() {
    let mut torn = config();
    torn.torn_writes = true;
    let history = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: None }",
        "Kill",
        "Boot(None, Read",
        "Engine(Advance)",
        "Sys(Fsync, Ok)",
        "Sys(WriteSlot(S1), Failed(Torn))",
        "Kill",
        "Evict",
        "Boot",
    ];
    broke(
        run(torn, &[&CREATE, &WRITE_S1, &history]),
        Property::Recovered,
    );
    let refused = run(config(), &[&CREATE, &WRITE_S1, &history]);
    assert!(matches!(refused, Err(ScriptError::NoSuchStep { .. })));
}

/// Model finding: a slot that cannot be read may hold a record numbered above the winner, so
/// the boot that hit the error re-establishes the file with what it read instead of
/// continuing below it.
#[test]
fn model_a_boot_that_could_not_read_a_slot_re_establishes() {
    let history = [
        "Kill",
        "Boot(Some(Slot(S0)), Read",
        "Engine(Advance)",
        "Sys(WriteZeros, Ok)",
    ];
    run(config(), &[&CREATE, &WRITE_S1, &WRITE_S0, &history]).unwrap();
}

/// Reviews 1 (8), 2 (2) and 3 (7): a record whose fdatasync failed is still what the next
/// process reads while the page stays cached; the debt it records survives a kill.
#[test]
fn rev1_a_record_whose_fdatasync_failed_is_read_by_the_next_process() {
    let failed = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: None }",
        "Kill",
        "Boot(None, Read",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &failed]).unwrap();
    assert_eq!(seq(&script, Slot::S0), Some(3));
}

/// Review 3 (7): once evicted, that record is gone, and boot falls back to the acknowledged
/// one, never to anything older.
#[test]
fn rev3_an_evicted_unsynced_record_falls_back_to_the_acknowledged_one() {
    let failed = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, SyncFailed(Pair { s0: Some(Old), s1: None }",
        "Kill",
        "Evict",
        "Boot(None, Read",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &failed]).unwrap();
    assert_eq!(
        (seq(&script, Slot::S0), seq(&script, Slot::S1)),
        (Some(1), Some(2))
    );
}

/// Reviews 1 (3), 3 (6) and 4 (2): after creation no write allocates, so a full disk refuses
/// none. A creation that has not finished can still be refused, which leaves main's behaviour.
#[test]
fn rev4_every_record_after_creation_is_written_on_a_full_disk() {
    let full = ["Fill", "Engine(Advance)", "Sys(WriteSlot(S1), Enospc)"];
    let refused = run(config(), &[&CREATE, &full]);
    assert!(matches!(refused, Err(ScriptError::NoSuchStep { .. })));
    run(config(), &[&CREATE, &["Fill"], &WRITE_S1, &WRITE_S0]).unwrap();
    let short = [
        "Boot(None, Missing)",
        "Engine(Advance)",
        "Sys(Create, Ok)",
        "Sys(WriteWhole, Ok)",
        "Power*s0: Some(New)*entry: true, len: Short1",
        "Boot(None, Read",
    ];
    run(config(), &[&short, &full]).unwrap();
}

/// Reviews 3 (1) and 4 (5): the floor names its destination. A changed destination is a logged
/// reset, after which the bound starts again; it is not a falling floor.
#[test]
fn rev3_a_destination_change_is_a_logged_reset() {
    let reset = ["Engine(Reset)", "Sys(WriteSlot(S0), Ok)", "Sys(Fsync, Ok)"];
    let script = run(config(), &[&CREATE, &WRITE_S1, &WRITE_S1[..0], &reset]).unwrap();
    let floor = script
        .state()
        .reads(Slot::S0)
        .and_then(|rec| rec.facts.floor);
    let before = script
        .state()
        .reads(Slot::S1)
        .and_then(|rec| rec.facts.floor);
    assert!(
        floor.is_some_and(|f| f.replica == 1 && f.bound == 0),
        "{floor:?}"
    );
    assert!(
        before.is_some_and(|f| f.replica == 0 && f.bound == 1),
        "{before:?}"
    );
}

/// Design, compatibility: a record from a newer binary is `Newer`, and this binary's next record
/// goes over the other slot, numbered above it.
#[test]
fn a_newer_binary_s_record_is_reported_and_the_other_slot_overwritten() {
    let newer = [
        "Kill",
        "NewerWrite",
        "Boot(None, Newer",
        "Engine(Advance)",
        "Sys(Fsync, Ok)",
        "Sys(WriteSlot(S0), Ok)",
        "Sys(Fsync, Ok)",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &WRITE_S0, &newer]).unwrap();
    assert_eq!(
        (seq(&script, Slot::S0), seq(&script, Slot::S1)),
        (Some(5), Some(4))
    );
}

/// Design crash table, row 2: a power loss during the write of one slot leaves the other, the
/// last durable record, untouched.
#[test]
fn a_power_loss_during_a_write_leaves_the_last_durable_record() {
    let lost = [
        "Engine(Advance)",
        "Sys(WriteSlot(S0), Ok)",
        "Power*s0: Some(Old)",
        "Boot(None, Read",
    ];
    let script = run(config(), &[&CREATE, &WRITE_S1, &lost]).unwrap();
    assert_eq!(
        (seq(&script, Slot::S0), seq(&script, Slot::S1)),
        (Some(1), Some(2))
    );
}
