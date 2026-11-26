use super::*;

fn counts(requests: u64, read: u64, written: u64) -> RecorderCounts {
    RecorderCounts {
        reads: requests,
        read_bytes: read,
        written_bytes: written,
        ..RecorderCounts::default()
    }
}

const ALLOWANCE: Allowance = Allowance {
    slice: 64 << 10,
    staged: 1 << 20,
    part: None,
};

fn judged(
    remote: &RecorderCounts,
    staging: &RecorderCounts,
    spare: &mut Spare,
) -> Result<(), Violation> {
    judge(
        &Profile::DEFAULT,
        ALLOWANCE,
        (remote, 1),
        (staging, 1),
        spare,
    )
}

fn over(result: Result<(), Violation>, bound: Bound) -> bool {
    matches!(result, Err(Violation::ResourceBound { what, .. }) if what == bound)
}

#[test]
fn a_link_prices_requests_and_bytes() {
    let link = Profile::DEFAULT.remote;
    assert_eq!(link.micros(3, 0), 30_000);
    assert_eq!(link.micros(0, 1 << 20), 1_000_000);
    assert_eq!(link.spent(&counts(2, 1 << 19, 1 << 19)), 1_020_000);
}

/// The checker can fail: a slice each way passes, a job whose inputs are read whole in one
/// tick does not, though it writes only a slice.
#[test]
fn a_tick_within_its_slices_passes_and_a_whole_read_does_not() {
    let quiet = counts(0, 0, 0);
    let slice = counts(4, 64 << 10, 64 << 10);
    let mut spare = Spare::default();
    judged(&slice, &quiet, &mut spare).unwrap();
    let whole = counts(4, 10 << 20, 64 << 10);
    assert!(over(
        judged(&whole, &quiet, &mut spare),
        Bound::RemoteTickMicros
    ));
}

/// The checker can fail: a tick whose staging reads pass their share fails, though the share
/// a sync's writes are allowed would have covered them.
#[test]
fn reads_cannot_hide_in_the_share_of_writes() {
    let quiet = counts(0, 0, 0);
    let reads = counts(4, ALLOWANCE.read() + (64 << 10), 0);
    assert!(over(
        judged(&quiet, &reads, &mut Spare::default()),
        Bound::StagingTickMicros
    ));
    let writes = counts(4, 0, ALLOWANCE.staged);
    judged(&quiet, &writes, &mut Spare::default()).unwrap();
}

#[test]
fn a_tick_of_many_requests_fails_as_one_of_many_bytes_does() {
    let quiet = counts(0, 0, 0);
    let chatty = counts(BASE_REQUESTS + 200, 0, 0);
    assert!(over(
        judged(&chatty, &quiet, &mut Spare::default()),
        Bound::RemoteTickMicros
    ));
}

#[test]
fn a_boot_reads_its_newest_files_whole_once() {
    let quiet = counts(0, 0, 0);
    let read = counts(10, 8 << 20, 0);
    let mut spare = Spare::boot(&Profile::DEFAULT, 4 << 20, (0, 0));
    judged(&read, &quiet, &mut spare).unwrap();
    assert!(judged(&read, &quiet, &mut spare).is_err());
}

#[test]
fn a_boot_with_nothing_staged_may_restore_the_replica_once() {
    let quiet = counts(0, 0, 0);
    let restore = counts(BASE_REQUESTS + 200, 1 << 20, 0);
    let mut spare = Spare::default();
    spare.restore(&Profile::DEFAULT.remote, 100, 1 << 20);
    judged(&restore, &quiet, &mut spare).unwrap();
    assert!(judged(&restore, &quiet, &mut spare).is_err());
}

#[test]
fn staging_is_judged_on_its_own_bound_and_spare() {
    let quiet = counts(0, 0, 0);
    let staged = counts(4, 0, 64 << 20);
    assert!(over(
        judged(&quiet, &staged, &mut Spare::default()),
        Bound::StagingTickMicros
    ));
    let read = counts(4, 32 << 20, 0);
    let mut spare = Spare::boot(&Profile::DEFAULT, 0, (1, 16 << 20));
    judged(&quiet, &read, &mut spare).unwrap();
    assert!(over(
        judged(&quiet, &read, &mut spare),
        Bound::StagingTickMicros
    ));
}

/// A replica sending in parts reads each part from staging once more: a request a part.
#[test]
fn each_part_sent_may_read_staging_once_more() {
    let parted = Allowance {
        part: Some(64),
        ..ALLOWANCE
    };
    let sent = counts(0, 0, 64 * 300);
    let reread = counts(600, 64 * 300, 0);
    let judged = |allowance| {
        judge(
            &Profile::DEFAULT,
            allowance,
            (&sent, 0),
            (&reread, 0),
            &mut Spare::default(),
        )
    };
    judged(parted).unwrap();
    assert!(over(judged(ALLOWANCE), Bound::StagingTickMicros));
}
