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

/// The checker can fail: a boot's spare covers staging alone, so a boot that reads a whole
/// replica file in one tick fails as any other tick would, whatever it found staged.
#[test]
fn a_boot_that_reads_a_replica_file_whole_fails() {
    let quiet = counts(0, 0, 0);
    let read = counts(10, 8 << 20, 0);
    let mut spare = Spare::boot(&Profile::DEFAULT, (100, 64 << 20));
    assert!(over(
        judged(&read, &quiet, &mut spare),
        Bound::RemoteTickMicros
    ));
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
    let mut spare = Spare::boot(&Profile::DEFAULT, (1, 16 << 20));
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

/// A tick that uploads many small files reads each one from staging: a request a file the
/// replica was given, and no more.
#[test]
fn staging_may_read_each_file_the_replica_was_given_and_no_more() {
    let quiet = counts(0, 0, 0);
    let judged = |files, reads| {
        judge(
            &Profile::DEFAULT,
            ALLOWANCE,
            (&quiet, files),
            (&counts(reads, 0, 0), 0),
            &mut Spare::default(),
        )
    };
    judged(133, BASE_REQUESTS + 133).unwrap();
    assert!(over(
        judged(133, BASE_REQUESTS + 133 + 5),
        Bound::StagingTickMicros
    ));
    assert!(over(judged(0, BASE_REQUESTS + 5), Bound::StagingTickMicros));
}

/// The S3 re-read defect allowance is a staging request for each re-read the S3 store was
/// given, and no more.
#[test]
fn staging_may_make_a_request_for_each_s3_reread_and_no_more() {
    let reads = counts(BASE_REQUESTS + 4 + 300, 0, 0);
    let rereads = |n| RecorderCounts {
        rereads_given: n,
        ..RecorderCounts::default()
    };
    judged(&rereads(300), &reads, &mut Spare::default()).unwrap();
    assert!(over(
        judged(&rereads(200), &reads, &mut Spare::default()),
        Bound::StagingTickMicros
    ));
}

/// The tail's shape, over every draw: the chance a request takes at least `k` times its
/// latency is `k^-1.5` (within the draws' resolution), it never takes less than the latency,
/// and never more than the cap, which it reaches.
#[test]
fn the_latency_tail_is_pareto_of_shape_one_and_a_half_cut_at_a_thousand() {
    let mut at_least = [0_u64; 4];
    let marks = [2_u64, 10, 100, 1000];
    let mut most = 0;
    for draw in 0..TAIL_DRAWS {
        let factor = tail_factor(draw);
        assert!((1..=TAIL_MAX).contains(&factor), "{draw}: {factor}");
        most = most.max(factor);
        for (slot, mark) in at_least.iter_mut().zip(marks) {
            *slot += u64::from(factor >= mark);
        }
    }
    assert_eq!(most, TAIL_MAX);
    // count / N against k^-1.5, squared to stay in integers: count² · k³ against N², within 4
    // percent (2 percent before squaring).
    let all = u128::from(TAIL_DRAWS) * u128::from(TAIL_DRAWS);
    for (count, mark) in at_least.into_iter().zip(marks) {
        let seen = u128::from(count) * u128::from(count) * u128::from(mark).pow(3);
        assert!(
            seen * 100 >= all * 96 && seen * 100 <= all * 104,
            "P(>= {mark}): {count} of {TAIL_DRAWS}"
        );
    }
    assert!(tail_factor(TAIL_DRAWS - 1) == 1 && tail_factor(u64::MAX) == 1);
}

/// A slowed store costs the factor times as much for the same calls; a free one costs nothing.
#[test]
fn a_slowed_link_costs_its_factor_more() {
    let link = Profile::DEFAULT.staging;
    let slow = link.slowed(100);
    let calls = counts(10, 1 << 20, 1 << 20);
    let ratio = slow.spent(&calls) / link.spent(&calls);
    assert!((99..=100).contains(&ratio), "{ratio}");
    assert_eq!(Link::FREE.spent(&calls), 0);
}
