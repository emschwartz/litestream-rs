//! One task's retry memory, on its own: what it counts, what a success keeps,
//! and what a level's own wait clears.

use lsr_ltx::{PageNo, PageSize};

use super::*;

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

fn fail(retry: &mut Retry, until: i64) {
    retry.fail(|_| ts(until));
}

/// A failure that only retries after the task degraded still counts in what
/// the degraded state reports.
#[test]
fn a_degraded_count_follows_the_retries_after_it() {
    let mut retry = Retry::default();
    fail(&mut retry, 10);
    retry.degrade(PersistentCause::StoreRefused, ts(1));
    assert_eq!(retry.degraded().map(|d| d.retries), Some(1));
    fail(&mut retry, 20);
    fail(&mut retry, 30);
    assert_eq!(retry.degraded().map(|d| d.retries), Some(3));
}

/// A success ends every degradation but a mismatched database page, which
/// stays reported with its count started again.
#[test]
fn a_success_keeps_only_a_page_mismatch_and_counts_it_from_one() {
    let pgno = PageNo::new(3, PageSize::MIN).unwrap();
    let cases = [
        (PersistentCause::StoreRefused, None),
        (PersistentCause::DatabasePageMismatch { pgno }, Some(1)),
    ];
    for (cause, kept) in cases {
        let mut retry = Retry::default();
        for until in [10, 20, 30] {
            fail(&mut retry, until);
        }
        retry.degrade(cause, ts(1));
        assert_eq!(retry.degraded().map(|d| d.retries), Some(3), "{cause:?}");
        retry.succeed();
        assert_eq!(retry.degraded().map(|d| d.retries), kept, "{cause:?}");
        assert_eq!((retry.failures(), retry.until()), (0, None), "{cause:?}");
    }
}

/// A maintenance job whose level keeps its own wait clears the task's wait
/// and count, so the other levels run, and leaves its degraded state.
#[test]
fn clearing_the_wait_frees_the_task_and_keeps_the_degradation() {
    let mut retry = Retry::default();
    fail(&mut retry, 50);
    retry.degrade(PersistentCause::StoreRefused, ts(1));
    assert!(!retry.ready(ts(10)));
    retry.clear_wait();
    assert!(retry.ready(ts(10)));
    assert_eq!((retry.failures(), retry.until()), (0, None));
    assert_eq!(
        retry.degraded().map(|d| (d.cause, d.since)),
        Some((PersistentCause::StoreRefused, ts(1)))
    );
}
