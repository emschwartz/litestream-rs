//! What a finished level-0 delete job teaches the maintenance state: a pass that was not cut at
//! the delete limit leaves nothing more to plan until the next file ages, so the next listing
//! waits; a pass that was cut leaves more to delete, so the next one lists at once.

use super::*;
use crate::maintain::{DeleteJob, Job, MAX_DELETE};

/// An actor whose next maintenance tick is due to list, after a delete job of `count` files
/// with `wake` as the time the short pass said to ask again; returns whether it still lists
/// at once and when it waits until.
fn after_delete(count: usize, wake: i64) -> (bool, Option<Timestamp>) {
    let mut actor = Actor::new(go_like());
    actor.engine.maintain.stale = true;
    actor.engine.maintain.until = None;
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(1).unwrap()));
    let job = Job::Delete(DeleteJob::new(vec![key]).unwrap());
    let event = Event::Maintained {
        event: MaintainedEvent::Deleted(count),
        created_at_read: 0,
    };
    let tick = actor.ran(event, ts(T0));
    actor.after_job(&job, &tick, Some(Some(ts(wake))), ts(T0));
    (actor.engine.maintain.stale, actor.engine.maintain.until)
}

/// A delete that took fewer files than the limit finished the level: the listing is no longer
/// stale and the next one waits for `wake`.
#[test]
fn a_delete_under_the_limit_waits_for_the_next_file_to_age() {
    for count in [1, MAX_DELETE - 1] {
        assert_eq!(
            after_delete(count, T0 + 5_000),
            (false, Some(ts(T0 + 5_000))),
            "{count} files"
        );
    }
}

/// A delete cut at the limit may have left more: the listing stays stale and no wait is set.
#[test]
fn a_delete_cut_at_the_limit_lists_again_at_once() {
    assert_eq!(after_delete(MAX_DELETE, T0 + 5_000), (true, None));
}
