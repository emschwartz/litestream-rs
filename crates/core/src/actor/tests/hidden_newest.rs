//! A replica whose listings keep missing its newest file: a listing that hides the file
//! written last (a store whose listings lag, unsupported, docs/decisions/D173.md), and a job
//! that deletes each new file as it lands (supported, docs/BEHAVIOR.md row UP-11). Each round
//! ends below what the engine confirmed, so it is a regression every time. The engine must
//! say so on its health, and wait longer each round up to the cap instead of sending the file
//! again with no wait (issue #540); restores stay exact.

use super::*;

/// How often the application commits, and for how long the fault holds.
const STEP_MS: i64 = 500;
const FAULT_MS: i64 = 120_000;

/// Commits every [`STEP_MS`] from `from` until `until`, ticking the actor as its wake times
/// ask between commits; returns the time reached and the byte last committed. Every sample
/// from `watch_from` on records the upload task's degraded cause.
fn run(
    rig: &mut Rig,
    actor: &mut Actor,
    (from, until): (i64, i64),
    mut byte: u8,
) -> (u8, Vec<Option<DegradedCause>>) {
    let mut now = from;
    let mut causes = Vec::new();
    while now < until {
        byte = (byte % 250).saturating_add(1);
        rig.commit(byte);
        let end = now.saturating_add(STEP_MS);
        for _ in 0..1000 {
            let tick = rig.tick(actor, now);
            match next_of(&tick) {
                Some(Next::Now) => {}
                Some(Next::At(at)) if at.unix_millis() < end => now = now.max(at.unix_millis()),
                _ => break,
            }
        }
        now = end;
        causes.push(actor.degraded(Task::Upload).map(|d| d.cause));
    }
    (byte, causes)
}

fn lists_and_puts(rig: &Rig, from: usize) -> (usize, usize) {
    let tally = rig.remote.requests_from(from);
    let of = |kind: Request| -> usize {
        tally
            .iter()
            .filter(|((request, _), _)| *request == kind)
            .map(|(_, n)| *n)
            .sum()
    };
    (of(Request::List), of(Request::Put))
}

/// Runs two minutes of the fault against a caught-up engine, then the longest wait and a
/// little more without it;
/// returns the requests made in the fault's second minute and the causes seen.
fn under_fault(
    fault: &Arc<AtomicBool>,
    rig: &mut Rig,
) -> ((usize, usize), Vec<Option<DegradedCause>>) {
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.remote.request_cap.store(5_000, Ordering::SeqCst);
    fault.store(true, Ordering::SeqCst);
    let half = T0 + FAULT_MS / 2;
    let (byte, mut causes) = run(rig, &mut actor, (T0, half), 1);
    let from = rig.remote.requests_made();
    let (byte, late) = run(rig, &mut actor, (half, T0 + FAULT_MS), byte);
    let late_cost = lists_and_puts(rig, from);
    causes.extend(late);
    fault.store(false, Ordering::SeqCst);
    // The wait after the last regression is at most the cap; the listing after it is whole.
    let healed = i64::try_from(crate::fault::MAX_BACKOFF.as_millis())
        .unwrap()
        .saturating_add(T0 + FAULT_MS + 30_000);
    let (byte, _) = run(rig, &mut actor, (T0 + FAULT_MS, healed), byte);
    assert_eq!(
        actor.degraded(Task::Upload),
        None,
        "a whole listing ends the regression"
    );
    assert_eq!(
        restored(rig),
        byte,
        "the replica catches up and restores exactly"
    );
    (late_cost, causes)
}

/// The defect: a listing that left out the newest file had the engine take its own upload as
/// lost and send it again, with no wait, round after round, while `/healthz` showed nothing
/// degraded. Once a staged file was replaced by a snapshot under the hidden id, the refusal,
/// the read-back and the relist repeated inside one tick. Now each round is a regression, the
/// upload task stays degraded by it until a listing shows the replica whole, and its wait
/// doubles: in the fault's second minute the engine lists the replica and sends a file only a
/// handful of times.
#[test]
fn a_listing_that_hides_the_newest_file_degrades_and_backs_off() {
    let mut rig = Rig::new();
    let fault = Arc::clone(&rig.remote.hides_newest);
    let ((lists, puts), causes) = under_fault(&fault, &mut rig);
    assert_backed_off(lists, puts, &causes);
}

/// The supported case beside it: a job that deletes each new file as it lands. Each
/// maintenance listing ends below what the engine confirmed and the file is sent again, which
/// is the same loop, and the same wait and signal bound it.
#[test]
fn a_job_that_keeps_deleting_the_newest_file_degrades_and_backs_off() {
    let mut rig = Rig::new();
    let fault = Arc::clone(&rig.remote.drops_newest);
    let ((lists, puts), causes) = under_fault(&fault, &mut rig);
    assert_backed_off(lists, puts, &causes);
}

fn assert_backed_off(lists: usize, puts: usize, causes: &[Option<DegradedCause>]) {
    // A listing is one request a level, ten levels at most: the uploader's relist and
    // maintenance's listing each round, and maintenance's short pass about once a second.
    assert!(lists <= 600, "{lists} listing requests in a minute");
    assert!(puts <= 12, "{puts} uploads in a minute");
    // From the first regression on, the health handle says so at every sample.
    let first = causes
        .iter()
        .position(Option::is_some)
        .expect("never degraded");
    assert!(
        causes[first..]
            .iter()
            .all(|cause| *cause == Some(DegradedCause::RemoteRegressed)),
        "{causes:?}"
    );
}
