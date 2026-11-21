//! The retention switch (Go's `retention.enabled`): off, the planner deletes
//! nothing while compaction and snapshots run as usual; back on, deletes
//! resume under the usual rules; the kept restore point behaves as before either way.

use super::*;
use crate::maintain::tests::{due_state, lazy_due};
use crate::maintain::{RetentionMode, SnapshotJob};

fn off(config: Maintenance) -> Maintenance {
    config.with_retention(RetentionMode::Off)
}

fn due_on(files: Vec<FileInfo>, config: &Maintenance, state: &mut MaintainState) -> Due {
    let now = newest_created_at(&files);
    due_state(&inv(files), at(now), config, None, state)
}

fn deleted(due: &Due) -> Vec<FileKey> {
    match due {
        Due::Run(Job::Delete(job)) => job.keys().to_vec(),
        other @ (Due::Run(_) | Due::Wait { .. }) => panic!("not a delete: {other:?}"),
    }
}

const QUIET: Due = Due::Wait {
    until: None,
    held: None,
};

/// Go enforces retention unless told otherwise (`store.go:168`, `db.go:353`,
/// `compactor.go:64`), and so does every way to build a schedule here.
#[test]
fn retention_is_enforced_by_default_as_in_go() {
    assert_eq!(RetentionMode::default(), RetentionMode::Enforce);
    assert_eq!(defaults().retention(), RetentionMode::Enforce);
    assert_eq!(one_level().retention(), RetentionMode::Enforce);
    assert_eq!(small().retention(), RetentionMode::Enforce);
    let mut switched = off(defaults());
    assert_eq!(switched.retention(), RetentionMode::Off);
    assert_ne!(switched, defaults());
    switched.set_retention(RetentionMode::Enforce);
    assert_eq!(switched, defaults());
}

/// The ladder whose old snapshots, the level 1 file under their floor and
/// covered level 0 files would all go: off, not one of them is named, and
/// no hold is reported, since nothing is being refused.
#[test]
fn with_retention_off_no_snapshot_compacted_or_level_zero_file_is_deleted() {
    let enforced = due_on(ladder(), &one_level(), &mut MaintainState::default());
    assert_eq!(deleted(&enforced), expected_ladder_deletes());
    let got = due_on(ladder(), &off(one_level()), &mut MaintainState::default());
    assert_eq!(got, QUIET);
}

/// Off, retention reads no created-at time: on a listing that carries none, the
/// ladder costs no created-at time request at all, where enforcing it reads several
/// (Go skips only the delete call; reading created-at times for deletes that never
/// happen buys nothing).
#[test]
fn with_retention_off_no_created_at_is_read_for_retention() {
    let files = ladder();
    let now = newest_created_at(&files);
    let (enforced, asked) = lazy_due(&files, now, &one_level(), None);
    assert_eq!(deleted(&enforced), expected_ladder_deletes());
    assert!(!asked.is_empty());
    let (got, asked) = lazy_due(&files, now, &off(one_level()), None);
    assert_eq!(got, QUIET);
    assert_eq!(asked, Vec::<FileKey>::new());
}

/// A kept restore point behaves the same whether retention was always on or was switched
/// off and back on: the pinned chain stays and the rest goes. Off, the kept restore point
/// changes nothing, and even a kept restore point that does not plan holds nothing.
#[test]
fn a_degraded_tasks_kept_restore_point_behaves_the_same_once_retention_is_back_on() {
    for restore_point in [10, 20, 27] {
        let always = due_on(ladder(), &one_level(), &mut pinned(restore_point));
        let mut config = off(one_level());
        let while_off = due_on(ladder(), &config, &mut pinned(restore_point));
        assert_eq!(while_off, QUIET, "kept restore point {restore_point}");
        config.set_retention(RetentionMode::Enforce);
        let back_on = due_on(ladder(), &config, &mut pinned(restore_point));
        assert_eq!(back_on, always, "kept restore point {restore_point}");
    }
    let kept = key(9, 1, 10);
    let want: Vec<FileKey> = expected_ladder_deletes()
        .into_iter()
        .filter(|k| *k != kept)
        .collect();
    assert_eq!(
        deleted(&due_on(ladder(), &one_level(), &mut pinned(10))),
        want
    );
    let held = due_on(ladder(), &one_level(), &mut pinned(27));
    assert!(
        matches!(
            &held,
            Due::Wait {
                held: Some(Hold::RestorePointUnplannable { .. }),
                ..
            }
        ),
        "{held:?}"
    );
}

// A long run.

/// Levels of 10 s, 1 min and 10 min, hourly snapshots kept an hour, level 0
/// kept a minute once covered: hours of writes exercise every rule many times.
fn busy() -> Maintenance {
    Maintenance::new(
        vec![secs(10), secs(60), secs(600)],
        secs(3600),
        secs(3600),
        L0Retention::After(secs(60)),
    )
    .unwrap()
}

/// What a run's jobs were.
#[derive(Default, Debug)]
struct Tally {
    compacted: [usize; 3],
    snapshots: usize,
    deleted: usize,
}

/// Runs `due` until it waits, applying each job to the model. The newest
/// state plans after every job, as the simulator's oracle asks.
fn settle(model: &mut Model, now: i64, cfg: &Maintenance, tally: &mut Tally) {
    for _ in 0..5000 {
        let before = model.inv();
        let job = match due(&before, at(now), cfg, None) {
            Due::Run(job) => job,
            Due::Wait { held, .. } => {
                assert_eq!(held, None, "a hold at {now}");
                return;
            }
        };
        match &job {
            Job::Compact(c) => tally.compacted[usize::from(c.level().get()) - 1] += 1,
            Job::Snapshot(SnapshotJob { .. }) => tally.snapshots += 1,
            Job::Delete(d) => tally.deleted += d.keys().len(),
        }
        model.apply(&job, now);
        let newest = before.newest().unwrap();
        let latest = plan(&model.inv(), KeyTarget::Latest);
        assert!(
            matches!(&latest, PlanOutcome::Plan(c) if c.reaches() == newest),
            "after {job:?}: {latest:?}"
        );
    }
    panic!("due never settled at {now}");
}

/// One transaction a minute for `hours`, settling after each, starting
/// from the instant and the transaction in `from`.
fn write_for(
    model: &mut Model,
    cfg: &Maintenance,
    from: (i64, u64),
    hours: i64,
) -> (i64, u64, Tally) {
    let mut tally = Tally::default();
    let (mut now, mut next) = from;
    let end = now + hours * HOUR;
    while now < end {
        now += 60_000;
        model.files.push(file(0, next, next, now));
        next += 1;
        settle(model, now, cfg, &mut tally);
    }
    (now, next, tally)
}

fn every_transaction_plans(model: &Model, through: u64) {
    let inventory = model.inv();
    for n in 1..=through {
        let got = plan(&inventory, KeyTarget::Txid(txid(n)));
        assert!(
            matches!(&got, PlanOutcome::Plan(c) if c.reaches() == txid(n)),
            "transaction {n}: {got:?}"
        );
    }
}

fn sorted(model: &Model) -> Vec<FileKey> {
    let mut keys: Vec<FileKey> = model.files.iter().map(|f| f.key).collect();
    keys.sort();
    keys
}

/// Six hours with retention off: every level compacts, a snapshot lands
/// each hour, not one file is deleted, the newest state plans after every
/// job, and every transaction ever written still restores. The same history
/// with retention on deletes, and compacts and snapshots exactly as often.
#[test]
fn six_hours_with_retention_off_compact_and_snapshot_and_delete_nothing() {
    let mut model = Model::default();
    let (_, next, tally) = write_for(&mut model, &off(busy()), (T0, 1), 6);
    assert_eq!(tally.deleted, 0, "{tally:?}");
    assert!(tally.compacted.iter().all(|n| *n > 0), "{tally:?}");
    assert!(tally.snapshots >= 6, "{tally:?}");
    let written = next - 1;
    let level_zero = model.inv().level(Level::ZERO).len();
    assert_eq!(level_zero, usize::try_from(written).unwrap());
    every_transaction_plans(&model, written);

    let mut enforced = Model::default();
    let (_, _, on) = write_for(&mut enforced, &busy(), (T0, 1), 6);
    assert!(on.deleted > 0, "{on:?}");
    assert_eq!(on.compacted, tally.compacted);
    assert_eq!(on.snapshots, tally.snapshots);
}

/// Off for three hours, then on: deletes resume at the next listing, and
/// once settled the replica holds exactly the files it would hold had
/// retention never been off. Writing on, the two stay the same.
#[test]
fn switching_retention_back_on_resumes_deletes_under_the_usual_rules() {
    let mut cfg = off(busy());
    let mut model = Model::default();
    let (now, next, _) = write_for(&mut model, &cfg, (T0, 1), 3);
    let mut always = Model::default();
    let (same_now, same_next, _) = write_for(&mut always, &busy(), (T0, 1), 3);
    assert_eq!((now, next), (same_now, same_next));
    assert!(model.files.len() > always.files.len());

    cfg.set_retention(RetentionMode::Enforce);
    let mut tally = Tally::default();
    settle(&mut model, now, &cfg, &mut tally);
    assert!(tally.deleted > 0, "{tally:?}");
    assert_eq!(sorted(&model), sorted(&always));

    let (_, _, later) = write_for(&mut model, &cfg, (now, next), 2);
    let (_, _, later_always) = write_for(&mut always, &busy(), (now, next), 2);
    assert!(later.deleted > 0);
    assert_eq!(later.deleted, later_always.deleted);
    assert_eq!(sorted(&model), sorted(&always));
}
