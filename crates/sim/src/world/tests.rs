use lsr_core::{ObjectStore, PageLimit, StoreOp};
use lsr_ltx::PageNo;

use super::*;
use crate::store::FaultShape;
use crate::testkit::{PAGE, pgno, ps};

fn world() -> World {
    World::new(Seed::new(0x5eed), ps(), 4)
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE).unwrap()]
}

fn commit(w: &mut World, fill: u8) {
    let pages: Vec<(PageNo, Vec<u8>)> = vec![(pgno(1), page(fill)), (pgno(2), page(fill))];
    assert!(w.app_commit(&pages, 4).is_some());
}

#[test]
fn the_model_tracks_what_the_rules_hold() {
    let mut w = world();
    assert!(w.model_matches_sqlite());
    commit(&mut w, 1);
    commit(&mut w, 2);
    assert!(w.model_matches_sqlite());
    assert!(w.app_shrink(2).is_some());
    assert!(w.model_matches_sqlite());
    // Growing again, the app writes every page it adds.
    let grown: Vec<(PageNo, Vec<u8>)> = (1..=4).map(|n| (pgno(n), page(3))).collect();
    assert!(w.app_commit(&grown, 4).is_some());
    assert!(w.model_matches_sqlite());
}

#[test]
fn a_page_that_comes_back_unwritten_is_what_the_rules_hold() {
    let mut w = world();
    commit(&mut w, 1);
    assert!(w.app_shrink(2).is_some());
    // Growing back without writing pages 3 and 4: the model asks the rules.
    let page_one: Vec<(PageNo, Vec<u8>)> = vec![(pgno(1), page(9))];
    assert!(w.app_commit(&page_one, 4).is_some());
    assert!(w.model_matches_sqlite());
}

#[test]
fn the_engines_own_bump_commit_joins_the_model() {
    let mut w = world();
    commit(&mut w, 1);
    let before = w.model.commits().len();
    // What a checkpoint's bump does: a one-page commit by the pool connection.
    let mut ck = w.sqlite.checkpointer();
    lsr_core::Checkpointer::bump_seq(&mut ck).unwrap();
    assert_eq!(w.model.commits().len(), before);
    w.sync_model_at(0);
    assert_eq!(w.model.commits().len(), before + 1);
    w.sync_model_at(0);
    assert_eq!(w.model.commits().len(), before + 1, "once only");
    assert!(w.model_matches_sqlite());
}

#[test]
fn a_vacuum_to_a_new_page_size_restarts_the_model_on_the_new_image() {
    let mut w = world();
    commit(&mut w, 1);
    assert!(w.app_vacuum(ps()).is_none(), "the same size is no vacuum");
    let big = lsr_ltx::PageSize::new(1024).unwrap();
    assert!(w.app_vacuum(big).is_some());
    assert!(w.model_matches_sqlite());
    assert_eq!(w.model.tip_image().page_size(), big);
}

#[test]
fn a_power_loss_leaves_the_model_where_the_rules_are() {
    let mut w = world();
    commit(&mut w, 1);
    assert!(w.stranger_checkpoint(StrangerMode::Passive));
    commit(&mut w, 2);
    commit(&mut w, 3);
    w.crash(CrashKind::Power);
    assert!(
        w.model_matches_sqlite(),
        "model and rules agree after the loss"
    );
    assert!(w.model.rewound());
    assert_eq!(w.acked.highest(), None);
}

#[test]
fn a_process_crash_changes_neither_the_model_nor_the_acks() {
    let mut w = world();
    commit(&mut w, 1);
    w.acked.record(crate::testkit::txid(1));
    w.crash(CrashKind::Process);
    assert!(!w.model.rewound());
    assert!(w.model_matches_sqlite());
    assert!(w.acked.highest().is_some());
}

#[test]
fn run_tick_crashes_a_store_call_at_the_planned_pending() {
    let mut w = world();
    let remote = w.remote.clone();
    let key = crate::testkit::fkey(0, 1, 1);
    let bytes = crate::testkit::ltx(1, 1, 1, 1, None);
    let plan = Some(CrashPlan {
        at_poll: Polls::new(2),
        kind: CrashKind::Process,
    });
    let end = w.run_tick(
        async {
            use lsr_core::Upload;
            let mut up = remote
                .begin_upload(&key, lsr_ltx::Timestamp::from_unix_millis(1))
                .await?;
            up.write(&bytes).await?;
            up.commit().await
        },
        plan,
    );
    assert_eq!(
        end,
        TickEnd::Crashed {
            polls: 2,
            kind: CrashKind::Process,
            lost: Vec::new()
        }
    );
    let listed = block_on(w.remote_raw.list(
        lsr_core::Level::new(0).unwrap(),
        None,
        PageLimit::MAX,
    ))
    .unwrap();
    assert!(listed.files.is_empty());
    // Faults counts a call before the crash point inside it: begin, write, commit.
    assert_eq!(w.remote_faults.calls(StoreOp::Upload), 3);
}

#[test]
fn a_power_loss_tick_drops_unsynced_staged_files() {
    let mut w = world();
    let staging = w.staging.clone();
    w.staging_faults.push_in(
        StoreOp::Upload,
        0,
        FaultShape::ErrorAfterPublish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: std::io::ErrorKind::Other,
        }),
    );
    let key = crate::testkit::fkey(0, 1, 1);
    let bytes = crate::testkit::ltx(1, 1, 1, 1, None);
    let end = w.run_tick(
        async {
            use lsr_core::Upload;
            let mut up = staging
                .begin_upload(&key, lsr_ltx::Timestamp::from_unix_millis(1))
                .await?;
            up.write(&bytes).await?;
            up.commit().await
        },
        None,
    );
    assert!(matches!(end, TickEnd::Done(Err(_))));
    assert_eq!(w.stage.volatile(), vec![key]);
    let lost = w.crash(CrashKind::Power);
    assert_eq!(lost.len(), 1);
    assert!(w.stage.volatile().is_empty());
}

#[test]
fn wipe_staging_removes_every_file() {
    let w = world();
    crate::testkit::put(
        &w.staging_raw,
        0,
        1,
        1,
        &crate::testkit::ltx(1, 1, 1, 1, None),
    );
    crate::testkit::put(
        &w.staging_raw,
        9,
        1,
        3,
        &crate::testkit::ltx(1, 3, 1, 1, None),
    );
    w.wipe_staging().unwrap();
    for level in lsr_core::Level::ALL {
        let listed = block_on(w.staging_raw.list(level, None, PageLimit::MAX)).unwrap();
        assert!(listed.files.is_empty());
    }
}

#[test]
fn pick_crash_is_seeded_and_in_range() {
    let draw = |seed| {
        let e = SeededEntropy::new(seed);
        (0..200).map(|_| pick_crash(&e, 7)).collect::<Vec<_>>()
    };
    assert_eq!(draw(3), draw(3));
    let plans = draw(3);
    assert!(plans.iter().all(|p| p.at_poll.get() < 7));
    let power = plans.iter().filter(|p| p.kind == CrashKind::Power).count();
    assert!(
        (20..=80).contains(&power),
        "{power} of 200 are power losses"
    );
}
