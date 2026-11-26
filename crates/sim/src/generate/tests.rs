use super::*;

fn seed(n: u64) -> Seed {
    Seed::new(n)
}

#[test]
fn history_is_prefix_stable() {
    let weights = Weights::default();
    for n in 0..16 {
        let long = history(seed(n), 300, &weights);
        for cut in [0, 1, 117, 299] {
            let short = history(seed(n), cut, &weights);
            assert_eq!(short[..], long[..usize::try_from(cut).unwrap()]);
        }
    }
}

#[test]
fn histories_differ_by_seed_and_repeat_by_seed() {
    let weights = Weights::default();
    assert_eq!(
        history(seed(1), 100, &weights),
        history(seed(1), 100, &weights)
    );
    assert_ne!(
        history(seed(1), 100, &weights),
        history(seed(2), 100, &weights)
    );
}

#[test]
fn the_default_weights_favour_ticks_and_commits_over_faults() {
    let ops = history(seed(7), 5_000, &Weights::default());
    let count = |f: fn(&Op) -> bool| ops.iter().filter(|op| f(op)).count();
    let ticks = count(|op| matches!(op, Op::Tick));
    let commits = count(|op| matches!(op, Op::Commit { .. }));
    let faults = count(|op| matches!(op, Op::StoreFault { .. } | Op::EnvFault(_) | Op::Crash(_)));
    assert!(ticks + commits > ops.len() / 2, "{ticks} {commits}");
    assert!(faults < ops.len() / 8, "{faults}");
}

#[test]
fn a_vacuum_keeps_the_page_size_when_resizes_are_off() {
    let weights = Weights {
        vacuum_resizes: false,
        ..Weights::default()
    };
    let ops = history(seed(3), 20_000, &weights);
    assert!(
        !ops.iter()
            .any(|op| matches!(op, Op::Vacuum { page_size: Some(_) }))
    );
}

#[test]
fn every_kind_of_operation_appears() {
    let ops = history(seed(3), 20_000, &Weights::default());
    let has = |f: fn(&Op) -> bool| ops.iter().any(f);
    assert!(has(|op| matches!(op, Op::CommitInTick { .. })));
    assert!(has(|op| matches!(op, Op::Spill)));
    assert!(has(|op| matches!(op, Op::Rollback)));
    assert!(has(|op| matches!(op, Op::Vacuum { page_size: None })));
    assert!(has(|op| matches!(op, Op::Vacuum { page_size: Some(_) })));
    assert!(has(|op| matches!(
        op,
        Op::OtherProcessReader { begin: true }
    )));
    assert!(has(|op| matches!(
        op,
        Op::OtherProcessReader { begin: false }
    )));
    assert!(has(|op| matches!(
        op,
        Op::OtherProcessCheckpoint(OtherProcessMode::Restart)
    )));
    assert!(has(|op| matches!(op, Op::Crash(CrashKind::Power))));
    assert!(has(|op| matches!(op, Op::Crash(CrashKind::Process))));
    assert!(has(|op| matches!(
        op,
        Op::StoreFault {
            shape: FaultShape::ErrorAfterPublish(_),
            ..
        }
    )));
    assert!(has(|op| matches!(
        op,
        Op::StoreFault {
            shape: FaultShape::SizeMismatch,
            ..
        }
    )));
    assert!(has(|op| matches!(op, Op::EnvFault(EnvFault::DeleteWal))));
    assert!(has(|op| matches!(op, Op::EnvFault(EnvFault::WipeStaging))));
    assert!(has(|op| matches!(op, Op::EnvFault(EnvFault::Source(..)))));
    assert!(has(|op| matches!(op, Op::ClockJump(ms) if *ms < 0)));
    assert!(has(|op| matches!(op, Op::Shutdown)));
    assert!(has(|op| matches!(op, Op::RequestSnapshot)));
    assert!(has(|op| matches!(op, Op::Restore)));
}

#[test]
fn a_fault_shape_only_pairs_with_the_operation_it_means() {
    let mut drawn = std::collections::BTreeSet::new();
    for op in history(seed(9), 20_000, &Weights::default()) {
        if let Op::StoreFault { op, shape, .. } = op {
            drawn.insert(crate::reach::variant(&shape));
            match shape {
                FaultShape::ErrorAfterPublish(_)
                | FaultShape::SizeMismatch
                | FaultShape::ChecksumMismatch
                | FaultShape::CorruptPublish => assert_eq!(op, StoreOp::Upload),
                FaultShape::FlipByteOnRead { .. } => assert_eq!(op, StoreOp::Read),
                FaultShape::Error(_) | FaultShape::Rate { .. } => {}
                // A scenario step: drawing it would move every frozen schedule.
                FaultShape::Scratch(_) => panic!("drawn: {shape:?}"),
            }
        }
    }
    assert_eq!(drawn.len(), 7, "{drawn:?}");
}

#[test]
fn the_scenario_only_steps_are_drawn_too() {
    let ops = history(seed(4), 20_000, &Weights::default());
    let has = |f: fn(&Op) -> bool| ops.iter().any(f);
    assert!(has(|op| matches!(op, Op::Shrink { .. })));
    assert!(has(|op| matches!(op, Op::WalDamage { .. })));
    assert!(has(|op| matches!(op, Op::StagingDamage(_))));
    assert!(has(|op| matches!(op, Op::Heal)));
    assert!(has(|op| matches!(
        op,
        Op::StoreFault {
            op: StoreOp::Delete,
            ..
        }
    )));
}

#[test]
fn holds_replace_drawn_steps_and_leave_every_other_step_as_it_was() {
    let mut replaced = 0;
    for n in 0..50 {
        let mut weights = Weights::mixed(seed(n));
        weights.hold = 50;
        let with = history(seed(n), 300, &weights);
        weights.hold = 0;
        let without = history(seed(n), 300, &weights);
        assert_eq!(with.len(), without.len());
        for (a, b) in with.iter().zip(&without) {
            if a != b {
                assert!(matches!(a, Op::Hold { .. }), "seed {n}: {a:?} for {b:?}");
                replaced += 1;
            }
        }
    }
    assert!(replaced > 0);
}

#[test]
fn the_large_sizes_scale_commits_and_leave_every_draw_in_place() {
    let small = Weights::default();
    let large = Weights::default().with_large_sizes();
    let scale = large.size_scale;
    assert!(scale > 1);
    let mut commits = 0;
    for n in 0..8 {
        let (a, b) = (history(seed(n), 300, &small), history(seed(n), 300, &large));
        assert_eq!(a.len(), b.len());
        for (was, now) in a.iter().zip(&b) {
            match (was, now) {
                (Op::Commit { pages: p, grow: g }, Op::Commit { pages: q, grow: h }) => {
                    commits += 1;
                    assert_eq!(*q, p * scale);
                    assert!(!g || *h, "a commit that grew still grows");
                }
                (Op::CommitInTick { pages: p }, Op::CommitInTick { pages: q }) => {
                    assert_eq!(*q, p * scale);
                }
                (Op::Vacuum { .. }, Op::Vacuum { page_size }) => assert_eq!(*page_size, None),
                (was, now) => assert_eq!(was, now),
            }
        }
    }
    assert!(commits > 0);
}
