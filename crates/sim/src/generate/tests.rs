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
fn a_vacuum_changes_the_page_size_only_when_asked() {
    let weights = Weights {
        vacuum_resizes: true,
        ..Weights::default()
    };
    let ops = history(seed(3), 20_000, &weights);
    assert!(
        ops.iter()
            .any(|op| matches!(op, Op::Vacuum { page_size: Some(_) }))
    );
}

#[test]
fn every_kind_of_operation_appears() {
    let ops = history(seed(3), 20_000, &Weights::default());
    let has = |f: fn(&Op) -> bool| ops.iter().any(f);
    assert!(has(|op| matches!(op, Op::Spill)));
    assert!(has(|op| matches!(op, Op::Rollback)));
    assert!(has(|op| matches!(op, Op::Vacuum { page_size: None })));
    assert!(!has(|op| matches!(op, Op::Vacuum { page_size: Some(_) })));
    assert!(has(|op| matches!(op, Op::StrangerReader { begin: true })));
    assert!(has(|op| matches!(op, Op::StrangerReader { begin: false })));
    assert!(has(|op| matches!(
        op,
        Op::StrangerCheckpoint(StrangerMode::Restart)
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
    for op in history(seed(9), 20_000, &Weights::default()) {
        if let Op::StoreFault { op, shape, .. } = op {
            match shape {
                FaultShape::ErrorAfterPublish(_) | FaultShape::SizeMismatch => {
                    assert_eq!(op, StoreOp::Upload);
                }
                FaultShape::Error(_) => {}
                FaultShape::FlipByteOnRead { .. } | FaultShape::Rate { .. } => {
                    panic!("the generator never draws {shape:?}");
                }
            }
        }
    }
}
