use std::collections::BTreeSet;

use super::*;

fn entropy(seed: u64) -> SeededEntropy {
    SeededEntropy::new(seed)
}

#[test]
fn the_operation_and_class_lists_hold_every_variant_once_in_order() {
    for (at, op) in SOURCE_OPS.iter().enumerate() {
        assert_eq!(source_ordinal(*op), at, "{op:?}");
    }
    for (at, op) in STORE_OPS.iter().enumerate() {
        assert_eq!(store_ordinal(*op), at, "{op:?}");
    }
    let classes: Vec<SourceClass> = [
        SourceError::Io(ErrorKind::Other),
        SourceError::DiskFull,
        SourceError::FsyncFailed,
        SourceError::Busy,
        SourceError::ReadersHeld,
        SourceError::Short {
            pgno: PageNo::new(1, PageSize::new(4096).unwrap()).unwrap(),
        },
        SourceError::Unlinked,
        SourceError::ReadTxLost,
    ]
    .into_iter()
    .map(source_class)
    .collect();
    assert_eq!(classes, SOURCE_CLASSES);
    let denials = STORE_CLASSES
        .iter()
        .filter(|class| matches!(class, StoreClass::Denied(_)))
        .count();
    assert_eq!(denials, 5, "one class per denial kind");
    let distinct: BTreeSet<String> = STORE_CLASSES.iter().map(|c| format!("{c:?}")).collect();
    assert_eq!(distinct.len(), STORE_CLASSES.len());
}

/// How many cells of each kind a table holds: drawn (or world), known wrong, unmodelled.
fn tally(cells: impl Iterator<Item = (String, Cell)>) -> [usize; 3] {
    let mut counts = [0_usize; 3];
    for (name, cell) in cells {
        let at = match cell {
            Cell::Drawn | Cell::World(_) => 0,
            Cell::KnownWrong(known) => {
                assert!(known.tracking().starts_with("#99"), "{name}: {known:?}");
                1
            }
            Cell::Unmodelled(why) => {
                assert!(why.len() > 20, "{name}: {why}");
                2
            }
            Cell::Impossible(why) => {
                assert!(why.len() > 20, "{name}: {why}");
                continue;
            }
        };
        if let Some(count) = counts.get_mut(at) {
            *count = count.saturating_add(1);
        }
    }
    counts
}

fn source_cells() -> impl Iterator<Item = (String, Cell)> {
    SOURCE_OPS.into_iter().flat_map(|op| {
        SOURCE_CLASSES
            .into_iter()
            .map(move |class| (format!("{op:?} {class:?}"), source_cell(op, class)))
    })
}

fn store_cells() -> impl Iterator<Item = (String, Cell)> {
    TARGETS.into_iter().flat_map(|target| {
        STORE_OPS.into_iter().flat_map(move |op| {
            STORE_CLASSES.into_iter().map(move |class| {
                (
                    format!("{target:?} {op:?} {class:?}"),
                    store_cell(target, op, class),
                )
            })
        })
    })
}

#[test]
fn every_cell_is_drawn_or_says_why_not() {
    // Drawn, known wrong, unmodelled. A move in these numbers is a change to the domain: say
    // it in the pull request.
    assert_eq!(tally(source_cells()), [22, 1, 3], "source cells");
    assert_eq!(tally(store_cells()), [41, 0, 0], "store cells");
    // Each SQLite call fails at least one way where known failures are counted.
    for op in SOURCE_OPS {
        assert!(
            SOURCE_CLASSES
                .iter()
                .any(|class| drawable(source_cell(op, *class), true)),
            "{op:?} is never failed"
        );
    }
    // Every known defect is reached by some cell.
    for known in KnownWrong::ALL {
        assert!(
            source_cells()
                .chain(store_cells())
                .any(|(_, cell)| cell == Cell::KnownWrong(known)),
            "{known:?} has no cell"
        );
    }
}

#[test]
fn the_source_draw_reaches_every_drawable_cell_and_no_other() {
    for known_wrong in [false, true] {
        let rng = entropy(7);
        let mut seen = BTreeSet::new();
        for _ in 0..20_000 {
            match draw_source(&rng, known_wrong) {
                SourceDraw::Inject(op, err) => {
                    let cell = source_cell(op, source_class(err));
                    assert!(drawable(cell, known_wrong), "{op:?} {err:?}: {cell:?}");
                    seen.insert(format!("{op:?} {:?}", source_class(err)));
                }
                SourceDraw::World(fault) => panic!("no world cell is drawn today: {fault:?}"),
            }
        }
        let want = source_cells()
            .filter(|(_, cell)| drawable(*cell, known_wrong))
            .count();
        assert_eq!(seen.len(), want, "known_wrong {known_wrong}: {seen:?}");
    }
}

#[test]
fn the_store_draw_reaches_every_drawable_class_and_no_other() {
    for known_wrong in [false, true] {
        let rng = entropy(11);
        for target in TARGETS {
            for op in STORE_OPS {
                let mut seen = BTreeSet::new();
                for _ in 0..2_000 {
                    let err = draw_store(&rng, target, op, known_wrong);
                    let cell = store_cell(target, op, store_class(&err));
                    assert!(drawable(cell, known_wrong), "{target:?} {err:?}: {cell:?}");
                    seen.insert(format!("{:?}", store_class(&err)));
                }
                let want = STORE_CLASSES
                    .iter()
                    .filter(|class| drawable(store_cell(target, op, **class), known_wrong))
                    .count();
                assert_eq!(seen.len(), want, "{target:?} {op:?}: {seen:?}");
            }
        }
    }
}

#[test]
fn a_known_defect_matches_only_its_own_failure() {
    let diverged = crate::oracle::bug_fault(crate::oracle::MODEL_DIVERGED);
    let restored = Violation::CorruptionRestored {
        target: lsr_core::RestoreTarget::Latest,
    };
    let staged = Violation::StagedNotDurable {
        key: "0000/1-1.ltx".to_owned(),
    };
    let other = Violation::UnexpectedDegraded {
        cause: lsr_core::PersistentCause::StoreDenied(Denied::Permission),
    };
    for known in [&diverged, &restored] {
        assert_eq!(
            KnownWrong::of(known, &KnownWrong::ALL),
            Some(KnownWrong::DatabaseAheadOfWal)
        );
    }
    let another_bug = crate::oracle::bug_fault("something else");
    for unknown in [&staged, &other, &another_bug] {
        assert_eq!(
            KnownWrong::of(unknown, &KnownWrong::ALL),
            None,
            "{unknown:?}"
        );
    }
    assert_eq!(
        KnownWrong::of(&diverged, &[]),
        None,
        "only defects the run drew"
    );
}

#[test]
fn only_the_errors_the_contract_names_stop_the_engine() {
    assert_eq!(
        source_stop(SourceError::FsyncFailed),
        Some(StopCause::WalSyncFailed)
    );
    assert_eq!(
        source_stop(SourceError::Unlinked),
        Some(StopCause::WalUnlinked)
    );
    assert_eq!(source_stop(SourceError::Busy), None);
    let denied = StoreError::Denied {
        op: StoreOp::List,
        why: Denied::ClockSkew,
    };
    assert_eq!(
        store_stop(Target::Remote, &denied),
        Some(StopCause::StoreDenied(Denied::ClockSkew))
    );
    assert_eq!(store_stop(Target::Staging, &denied), None);
    assert!(refused_before_acting(&denied));
    let reset = StoreError::Transient {
        op: StoreOp::Upload,
        kind: ErrorKind::ConnectionReset,
    };
    assert_eq!(store_stop(Target::Remote, &reset), None);
    assert!(!refused_before_acting(&reset));
}
