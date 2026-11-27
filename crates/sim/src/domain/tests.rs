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
        SourceError::WritesHeld,
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

/// How many cells of each kind a table holds: drawn (or world), unmodelled.
fn tally(cells: impl Iterator<Item = (String, Cell)>) -> [usize; 2] {
    let mut counts = [0_usize; 2];
    for (name, cell) in cells {
        let at = match cell {
            Cell::Drawn | Cell::World(_) => 0,
            Cell::Unmodelled(why) => {
                assert!(why.len() > 20, "{name}: {why}");
                1
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
    // Drawn, unmodelled. A move in these numbers is a change to the domain: say
    // it in the pull request.
    assert_eq!(tally(source_cells()), [20, 4], "source cells");
    assert_eq!(tally(store_cells()), [41, 0], "store cells");
    // Each SQLite call fails at least one way, but the sync of
    // the WAL: a failed sync is modelled by its scenarios, not drawn (`docs/decisions/D173.md`);
    // and the write lock's calls, which the engine never makes.
    let never_called = [
        SourceOp::Sync,
        SourceOp::BeginWriteLock,
        SourceOp::EndWriteLock,
    ];
    for op in SOURCE_OPS
        .into_iter()
        .filter(|op| !never_called.contains(op))
    {
        assert!(
            SOURCE_CLASSES
                .iter()
                .any(|class| drawable(source_cell(op, *class))),
            "{op:?} is never failed"
        );
    }
}

#[test]
fn the_source_draw_reaches_every_drawable_cell_and_no_other() {
    let rng = entropy(7);
    let mut seen = BTreeSet::new();
    for _ in 0..20_000 {
        match draw_source(&rng) {
            SourceDraw::Inject(op, err) => {
                let cell = source_cell(op, source_class(err));
                assert!(drawable(cell), "{op:?} {err:?}: {cell:?}");
                seen.insert(format!("{op:?} {:?}", source_class(err)));
            }
            SourceDraw::World(fault) => panic!("no world cell is drawn today: {fault:?}"),
        }
    }
    let want = source_cells().filter(|(_, cell)| drawable(*cell)).count();
    assert_eq!(seen.len(), want, "{seen:?}");
}

#[test]
fn the_store_draw_reaches_every_drawable_class_and_no_other() {
    let rng = entropy(11);
    for target in TARGETS {
        for op in STORE_OPS {
            let mut seen = BTreeSet::new();
            for _ in 0..2_000 {
                let err = draw_store(&rng, target, op);
                let cell = store_cell(target, op, store_class(&err));
                assert!(drawable(cell), "{target:?} {err:?}: {cell:?}");
                seen.insert(format!("{:?}", store_class(&err)));
            }
            let want = STORE_CLASSES
                .iter()
                .filter(|class| drawable(store_cell(target, op, **class)))
                .count();
            assert_eq!(seen.len(), want, "{target:?} {op:?}: {seen:?}");
        }
    }
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
