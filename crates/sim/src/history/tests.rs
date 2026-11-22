use lsr_core::MemStore;
use lsr_ltx::{PageNo, Timestamp};

use super::*;
use crate::sqlite::AppCommit;
use crate::testkit::{PAGE, SALTS, pgno, ps, put, wal_end};

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE).unwrap()]
}

const NOW: Timestamp = Timestamp::from_unix_millis(1_700_000_000_000);

/// A model with one commit of page 1 filled 10, then the database as damage left it (page 1
/// filled 99) adopted on top, and a replica whose snapshot holds the damaged page.
fn damaged() -> (Model, History, MemStore) {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    let mut history = History::new(&model, NOW, 1, AheadMode::Count);
    let pages: Vec<(PageNo, Vec<u8>)> = vec![(pgno(1), page(10))];
    let app = |frames| AppCommit {
        salts: SALTS,
        end: wal_end(frames),
    };
    model.commit(&pages, 1, app(1), 0);
    history.log_commits(&model, NOW);
    model.vacuum(PageImage::from_bytes(ps(), &page(99)), app(2), 0);
    history.log_commits(&model, NOW);
    let remote = MemStore::new();
    put(&remote, 0, 1, 1, &crate::testkit::ltx(1, 1, 1, 99, None));
    (model, history, remote)
}

/// The model must follow damage to go on, but a restore that gives the damaged state is no
/// node of the history: the world adopted it, no commit made it.
#[test]
fn a_restore_of_a_state_adopted_from_damage_fails() {
    let (model, mut history, remote) = damaged();
    // Adopted as plain truth, the damaged state passes as a node: the hole.
    history.judge(&remote, NOW, &[], Ask::default()).unwrap();
    history.adopted_corruption(&model);
    let got = history
        .judge(&remote, NOW, &[], Ask::default())
        .unwrap_err();
    assert!(
        matches!(
            got,
            Violation::CorruptionRestored {
                target: RestoreTarget::Latest
            }
        ),
        "{got:?}"
    );
}
