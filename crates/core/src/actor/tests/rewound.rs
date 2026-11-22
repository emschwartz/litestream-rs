//! A snapshot of a database that rewound below the replica is a new base.

use super::*;

/// Commits 2 and 3 are uploaded, then a power loss keeps only commit 1 of
/// the WAL: the replica holds two transactions the database lost. The boot
/// stages a snapshot of the database as it now is, above the replica's
/// newest transaction, and the process stops before it is uploaded. The
/// next boot must take that snapshot as the replica's next file. Judging it
/// an earlier state of the WAL (it starts at the first frame of the
/// replica's generation and ends short of it) deleted it, gave up its
/// restore point and staged a second whole snapshot.
#[test]
fn a_staged_snapshot_of_a_rewound_database_is_uploaded_after_a_restart() {
    let (mut rig, mut actor) = caught_up();
    for n in [2, 3] {
        rig.commit(n);
        rig.settle(&mut actor, T0 + 1);
    }
    assert_eq!(remote_newest(&rig), Some(3));
    rig.wal.lose_frames_after(1);

    let mut rewound = Actor::new(cfg());
    let boot = rig.tick(&mut rewound, T0 + 2);
    assert!(
        matches!(
            event(&boot),
            Some(Event::Booted {
                cause: Some(SnapshotCause::LocalRewound { .. })
            })
        ),
        "{boot:?}"
    );
    let synced = rig.tick(&mut rewound, T0 + 2);
    assert_eq!(event(&synced), Some(&Event::Synced { files: 1 }));
    assert_eq!(
        staged(&rig).last(),
        Some(&4),
        "the snapshot is staged above 3"
    );
    let dropped_before = rig.staging.deleted().len();

    // The process stops here, before the upload.
    let mut next = Actor::new(cfg());
    let boot = rig.tick(&mut next, T0 + 3);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted { cause: None }),
        "the staged snapshot is the replica's next file"
    );
    assert_eq!(
        rig.staging.deleted()[dropped_before..].to_vec(),
        Vec::<FileKey>::new(),
        "nothing staged is given up"
    );
    rig.settle(&mut next, T0 + 3);
    assert_eq!(remote_newest(&rig), Some(4));
    assert_eq!(next.snapshots().total(), 0, "no second snapshot");
    assert_eq!(
        restored(&rig),
        1,
        "the latest restore is the database as it now is"
    );
}

fn remote_newest(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::ZERO)
        .last()
        .map(|f| f.key.range().max().get())
}
