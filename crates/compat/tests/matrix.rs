//! The Go to Rust matrix, the half that needs no Go: every committed fixture, every target
//! its manifest records, restored by the Rust executor and compared with what Go wrote.
//! Equality is on restored pages, never on LTX bytes. The half that runs Go is in `live.rs`.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod matrix {
    use std::fs;

    use lsr_compat::{
        Manifest, Outcome, Staged, Target, TargetEntry, all_fixtures, hash_pages, integrity_check,
        parse_rel, restore_replica, stage,
    };
    use lsr_core::{PlanOutcome, RestoreError, RestoreTarget};
    use lsr_ltx::{ReadDecoder, Timestamp, Txid};

    /// How one target of one fixture came out.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Class {
        /// Rust restored the target directly and its pages equal Go's.
        Same,
        /// The instant fell inside a compacted file; restoring the chain's last TXID equals Go's.
        ViaInsideFile,
        /// The manifest records a hole and Rust named the same hole without writing a file.
        Gap,
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Tally {
        same: usize,
        via_inside_file: usize,
        gap: usize,
    }

    fn restore_target(target: Target) -> RestoreTarget {
        target.restore_target().expect("a manifest target is valid")
    }

    /// Asserts Rust's restore of `target` to `out` equals Go's recorded pages.
    fn assert_pages_equal(staged: &Staged, out: &str, want: &Outcome, label: &str) {
        let Outcome::Restored {
            page_size,
            page_count,
            page_hashes,
        } = want
        else {
            panic!("{label}: not a restored outcome")
        };
        let path = staged.output(out);
        let got = hash_pages(&path).expect("hash the restored pages");
        assert_eq!(got.page_size, *page_size, "{label}: page size");
        assert_eq!(got.page_count, *page_count, "{label}: length in pages");
        assert_eq!(&got.page_hashes, page_hashes, "{label}: page hashes");
        integrity_check(&path).expect("integrity_check");
    }

    fn classify(fixture: &std::path::Path, manifest: &Manifest, entry: &TargetEntry) -> Class {
        let label = format!(
            "{} {} {:?}",
            manifest.go_version, manifest.scenario, entry.target
        );
        let staged = stage(fixture, manifest).expect("stage");
        let attempt = restore_replica(
            &staged.replica(),
            restore_target(entry.target),
            &staged.output("rs.db"),
        )
        .expect("sink");
        match (&entry.outcome, attempt) {
            (want @ Outcome::Restored { .. }, Ok(_)) => {
                assert_pages_equal(&staged, "rs.db", want, &label);
                Class::Same
            }
            (
                want @ Outcome::Restored { .. },
                Err(RestoreError::NoPlan(PlanOutcome::AsOfInsideFile { chain, .. })),
            ) => {
                assert!(
                    matches!(entry.target, Target::AsOf { .. }),
                    "{label}: only as-of is inside a file"
                );
                assert_eq!(
                    staged.leftovers().expect("list"),
                    Vec::<String>::new(),
                    "{label}"
                );
                let reaches = restore_replica(
                    &staged.replica(),
                    RestoreTarget::Txid(chain.reaches()),
                    &staged.output("rs-chain.db"),
                )
                .expect("sink")
                .unwrap_or_else(|e| panic!("{label}: the chain's own end does not restore: {e}"));
                assert_eq!(reaches.reached(), chain.reaches(), "{label}");
                assert_pages_equal(&staged, "rs-chain.db", want, &label);
                Class::ViaInsideFile
            }
            (
                Outcome::Gap {
                    have_through,
                    next_starts,
                    ..
                },
                Err(RestoreError::NoPlan(PlanOutcome::Gap {
                    have_through: rs_have,
                    next_starts: rs_next,
                })),
            ) => {
                assert_eq!(
                    (rs_have.get(), rs_next.get()),
                    (*have_through, *next_starts),
                    "{label}"
                );
                assert_eq!(
                    staged.leftovers().expect("list"),
                    Vec::<String>::new(),
                    "{label}"
                );
                Class::Gap
            }
            (want, got) => panic!("{label}: Go recorded {want:?}, Rust answered {got:?}"),
        }
    }

    fn tally_all() -> Tally {
        let mut tally = Tally::default();
        for fixture in all_fixtures().expect("list") {
            let manifest = Manifest::load(&fixture).expect("load");
            for entry in &manifest.targets {
                match classify(&fixture, &manifest, entry) {
                    Class::Same => tally.same = tally.same.saturating_add(1),
                    Class::ViaInsideFile => {
                        tally.via_inside_file = tally.via_inside_file.saturating_add(1)
                    }
                    Class::Gap => tally.gap = tally.gap.saturating_add(1),
                }
            }
        }
        tally
    }

    /// Every target of every fixture agrees with Go, and the counts per outcome are pinned so a
    /// planner change that sends every target one way fails here.
    #[test]
    fn every_target_agrees_with_go() {
        // The manifests' own instants never fall inside a compacted file, so that outcome is
        // counted by the dense-instant test below.
        assert_eq!(
            tally_all(),
            Tally {
                same: 90,
                via_inside_file: 0,
                gap: 30
            }
        );
    }

    /// Every file of every fixture decodes to a verified file whose header range is its key.
    #[test]
    fn every_file_decodes_verified_under_its_key() {
        for fixture in all_fixtures().expect("list") {
            let manifest = Manifest::load(&fixture).expect("load");
            for entry in &manifest.files {
                let key = parse_rel(&entry.key).expect("key");
                let path = Manifest::replica_dir(&fixture).join(&entry.key);
                let mut decoder = ReadDecoder::new(fs::File::open(&path).expect("open"));
                while decoder.next_event().expect("decode").is_some() {}
                let verified = decoder.finish().expect("verified");
                assert_eq!(
                    verified.header().txids(),
                    key.range(),
                    "{} {}",
                    fixture.display(),
                    entry.key
                );
            }
        }
    }

    /// Flipping one byte of any file in the chain makes the restore fail naming that file and
    /// leaves nothing behind. One small fixture per Go version.
    #[test]
    fn a_flipped_byte_in_any_chain_file_fails_naming_it() {
        let mut flipped = 0_usize;
        for fixture in all_fixtures().expect("list") {
            let manifest = Manifest::load(&fixture).expect("load");
            if manifest.scenario != "small-4096" {
                continue;
            }
            let staged = stage(&fixture, &manifest).expect("stage");
            let chain =
                lsr_compat::chain_files(&staged.replica(), RestoreTarget::Latest).expect("chain");
            for key in chain {
                let path = staged.replica().join(lsr_compat::rel_of(&key));
                let original = fs::read(&path).expect("read");
                let last = original.len().saturating_sub(1);
                for at in [0, original.len().saturating_div(2), last] {
                    let mut bytes = original.clone();
                    bytes[at] ^= 0x01;
                    fs::write(&path, &bytes).expect("write");
                    let out = staged.output("flip.db");
                    let got = restore_replica(&staged.replica(), RestoreTarget::Latest, &out)
                        .expect("sink");
                    let label = format!("{} {key} byte {at}", manifest.go_version);
                    match got {
                        Err(RestoreError::File { key: named, .. }) => {
                            assert_eq!(named, key, "{label}")
                        }
                        other => {
                            panic!("{label}: expected a failure naming the file, got {other:?}")
                        }
                    }
                    assert!(!out.exists(), "{label}: a database was published");
                    assert_eq!(
                        staged.leftovers().expect("list"),
                        Vec::<String>::new(),
                        "{label}"
                    );
                    flipped = flipped.saturating_add(1);
                }
                fs::write(&path, &original).expect("restore the file");
            }
        }
        assert!(flipped > 0);
    }

    #[test]
    fn txid_zero_is_not_a_target() {
        assert!(Txid::new(0).is_none());
        assert!(Target::Txid { txid: 0 }.restore_target().is_none());
    }

    /// An as-of instant between every two consecutive file times, which the manifests do not
    /// record: Rust either restores, refuses with `NoSnapshot` (nothing old enough), or reports
    /// the instant inside a compacted file, and then the chain's own end restores. The counts are
    /// pinned so a planner change that turns every instant one way fails. Go's answer for the same
    /// instants is compared in `live.rs`.
    #[test]
    fn dense_as_of_instants_never_fall_between_outcomes() {
        let (mut restored, mut inside, mut no_snapshot) = (0_usize, 0_usize, 0_usize);
        for fixture in all_fixtures().expect("list") {
            let manifest = Manifest::load(&fixture).expect("load");
            if manifest.scenario == "mid-chain-gap" {
                continue;
            }
            let staged = stage(&fixture, &manifest).expect("stage");
            for at in manifest.midpoints() {
                let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(at));
                let out = staged.output("dense.db");
                match restore_replica(&staged.replica(), target, &out).expect("sink") {
                    Ok(_) => {
                        fs::remove_file(&out).expect("published");
                        restored = restored.saturating_add(1);
                    }
                    Err(RestoreError::NoPlan(PlanOutcome::NoSnapshot { .. })) => {
                        no_snapshot = no_snapshot.saturating_add(1)
                    }
                    Err(RestoreError::NoPlan(PlanOutcome::AsOfInsideFile { chain, .. })) => {
                        let end = RestoreTarget::Txid(chain.reaches());
                        restore_replica(&staged.replica(), end, &out)
                            .expect("sink")
                            .expect("the chain's own end restores");
                        fs::remove_file(&out).expect("published");
                        inside = inside.saturating_add(1);
                    }
                    other => panic!(
                        "{} {} {at}: {other:?}",
                        manifest.go_version, manifest.scenario
                    ),
                }
            }
        }
        assert_eq!((restored, inside, no_snapshot), (101, 1, 122));
    }
}
