//! The restore-plan spec checked on its own: worked examples from the plan.rs
//! doc, and a sweep showing the contract decides every small listing (an empty
//! judgement there would be a hole in the text) and that every chain it allows
//! is one by the chain definition.

use std::num::NonZeroU64;

use lsr_spec::restore_plan::{
    Clause, CreatedAt, Dating, File, Judgement, Key, Listing, Origin, Outcome, Target, dating,
    judge,
};

fn key(level: u8, min: u64, max: u64) -> Key {
    Key { level, min, max }
}

fn stored(level: u8, min: u64, max: u64, at_ms: i64) -> File {
    File {
        key: key(level, min, max),
        created_at: Some(CreatedAt {
            at_ms,
            origin: Origin::Mtime,
        }),
    }
}

fn listing(files: &[File]) -> Listing {
    Listing::new(files.iter().copied()).unwrap()
}

fn txid(n: u64) -> Target {
    Target::Txid(NonZeroU64::new(n).unwrap())
}

fn only(judgement: &Judgement) -> &Outcome {
    assert_eq!(judgement.allowed.len(), 1, "{judgement:?}");
    &judgement.allowed[0]
}

#[test]
fn a_listing_refuses_bad_and_repeated_keys() {
    assert!(Listing::new([stored(10, 1, 1, 0)]).is_none());
    assert!(Listing::new([stored(0, 0, 1, 0)]).is_none());
    assert!(Listing::new([stored(0, 3, 2, 0)]).is_none());
    assert!(Listing::new([stored(0, 1, 1, 0), stored(0, 1, 1, 5)]).is_none());
}

#[test]
fn the_provenance_table() {
    let at = |at_ms, origin| CreatedAt { at_ms, origin };
    assert_eq!(dating(0, at(5, Origin::Metadata)), Dating::Commit);
    assert_eq!(dating(8, at(5, Origin::Mtime)), Dating::Commit);
    assert_eq!(dating(1, at(5, Origin::Fallback)), Dating::NotBeforeCommit);
    assert_eq!(dating(9, at(5, Origin::Metadata)), Dating::NotBeforeCommit);
    assert_eq!(dating(0, at(0, Origin::Metadata)), Dating::Unset);
    assert_eq!(dating(9, at(-1, Origin::Fallback)), Dating::Unset);
}

/// plan.rs: "history held only in a refused file makes `Latest` refuse".
#[test]
fn history_only_in_a_refused_file_refuses_latest() {
    let files = listing(&[stored(9, 1, 3, 1), stored(9, 2, 6, 2)]);
    let judgement = judge(&files, Target::Latest);
    assert_eq!(judgement.by, Clause::MalformedSnapshot);
    assert_eq!(
        only(&judgement),
        &Outcome::MalformedSnapshot {
            have_through: 3,
            file: key(9, 2, 6)
        }
    );
}

/// The compacted-file case from the AsOfInsideFile doc: L1 4-10 created at
/// 1000 holds the instant 950 somewhere inside it.
#[test]
fn an_instant_inside_a_compacted_file() {
    let files = listing(&[stored(9, 1, 3, 300), stored(1, 4, 10, 1000)]);
    let judgement = judge(&files, Target::AsOf(950));
    assert_eq!(judgement.by, Clause::AsOfStraddle);
    assert_eq!(
        only(&judgement),
        &Outcome::AsOfInsideFile {
            chain: vec![key(9, 1, 3)],
            file: key(1, 4, 10)
        }
    );
}

/// `dates_next`: a level-0 file with its stored time ending right after the
/// chain dates that commit; the same file with a fallback time does not.
#[test]
fn only_a_commit_time_dates_the_next_transaction() {
    let mut files = vec![
        stored(9, 1, 3, 300),
        stored(0, 4, 4, 1000),
        stored(1, 4, 10, 2000),
    ];
    let judgement = judge(&listing(&files), Target::AsOf(950));
    assert_eq!(judgement.by, Clause::AsOfDatesNext);
    files[1].created_at = Some(CreatedAt {
        at_ms: 1000,
        origin: Origin::Fallback,
    });
    let judgement = judge(&listing(&files), Target::AsOf(950));
    assert_eq!(judgement.by, Clause::AsOfStraddle);
    assert_eq!(
        only(&judgement),
        &Outcome::AsOfInsideFile {
            chain: vec![key(9, 1, 3)],
            file: key(0, 4, 4)
        }
    );
}

/// Straddlers ending together: the design-tier tie-break names the higher
/// level, then the lower key.
#[test]
fn straddlers_ending_together_go_to_the_higher_level_then_the_lower_key() {
    let files = listing(&[
        stored(9, 1, 3, 300),
        stored(1, 4, 10, 1000),
        stored(2, 2, 10, 1000),
        stored(2, 3, 10, 1000),
    ]);
    let judgement = judge(&files, Target::AsOf(950));
    assert_eq!(judgement.by, Clause::AsOfStraddle);
    assert_eq!(
        only(&judgement),
        &Outcome::AsOfInsideFile {
            chain: vec![key(9, 1, 3)],
            file: key(2, 2, 10)
        }
    );
}

#[test]
fn an_as_of_hole_is_the_latest_refusal() {
    let files = listing(&[stored(9, 1, 3, 100), stored(0, 6, 6, 50)]);
    let judgement = judge(&files, Target::AsOf(200));
    assert_eq!(judgement.by, Clause::AsOfHole);
    assert_eq!(
        only(&judgement),
        &Outcome::Gap {
            have_through: 3,
            next_starts: 6
        }
    );
}

#[test]
fn a_target_inside_the_only_base_names_it() {
    let files = listing(&[stored(9, 1, 5, 1)]);
    let judgement = judge(&files, txid(3));
    assert_eq!(
        only(&judgement),
        &Outcome::InsideFile {
            requested: 3,
            file: key(9, 1, 5)
        }
    );
}

/// Every key over transactions 1..=`d` at each level.
fn universe(d: u64, levels: &[u8]) -> Vec<Key> {
    let mut keys = Vec::new();
    for &level in levels {
        for min in 1..=d {
            for max in min..=d {
                keys.push(key(level, min, max));
            }
        }
    }
    keys
}

fn subsets(keys: &[Key], n: usize, f: &mut impl FnMut(&[Key])) {
    fn go(keys: &[Key], from: usize, n: usize, held: &mut Vec<Key>, f: &mut impl FnMut(&[Key])) {
        f(held);
        if held.len() == n {
            return;
        }
        for i in from..keys.len() {
            held.push(keys[i]);
            go(keys, i + 1, n, held, f);
            held.pop();
        }
    }
    go(keys, 0, n, &mut Vec::new(), f);
}

/// A chain by the definition: base at 1, later files of levels 0 to 8, each
/// linking and advancing.
fn is_chain(chain: &[Key], files: &[File]) -> bool {
    let listed = |k: &Key| files.iter().any(|f| f.key == *k);
    let Some((base, rest)) = chain.split_first() else {
        return false;
    };
    let mut end = base.max;
    base.min == 1
        && listed(base)
        && rest.iter().all(|k| {
            let ok = listed(k) && k.level < 9 && k.min <= end + 1 && k.max > end;
            end = k.max;
            ok
        })
}

fn check_decided(files: &[File], target: Target) {
    let judgement = judge(&listing(files), target);
    assert!(
        !judgement.allowed.is_empty(),
        "the contract is silent: {files:?} {target:?} {judgement:?}"
    );
    for outcome in &judgement.allowed {
        if let Outcome::Plan { chain } | Outcome::AsOfInsideFile { chain, .. } = outcome {
            assert!(is_chain(chain, files), "{files:?} {target:?} {outcome:?}");
        }
    }
}

#[test]
fn the_contract_decides_every_small_listing() {
    let keys = universe(4, &[0, 1, 9]);
    let mut listings = 0u32;
    subsets(&keys, 3, &mut |held| {
        listings += 1;
        // Created-at times from a small set, rotated per file, with origins
        // that both date and do not date a commit.
        let files: Vec<File> = held
            .iter()
            .enumerate()
            .map(|(i, k)| File {
                key: *k,
                created_at: Some(CreatedAt {
                    at_ms: [0, 100, 200, 300][(i + usize::try_from(k.max).unwrap()) % 4],
                    origin: if (i + usize::try_from(k.min).unwrap()) % 3 == 0 {
                        Origin::Fallback
                    } else {
                        Origin::Mtime
                    },
                }),
            })
            .collect();
        check_decided(&files, Target::Latest);
        for n in 1..=5 {
            check_decided(&files, txid(n));
        }
        for instant in [0, 100, 150, 200, 300, 400] {
            check_decided(&files, Target::AsOf(instant));
        }
    });
    assert!(listings > 1000, "the sweep walked {listings} listings");
}
