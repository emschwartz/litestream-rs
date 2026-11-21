use super::*;

#[test]
fn entropy_stays_below_its_bound_and_varies() {
    let entropy = OsEntropy::new();
    let draws: Vec<u64> = (0..64).map(|_| entropy.below(1000)).collect();
    assert!(draws.iter().all(|draw| *draw < 1000));
    assert!(draws.iter().any(|draw| *draw != draws[0]));
    assert_eq!(entropy.below(0), 0);
}
