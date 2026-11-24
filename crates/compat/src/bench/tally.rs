//! The statistics of the snapshot-impact set: a paired change with its interval, the pair count
//! an A/A run asks for, and the words a verdict uses. The same numbers [`perfstats.py`] reports
//! (geometric-mean change of the ratio, a t interval on the log ratios), so the table prints
//! without Python; the CSVs it writes are what [`perfstats.py compare`] reads.

use super::to_f64;

/// Two-sided 95% Student t quantiles for 1 to 30 degrees of freedom.
const T95: [f64; 30] = [
    12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
    2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
    2.052, 2.048, 2.045, 2.042,
];

/// The t quantile for [`n`] pairs.
fn t95(n: usize) -> f64 {
    n.checked_sub(2)
        .and_then(|df| T95.get(df))
        .copied()
        .unwrap_or(1.96)
}

/// The change from the first number of each pair to the second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Change {
    /// Geometric-mean change in percent (+100 is twice as large).
    pub pct: f64,
    /// The 95% interval's ends, in percent.
    pub lo: f64,
    pub hi: f64,
    /// Pairs used.
    pub n: usize,
    /// Standard deviation of the per-pair log ratio.
    pub sd_log: f64,
}

/// The change from [`a`] to [`b`] over pairs of positive finite numbers; pairs with anything
/// else (a metric a run did not have) are left out. [`None`] below two pairs.
pub(super) fn paired(rows: &[(f64, f64)]) -> Option<Change> {
    let logs: Vec<f64> = rows
        .iter()
        .filter(|(a, b)| a.is_finite() && b.is_finite() && *a > 0.0 && *b > 0.0)
        .map(|(a, b)| (b / a).ln())
        .collect();
    let n = logs.len();
    if n < 2 {
        return None;
    }
    let count = to_f64(u64::try_from(n).ok()?);
    let mean = logs.iter().sum::<f64>() / count;
    let var = logs.iter().map(|l| (l - mean).powi(2)).sum::<f64>() / (count - 1.0);
    let sd_log = var.sqrt();
    let half = t95(n) * sd_log / count.sqrt();
    let pct = |l: f64| (l.exp() - 1.0) * 100.0;
    Some(Change {
        pct: pct(mean),
        lo: pct(mean - half),
        hi: pct(mean + half),
        n,
        sd_log,
    })
}

impl Change {
    /// The smallest change the pairs could have seen, in percent: the interval's half width.
    pub(super) fn mde_pct(&self) -> f64 {
        (self.hi - self.lo) / 2.0
    }

    /// The verdict at [`sesoi`] percent, in the words of the skill's equivalence test.
    pub(super) fn verdict(&self, sesoi: f64) -> &'static str {
        if self.hi < 0.0 && self.pct <= -sesoi {
            "lower by more than SESOI"
        } else if self.lo > 0.0 && self.pct >= sesoi {
            "higher by more than SESOI"
        } else if self.lo > -sesoi && self.hi < sesoi {
            "equivalent within SESOI"
        } else {
            "inconclusive"
        }
    }

    /// "+12.3% [+4.0, +21.0] n=10".
    pub(super) fn show(&self) -> String {
        format!(
            "{:+.1}% [{:+.1}, {:+.1}] n={}",
            self.pct, self.lo, self.hi, self.n
        )
    }
}

/// A change, or a dash when there were too few pairs.
pub(super) fn show(change: Option<Change>) -> String {
    change.map_or_else(|| "-".to_owned(), |c| c.show())
}

/// The median of the finite values; NaN when there are none.
pub(super) fn median(values: &[f64]) -> f64 {
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    v.sort_by(f64::total_cmp);
    v.get(v.len() / 2).copied().unwrap_or(f64::NAN)
}

/// The nearest-rank value at [`per_mille`] thousandths (999 is the 99.9th percentile) of an
/// ascending list; 0 for an empty one.
pub(super) fn percentile(sorted: &[u64], per_mille: usize) -> u64 {
    let rank = sorted.len().saturating_mul(per_mille).div_ceil(1000).max(1);
    sorted
        .get(rank.saturating_sub(1))
        .copied()
        .unwrap_or_default()
}

/// Pairs needed to see [`sesoi_pct`] percent with 80% power at 5% significance when the per-pair
/// log ratio has standard deviation [`sd_log`]: [`(1.96 + 0.84)^2 (sd / ln(1 + sesoi))^2`], plus
/// one for the t distribution's heavier tails, clamped to [`min..=max`].
pub(super) fn pairs_needed(sd_log: f64, sesoi_pct: f64, min: usize, max: usize) -> usize {
    let effect = (1.0 + sesoi_pct / 100.0).ln();
    let raw = 7.84 * (sd_log / effect).powi(2);
    // Rounded up, and a NaN or infinite estimate asks for the most.
    let needed = if raw.is_finite() && raw >= 0.0 {
        let mut n = 0_usize;
        while to_f64(u64::try_from(n).unwrap_or(0)) < raw && n < max {
            n = n.saturating_add(1);
        }
        n.saturating_add(1)
    } else {
        max
    };
    needed.clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_doubling_in_every_pair_is_plus_one_hundred_percent() {
        let rows = [(1.0, 2.0), (2.0, 4.0), (3.0, 6.0)];
        let c = paired(&rows).expect("change");
        assert!((c.pct - 100.0).abs() < 1e-9);
        assert!(c.sd_log.abs() < 1e-9);
        assert_eq!(c.n, 3);
        assert_eq!(c.verdict(10.0), "higher by more than SESOI");
    }

    #[test]
    fn pairs_with_a_missing_number_are_left_out() {
        let rows = [(1.0, 1.0), (f64::NAN, 3.0), (0.0, 1.0), (2.0, 2.0)];
        assert_eq!(paired(&rows).expect("change").n, 2);
        assert!(paired(&rows[..1]).is_none());
    }

    #[test]
    fn noise_around_zero_is_equivalent_or_inconclusive_never_a_change() {
        let rows = [
            (1.0, 1.01),
            (1.0, 0.99),
            (1.0, 1.0),
            (1.0, 1.005),
            (1.0, 0.995),
        ];
        let c = paired(&rows).expect("change");
        assert_eq!(c.verdict(10.0), "equivalent within SESOI");
        assert_eq!(c.verdict(0.001), "inconclusive");
    }

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let v: Vec<u64> = (1..=1000).collect();
        assert_eq!(percentile(&v, 500), 500);
        assert_eq!(percentile(&v, 990), 990);
        assert_eq!(percentile(&v, 999), 999);
        assert_eq!(percentile(&[], 999), 0);
    }

    #[test]
    fn the_pair_count_follows_the_noise_and_is_clamped() {
        // sd 0.2 against ln(1.25) = 0.223: 7.84 * 0.80 = 6.3, so 7 pairs, raised to the minimum.
        assert_eq!(pairs_needed(0.2, 25.0, 10, 16), 10);
        // sd 0.4: 7.84 * 3.2 = 25, clamped to the maximum.
        assert_eq!(pairs_needed(0.4, 25.0, 10, 16), 16);
        assert_eq!(pairs_needed(f64::NAN, 25.0, 10, 16), 16);
    }

    #[test]
    fn the_median_ignores_what_is_not_a_number() {
        assert!((median(&[3.0, f64::NAN, 1.0, 2.0]) - 2.0).abs() < 1e-12);
        assert!(median(&[]).is_nan());
    }
}
