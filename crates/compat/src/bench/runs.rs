//! What one run of the snapshot-impact set measured, and the file every run is appended to as it
//! ends (so a crash after hours keeps what it measured, and a restart skips what is done).

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::error::CompatError;

/// The numbers of a run, in the order of [`Run::values`]. A number a run does not have (the
/// copy's figures of the arm with no copy) is NaN.
pub(super) const METRICS: [&str; 22] = [
    "p50_ms",
    "p99_ms",
    "p999_ms",
    "pre_p99_ms",
    "write_p99_ms",
    "app_major_faults",
    "sys_major_faults",
    "sys_scanned_kpages",
    "sys_stolen_kpages",
    "sys_refault_kpages",
    "alloc_stalls",
    "copy_s",
    "lsr_read_mib",
    "lsr_rchar_mib",
    "wal_growth_mib",
    "wal_peak_mib",
    "current_s",
    "steal_pct",
    "lsr_cpu_s",
    "lsr_peak_rss_mib",
    "timed_out",
    "reads_in_window",
];

/// Metric positions by name, so a table names what it reads.
pub(super) mod at {
    pub(in crate::bench) const P50: usize = 0;
    pub(in crate::bench) const P99: usize = 1;
    pub(in crate::bench) const P999: usize = 2;
    pub(in crate::bench) const PRE_P99: usize = 3;
    pub(in crate::bench) const APP_FAULTS: usize = 5;
    pub(in crate::bench) const COPY_S: usize = 11;
    pub(in crate::bench) const READ_MIB: usize = 12;
    pub(in crate::bench) const WAL_GROWTH: usize = 14;
    pub(in crate::bench) const CURRENT_S: usize = 16;
    pub(in crate::bench) const STEAL: usize = 17;
    pub(in crate::bench) const TIMED_OUT: usize = 20;
}

/// One run's numbers.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Run {
    pub values: Vec<f64>,
}

impl Run {
    /// The number at [`index`] of [`METRICS`]; NaN when there is none.
    pub(super) fn get(&self, index: usize) -> f64 {
        self.values.get(index).copied().unwrap_or(f64::NAN)
    }
}

/// Which stage of the experiment a run belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// Every arm once, unmeasured: finds the copy's duration and rate, checks each arm works.
    Calibration,
    /// A against A, for the noise floor.
    Gate,
    /// The measured rounds.
    Round,
}

impl Phase {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Calibration => "cal",
            Self::Gate => "gate",
            Self::Round => "round",
        }
    }

    fn of(name: &str) -> Option<Self> {
        [Self::Calibration, Self::Gate, Self::Round]
            .into_iter()
            .find(|p| p.name() == name)
    }
}

/// A run's place: its phase, round (or pair) number and arm label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Cell {
    pub phase: Phase,
    pub round: usize,
    pub arm: String,
}

/// Every finished run, and the file they are appended to.
#[derive(Debug)]
pub(super) struct Store {
    path: PathBuf,
    rows: Vec<(Cell, Run)>,
}

impl Store {
    /// Opens [`path`], reading the runs a previous invocation left; writes the header when new.
    pub(super) fn open(path: &Path) -> Result<Self, CompatError> {
        let mut rows = Vec::new();
        if let Ok(text) = fs::read_to_string(path) {
            for line in text.lines().skip(1) {
                let mut parts = line.split(',');
                let phase = parts.next().and_then(Phase::of);
                let round = parts.next().and_then(|r| r.parse().ok());
                let arm = parts.next();
                let values: Vec<f64> = parts.filter_map(|v| v.parse().ok()).collect();
                if let (Some(phase), Some(round), Some(arm)) = (phase, round, arm)
                    && values.len() == METRICS.len()
                {
                    rows.push((
                        Cell {
                            phase,
                            round,
                            arm: arm.to_owned(),
                        },
                        Run { values },
                    ));
                }
            }
        } else {
            let header = format!("phase,round,arm,{}\n", METRICS.join(","));
            fs::write(path, header).map_err(CompatError::io(path))?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            rows,
        })
    }

    /// The finished run at [`cell`].
    pub(super) fn find(&self, cell: &Cell) -> Option<&Run> {
        self.rows.iter().find(|(c, _)| c == cell).map(|(_, r)| r)
    }

    /// Appends a finished run to memory and to the file.
    pub(super) fn push(&mut self, cell: Cell, run: Run) -> Result<(), CompatError> {
        let values: Vec<String> = run.values.iter().map(f64::to_string).collect();
        let line = format!(
            "{},{},{},{}\n",
            cell.phase.name(),
            cell.round,
            cell.arm,
            values.join(",")
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(CompatError::io(&self.path))?;
        file.write_all(line.as_bytes())
            .map_err(CompatError::io(&self.path))?;
        self.rows.push((cell, run));
        Ok(())
    }

    /// The runs of [`phase`] for [`arm`], by round, in round order.
    pub(super) fn arm(&self, phase: Phase, arm: &str) -> Vec<(usize, &Run)> {
        let mut found: Vec<(usize, &Run)> = self
            .rows
            .iter()
            .filter(|(c, _)| c.phase == phase && c.arm == arm)
            .map(|(c, r)| (c.round, r))
            .collect();
        found.sort_by_key(|(round, _)| *round);
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(seed: f64) -> Run {
        Run {
            values: (0..METRICS.len())
                .map(|i| seed + f64::from(u32::try_from(i).unwrap_or(0)))
                .collect(),
        }
    }

    #[test]
    fn runs_survive_a_restart_and_nan_stays_nan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("runs.csv");
        let mut store = Store::open(&path).expect("open");
        let cell = Cell {
            phase: Phase::Round,
            round: 3,
            arm: "pace-1/2".to_owned(),
        };
        let mut r = run(1.5);
        r.values[at::COPY_S] = f64::NAN;
        store.push(cell.clone(), r).expect("push");
        let again = Store::open(&path).expect("reopen");
        let got = again.find(&cell).expect("kept");
        assert!(got.get(at::COPY_S).is_nan());
        assert!((got.get(at::P99) - 2.5).abs() < 1e-12);
        assert_eq!(again.arm(Phase::Round, "pace-1/2").len(), 1);
        assert!(again.arm(Phase::Gate, "pace-1/2").is_empty());
    }

    #[test]
    fn the_positions_name_the_metrics_they_mean() {
        let name = |i: usize| METRICS.get(i).copied().unwrap_or("");
        assert_eq!(name(at::P50), "p50_ms");
        assert_eq!(name(at::P99), "p99_ms");
        assert_eq!(name(at::P999), "p999_ms");
        assert_eq!(name(at::PRE_P99), "pre_p99_ms");
        assert_eq!(name(at::APP_FAULTS), "app_major_faults");
        assert_eq!(name(at::COPY_S), "copy_s");
        assert_eq!(name(at::READ_MIB), "lsr_read_mib");
        assert_eq!(name(at::WAL_GROWTH), "wal_growth_mib");
        assert_eq!(name(at::CURRENT_S), "current_s");
        assert_eq!(name(at::STEAL), "steal_pct");
        assert_eq!(name(at::TIMED_OUT), "timed_out");
    }
}
