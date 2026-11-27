//! The table of the snapshot-impact set: per-arm medians, then every arm's change against A and
//! against B with its interval and pair count, then each remedy against the pre-registered bar.
//! The CSVs of the paired numbers are written beside it for [`perfstats.py compare`].

use std::fs;
use std::path::Path;
use std::process::Command;

use super::plan::{self, ArmDef};
use super::runs::{METRICS, Phase, Run, Store, at};
use super::tally::{self, Change};
use crate::error::CompatError;

/// Pairs below which no verdict against the bar is given: an interval from fewer is no evidence.
const MIN_PAIRS_FOR_BAR: usize = 5;

type Runs<'a> = Vec<(usize, &'a Run)>;

/// The pairs (y's number, x's number) of the rounds both arms finished: x's change from y.
fn pairs(x: &Runs<'_>, y: &Runs<'_>, index: usize) -> Vec<(f64, f64)> {
    x.iter()
        .filter_map(|(round, rx)| {
            let (_, ry) = y.iter().find(|(r, _)| r == round)?;
            Some((ry.get(index), rx.get(index)))
        })
        .collect()
}

fn medians(runs: &Runs<'_>, index: usize) -> f64 {
    tally::median(&runs.iter().map(|(_, r)| r.get(index)).collect::<Vec<_>>())
}

fn metric_name(index: usize) -> &'static str {
    METRICS.get(index).copied().unwrap_or("?")
}

fn medians_table(arms: &[(&ArmDef, Runs<'_>)]) {
    println!(
        "\nmedians over rounds (p50 p99 p99.9 in ms during the copy window, commit50 and commit99 the fsync'd commits' own; pre p99 is the same run's own 20 s before it)"
    );
    println!(
        "{:<11} {:>3} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9} {:>8} {:>9} {:>11} {:>6} {:>8} {:>8}",
        "arm",
        "n",
        "p50",
        "p99",
        "p99.9",
        "pre p99",
        "copy s",
        "read MiB",
        "WAL MiB",
        "current s",
        "app majflt",
        "steal%",
        "commit50",
        "commit99"
    );
    for (arm, runs) in arms {
        let m = |i| medians(runs, i);
        println!(
            "{:<11} {:>3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.1} {:>9.0} {:>8.1} {:>9.1} {:>11.0} {:>6.2} {:>8.3} {:>8.3}",
            arm.label,
            runs.len(),
            m(at::P50),
            m(at::P99),
            m(at::P999),
            m(at::PRE_P99),
            m(at::COPY_S),
            m(at::READ_MIB),
            m(at::WAL_GROWTH),
            m(at::CURRENT_S),
            m(at::APP_FAULTS),
            m(at::STEAL),
            m(at::WRITE_P50),
            m(at::WRITE_P99),
        );
    }
}

fn change_of(x: &Runs<'_>, y: &Runs<'_>, index: usize) -> Option<Change> {
    tally::paired(&pairs(x, y, index))
}

fn deltas_table(arms: &[(&ArmDef, Runs<'_>)]) {
    let find = |label: &str| arms.iter().find(|(a, _)| a.label == label).map(|(_, r)| r);
    println!(
        "\nchange of the arm against another, per round pair: geometric-mean %, 95% interval, pairs; verdict at SESOI {}%",
        plan::IMPACT_SESOI
    );
    for (arm, runs) in arms {
        for (base, wanted) in [
            ("A", &[at::P50, at::P99, at::P999][..]),
            (
                "B",
                &[
                    at::P50,
                    at::P99,
                    at::P999,
                    at::COPY_S,
                    at::WAL_GROWTH,
                    at::CURRENT_S,
                ][..],
            ),
        ] {
            if arm.label == base || (base == "B" && arm.label == "A") {
                continue;
            }
            let Some(other) = find(base) else { continue };
            for index in wanted {
                let change = change_of(runs, other, *index);
                println!(
                    "  {:<11} vs {base}  {:<14} {:<34} {}",
                    arm.label,
                    metric_name(*index),
                    tally::show(change),
                    change.map_or("", |c| c.verdict(plan::IMPACT_SESOI)),
                );
            }
        }
    }
}

fn bars(arms: &[(&ArmDef, Runs<'_>)]) {
    let find = |label: &str| arms.iter().find(|(a, _)| a.label == label).map(|(_, r)| r);
    let (Some(a), Some(b)) = (find("A"), find("B")) else {
        return;
    };
    println!(
        "\nthe bar (pre-registered): p99 within {}x of A and copy within {}x of B",
        plan::IMPACT_BAR_P99_VS_A,
        plan::IMPACT_BAR_COPY_VS_B
    );
    if let Some(c) = change_of(b, a, at::P99) {
        println!(
            "  the problem: B's p99 is {:+.0}% of A's ({}), SESOI {}%: {}",
            c.pct,
            c.show(),
            plan::IMPACT_SESOI,
            c.verdict(plan::IMPACT_SESOI)
        );
    }
    for (arm, runs) in arms.iter().filter(|(x, _)| !x.idle && x.label != "B") {
        let p99 = change_of(runs, a, at::P99).filter(|c| c.n >= MIN_PAIRS_FOR_BAR);
        let copy = change_of(runs, b, at::COPY_S).filter(|c| c.n >= MIN_PAIRS_FOR_BAR);
        let ratio = |pct: f64| 1.0 + pct / 100.0;
        let word = match p99.zip(copy) {
            Some((p, c))
                if ratio(p.pct) <= plan::IMPACT_BAR_P99_VS_A
                    && ratio(c.pct) <= plan::IMPACT_BAR_COPY_VS_B =>
            {
                "MEETS THE BAR"
            }
            Some((p, _)) if ratio(p.pct) <= plan::IMPACT_BAR_P99_VS_A => {
                "latency met, copy too slow: a trade"
            }
            Some((_, c)) if ratio(c.pct) <= plan::IMPACT_BAR_COPY_VS_B => {
                "copy fine, latency not back"
            }
            Some(_) => "misses both",
            None => "too few pairs to judge",
        };
        let show = |c: Option<Change>| {
            c.map_or("-".to_owned(), |c| {
                format!("{:.2}x (to {:.2}x)", ratio(c.pct), ratio(c.hi))
            })
        };
        println!(
            "  {:<11} p99 {} of A, copy {} of B: {word}",
            arm.label,
            show(p99),
            show(copy),
        );
    }
    if let Some((_, twice)) = arms.iter().find(|(x, _)| x.label == "read-twice") {
        let copy = change_of(twice, b, at::COPY_S);
        let p99 = change_of(twice, b, at::P99);
        let loses = copy.is_some_and(|c| c.lo > 0.0) || p99.is_some_and(|c| c.lo > 0.0);
        let enough = [copy, p99]
            .into_iter()
            .flatten()
            .any(|c| c.n >= MIN_PAIRS_FOR_BAR);
        println!(
            "  control (read-twice must lose to B): {}",
            if !enough {
                "too few pairs to judge"
            } else if loses {
                "it loses: the harness sees this axis"
            } else {
                "IT DID NOT LOSE: the harness cannot see this axis, believe nothing above"
            }
        );
    }
}

fn csvs(arms: &[(&ArmDef, Runs<'_>)], dir: &Path) -> Result<Vec<std::path::PathBuf>, CompatError> {
    let mut written = Vec::new();
    let find = |label: &str| arms.iter().find(|(a, _)| a.label == label).map(|(_, r)| r);
    for (arm, runs) in arms {
        for base in ["A", "B"] {
            if arm.label == base || (base == "B" && arm.label == "A") {
                continue;
            }
            let Some(other) = find(base) else { continue };
            for index in [at::P50, at::P99, at::P999, at::COPY_S] {
                let name = format!(
                    "snapshot-impact-{}-{}-vs-{base}.csv",
                    metric_name(index),
                    arm.label.replace('/', "-")
                );
                let path = dir.join(name);
                let mut text = String::from("pair,a,b\n");
                for (i, (a, b)) in pairs(runs, other, index).iter().enumerate() {
                    text.push_str(&format!("{i},{a},{b}\n"));
                }
                fs::write(&path, text).map_err(CompatError::io(&path))?;
                written.push(path);
            }
        }
    }
    Ok(written)
}

/// Prints the report of [`total`] rounds and writes the pair CSVs under [`dir`].
pub(super) fn print(
    store: &Store,
    arms: &[ArmDef],
    total: usize,
    dir: &Path,
) -> Result<(), CompatError> {
    let by_arm: Vec<(&ArmDef, Runs<'_>)> = arms
        .iter()
        .map(|a| (a, store.arm(Phase::Round, a.label)))
        .collect();
    println!("\n#### snapshot-impact: {total} rounds planned");
    medians_table(&by_arm);
    deltas_table(&by_arm);
    bars(&by_arm);
    let files = csvs(&by_arm, dir)?;
    if let Some(script) = std::env::var_os("LSR_PERFSTATS") {
        for path in files.iter().filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().contains("p99_ms"))
        }) {
            let out = Command::new("uv")
                .arg("run")
                .arg(&script)
                .arg("compare")
                .arg(path)
                .args(["--sesoi", &plan::IMPACT_SESOI.to_string()])
                .output()
                .map_err(CompatError::io("uv"))?;
            println!(
                "\n{}\n{}",
                path.display(),
                String::from_utf8_lossy(&out.stdout)
            );
        }
    } else {
        println!(
            "\n(LSR_PERFSTATS is unset: no perfstats verdicts; the CSVs are in {})",
            dir.display()
        );
    }
    Ok(())
}
