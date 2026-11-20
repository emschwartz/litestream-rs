//! `compat-regen [--version V] [--scenario S]`: regenerates fixtures with the pinned Go
//! binaries (`just compat-regen`). Without flags it regenerates every version and scenario.

use std::process::ExitCode;

use lsr_compat::{GoVersion, Scenario, fixtures_root, generate};

fn parse_args() -> Result<(Vec<GoVersion>, Vec<Scenario>), String> {
    let mut versions = GoVersion::ALL.to_vec();
    let mut scenarios = Scenario::ALL.to_vec();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--version" => {
                versions = vec![
                    GoVersion::parse(&value).ok_or_else(|| format!("unknown version {value}"))?,
                ]
            }
            "--scenario" => {
                scenarios = vec![
                    Scenario::parse(&value).ok_or_else(|| format!("unknown scenario {value}"))?,
                ]
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok((versions, scenarios))
}

fn main() -> ExitCode {
    let (versions, scenarios) = match parse_args() {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("compat-regen: {message}");
            return ExitCode::from(2);
        }
    };
    let root = fixtures_root();
    for version in versions {
        for scenario in &scenarios {
            match generate(version, *scenario, &root) {
                Ok(manifest) => {
                    println!(
                        "{} {}: {} files, {} targets",
                        version.name(),
                        scenario.name(),
                        manifest.files.len(),
                        manifest.targets.len()
                    );
                }
                Err(err) => {
                    eprintln!(
                        "compat-regen: {} {}: {err}",
                        version.name(),
                        scenario.name()
                    );
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    ExitCode::SUCCESS
}
