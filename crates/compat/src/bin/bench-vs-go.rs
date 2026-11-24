//! `bench-vs-go --out DIR [--pairs N] <set>...`: the benchmark of `lsr` against the pinned Go
//! Litestream on a local MinIO (`just bench-vs-go`). `bench-vs-go exec ...` is the wrapper
//! the harness starts programs under.

use std::ffi::OsString;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    match lsr_compat::bench_vs_go(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("bench-vs-go: {err}");
            ExitCode::FAILURE
        }
    }
}
