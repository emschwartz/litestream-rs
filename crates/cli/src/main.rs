//! `lsr`: restore, verify and list a Litestream file replica.
//!
//! A thin root: collect arguments, parse, run, print, exit. Stdout carries only
//! the result; every failure is one line on stderr. The stores are async over
//! blocking file I/O and complete without a reactor, so the library's
//! thread-parking `block_on` drives them and no runtime is linked.

mod args;
mod render;
mod replica;
mod run;

use std::io::{self, Write};
use std::process::ExitCode;

use args::UsageError;
use lsr_replica::block_on;
use run::{CliError, Done, Exit};

fn command_line() -> Result<Vec<String>, UsageError> {
    std::env::args_os()
        .skip(1)
        .enumerate()
        .map(|(index, arg)| {
            arg.into_string().map_err(|_raw| UsageError::NotUtf8 {
                position: index.saturating_add(1),
            })
        })
        .collect()
}

fn execute() -> Result<Done, CliError> {
    let args = command_line()?;
    let command = args::parse(&args)?;
    block_on(run::run(command))
}

/// Writes `text` and reports whether it all went out; a closed pipe is the
/// reader's choice, and there is nowhere else to say so.
fn emit(mut out: impl Write, text: &str) -> io::Result<()> {
    out.write_all(text.as_bytes())?;
    out.flush()
}

fn finish() -> Exit {
    match execute() {
        Ok(done) => {
            let exit = run::exit_for_done(&done);
            match emit(io::stdout().lock(), &render::done(&done)) {
                Ok(()) => {
                    if emit(io::stderr().lock(), &render::warnings(&done)).is_err() {
                        // A warning that cannot be written has nowhere else to go.
                    }
                    exit
                }
                Err(err) => report(&CliError::Output(err)),
            }
        }
        Err(error) => report(&error),
    }
}

fn report(error: &CliError) -> Exit {
    let line = format!("{}\n", render::error_line(error));
    if emit(io::stderr().lock(), &line).is_err() {
        // Neither stream is writable; the exit code is all that is left.
    }
    run::exit_for_error(error)
}

/// Go's `main` exits 1 on every error (`cmd/litestream/main.go:89-104`), so for Go's own
/// commands, `replicate` and `restore`, a script written for Go sees the same code (decision
/// D47). Two codes stay: 0, and fail-stop's own 4, which Go never reaches (decision D84).
/// `lsr`'s own commands keep a code per class.
fn is_go_command() -> bool {
    std::env::args_os()
        .nth(1)
        .is_some_and(|word| word == "replicate" || word == "restore")
}

fn main() -> ExitCode {
    let exit = finish();
    let code = match exit {
        Exit::Success | Exit::AppStopped => exit.code(),
        Exit::Degraded | Exit::Refused | Exit::Environment | Exit::Usage | Exit::ExecFailed
            if is_go_command() =>
        {
            1
        }
        Exit::Degraded | Exit::Refused | Exit::Environment | Exit::Usage | Exit::ExecFailed => {
            exit.code()
        }
    };
    ExitCode::from(code)
}
