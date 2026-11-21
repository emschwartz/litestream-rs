//! `lsr`: restore, verify and list a Litestream file replica.
//!
//! A thin root: collect arguments, parse, run, print, exit. Stdout carries only
//! the result; every failure is one line on stderr. The stores are async over
//! blocking file I/O and complete without a reactor, so a thread-parking
//! `block_on` of a few lines drives them and no runtime is linked.

mod args;
mod render;
mod replica;
mod replicate;
mod run;

use std::future::Future;
use std::io::{self, Write};
use std::pin::pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use args::UsageError;
use run::{CliError, Done, Exit};

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}

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
                Ok(()) => exit,
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

fn main() -> ExitCode {
    ExitCode::from(finish().code())
}
