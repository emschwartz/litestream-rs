//! The executor: a loop that polls one future on the calling thread.
//!
//! The stores do their I/O inside `poll` and are ready at once (the file store, the S3 store
//! over ureq), or block on their own runtime inside it (over reqwest). A future that does return
//! `Pending` is woken by its own waker, which unparks this thread; that is the one wait here
//! that is not the runtime's `Park`, since only the future knows when it is ready.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs `fut` to completion on the calling thread.
pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
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
