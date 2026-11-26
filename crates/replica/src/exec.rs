//! The executor: a loop that polls one future on the calling thread.
//!
//! The stores do their I/O inside `poll` and are ready at once (the file store, the S3 store
//! over ureq), or block on their own runtime inside it (over reqwest). A future that does return
//! `Pending` is woken by its own waker, which unparks this thread; that is the one wait here
//! that is not the runtime's `Park`, since only the future knows when it is ready.
//!
//! A store may keep the waker past the call that showed it: the S3 store keeps the one its
//! upload was last asked about its parts with, and wakes it when a part's request ends on a
//! sender thread (`docs/decisions/D233.md`). By then the replicator is no longer inside the
//! tick, it waits in the runtime's `Park`, so the tick's waker ([`block_on_tick`]) ends that
//! park as well.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use crate::runtime::Park;

struct ThreadWaker {
    thread: Thread,
    /// The park the thread waits in between the futures it runs, when it has one.
    park: Option<Arc<dyn Park>>,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        if let Some(park) = &self.park {
            park.unpark();
        }
        self.thread.unpark();
    }
}

/// Runs `fut` to completion on the calling thread, parking it while the future is pending.
/// The one small executor of the workspace's blocking callers (the replicator thread, the
/// `lsr` commands); it links no runtime.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    run(fut, None)
}

/// As [`block_on`], for the replicator's tick: a wake that comes after the tick returned ends
/// the thread's next wait in `park`, so a part that lands between two ticks starts the next
/// one instead of waiting out the poll interval.
pub(crate) fn block_on_tick<F: Future>(fut: F, park: &Arc<dyn Park>) -> F::Output {
    run(fut, Some(Arc::clone(park)))
}

fn run<F: Future>(fut: F, park: Option<Arc<dyn Park>>) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker {
        thread: thread::current(),
        park,
    }));
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Pending until another thread sets the flag; that thread wakes the waker it was handed,
    /// once, after the flag is set.
    struct WokenFromElsewhere {
        done: Arc<AtomicBool>,
        started: bool,
    }

    impl Future for WokenFromElsewhere {
        type Output = ();

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.done.load(Ordering::SeqCst) {
                return Poll::Ready(());
            }
            if !self.started {
                self.started = true;
                let (done, waker) = (Arc::clone(&self.done), cx.waker().clone());
                thread::spawn(move || {
                    done.store(true, Ordering::SeqCst);
                    waker.wake();
                });
            }
            Poll::Pending
        }
    }

    /// A park that counts its unparks.
    #[derive(Default)]
    struct Counting(std::sync::atomic::AtomicU32);

    impl Park for Counting {
        fn park(&self, _until: Option<lsr_ltx::Timestamp>) {}

        fn unpark(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Ready at once, leaving the waker it was polled with where the test can reach it: what
    /// an upload does with the waker of the tick that asked about its parts.
    struct KeepsItsWaker(Arc<std::sync::Mutex<Option<Waker>>>);

    impl Future for KeepsItsWaker {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            *self.0.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        }
    }

    /// A part's request ends after the tick that started it returned. The replicator is then
    /// waiting in its park, not in the executor, so the waker the tick was polled with must end
    /// that park: without it the next slice of the upload waits for the poll interval.
    #[test]
    fn a_wake_after_the_tick_returned_ends_the_threads_park() {
        let counting = Arc::new(Counting::default());
        let park: Arc<dyn Park> = Arc::<Counting>::clone(&counting);
        let kept = Arc::new(std::sync::Mutex::new(None));
        block_on_tick(KeepsItsWaker(Arc::clone(&kept)), &park);
        assert_eq!(counting.0.load(Ordering::SeqCst), 0, "nothing woke yet");
        let waker = kept.lock().unwrap().take().unwrap();
        thread::spawn(move || waker.wake()).join().unwrap();
        assert_eq!(counting.0.load(Ordering::SeqCst), 1);
    }

    /// A future woken from another thread is polled again: the waker unparks the thread
    /// `block_on` parked.
    #[test]
    fn a_wake_from_another_thread_resumes_the_future() {
        block_on(WokenFromElsewhere {
            done: Arc::new(AtomicBool::new(false)),
            started: false,
        });
    }
}
