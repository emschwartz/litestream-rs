//! Where the parts of uploads are sent: a few lanes that are fed one part at a time.
//!
//! An upload hands each part to its store's [`Lanes`] as a [`Task`] the moment the part is
//! whole and goes on building the next one. `start` never waits: the upload counts the parts
//! it has started and holds its own bound (`upload.rs`), so the queue here is never longer than
//! what the store's live uploads may have started. A task reports its own end to the upload
//! that made it; nothing comes back through the lanes.
//!
//! Three kinds:
//! - [`Inline`] runs the task inside `start`. A transport with no threads gets it, so the
//!   in-memory and fault-injecting transports send one request at a time, in order.
//! - [`Sender`] is for a blocking socket client: up to `most` threads named `s3-part`,
//!   started as parts arrive and kept for the store's life, taking tasks from one queue. A
//!   thread holds the queue and never the store, and ends when the store is dropped. When the
//!   OS refuses the first thread the parts run on the thread that starts them, one at a time.
//! - [`Manual`] (tests and the simulator) keeps every task until it is told which one lands,
//!   so a test decides the order parts end in.
//!
//! Whoever waits for a part calls [`Lanes::help`] first: lanes with no thread of their own run
//! one task on the caller, and the wait cannot outlast the work.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, PoisonError};

// Under `--cfg shuttle` (`just shuttle`) the lanes and what waits on them run on shuttle's
// scheduler, which explores thread interleavings; every other build uses std.
#[cfg(shuttle)]
pub(crate) use shuttle::sync::{Condvar, Mutex, MutexGuard};
#[cfg(shuttle)]
use shuttle::thread;
#[cfg(not(shuttle))]
pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(shuttle))]
use std::thread;

/// One part's work: hash it, send it, report the answer to its upload.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// Where a store's uploads send their parts (the module docs).
pub trait Lanes: Send + Sync {
    /// Takes `task` to run, without waiting for it or for room: the caller bounds how many it
    /// has started.
    fn start(&self, task: Task);

    /// Runs one started task on the calling thread when these lanes have no thread that will;
    /// `false` when it ran none. A caller about to wait for a part asks this first.
    fn help(&self) -> bool;
}

/// Lanes with no thread: a task runs inside `start`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Inline;

impl Lanes for Inline {
    fn start(&self, task: Task) {
        task();
    }

    fn help(&self) -> bool {
        false
    }
}

/// Locks through poisoning: a task that panicked reported its own end already.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The stack of a sender thread: an HTTPS request through ureq and rustls needs 48 KiB (see
/// `lsr-replica`'s `stack` module for how that was measured), and this is about five times
/// that.
// Shuttle's threads have no stack size to set.
#[cfg(not(shuttle))]
const SENDER_STACK: usize = 256 * 1024;

/// The tasks waiting for a thread, and the threads.
#[derive(Default)]
struct Waiting {
    tasks: VecDeque<Task>,
    /// Threads waiting for a task.
    idle: usize,
    threads: usize,
    /// The store is gone: a thread that finds no task ends.
    closed: bool,
}

#[derive(Default)]
struct Queue {
    waiting: Mutex<Waiting>,
    more: Condvar,
}

impl Queue {
    /// A sender thread: takes tasks until the store is gone and none is left.
    fn work(&self) {
        let mut waiting = lock(&self.waiting);
        loop {
            if let Some(task) = waiting.tasks.pop_front() {
                drop(waiting);
                run(task);
                waiting = lock(&self.waiting);
            } else if waiting.closed {
                waiting.threads = waiting.threads.saturating_sub(1);
                return;
            } else {
                waiting.idle = waiting.idle.saturating_add(1);
                waiting = self
                    .more
                    .wait(waiting)
                    .unwrap_or_else(PoisonError::into_inner);
                waiting.idle = waiting.idle.saturating_sub(1);
            }
        }
    }
}

/// Runs a task to its end. One that panics has reported its end to its upload on the way out
/// (its guard), so the thread goes on to the next.
fn run(task: Task) {
    match panic::catch_unwind(AssertUnwindSafe(task)) {
        Ok(()) | Err(_) => {}
    }
}

/// Starts one sender thread; `false` when the OS refuses it.
#[cfg(not(shuttle))]
fn spawn(queue: Arc<Queue>) -> bool {
    thread::Builder::new()
        .name("s3-part".to_owned())
        .stack_size(SENDER_STACK)
        .spawn(move || queue.work())
        .is_ok()
}

/// Shuttle's threads never fail to start.
#[cfg(shuttle)]
fn spawn(queue: Arc<Queue>) -> bool {
    drop(thread::spawn(move || queue.work()));
    true
}

/// Lanes on threads, for a blocking client (the module docs).
pub struct Sender {
    queue: Arc<Queue>,
    most: usize,
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender")
            .field("most", &self.most)
            .finish_non_exhaustive()
    }
}

impl Sender {
    /// Lanes of at most `most` threads. None is started until a part arrives.
    #[must_use]
    pub fn new(most: NonZeroUsize) -> Self {
        Self {
            queue: Arc::new(Queue::default()),
            most: most.get(),
        }
    }

    /// How many threads are alive.
    #[cfg(test)]
    fn threads(&self) -> usize {
        lock(&self.queue.waiting).threads
    }
}

impl Lanes for Sender {
    fn start(&self, task: Task) {
        let grow = {
            let mut waiting = lock(&self.queue.waiting);
            waiting.tasks.push_back(task);
            // A thread for every task no idle one will take, up to the most.
            let grow = waiting.tasks.len() > waiting.idle && waiting.threads < self.most;
            if grow {
                waiting.threads = waiting.threads.saturating_add(1);
            }
            grow
        };
        if !grow || spawn(Arc::clone(&self.queue)) {
            self.queue.more.notify_one();
            return;
        }
        // The OS gave no thread. With another alive the task waits its turn; with none, the
        // caller is the only thread there is.
        let stranded = {
            let mut waiting = lock(&self.queue.waiting);
            waiting.threads = waiting.threads.saturating_sub(1);
            if waiting.threads == 0 {
                std::mem::take(&mut waiting.tasks)
            } else {
                VecDeque::new()
            }
        };
        stranded.into_iter().for_each(run);
    }

    fn help(&self) -> bool {
        false
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        lock(&self.queue.waiting).closed = true;
        self.queue.more.notify_all();
    }
}

/// Which of the tasks started, counted from zero, lands next.
#[cfg(any(test, feature = "testing"))]
type Pick = Box<dyn FnMut(usize) -> usize + Send>;

/// Lanes a test drives: every task waits until the test, or a caller that waits for a part,
/// lands one.
#[cfg(any(test, feature = "testing"))]
pub struct Manual {
    tasks: Mutex<VecDeque<Task>>,
    pick: Mutex<Pick>,
}

#[cfg(any(test, feature = "testing"))]
impl std::fmt::Debug for Manual {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manual")
            .field("started", &self.started())
            .finish_non_exhaustive()
    }
}

#[cfg(any(test, feature = "testing"))]
impl Manual {
    /// Lanes whose waiting callers land the task `pick` names, given how many are started
    /// (taken modulo that count).
    #[must_use]
    pub fn new(pick: impl FnMut(usize) -> usize + Send + 'static) -> Self {
        Self {
            tasks: Mutex::new(VecDeque::new()),
            pick: Mutex::new(Box::new(pick)),
        }
    }

    /// How many tasks are started and have not run.
    #[must_use]
    pub fn started(&self) -> usize {
        lock(&self.tasks).len()
    }

    /// Runs the task at `at`, counted from zero in the order they were started; `false` when
    /// there is none.
    pub fn land(&self, at: usize) -> bool {
        let task = lock(&self.tasks).remove(at);
        task.map(|task| task()).is_some()
    }

    /// Drops the task at `at` without running it, as a process killed before the request
    /// left would; `false` when there is none.
    pub fn lose(&self, at: usize) -> bool {
        let task = lock(&self.tasks).remove(at);
        task.is_some()
    }
}

#[cfg(any(test, feature = "testing"))]
impl Lanes for Manual {
    fn start(&self, task: Task) {
        lock(&self.tasks).push_back(task);
    }

    fn help(&self) -> bool {
        let started = self.started();
        let Some(count) = NonZeroUsize::new(started) else {
            return false;
        };
        let at = (lock(&self.pick))(started).checked_rem(count.get());
        at.is_some_and(|at| self.land(at))
    }
}

#[cfg(test)]
#[cfg(not(shuttle))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// What the tasks of a test share: how many are inside at once, the most that ever were,
    /// how many ended, and a gate that holds them inside.
    #[derive(Default)]
    struct Seen {
        state: Mutex<(usize, usize, usize, bool)>,
        changed: Condvar,
    }

    impl Seen {
        /// A task that waits inside until the gate opens.
        fn task(self: &Arc<Self>) -> Task {
            let seen = Arc::clone(self);
            Box::new(move || {
                let mut state = lock(&seen.state);
                state.0 = state.0.saturating_add(1);
                state.1 = state.1.max(state.0);
                seen.changed.notify_all();
                while !state.3 {
                    state = seen.changed.wait(state).unwrap();
                }
                state.0 = state.0.saturating_sub(1);
                state.2 = state.2.saturating_add(1);
                seen.changed.notify_all();
            })
        }

        fn until(&self, holds: impl Fn(&(usize, usize, usize, bool)) -> bool) {
            let mut state = lock(&self.state);
            while !holds(&state) {
                state = self.changed.wait(state).unwrap();
            }
        }

        fn open(&self) {
            lock(&self.state).3 = true;
            self.changed.notify_all();
        }
    }

    #[test]
    fn inline_lanes_run_a_task_before_start_returns() {
        let ran = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&ran);
        Inline.start(Box::new(move || {
            counted.fetch_add(1, Ordering::Relaxed);
        }));
        assert_eq!(ran.load(Ordering::Relaxed), 1);
        assert!(!Inline.help());
    }

    #[test]
    fn a_sender_runs_as_many_at_once_as_it_has_lanes_and_no_more() {
        let sender = Sender::new(NonZeroUsize::new(3).unwrap());
        assert_eq!(sender.threads(), 0, "no thread before a part");
        let seen = Arc::new(Seen::default());
        for _ in 0..8 {
            sender.start(seen.task());
        }
        // `start` came back eight times with every task still held inside or queued.
        seen.until(|state| state.0 == 3);
        assert_eq!(sender.threads(), 3);
        seen.open();
        seen.until(|state| state.2 == 8);
        assert_eq!(lock(&seen.state).1, 3, "the most inside at once");
        assert_eq!(sender.threads(), 3, "the threads stay for the next part");
    }

    #[test]
    fn a_sender_starts_one_thread_for_parts_that_come_one_at_a_time() {
        let sender = Sender::new(NonZeroUsize::new(4).unwrap());
        let seen = Arc::new(Seen::default());
        seen.open();
        for n in 1..=5 {
            sender.start(seen.task());
            seen.until(|state| state.2 == n);
            // The thread is idle again before the next part comes.
            while lock(&sender.queue.waiting).idle == 0 {
                std::thread::yield_now();
            }
        }
        assert_eq!(sender.threads(), 1);
    }

    #[test]
    fn a_task_that_panics_leaves_its_thread_to_the_next() {
        let sender = Sender::new(NonZeroUsize::MIN);
        sender.start(Box::new(|| panic!("a part's work panicked")));
        let seen = Arc::new(Seen::default());
        seen.open();
        sender.start(seen.task());
        seen.until(|state| state.2 == 1);
        assert_eq!(sender.threads(), 1);
    }

    #[test]
    fn the_threads_end_when_the_sender_is_dropped_and_finish_what_was_started() {
        let sender = Sender::new(NonZeroUsize::new(2).unwrap());
        let seen = Arc::new(Seen::default());
        for _ in 0..4 {
            sender.start(seen.task());
        }
        seen.until(|state| state.0 == 2);
        let queue = Arc::clone(&sender.queue);
        drop(sender);
        seen.open();
        seen.until(|state| state.2 == 4);
        while lock(&queue.waiting).threads > 0 {
            std::thread::yield_now();
        }
    }

    #[test]
    fn manual_lanes_land_what_they_are_told_to_and_lose_what_they_are_told_to() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let task = |n: u8| -> Task {
            let order = Arc::clone(&order);
            Box::new(move || lock(&order).push(n))
        };
        // A waiting caller lands the newest.
        let lanes = Manual::new(|started| started - 1);
        for n in 1..=4 {
            lanes.start(task(n));
        }
        assert_eq!(lanes.started(), 4);
        assert!(lanes.land(1));
        assert!(lanes.lose(0));
        assert!(lanes.help());
        assert!(lanes.help());
        assert!(!lanes.help());
        assert!(!lanes.land(0));
        assert_eq!(*lock(&order), [2, 4, 3]);
    }
}
