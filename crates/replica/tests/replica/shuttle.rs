//! Schedule exploration of the replicator thread's command and drain handshake (`just shuttle`).
//!
//! Shuttle replaces `std::sync` with versions it schedules, so the real `Shared` (std locks
//! behind a private type) cannot run under it. This is a model of the same protocol, one
//! primitive for one primitive, kept small enough to read against `replicator.rs` and
//! `runtime.rs`:
//!
//! - `Commands` is `replicator::Commands`: slots, not a queue, set under one lock;
//!   `Shared::command` sets a slot and then unparks the thread.
//! - `Park` is `runtime::fake::FakeTime`'s park: a `woken` flag and a condvar, so an unpark
//!   before the park is not lost.
//! - `Worker::turn` takes the commands, does the pending work, publishes `State` and wakes
//!   every waiter; a stop makes the next turn a drain that ends the thread with an outcome.
//! - A waiter blocks on the state's condvar until the outcome is set.
//!
//! The invariants, over many random interleavings: no command is lost (a stop always ends
//! the thread, a snapshot request made before the thread ended was seen by a turn or by the
//! drain), commands are two slots however many are sent, a drain syncs every commit made before the stop was asked,
//! every waiter wakes to the one outcome, and the thread is joined.
//! A lost wakeup shows as shuttle's deadlock error, with the schedule that caused it.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "a test model: the counters are tiny"
)]

use std::sync::PoisonError;

use shuttle::sync::atomic::{AtomicU64, Ordering};
use shuttle::sync::{Arc, Condvar, Mutex};
use shuttle::thread;

/// How many random schedules each test explores.
fn iterations() -> usize {
    std::env::var("SHUTTLE_ITERATIONS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(200)
}

/// `runtime::fake::FakeTime`'s park.
#[derive(Default)]
struct Park {
    woken: Mutex<bool>,
    signal: Condvar,
}

impl Park {
    fn park(&self) {
        let mut woken = self.woken.lock().unwrap_or_else(PoisonError::into_inner);
        while !std::mem::take(&mut *woken) {
            woken = self
                .signal
                .wait(woken)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn unpark(&self) {
        *self.woken.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.signal.notify_all();
    }
}

/// `replicator::Commands`, less the event sink.
#[derive(Default)]
struct Commands {
    stop: Option<u64>,
    snapshot: bool,
}

/// `replicator::State`, less health.
#[derive(Default)]
struct State {
    outcome: Option<Outcome>,
    stopping: bool,
}

/// How the thread ended.
#[derive(Clone, Copy, Debug)]
struct Outcome {
    /// Commits the final drain had synced.
    synced: u64,
    /// The deadline the stop carried.
    deadline: u64,
    /// Snapshot requests the thread had acted on.
    snapshots: u64,
}

struct Shared {
    park: Park,
    commands: Mutex<Commands>,
    state: Mutex<State>,
    changed: Condvar,
    /// Commits the application made (the database's WAL growing).
    committed: AtomicU64,
    /// Snapshot requests made.
    asked_snapshots: AtomicU64,
}

impl Shared {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            park: Park::default(),
            commands: Mutex::new(Commands::default()),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            committed: AtomicU64::new(0),
            asked_snapshots: AtomicU64::new(0),
        })
    }

    /// `Shared::command`: set a slot under the lock, then unpark.
    fn command(&self, set: impl FnOnce(&mut Commands)) {
        set(&mut self.commands.lock().unwrap_or_else(PoisonError::into_inner));
        self.park.unpark();
    }

    /// `Shared::ask_stop`.
    fn ask_stop(&self, deadline: u64) {
        self.update(|state| state.stopping = true);
        self.command(|commands| {
            let earlier = commands.stop.map_or(deadline, |was| was.min(deadline));
            commands.stop = Some(earlier);
        });
    }

    /// `Replicator::request_snapshot`.
    fn request_snapshot(&self) {
        self.asked_snapshots.fetch_add(1, Ordering::SeqCst);
        self.command(|commands| commands.snapshot = true);
    }

    /// `Shared::update`: change the state and wake every waiter.
    fn update<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        let out = change(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
        out
    }

    /// A caller's `stop(..).wait()`: blocks until the thread says how it ended.
    fn wait_outcome(&self) -> Outcome {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(outcome) = state.outcome {
                return outcome;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// The thread: `Worker::main_loop`.
fn worker(shared: &Shared) {
    let mut snapshots = 0;
    loop {
        let taken = std::mem::take(
            &mut *shared
                .commands
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if taken.snapshot {
            snapshots += 1;
        }
        shared.update(|_| ());
        if let Some(deadline) = taken.stop {
            // The drain: one more look, so a commit that landed during the tick is synced.
            let synced = shared.committed.load(Ordering::SeqCst);
            // A snapshot slot set after the take above is still seen by the drain.
            let late = std::mem::take(
                &mut *shared
                    .commands
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            if late.snapshot {
                snapshots += 1;
            }
            shared.update(|state| {
                state.outcome = Some(Outcome {
                    synced,
                    deadline,
                    snapshots,
                });
            });
            return;
        }
        shared.park.park();
    }
}

#[test]
fn a_stop_always_ends_the_thread_and_drains_what_was_committed_before_it() {
    shuttle::check_random(
        || {
            let shared = Shared::new();
            let thread = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || worker(&shared))
            };
            // Two handles ask for a stop. Each notes the commits that came before its ask; the
            // drain follows whichever ask the thread took first, so it owes the fewer of the two.
            let app = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || {
                    shared.committed.fetch_add(1, Ordering::SeqCst);
                    shared.committed.fetch_add(1, Ordering::SeqCst);
                    let before = shared.committed.load(Ordering::SeqCst);
                    shared.ask_stop(9);
                    before
                })
            };
            let second = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || {
                    let before = shared.committed.load(Ordering::SeqCst);
                    shared.ask_stop(4);
                    before
                })
            };
            let owed = app.join().unwrap().min(second.join().unwrap());
            let outcome = shared.wait_outcome();
            thread.join().unwrap();
            assert!(
                outcome.synced >= owed,
                "the drain synced {} of the {owed} commits made before the stop",
                outcome.synced
            );
            // The thread may have taken the first stop (9) before the second landed.
            assert!(
                outcome.deadline == 4 || outcome.deadline == 9,
                "{outcome:?}"
            );
        },
        iterations(),
    );
}

#[test]
fn a_snapshot_request_is_never_lost_before_the_thread_ends() {
    shuttle::check_random(
        || {
            let shared = Shared::new();
            let thread = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || worker(&shared))
            };
            let asker = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || {
                    shared.request_snapshot();
                    shared.request_snapshot();
                    shared.ask_stop(1);
                })
            };
            asker.join().unwrap();
            let outcome = shared.wait_outcome();
            thread.join().unwrap();
            // Slots are idempotent: two requests may make one snapshot, never none.
            assert!(
                (1..=2).contains(&outcome.snapshots),
                "{} snapshots for two requests: {outcome:?}",
                outcome.snapshots
            );
        },
        iterations(),
    );
}

#[test]
fn every_waiter_wakes_to_the_one_outcome_however_many_commands_arrive() {
    shuttle::check_random(
        || {
            let shared = Shared::new();
            let thread = {
                let shared = Arc::clone(&shared);
                thread::spawn(move || worker(&shared))
            };
            let waiters: Vec<_> = (0..3)
                .map(|_| {
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || shared.wait_outcome().deadline)
                })
                .collect();
            let askers: Vec<_> = (0..4_u64)
                .map(|n| {
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || {
                        shared.request_snapshot();
                        shared.ask_stop(10 + n);
                    })
                })
                .collect();
            for asker in askers {
                asker.join().unwrap();
            }
            let deadlines: Vec<u64> = waiters.into_iter().map(|w| w.join().unwrap()).collect();
            thread.join().unwrap();
            assert!(
                deadlines
                    .iter()
                    .all(|deadline| Some(deadline) == deadlines.first()),
                "every waiter saw the one outcome: {deadlines:?}"
            );
        },
        iterations(),
    );
}
