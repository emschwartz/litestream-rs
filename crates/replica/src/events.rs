//! Events, by callback: the replicator thread calls the application's [`EventSink`] once per event,
//! and [`ChannelSink`] is a ready-made bounded queue for an application that would rather poll.
//!
//! The sink runs on the replicator's thread, so it must not block: a slow sink is a slow
//! replicator. A panic in it is caught and counted (`Counters::sink_panics`), never fatal.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use lsr_core::{AppCheckpoint, StopCause, WhyBlocked};
use lsr_ltx::Txid;

use crate::health::VerifyOutcome;

/// Something the replicator did or found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// What one tick of the engine did (never `Idle`).
    Engine(lsr_core::Event),
    /// The application checkpointed, or its automatic checkpoint is attempting checkpoints that
    /// cannot run (`Health::app_notice` says what it costs). `total` counts every
    /// observation since the start.
    AppCheckpoint {
        /// What was seen.
        seen: AppCheckpoint,
        /// Every observation so far.
        total: u64,
    },
    /// The thread caught a panic. Under `Responses::bug = KeepTrying` the engine is rebuilt
    /// from nothing and boots again after the wait; under `Stop` the replicator stops.
    Panicked {
        /// Whether the engine was rebuilt (else it stopped).
        rebuilt: bool,
    },
    /// The boot refused to replicate: the replica holds transactions through `through` that
    /// this database has no record of writing, which neither the live WAL nor a takeover
    /// record vouches for (`lsr_core::TakeoverRecord`). `Stopped(OtherWriter)` follows.
    /// If this database is the right one, `lsr trust-local` writes a takeover record and the
    /// next start replicates above them; if the replica is, `lsr restore` gets those
    /// transactions.
    Refused {
        /// The replica's newest transaction.
        through: Txid,
    },
    /// The replica refused the boot's sweep of leftover scratch files and open multipart
    /// uploads for want of permission, so the sweep is skipped for the rest of this process.
    /// Replication is unaffected, but uploads a killed process left open stay (and are billed)
    /// until a bucket lifecycle rule (`AbortIncompleteMultipartUpload`) or an operator
    /// removes them. Emitted once. `Counters::scratch_sweeps_denied` says the same at any time.
    ScratchSweepDenied,
    /// The engine stopped by a cause and waits for a person.
    Stopped(StopCause),
    /// The replicator now holds the database's write lock, so every write, from any connection
    /// in any process, fails with SQLite's "database is locked" (`SQLITE_BUSY`); reads go on.
    /// Emitted when the lock is taken, and again if the reason changes while it is held.
    /// `Health::writes` says the same at any time.
    WritesBlocked {
        /// Why writes are held.
        why: WhyBlocked,
    },
    /// The replicator let go of the write lock it held: writes go through again.
    WritesAllowed,
    /// The takeover record in the meta directory (`lsr-takeover`) could not be read or did not
    /// decode at start. It is ignored, as if there were none: the boot builds only on what the
    /// live WAL vouches for, and refuses the rest until `lsr trust-local` writes a sound record.
    TakeoverRecordUnusable(TakeoverUnusable),
    /// The engine asked for a takeover record before deleting staged files the replica holds
    /// (`lsr_core::Event::TakeoverWanted`), and it could not be written in the meta directory.
    /// The engine deletes nothing and retries after its backoff.
    TakeoverNotRecorded {
        /// What the file system said.
        kind: std::io::ErrorKind,
    },
    /// The engine spent its takeover record by confirming a put above it
    /// (`lsr_core::Actor::takeover_spent`), and the record could not be deleted from the meta
    /// directory. Deleting it is retried every turn; a restart before it succeeds reads the
    /// stale record back.
    TakeoverNotRemoved {
        /// What the file system said.
        kind: std::io::ErrorKind,
    },
    /// A scheduled verify ended: the restore of `txid` compared with the live database at
    /// `txid`. A failure is a degraded state (`Health::verification`) that names what differed.
    Verified {
        /// The transaction compared.
        txid: Txid,
        /// How it ended.
        outcome: VerifyOutcome,
    },
}

/// Takes every event but `Idle`, on the replicator's thread; must not block.
pub trait EventSink: Send {
    /// One event.
    fn event(&mut self, event: &Event);
}

#[derive(Debug)]
struct Queue {
    events: VecDeque<Event>,
    dropped: u64,
    closed: bool,
}

#[derive(Debug)]
struct Channel {
    capacity: NonZeroUsize,
    queue: Mutex<Queue>,
    ready: Condvar,
}

impl Channel {
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A bounded queue of events: when it is full the oldest event goes, and the drop is counted.
#[derive(Debug)]
pub struct ChannelSink {
    channel: Arc<Channel>,
}

/// The reading end of a [`ChannelSink`].
#[derive(Clone, Debug)]
pub struct EventReceiver {
    channel: Arc<Channel>,
}

impl ChannelSink {
    /// A sink holding at most `capacity` events, and its receiver.
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> (Self, EventReceiver) {
        let channel = Arc::new(Channel {
            capacity,
            queue: Mutex::new(Queue {
                events: VecDeque::with_capacity(capacity.get()),
                dropped: 0,
                closed: false,
            }),
            ready: Condvar::new(),
        });
        let receiver = EventReceiver {
            channel: Arc::clone(&channel),
        };
        (Self { channel }, receiver)
    }
}

impl EventSink for ChannelSink {
    fn event(&mut self, event: &Event) {
        let mut queue = self.channel.lock();
        if queue.events.len() >= self.channel.capacity.get() {
            queue.events.pop_front();
            queue.dropped = queue.dropped.saturating_add(1);
        }
        queue.events.push_back(event.clone());
        drop(queue);
        self.channel.ready.notify_all();
    }
}

impl Drop for ChannelSink {
    fn drop(&mut self) {
        self.channel.lock().closed = true;
        self.channel.ready.notify_all();
    }
}

impl EventReceiver {
    /// The oldest event, if one is queued.
    #[must_use]
    pub fn try_recv(&self) -> Option<Event> {
        self.channel.lock().events.pop_front()
    }

    /// The oldest event, waiting for one; `None` once the sink is gone (the replicator
    /// stopped or took another sink) and the queue is empty.
    #[must_use]
    pub fn recv(&self) -> Option<Event> {
        let mut queue = self.channel.lock();
        loop {
            if let Some(event) = queue.events.pop_front() {
                return Some(event);
            }
            if queue.closed {
                return None;
            }
            queue = self
                .channel
                .ready
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Every queued event, oldest first.
    #[must_use]
    pub fn drain(&self) -> Vec<Event> {
        self.channel.lock().events.drain(..).collect()
    }

    /// Events dropped because the queue was full.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.channel.lock().dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_channel_drops_the_oldest_and_counts_it() {
        let (mut sink, rx) = ChannelSink::new(NonZeroUsize::new(2).unwrap());
        for total in 1..=3 {
            sink.event(&Event::Panicked {
                rebuilt: total == 2,
            });
        }
        assert_eq!(rx.dropped(), 1);
        assert_eq!(
            rx.drain(),
            [
                Event::Panicked { rebuilt: true },
                Event::Panicked { rebuilt: false }
            ]
        );
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn recv_ends_when_the_sink_is_gone() {
        let (mut sink, rx) = ChannelSink::new(NonZeroUsize::new(4).unwrap());
        sink.event(&Event::Panicked { rebuilt: true });
        drop(sink);
        assert_eq!(rx.recv(), Some(Event::Panicked { rebuilt: true }));
        assert_eq!(rx.recv(), None);
    }
}

/// Why the takeover record at start was not used ([`Event::TakeoverRecordUnusable`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeoverUnusable {
    /// The file exists and cannot be read.
    Unreadable {
        /// What the file system said.
        kind: std::io::ErrorKind,
    },
    /// The file does not decode: a write cut short, an edit, a damaged disk.
    Invalid(lsr_core::TakeoverRecordError),
}
