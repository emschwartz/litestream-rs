//! Events, by callback: the replicator thread calls the host's [`EventSink`] once per event,
//! and [`ChannelSink`] is a ready-made bounded queue for a host that would rather poll.
//!
//! The sink runs on the replicator's thread, so it must not block: a slow sink is a slow
//! replicator. A panic in it is caught and counted (`Counters::sink_panics`), never fatal.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use lsr_core::{HaltCause, HostCheckpoint};
use lsr_ltx::Txid;

use crate::health::VerifyOutcome;

/// Something the replicator did or found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// What one tick of the engine did (never `Idle`).
    Engine(lsr_core::Event),
    /// Someone else checkpointed the database: the host promised not to. `total` counts every
    /// one seen since the start.
    ForeignCheckpoint {
        /// Every one seen so far.
        total: u64,
    },
    /// The host checkpointed, or its automatic checkpoint is attempting checkpoints that
    /// cannot run (`Health::host_notice` says what it costs). `total` counts every
    /// observation since the start.
    HostCheckpoint {
        /// What was seen.
        seen: HostCheckpoint,
        /// Every observation so far.
        total: u64,
    },
    /// The thread caught a panic. Under `Responses::bug = KeepTrying` the engine is rebuilt
    /// from nothing and boots again after the wait; under `Stop` the replicator stops.
    Panicked {
        /// Whether the engine was rebuilt (else it stopped).
        rebuilt: bool,
    },
    /// The engine stopped by a cause and waits for a person.
    Stopped(HaltCause),
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
            sink.event(&Event::ForeignCheckpoint { total });
        }
        assert_eq!(rx.dropped(), 1);
        assert_eq!(
            rx.drain(),
            [
                Event::ForeignCheckpoint { total: 2 },
                Event::ForeignCheckpoint { total: 3 }
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
