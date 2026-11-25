//! A latency-modelling transport on a virtual clock, for the request-shape gates.
//!
//! The wall time of a restore or an upload is the number of sequential round
//! trips on its critical path times the link's latency, plus transfer time. A
//! test that sleeps cannot hold a bound on that (AGENTS.md bans sleeps), and a
//! test that only counts requests cannot see a batch that was serialised. This
//! transport models the link instead: a lone request is one round trip, and a
//! batch of `n` requests at limit `w` is `ceil(n / w)` waves, each as long as
//! its slowest member. The virtual clock adds each wave's time and nothing
//! sleeps, so a test reads the critical-path depth and the elapsed time of the
//! store's own batching exactly and in microseconds of real time.
//!
//! The requests are really sent, one after another, to the transport inside, so
//! the answers are the fake bucket's and the run is deterministic.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "tallies and virtual time on small counts in test code"
)]

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use lsr_store_s3::{HttpRequest, HttpResponse, Lanes, Shape, Task, Transport, TransportError};

/// One round trip with no payload: a plausible bucket far from its client.
pub const ROUND_TRIP: Duration = Duration::from_millis(40);

/// What one stream moves per second once a request is in flight.
const STREAM_BYTES_PER_SECOND: u64 = 80_000_000;

/// Requests sent, by kind, and how the batches were cut.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Tally {
    /// ListObjectsV2 requests.
    pub list: usize,
    /// HEAD requests.
    pub head: usize,
    /// GET requests.
    pub get: usize,
    /// Parts of multipart uploads.
    pub part: usize,
    /// DeleteObjects requests.
    pub delete: usize,
    /// Every other shape (PUT, create, complete, abort).
    pub other: usize,
    /// Sequential waves of requests: the critical-path depth in round trips.
    pub rounds: usize,
    /// The most requests that were ever in flight at once.
    pub widest: usize,
    /// Virtual time spent: each wave costs [`ROUND_TRIP`] plus its largest body at stream speed.
    pub elapsed: Duration,
}

impl Tally {
    /// Every request sent.
    pub fn requests(&self) -> usize {
        self.list + self.head + self.get + self.part + self.delete + self.other
    }

    fn add(&mut self, request: &HttpRequest) {
        let slot = match Shape::of(request) {
            Some(Shape::List) => &mut self.list,
            Some(Shape::Head) => &mut self.head,
            Some(Shape::Get) => &mut self.get,
            Some(Shape::UploadPart) => &mut self.part,
            Some(Shape::DeleteObjects) => &mut self.delete,
            _ => &mut self.other,
        };
        *slot += 1;
    }
}

/// The bytes a wave moves on its slowest stream, at stream speed.
fn transfer(bytes: usize) -> Duration {
    let bytes = u64::try_from(bytes).unwrap();
    Duration::from_micros(bytes * 1_000_000 / STREAM_BYTES_PER_SECOND)
}

/// Wraps a transport and tallies what crosses it on a virtual clock. Clones
/// share one tally, so the test keeps a handle after the store takes its own.
pub struct Link<T>(Arc<Shared<T>>);

impl<T> Clone for Link<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

struct Shared<T> {
    inner: T,
    tally: Mutex<Tally>,
    /// Parts of uploads started and not landed.
    started: Mutex<Vec<Task>>,
    /// While a wave of parts lands: how many it holds and its largest payload.
    wave: Mutex<Option<(usize, usize)>>,
}

impl<T: Transport> Link<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(Shared {
            inner,
            tally: Mutex::new(Tally::default()),
            started: Mutex::new(Vec::new()),
            wave: Mutex::new(None),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, Tally> {
        self.0.tally.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Everything tallied so far.
    pub fn tally(&self) -> Tally {
        *self.lock()
    }

    /// What `op` added to the tally, so a test measures one operation of many.
    pub fn measure<R>(&self, op: impl FnOnce() -> R) -> (R, Tally) {
        let before = self.tally();
        let out = op();
        let after = self.tally();
        let delta = Tally {
            list: after.list - before.list,
            head: after.head - before.head,
            get: after.get - before.get,
            part: after.part - before.part,
            delete: after.delete - before.delete,
            other: after.other - before.other,
            rounds: after.rounds - before.rounds,
            widest: after.widest,
            elapsed: after.elapsed.saturating_sub(before.elapsed),
        };
        (out, delta)
    }
}

/// The bytes a request carries out plus the bytes its answer carries back.
fn payload(request: &HttpRequest, answer: &Result<HttpResponse, TransportError>) -> usize {
    request.body().len() + answer.as_ref().map_or(0, |response| response.body().len())
}

impl<T: Transport> Transport for Link<T> {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let sent = request.clone();
        let answer = self.0.inner.send(request);
        let mut tally = self.lock();
        tally.add(&sent);
        let mut wave = self.0.wave.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((members, bytes)) = wave.as_mut() {
            // One of a wave of parts: the wave is the round trip.
            *members += 1;
            *bytes = (*bytes).max(payload(&sent, &answer));
            return answer;
        }
        tally.rounds += 1;
        tally.widest = tally.widest.max(1);
        tally.elapsed += ROUND_TRIP + transfer(payload(&sent, &answer));
        answer
    }

    /// The link's own lanes: parts stay in flight until the upload waits for one.
    fn lanes(&self, _most: NonZeroUsize) -> Arc<dyn Lanes> {
        Arc::new(self.clone())
    }

    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        let width = limit.get();
        let mut answers = Vec::with_capacity(requests.len());
        let mut wave_bytes = 0;
        let mut in_wave = 0;
        let mut tally = Tally::default();
        for request in requests {
            let sent = request.clone();
            let answer = self.0.inner.send(request);
            tally.add(&sent);
            wave_bytes = wave_bytes.max(payload(&sent, &answer));
            in_wave += 1;
            answers.push(answer);
            if in_wave == width {
                tally.rounds += 1;
                tally.widest = tally.widest.max(in_wave);
                tally.elapsed += ROUND_TRIP + transfer(wave_bytes);
                (wave_bytes, in_wave) = (0, 0);
            }
        }
        if in_wave > 0 {
            tally.rounds += 1;
            tally.widest = tally.widest.max(in_wave);
            tally.elapsed += ROUND_TRIP + transfer(wave_bytes);
        }
        let mut total = self.lock();
        total.list += tally.list;
        total.head += tally.head;
        total.get += tally.get;
        total.part += tally.part;
        total.delete += tally.delete;
        total.other += tally.other;
        total.rounds += tally.rounds;
        total.widest = total.widest.max(tally.widest);
        total.elapsed += tally.elapsed;
        answers
    }
}

/// The parts of uploads on the model link: every part started is in flight until a caller
/// has to wait for one, and then all of them land as one wave, as long as its slowest
/// member. The upload bounds how many it starts, so a wave is never wider than that.
impl<T: Transport> Lanes for Link<T> {
    fn start(&self, task: Task) {
        self.0
            .started
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
    }

    fn help(&self) -> bool {
        let tasks = std::mem::take(
            &mut *self
                .0
                .started
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if tasks.is_empty() {
            return false;
        }
        *self.0.wave.lock().unwrap_or_else(PoisonError::into_inner) = Some((0, 0));
        for task in tasks {
            task();
        }
        let wave = self
            .0
            .wave
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((members, bytes)) = wave {
            let mut tally = self.lock();
            tally.rounds += 1;
            tally.widest = tally.widest.max(members);
            tally.elapsed += ROUND_TRIP + transfer(bytes);
        }
        true
    }
}
