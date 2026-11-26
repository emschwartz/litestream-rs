//! What a tick's I/O costs in simulated time, and the bound every tick is judged by.
//!
//! The stores answer at once, so without a cost a job that would take minutes against a real
//! bucket is one cheap tick, and nothing the simulator measures in time can see it hold every
//! other task off. A [`Profile`] prices each store's calls: a latency per request and a
//! bandwidth for the bytes read and written. The counts come from the recorders (what the
//! stores were asked and answered), never from the engine's reports, and the price is
//! arithmetic on them, so a run's times are as deterministic as its seed.
//!
//! The bound ([`Allowance`]) is what one tick may cost, fixed by configuration before any run
//! (`docs/decisions/D265.md` gives the numbers and their derivation): on each store, a fixed
//! number of requests plus a few for each file put there, a slice of input and the largest
//! single read the engine asks for, and a slice of output to the replica or what one sync may
//! stage to staging. It holds of every tick whatever event ends it, so any job that does
//! unbounded work in one tick fails it, not only the jobs written so far.

use std::time::Duration;

use crate::oracle::{Bound, Violation, within};
use crate::store::RecorderCounts;

/// One store's price: a latency per request and a bandwidth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// Microseconds each request waits before its first byte.
    pub latency_us: u64,
    /// Bytes a second, read or written.
    pub bytes_per_sec: u64,
}

impl Link {
    /// Microseconds `requests` requests moving `bytes` bytes take, one after another.
    #[must_use]
    pub fn micros(&self, requests: u64, bytes: u64) -> u64 {
        let moving = u128::from(bytes)
            .saturating_mul(1_000_000)
            .checked_div(u128::from(self.bytes_per_sec.max(1)))
            .unwrap_or(u128::MAX);
        requests
            .saturating_mul(self.latency_us)
            .saturating_add(u64::try_from(moving).unwrap_or(u64::MAX))
    }

    /// What one tick's calls on this store cost.
    #[must_use]
    pub fn spent(&self, counts: &RecorderCounts) -> u64 {
        self.micros(requests(counts), moved(counts))
    }
}

/// The requests a tick's counts name: every call but a chunk written into an open upload,
/// which streams on the request its upload began.
#[must_use]
pub fn requests(counts: &RecorderCounts) -> u64 {
    [
        counts.lists,
        counts.reads,
        counts.begins,
        counts.commits,
        counts.deletes,
    ]
    .iter()
    .fold(0, |sum, n| sum.saturating_add(*n))
}

/// The bytes a tick's counts moved, read and written.
#[must_use]
pub const fn moved(counts: &RecorderCounts) -> u64 {
    counts.read_bytes.saturating_add(counts.written_bytes)
}

/// What each store costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Profile {
    /// The replica: an object store across a network.
    pub remote: Link,
    /// Local staging: a disk.
    pub staging: Link,
}

impl Profile {
    /// The prices every run is judged at (`docs/decisions/D265.md`): the replica at 10 ms a
    /// request and 1 MiB a second, staging at 0.1 ms and 16 MiB a second. Scaled down with
    /// the databases, so a large-profile snapshot of 10 MiB takes seconds as a real one of
    /// tens of gigabytes takes minutes.
    pub const DEFAULT: Self = Self {
        remote: Link {
            latency_us: 10_000,
            bytes_per_sec: 1 << 20,
        },
        staging: Link {
            latency_us: 100,
            bytes_per_sec: 16 << 20,
        },
    };

    /// What one tick's calls cost, both stores' one after the other.
    #[must_use]
    pub fn tick(&self, remote: &RecorderCounts, staging: &RecorderCounts) -> Duration {
        Duration::from_micros(
            self.remote
                .spent(remote)
                .saturating_add(self.staging.spent(staging)),
        )
    }
}

/// Requests any tick may make on a store besides those for the files it puts: a boot's
/// listings (ten levels, three times over) and its reads (a header and a body for the newest
/// file of each level, and the upload lane's read-back), or a merge's first slice opening each
/// of its inputs (the swarm's largest seen was 60 reads and 10 listings), with room to spare.
pub const BASE_REQUESTS: u64 = 128;

/// Staging reads one part the S3 replica sends may make of its staged bytes: one to hash the
/// part and one to send it (`lsr-store-s3`'s lanes).
pub const REREADS_PER_PART: u64 = 2;

/// Requests a boot may make per staged file it finds: a header and a body read, a copy put
/// under its own name (a begin and a commit), the delete of the old name, and room to spare.
pub const REQUESTS_PER_STAGED_FILE: u64 = 8;

/// Requests a file put in the tick may add: its begin, its commit, the read of its staged
/// copy, and a delete of what it replaced.
pub const REQUESTS_PER_PUT: u64 = 4;

/// The largest single read the engine asks a store for (`ReadLen::CHUNK`, 1 MiB): a slice
/// that reads on from a stream may end one such read past its bytes.
pub const READ_PIECE: u64 = 1 << 20;
/// Which store a bound is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    /// The replica.
    Remote,
    /// Local staging.
    Staging,
}

/// What one tick may do, from the run's configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allowance {
    /// A tick's bytes of staged files (`UploadConfig::bytes_per_tick`).
    pub slice: u64,
    /// The most bytes one tick may stage (`resources::Limits::staged_bytes`): a sync's
    /// backlog, or the whole database when the engine copies it to level 0 in one unit, as
    /// Go's sync does (`docs/decisions/D255.md`).
    pub staged: u64,
    /// The S3 replica's part size, when it sends in parts: each part it sends reads its bytes
    /// from the staged file again, [`REREADS_PER_PART`] staging requests a part.
    pub part: Option<u64>,
}

impl Allowance {
    /// Bytes one tick may write to `store`: a slice and a file's header and trailer to the
    /// replica, what one sync may stage to staging.
    #[must_use]
    pub const fn written(&self, store: Store) -> u64 {
        match store {
            Store::Remote => self.slice.saturating_add(crate::budget::PER_FILE),
            Store::Staging => self.staged,
        }
    }

    /// Bytes one tick may read from either store: a slice, and one read past it.
    #[must_use]
    pub const fn read(&self) -> u64 {
        self.slice.saturating_add(READ_PIECE)
    }

    /// Requests one tick may make of a store it put `puts` files to, and `extra` more.
    #[must_use]
    pub const fn requests(puts: u64, extra: u64) -> u64 {
        BASE_REQUESTS
            .saturating_add(REQUESTS_PER_PUT.saturating_mul(puts))
            .saturating_add(extra)
    }

    /// The bound on one tick's calls to `store`, in microseconds at `link`, for a tick that
    /// put `puts` files there and may make `extra` requests more.
    #[must_use]
    pub fn micros(&self, store: Store, link: &Link, puts: u64, extra: u64) -> u64 {
        link.micros(
            Self::requests(puts, extra),
            self.written(store).saturating_add(self.read()),
        )
    }

    /// What a tick's calls to `store` cost as judged: their price, or, when the requests,
    /// the bytes read or the bytes written pass their own share of the bound, the bound and
    /// the price of every excess. One kind of work cannot hide in another's share: a tick
    /// that reads a whole input while writing a slice fails though both together fit.
    #[must_use]
    pub fn judged(
        &self,
        store: Store,
        link: &Link,
        counts: &RecorderCounts,
        puts: u64,
        extra: u64,
    ) -> u64 {
        let over = link
            .micros(
                requests(counts).saturating_sub(Self::requests(puts, extra)),
                0,
            )
            .saturating_add(
                link.micros(
                    0,
                    counts
                        .read_bytes
                        .saturating_sub(self.read())
                        .saturating_add(counts.written_bytes.saturating_sub(self.written(store))),
                ),
            );
        if over == 0 {
            link.spent(counts)
        } else {
            self.micros(store, link, puts, extra).saturating_add(over)
        }
    }
}

/// Staging reads the S3 replica's own retry wrapper may make of a staged body it is putting
/// whole, past the upload's own read: the S3 store hashes the body in one read and sends it in
/// another, each counted by the recorder as a re-read given (`RecorderCounts::rereads_given`).
/// The bound allows a staging request for each one, so a tick of many small uploads to an S3
/// replica stays within it. A defect allowance (<https://github.com/emschwartz/litestream-rs/issues/478>): when the store hashes and
/// sends from one read, the re-reads go, the reach count `tick_cost.s3_rereads_allowed` stays at
/// zero and `liveness::an_s3_put_reads_its_staged_body_again` fails, and this allowance goes.
#[must_use]
pub const fn s3_rereads(remote: &RecorderCounts) -> u64 {
    remote.rereads_given
}

/// Judges one tick's cost on each store against its bound. `spare` holds what a boot may
/// spend past the bound ([`Spare`]); what the tick spent past its own bound is taken from it.
///
/// # Errors
/// [`Violation::ResourceBound`] of [`Bound::RemoteTickMicros`] or [`Bound::StagingTickMicros`].
pub fn judge(
    profile: &Profile,
    allowance: Allowance,
    remote: (&RecorderCounts, u64),
    staging: (&RecorderCounts, u64),
    spare: &mut Spare,
) -> Result<(), Violation> {
    // Each part the replica sent this tick may have read its bytes from staging again.
    let parts = allowance.part.map_or(0, |part| {
        let parts = remote
            .0
            .written_bytes
            .div_ceil(part.max(1))
            .saturating_add(1);
        parts.saturating_mul(REREADS_PER_PART)
    });
    let staging_extra = parts.saturating_add(s3_rereads(remote.0));
    for (store, link, (counts, puts), extra, what) in [
        (
            Store::Remote,
            &profile.remote,
            remote,
            0,
            Bound::RemoteTickMicros,
        ),
        (
            Store::Staging,
            &profile.staging,
            staging,
            staging_extra,
            Bound::StagingTickMicros,
        ),
    ] {
        let limit = allowance.micros(store, link, puts, extra);
        let spent = allowance.judged(store, link, counts, puts, extra);
        let excused = match store {
            Store::Remote => 0,
            Store::Staging => spare.take(spent.saturating_sub(limit)),
        };
        within(what, spent.saturating_sub(excused), limit)?;
    }
    Ok(())
}

/// What a boot may spend past one tick's bound on staging, in microseconds, for its own tick
/// only (emptied on the first tick after it): every staged file read whole by the boot's check
/// of what it finds there, and moved to its own name. Staging is the local disk.
///
/// No boot read of the replica is allowed past the bound (`docs/decisions/D265.md`).
/// Filled from the stores' own sizes and counts, never from the engine's; a tick that spends
/// more than this and its own bound fails.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Spare {
    staging: u64,
}

impl Spare {
    /// A boot that finds `staged` files holding `bytes` bytes staged.
    #[must_use]
    pub fn boot(profile: &Profile, (staged, bytes): (u64, u64)) -> Self {
        Self {
            staging: profile.staging.micros(
                staged.saturating_mul(REQUESTS_PER_STAGED_FILE),
                bytes.saturating_mul(2),
            ),
        }
    }

    /// Takes up to `want` microseconds of the spare; what it gave.
    pub fn take(&mut self, want: u64) -> u64 {
        let given = want.min(self.staging);
        self.staging = self.staging.saturating_sub(given);
        given
    }
}

#[cfg(test)]
mod tests;
