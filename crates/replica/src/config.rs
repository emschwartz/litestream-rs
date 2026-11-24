//! How a replicator runs: Go's behaviour by default (`docs/PLAN.md`, the Go-compatibility rule).
//!
//! A setting that cannot be zero is a type that cannot hold zero ([`NonZeroDuration`],
//! `NonZeroU32`, `NonZeroU64`), so Go's "zero means the default" cannot happen here, and a
//! setting that can be off is an `Option`.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{
    ActorConfig, CheckpointConfig, IfReplicationStops, Maintenance, MaxUnreplicatedAge, Responses,
    SQLITE_AUTOCHECKPOINT, SyncConfig, UploadConfig,
};

/// A duration that is never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    /// One second.
    pub const SECOND: Self = Self(Duration::from_secs(1));

    /// `None` for zero.
    #[must_use]
    pub const fn new(duration: Duration) -> Option<Self> {
        if duration.is_zero() {
            None
        } else {
            Some(Self(duration))
        }
    }

    /// A whole number of seconds.
    #[must_use]
    pub const fn from_secs(secs: NonZeroU64) -> Self {
        Self(Duration::from_secs(secs.get()))
    }

    /// The duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

/// Whether the application says when it holds a long read (TRUNCATE needs the application's word).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReaderTracking {
    /// The application gives no signal, so a reader may always be running: the replicator never
    /// runs a TRUNCATE checkpoint, only PASSIVE ones.
    #[default]
    Untracked,
    /// The application holds a [`crate::ReaderGuard`] for every long read. TRUNCATE runs only while
    /// none is held; while one is, the checkpoint reports busy and waits its backoff.
    Tracked,
}

/// What one sync step and one upload tick may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncLimits {
    /// A step stops after the first transaction that brings it to this many WAL bytes; `None`
    /// is no cap. A snapshot ignores it.
    pub wal_bytes_per_step: Option<NonZeroU64>,
    /// Files uploaded per tick before the other tasks get a turn; `None` is all staged.
    pub files_per_upload: Option<NonZeroU32>,
}

impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            wal_bytes_per_step: SyncConfig::default().cap,
            files_per_upload: UploadConfig::default().files_per_tick,
        }
    }
}

/// How a replicator runs. Go's behaviour by default. The event sink is not part of it
/// ([`crate::Replicator::start_with_events`], [`crate::Replicator::set_events`]).
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// Go's meta directory, `<dir>/.<name>-litestream` beside the database unless set, so Go
    /// and this library resume from each other's staged files.
    pub meta_dir: Option<PathBuf>,
    /// How often the WAL is polled for new commits (Go: 1 s).
    pub poll_interval: NonZeroDuration,
    /// The first retry wait, doubling to 5 minutes (Go's sync interval and backoff cap).
    pub retry_interval: NonZeroDuration,
    /// Go: PASSIVE at 1,000 pages or every minute, TRUNCATE at 121,359 pages (`db.go:32-35`).
    /// The TRUNCATE threshold applies only under [`ReaderTracking::Tracked`].
    pub checkpoint: CheckpointConfig,
    /// Compaction levels, the snapshot interval and retention, and level 0 retention.
    pub maintenance: Maintenance,
    /// Bytes per sync step and files per upload tick.
    pub sync_limits: SyncLimits,
    /// What each persistent failure does. Default [`Responses::GO_LIKE`]: keep trying,
    /// stop on another writer.
    pub responses: Responses,
    /// Whether the application signals long reads.
    pub readers: ReaderTracking,
    /// How long a drop may drain (Go's shutdown sync timeout, 30 s, `db.go:36`).
    pub shutdown_grace: Duration,
    /// The `wal_autocheckpoint` the application's connections run, in frames, or `None` when every
    /// one of them sets it to 0 (`app_connection` does). Default: SQLite's own, 1000, since
    /// the replicator cannot read another connection's setting. It decides nothing; it only
    /// lets [`crate::Health::app_notice`] infer the application's checkpoint attempts that cannot
    /// run, which the wal-index alone cannot tell from an application that runs none.
    pub app_autocheckpoint: Option<NonZeroU32>,
    /// How often the scheduled verify runs, or `None` (the default, as in Go) for never: Go's
    /// `validation.interval`. Each run waits for the replica to hold everything staged, reads
    /// the live database's pages at that transaction on the replicator's thread (one read of
    /// the database file, as a level-9 snapshot reads it), then lists the replica and restores
    /// that transaction on a thread of its own, one run at a time, and compares the two. A
    /// mismatch is a degraded state in [`crate::Health::verification`], never a stop.
    pub verify_interval: Option<NonZeroDuration>,
    /// What a fail-stop trip does to the application (`if-replication-stops`, decisions D13
    /// and D14): `HoldStart` by default. For the library, a held gate, and a boot not yet
    /// judged except under `Report`, hold the application's writes with the write lock the
    /// replicator keeps (`docs/decisions/D192.md`); see [`crate::Health::held_by`] and
    /// [`crate::Health::writes`].
    pub if_replication_stops: IfReplicationStops,
    /// How long a commit may stay off the replica before fail-stop trips for it (decision
    /// D22): 10 minutes by default, at least 5.
    pub max_unreplicated_age: MaxUnreplicatedAge,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            meta_dir: None,
            poll_interval: NonZeroDuration::SECOND,
            retry_interval: NonZeroDuration::SECOND,
            checkpoint: CheckpointConfig::default(),
            maintenance: Maintenance::go_default(),
            sync_limits: SyncLimits::default(),
            responses: Responses::GO_LIKE,
            readers: ReaderTracking::Untracked,
            shutdown_grace: Duration::from_secs(30),
            app_autocheckpoint: Some(SQLITE_AUTOCHECKPOINT),
            verify_interval: None,
            if_replication_stops: IfReplicationStops::HoldStart,
            max_unreplicated_age: MaxUnreplicatedAge::DEFAULT,
        }
    }
}

impl Config {
    /// The meta directory for `database`: [`Config::meta_dir`] when set, else Go's
    /// `.<name>-litestream` beside it. `None` when the path names no file.
    #[must_use]
    pub fn meta_dir_for(&self, database: &Path) -> Option<PathBuf> {
        if let Some(dir) = &self.meta_dir {
            return Some(dir.clone());
        }
        let name = database.file_name()?.to_str()?;
        let parent = database.parent().unwrap_or_else(|| Path::new(""));
        Some(parent.join(format!(".{name}-litestream")))
    }

    /// The engine's configuration. Untracked readers turn the TRUNCATE threshold off.
    pub(crate) fn actor(&self) -> ActorConfig {
        let checkpoint = match self.readers {
            ReaderTracking::Untracked => CheckpointConfig {
                truncate_pages: None,
                ..self.checkpoint
            },
            ReaderTracking::Tracked => self.checkpoint,
        };
        ActorConfig {
            sync: SyncConfig {
                cap: self.sync_limits.wal_bytes_per_step,
                node_id: 0,
            },
            checkpoint,
            upload: UploadConfig {
                files_per_tick: self.sync_limits.files_per_upload,
            },
            maintenance: self.maintenance.clone(),
            retry_interval: self.retry_interval.get(),
            shutdown_grace: self.shutdown_grace,
            responses: self.responses,
            max_unreplicated_age: self.max_unreplicated_age,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_not_a_duration_here() {
        assert_eq!(NonZeroDuration::new(Duration::ZERO), None);
        assert_eq!(
            NonZeroDuration::new(Duration::from_millis(5)).map(NonZeroDuration::get),
            Some(Duration::from_millis(5))
        );
    }

    #[test]
    fn the_meta_dir_is_go_s_beside_the_database() {
        let cfg = Config::default();
        assert_eq!(
            cfg.meta_dir_for(Path::new("/srv/app.db")),
            Some(PathBuf::from("/srv/.app.db-litestream"))
        );
        assert_eq!(
            cfg.meta_dir_for(Path::new("app.db")),
            Some(PathBuf::from(".app.db-litestream"))
        );
        assert_eq!(cfg.meta_dir_for(Path::new("/")), None);
    }

    #[test]
    fn untracked_readers_never_truncate() {
        let cfg = Config::default();
        assert_eq!(cfg.actor().checkpoint.truncate_pages, None);
        let tracked = Config {
            readers: ReaderTracking::Tracked,
            ..Config::default()
        };
        assert_eq!(
            tracked.actor().checkpoint.truncate_pages,
            CheckpointConfig::default().truncate_pages
        );
    }
}
