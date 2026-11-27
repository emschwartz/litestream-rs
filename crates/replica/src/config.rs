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

/// What one sync step and one upload tick may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncLimits {
    /// A step stops after the first transaction that brings it to this many WAL bytes; `None`
    /// is no cap. A snapshot ignores it.
    pub wal_bytes_per_step: Option<NonZeroU64>,
    /// Files uploaded per tick before the other tasks get a turn; `None` is all staged, which
    /// after a long outage is thousands of sequential puts with no WAL sync, checkpoint or
    /// health update between them. The default is [`SyncLimits::FILES_PER_UPLOAD`].
    pub files_per_upload: Option<NonZeroU32>,
}

impl SyncLimits {
    /// Go's `DefaultMaxSyncLTXFiles` (256, `replica.go:208-218`): files uploaded per tick by
    /// default, for the library as for the binary.
    pub const FILES_PER_UPLOAD: NonZeroU32 = match NonZeroU32::new(256) {
        Some(n) => n,
        None => NonZeroU32::MIN,
    };
}

impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            wal_bytes_per_step: SyncConfig::default().cap,
            files_per_upload: Some(Self::FILES_PER_UPLOAD),
        }
    }
}

/// How a replicator runs. Go's behaviour by default. The event sink is not part of it
/// ([`crate::StartOptions::events`], [`crate::Replicator::set_events`]).
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// Go's meta directory, `<dir>/.<name>-litestream` beside the database unless set, so Go
    /// and this library resume from each other's staged files.
    pub meta_dir: Option<PathBuf>,
    /// How often the WAL is polled for new commits (Go: 1 s). A steady writer costs one
    /// level-0 file per interval: the engine ticks more often when it has other work, and
    /// the commits made meanwhile wait for the next interval.
    pub poll_interval: NonZeroDuration,
    /// The first retry wait, doubling to 5 minutes (Go's sync interval and backoff cap).
    pub retry_interval: NonZeroDuration,
    /// Go: PASSIVE at 1,000 pages or every minute, TRUNCATE at 121,359 pages (`db.go:32-35`).
    pub checkpoint: CheckpointConfig,
    /// Compaction levels, the snapshot interval and retention, and level 0 retention.
    pub maintenance: Maintenance,
    /// Bytes per sync step and files per upload tick.
    pub sync_limits: SyncLimits,
    /// What each persistent failure does. Default [`Responses::GO_LIKE`]: keep trying,
    /// stop on another writer.
    pub responses: Responses,
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
    /// replicator keeps (`docs/decisions/D192.md`); see [`crate::Health::fail_stop`] and
    /// [`crate::Health::writes`].
    pub if_replication_stops: IfReplicationStops,
    /// How long a commit may stay off the replica before fail-stop trips for it (decision
    /// D22): 10 minutes by default, at least 5.
    pub max_unreplicated_age: MaxUnreplicatedAge,
    /// The most bytes a second the engine reads from the database file for a whole-database
    /// read: the boot copy, a level-9 snapshot and the scheduled verify (`snapshot-read-limit`,
    /// issue 315). `None` (the default, as Go) reads as fast as the disk allows. A slower read
    /// holds its read transaction for longer, so the WAL grows, and the replica gets nothing
    /// newer than the read's start until the snapshot lands, for about twice the database's size
    /// over the limit (`docs/decisions/D309.md`).
    pub snapshot_read_limit: Option<NonZeroU64>,
    /// Whether the staged files this library writes are written by a thread of their own, one
    /// block behind the engine (`lsr_store_fs::FsStore::with_write_behind`). Default `false`: no
    /// thread beside the engine's. The binary turns it on when the machine has more than one core
    /// (`docs/decisions/D244.md`).
    pub write_behind: bool,
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
            shutdown_grace: Duration::from_secs(30),
            app_autocheckpoint: Some(SQLITE_AUTOCHECKPOINT),
            verify_interval: None,
            if_replication_stops: IfReplicationStops::HoldStart,
            max_unreplicated_age: MaxUnreplicatedAge::DEFAULT,
            snapshot_read_limit: None,
            write_behind: false,
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

    /// The upload limits: a tick's bytes shrink under a read limit, so a sliced snapshot waits
    /// only a tenth of a second at a time and the engine's other work keeps its turns.
    fn upload(&self) -> UploadConfig {
        let default = UploadConfig::default();
        UploadConfig {
            files_per_tick: self.sync_limits.files_per_upload,
            bytes_per_tick: self
                .snapshot_read_limit
                .map_or(default.bytes_per_tick, |limit| {
                    crate::pace::slice_bytes(limit, default.bytes_per_tick)
                }),
        }
    }

    /// The engine's configuration.
    pub(crate) fn actor(&self) -> ActorConfig {
        ActorConfig {
            sync: SyncConfig {
                cap: self.sync_limits.wal_bytes_per_step,
                node_id: 0,
            },
            sync_interval: self.poll_interval.get(),
            checkpoint: self.checkpoint,
            upload: self.upload(),
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

    /// A read limit shrinks the bytes of an upload tick (a tenth of a second of reading), so a
    /// sliced snapshot never waits long at a stretch; with none the engine's own bound stands.
    #[test]
    fn a_read_limit_shrinks_the_bytes_of_an_upload_tick() {
        let none = Config::default().actor().upload.bytes_per_tick;
        assert_eq!(none, UploadConfig::default().bytes_per_tick);
        let limited = Config {
            snapshot_read_limit: NonZeroU64::new(20 << 20),
            ..Config::default()
        };
        assert_eq!(limited.actor().upload.bytes_per_tick.get(), 2 << 20);
    }

    /// The library bounds a tick's uploads as the binary does: with no bound, the first tick
    /// after an outage uploads the whole backlog before anything else runs.
    #[test]
    fn the_default_bounds_the_files_an_upload_tick_sends() {
        let limits = SyncLimits::default();
        assert_eq!(limits.files_per_upload, Some(SyncLimits::FILES_PER_UPLOAD));
        assert_eq!(SyncLimits::FILES_PER_UPLOAD.get(), 256);
        let engine = Config::default().actor();
        assert_eq!(
            engine.upload.files_per_tick,
            Some(SyncLimits::FILES_PER_UPLOAD)
        );
    }

    /// The engine ticks more often than it polls (a file to upload, a compaction), and
    /// holds the commits made meanwhile until one poll interval after its last sync: a
    /// steady writer costs one level-0 file per interval, as under Go.
    #[test]
    fn new_commits_are_synced_once_per_poll_interval() {
        let config = Config {
            poll_interval: NonZeroDuration::new(Duration::from_millis(250)).unwrap(),
            ..Config::default()
        };
        assert_eq!(config.actor().sync_interval, Duration::from_millis(250));
        assert_eq!(
            Config::default().actor().sync_interval,
            Duration::from_secs(1)
        );
    }

    #[test]
    fn the_default_bounds_the_wal_bytes_a_sync_step_reads() {
        let limits = SyncLimits::default();
        assert_eq!(
            limits.wal_bytes_per_step.map(NonZeroU64::get),
            Some(64 << 20)
        );
        assert_eq!(
            Config::default().actor().sync.cap,
            limits.wal_bytes_per_step
        );
    }

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
    fn the_default_truncates_at_go_s_threshold() {
        assert_eq!(
            Config::default().actor().checkpoint.truncate_pages,
            NonZeroU32::new(121_359)
        );
    }
}
