//! Lines as Go's slog handlers write them.

use super::*;

/// slog's `TextHandler` output for the same record, byte for byte.
#[test]
fn text_lines_are_slog_s() {
    let line = text_line(
        "2026-11-21T10:00:00.000Z",
        LogLevel::Info,
        "initialized db",
        &[("path", "/data/app.db".into()), ("n", 3_u64.into())],
    );
    assert_eq!(
        line,
        "time=2026-11-21T10:00:00.000Z level=INFO msg=\"initialized db\" path=/data/app.db n=3\n"
    );
    let line = text_line(
        "t",
        LogLevel::Trace,
        "x",
        &[
            ("a", "".into()),
            ("b", "k=v \"q\"\n".into()),
            ("c", true.into()),
        ],
    );
    assert_eq!(
        line,
        "time=t level=TRACE msg=x a=\"\" b=\"k=v \\\"q\\\"\\n\" c=true\n"
    );
}

/// slog's `JSONHandler` output for the same record.
#[test]
fn json_lines_are_slog_s() {
    let line = json_line(
        "2026-11-21T10:00:00.000Z",
        LogLevel::Warn,
        "replication degraded",
        &[
            ("db", "app.db".into()),
            ("n", 7_u64.into()),
            ("ok", false.into()),
        ],
    );
    assert_eq!(
        line,
        "{\"time\":\"2026-11-21T10:00:00.000Z\",\"level\":\"WARN\",\"msg\":\"replication degraded\",\"db\":\"app.db\",\"n\":7,\"ok\":false}\n"
    );
    assert_eq!(
        json_line("t", LogLevel::Error, "a\"b\u{1}", &[]),
        "{\"time\":\"t\",\"level\":\"ERROR\",\"msg\":\"a\\\"b\\u0001\"}\n"
    );
}

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct At;

impl Clock for At {
    fn now(&self) -> lsr_ltx::Timestamp {
        lsr_ltx::Timestamp::from_unix_millis(0)
    }
}

#[test]
fn lines_below_the_level_are_dropped_and_events_carry_the_database_and_cause() {
    let out = Captured::default();
    let log = Arc::new(Logger::to(
        LogLevel::Info,
        LogFormat::Text,
        Arc::new(At),
        Box::new(out.clone()),
    ));
    log.log(LogLevel::Debug, "hidden", &[]);
    let mut events = DbEvents::new(Arc::clone(&log), Path::new("/data/app.db"));
    events.event(&Event::Engine(EngineEvent::Idle));
    events.event(&Event::Engine(EngineEvent::Resnapshot(
        lsr_core::SnapshotCause::OperatorRequested,
    )));
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    assert_eq!(
        text,
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"snapshot required\" db=app.db cause=OperatorRequested\n"
    );
}

/// The line for the application's checkpoints says a checkpoint ran only when one did: a WAL
/// that grew with nothing copied is reported as that, and a skipped checkpoint of our own is
/// a debug line, not a retry.
#[test]
fn a_stalled_wal_is_not_logged_as_an_application_checkpoint() {
    let ran = "the application checkpointed the database";
    assert_eq!(
        app_checkpoint_message(AppCheckpoint::Backfilled { from: 1, to: 9 }),
        ran
    );
    assert_eq!(app_checkpoint_message(AppCheckpoint::Restarted), ran);
    let out = Captured::default();
    let log = Arc::new(Logger::to(
        LogLevel::Debug,
        LogFormat::Text,
        Arc::new(At),
        Box::new(out.clone()),
    ));
    let mut events = DbEvents::new(Arc::clone(&log), Path::new("/data/app.db"));
    events.event(&Event::AppCheckpoint {
        seen: AppCheckpoint::Stalled { frames: 4616 },
        total: 1,
    });
    events.event(&Event::Engine(EngineEvent::CheckpointDeferred {
        until: lsr_ltx::Timestamp::from_unix_millis(10),
    }));
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    assert_eq!(
        text,
        "time=1970-01-01T00:00:00.000Z level=WARN msg=\"the WAL grew past the application's automatic checkpoint threshold with nothing new checkpointed\" db=app.db seen=\"Stalled { frames: 4616 }\" total=1\n\
         time=1970-01-01T00:00:00.000Z level=DEBUG msg=\"checkpoint skipped: the database's write lock is busy\" db=app.db retry_at=1970-01-01T00:00:00.010Z\n"
    );
}

/// The refusal's text is the operator's instruction, approved word for word.
#[test]
fn a_refusal_logs_the_approved_text() {
    let txid = Txid::new(0x2a).unwrap();
    assert_eq!(
        refused_message(txid),
        format!(
            "replicate refused: the replica holds transactions up to {txid} that this database has no record of writing. Another replicator may be using this prefix. If this database is the right one, run `lsr trust-local <db> --replica <url>` and start again. If the replica is the right one, restore from it with `lsr restore`."
        )
    );
}

/// A boot that found the database ahead of the replica warns, naming the replica's newest
/// transaction in the text and in a field of its own.
#[test]
fn a_boot_ahead_of_the_replica_warns_and_names_its_newest_transaction() {
    let out = Captured::default();
    let log = Arc::new(Logger::to(
        LogLevel::Info,
        LogFormat::Text,
        Arc::new(At),
        Box::new(out.clone()),
    ));
    let mut events = DbEvents::new(Arc::clone(&log), Path::new("/data/app.db"));
    let tip = Txid::new(0x2a).unwrap();
    events.event(&Event::Engine(EngineEvent::Booted {
        cause: Some(SnapshotCause::AheadOfReplica { tip }),
    }));
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let msg = ahead_message(tip);
    assert_eq!(
        msg,
        format!(
            "the meta directory has no record of the replica's newest transaction {tip}, but this database was written after it and holds most of its pages: replicating it as the same database with commits that never reached the replica, starting with a snapshot above {tip}. If another replicator writes to this prefix, stop it."
        )
    );
    assert!(
        text.starts_with("time=1970-01-01T00:00:00.000Z level=WARN msg="),
        "{text}"
    );
    assert!(text.ends_with(" db=app.db tip=42\n"), "{text}");
}
/// Go logs a finished compaction and a finished snapshot at info level with its own message and
/// attribute names (`store.go:828`, `store.go:849`), and a deploy script may grep for either:
/// the same events here carry the same text, level and keys, and the database's name beside.
#[test]
fn a_compaction_and_a_snapshot_are_logged_as_go_logs_them() {
    use lsr_core::{FileInfo, FileKey, Level, ListedTime, MaintainedEvent};
    use lsr_ltx::TxidRange;
    let info = |level: u8, min: u64, max: u64, size: u64| FileInfo {
        key: FileKey::new(
            Level::new(level).unwrap(),
            TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap(),
        ),
        size,
        listed: ListedTime::LastModified(lsr_ltx::Timestamp::from_unix_millis(0)),
    };
    let run = |format: LogFormat, event: MaintainedEvent| {
        let out = Captured::default();
        let log = Arc::new(Logger::to(
            LogLevel::Info,
            format,
            Arc::new(At),
            Box::new(out.clone()),
        ));
        let mut events = DbEvents::new(Arc::clone(&log), Path::new("/data/app.db"));
        events.event(&Event::Engine(EngineEvent::Maintained {
            event,
            created_at_read: 0,
        }));
        String::from_utf8(out.0.lock().unwrap().clone()).unwrap()
    };
    let compacted = MaintainedEvent::Compacted(info(1, 2, 0x1a, 9693));
    assert_eq!(
        run(LogFormat::Text, compacted.clone()),
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"compaction complete\" system=store db=app.db level=1 txid.min=0000000000000002 txid.max=000000000000001a size=9693\n"
    );
    assert_eq!(
        run(LogFormat::Json, compacted),
        "{\"time\":\"1970-01-01T00:00:00.000Z\",\"level\":\"INFO\",\"msg\":\"compaction complete\",\"system\":\"store\",\"db\":\"app.db\",\"level\":1,\"txid\":{\"min\":\"0000000000000002\",\"max\":\"000000000000001a\"},\"size\":9693}\n"
    );
    let snapshot = MaintainedEvent::Snapshot(info(9, 1, 7, 28231));
    assert_eq!(
        run(LogFormat::Text, snapshot),
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"snapshot complete\" system=store db=app.db txid=0000000000000007 size=28231\n"
    );
    // The other maintenance steps stay below info.
    assert_eq!(run(LogFormat::Text, MaintainedEvent::Deleted(3)), "");
}

/// A group's keys (`txid.min`) are slog's text spelling; the JSON handler nests them.
#[test]
fn json_nests_a_group_and_closes_it_before_the_next_key() {
    assert_eq!(
        json_line(
            "t",
            LogLevel::Info,
            "m",
            &[
                ("a", 1_u64.into()),
                ("g.x", "1".into()),
                ("g.y", "2".into()),
                ("b", 3_u64.into()),
                ("h.z", true.into()),
            ],
        ),
        "{\"time\":\"t\",\"level\":\"INFO\",\"msg\":\"m\",\"a\":1,\"g\":{\"x\":\"1\",\"y\":\"2\"},\"b\":3,\"h\":{\"z\":true}}\n"
    );
}
/// The boot lines keep this daemon's wording: Go has no line at these events (its reasons for a
/// snapshot, "wal truncated by another process" and "wal header salt reset, snapshotting", ride on
/// a debug "sync" line), so a script cannot be written against them; docs/differences-from-go.md
/// lists the pair.
#[test]
fn the_boot_lines_keep_their_wording() {
    let run = |event: EngineEvent| {
        let out = Captured::default();
        let log = Arc::new(Logger::to(
            LogLevel::Info,
            LogFormat::Text,
            Arc::new(At),
            Box::new(out.clone()),
        ));
        DbEvents::new(Arc::clone(&log), Path::new("/data/app.db")).event(&Event::Engine(event));
        String::from_utf8(out.0.lock().unwrap().clone()).unwrap()
    };
    assert_eq!(
        run(EngineEvent::Booted { cause: None }),
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"replica position recovered\" db=app.db\n"
    );
    assert_eq!(
        run(EngineEvent::Booted {
            cause: Some(SnapshotCause::FirstSync)
        }),
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"replica position recovered; snapshot required\" db=app.db cause=FirstSync\n"
    );
}
