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
            "the meta directory has no record of the replica's newest transaction {tip}. This database was written after it, holds most of its pages and shows no sign of being older, so it is replicated as that state plus commits that never reached the replica, starting with a snapshot above {tip}. If it is an older copy of the database, the commits the replica holds after it leave the newest state and stay reachable by transaction id until retention removes them. If another replicator writes to this prefix, stop it."
        )
    );
    assert!(
        text.starts_with("time=1970-01-01T00:00:00.000Z level=WARN msg="),
        "{text}"
    );
    assert!(text.ends_with(" db=app.db tip=42\n"), "{text}");
}
