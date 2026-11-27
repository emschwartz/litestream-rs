//! An application writer in another process, SIGKILLed mid-transaction: nothing it did not commit, and
//! nothing it committed that SQLite never indexed, reaches any LTX file, staged or replicated,
//! and the next writer's transactions over the same WAL slots replicate as normal. Each case
//! runs with the rig's own application in this process and in another one at SQLite's defaults
//! (`lsr_sqlite::testing::Placement`).

use std::io::Write;
use std::num::NonZeroU32;
use std::path::Path;

use lsr_ltx::{Event, ReadDecoder};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{
    Backend, Effect, Fault, FaultVfs, FileKind, HANG_LINE, Op, Placement, Rig, RigConfig,
};
use tempfile::TempDir;

use crate::common;

/// What the killed writer writes: a row no other writer writes, so its bytes are findable.
const MARKER: &[u8] = b"LSR-KILLED-WRITER";
const INSERT_MARKED: &str =
    "INSERT INTO t(v) VALUES (replace(printf('%.150c', 'x'), 'x', 'LSR-KILLED-WRITER '))";
const READY: &str = "lsr-kill-test: mid-transaction";

/// This file's roles for the binary's `child_process_entry` (`common::spawn`): whether `role`
/// is one, after running it.
pub fn child(role: &str, db: &Path) -> bool {
    match role {
        "spill-then-wait" => spill_then_wait(db),
        "commit-hangs-before-the-index" => commit_hangs_before_the_index(db),
        _ => return false,
    }
    true
}

/// A transaction bigger than a two-page cache: SQLite spills its dirty pages into the WAL as
/// frames with no commit flag, then the process waits to be killed.
fn spill_then_wait(db: &Path) {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.execute_batch("PRAGMA cache_size = 2; BEGIN;").unwrap();
    for _ in 0..40 {
        conn.execute_batch(INSERT_MARKED).unwrap();
    }
    let mut out = std::io::stdout().lock();
    writeln!(out, "{READY}").unwrap();
    out.flush().unwrap();
    drop(out);
    loop {
        std::thread::park();
    }
}

/// A commit under `synchronous = FULL`, whose WAL sync (after its frames, before the wal-index
/// update, `sqlite3WalFrames`) hangs: the frames are written and durable, and no reader can see
/// them.
fn commit_hangs_before_the_index(db: &Path) {
    let vfs = FaultVfs::register("lsr-kill-child").unwrap();
    vfs.arm(Fault {
        kind: FileKind::Wal,
        op: Op::Sync,
        nth: NonZeroU32::MIN,
        effect: Effect::Hang,
    });
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(db, flags, vfs.name()).unwrap();
    conn.execute_batch("PRAGMA synchronous = FULL; BEGIN;")
        .unwrap();
    for _ in 0..3 {
        conn.execute_batch(INSERT_MARKED).unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    panic!("the commit's sync should have hung");
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn ltx_files(root: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            ltx_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "ltx") {
            out.push(path);
        }
    }
}

/// Whether any page of any LTX file under `root` holds `needle`; every file must decode whole.
fn any_page_holds(root: &Path, needle: &[u8]) -> bool {
    let mut files = Vec::new();
    ltx_files(root, &mut files);
    assert!(!files.is_empty(), "{} holds LTX files", root.display());
    let mut found = false;
    for path in files {
        #[expect(
            clippy::disallowed_methods,
            reason = "an LTX file, not the database or its -shm"
        )]
        let file = std::fs::File::open(&path).unwrap();
        let mut decoder = ReadDecoder::new(file);
        while let Some(event) = decoder.next_event().unwrap() {
            if let Event::Page(page) = event {
                let data = page.data();
                found |= contains(data, needle);
            }
        }
        decoder.finish().unwrap();
    }
    found
}

fn wal_holds_marker(rig: &Rig<impl Backend>) -> bool {
    #[expect(clippy::disallowed_methods, reason = "the WAL carries no POSIX lock")]
    let wal = std::fs::read(rig.wal()).unwrap();
    contains(&wal, MARKER)
}

/// The killed writer is a process of its own in both placements of the application.
fn killed_writer<B: Backend>(role: &str) {
    for placement in Placement::ALL {
        killed_writer_beside::<B>(role, placement);
    }
}

fn killed_writer_beside<B: Backend>(role: &str, placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = Rig::<B>::new(dir.path(), RigConfig::placed(placement)).unwrap();
    for _ in 0..6 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
    }
    // One commit not yet checkpointed, so the child's write appends rather than restarting
    // the log (a restart's header sync would be the sync the second role hangs at).
    rig.insert(1500).unwrap();
    let mut child = common::spawn("child_process_entry", role, rig.db());
    let line = if role == "spill-then-wait" {
        READY
    } else {
        HANG_LINE
    };
    common::wait_for_line(&mut child, line);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        wal_holds_marker(&rig),
        "the killed writer's frames are in the WAL file"
    );
    rig.settle().unwrap();
    for _ in 0..30 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
    }
    let checked = rig.check().unwrap();
    assert!(checked.restored > 30, "{checked:?}");
    let rows = rig.query("SELECT count(*) FROM t").unwrap();
    assert_eq!(
        rows,
        [37],
        "{placement:?}: none of the killed writer's rows"
    );
    assert!(!any_page_holds(&dir.path().join("meta"), MARKER), "staged");
    assert!(!any_page_holds(rig.replica_root(), MARKER), "replicated");
}

/// SQLite belief: the frames of a transaction that never committed (a cache spill) sit past the
/// wal-index's committed end, where the engine never reads, and the next commit overwrites them.
#[test]
fn a_writer_killed_mid_transaction_leaves_no_page_in_any_file() {
    killed_writer::<Sqlite>("spill-then-wait");
}

/// Upstream `TestDB_WriterKilledBeforeWALIndexUpdate`: a writer killed after writing and syncing
/// its commit's frames but before publishing them in the wal-index. The transaction never
/// appears in any LTX file, and the next writer's frames over the same slots replicate.
#[test]
fn a_writer_killed_before_the_wal_index_update_is_never_replicated() {
    killed_writer::<Sqlite>("commit-hangs-before-the-index");
}
