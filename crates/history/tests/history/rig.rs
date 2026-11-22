//! The real engine on real SQLite, through power losses, judged from outside.
//!
//! The actor runs tick by tick over a live database (`lsr_sqlite::testing::Rig`: real SQLite
//! through the fault VFS, staging and replica on `FsStore`s in a temp dir, a fake clock). The
//! application writes the self-certifying chain and logs each commit SQLite acknowledged. A
//! power loss is armed at a drawn WAL sync; when it fires, every connection dies, the power comes
//! back, SQLite recovers, and the application reads its own chain tip to learn which node the
//! database came back at: a rewind forks the history tree there. At random moments, after each
//! reboot and at the end, the replica is restored for real (`lsr_replica::restore_with` into a
//! file) and judged by `lsr_history` through SQLite alone.
//!
//! The rig's power loss is the VFS's disk law: undo every write since a file's last sync. A
//! different law (a block-subset crash law, say) plugs in under the VFS with nothing here
//! changing: the judge never learns what the disk did, only what the application saw and what
//! a restore holds.

use lsr_core::Clock;
use lsr_history::{AheadMode, Counts, Judge, Log, chain};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{Effect, Fault, FileKind, Op, Rig, RigConfig, RigError};
use lsr_store_fs::FsStore;
use std::num::NonZeroU32;
use tempfile::TempDir;

use crate::real::{Rng, Scratch, judge};

struct Run {
    rig: Rig<Sqlite>,
    log: Log,
    judge: Judge,
    tip: chain::Chain,
    rng: Rng,
    scratch: Scratch,
    start: i64,
    _dir: TempDir,
}

impl Run {
    fn new(seed: u64) -> Self {
        let dir = TempDir::new().unwrap();
        let cfg = RigConfig {
            power: true,
            oracle: false,
            synchronous: "NORMAL",
            ..RigConfig::default()
        };
        let mut rig = Rig::<Sqlite>::new(dir.path(), cfg).unwrap();
        rig.exec(chain::SCHEMA).unwrap();
        let start = rig.clock().now().unix_millis();
        let tip = chain::Chain::new();
        Self {
            log: Log::new(tip.digest(), start),
            judge: Judge::new(AheadMode::Count),
            scratch: Scratch::new(&dir.path().join("restores")),
            rig,
            tip,
            rng: Rng::new(seed),
            start,
            _dir: dir,
        }
    }

    fn now(&self) -> i64 {
        self.rig.clock().now().unix_millis()
    }

    /// One chain row. SQLite's answer decides what the log records: acknowledged, or unknown.
    fn commit(&mut self) {
        let payload = self.rng.payload();
        let next = self.tip.next(&payload);
        match self.rig.exec(&next.insert_sql(&payload)) {
            Ok(()) => {
                self.log.commit(next.digest(), self.now());
                self.tip = next;
            }
            Err(_) => {
                self.log.pending(next.digest(), self.now());
            }
        }
    }

    fn judged(&mut self, sweep: bool, lag: Option<i64>) {
        let store = FsStore::new(self.rig.replica_root());
        let now = self.rig.clock().now();
        let instants = self.rng.instants(2, self.start, now);
        judge(
            &store,
            &self.scratch,
            &self.log,
            &mut self.judge,
            now,
            &instants,
            sweep,
            lag,
        )
        .unwrap_or_else(|anomaly| panic!("{anomaly}"));
    }

    /// Settles. A boot after a loss may have nothing to upload, so it acknowledges nothing and
    /// the rig cannot tell it settled, or it waits for the WAL to move: one more commit ends
    /// either.
    fn settle(&mut self) {
        if let Err(RigError::Unsettled(_)) = self.rig.settle() {
            self.commit();
            self.rig
                .settle()
                .unwrap_or_else(|err| panic!("settle: {err}; {:?}", self.rig.faults()));
        }
    }

    /// Commits, ticks and judges until the armed loss fires or `steps` run out.
    fn until_dark(&mut self, steps: u32) {
        for _ in 0..steps {
            if !self.rig.vfs().powered() {
                return;
            }
            match self.rng.below(10) {
                0..=5 => self.commit(),
                6 | 7 => drop(self.rig.tick()),
                8 => drop(self.rig.settle()),
                _ if self.rig.vfs().powered() => self.judged(false, None),
                _ => {}
            }
        }
    }

    /// The power comes back; the application reads where its chain stands.
    fn reboot(&mut self) {
        self.rig.faults_everywhere(false);
        self.rig.reboot_after_power_loss().unwrap();
        let tip = chain::verify(self.rig.app().unwrap()).unwrap();
        let node = self
            .log
            .find_from(&tip.digest(), self.log.head())
            .unwrap_or_else(|| {
                panic!(
                    "the database came back at a state never committed (row {})",
                    tip.k()
                )
            });
        self.log.recover(node, self.now());
        self.tip = tip;
    }

    fn cycle(&mut self) -> bool {
        let nth =
            NonZeroU32::new(u32::try_from(self.rng.below(3)).unwrap().saturating_add(1)).unwrap();
        self.rig.vfs().arm(Fault {
            kind: FileKind::Wal,
            op: Op::Sync,
            nth,
            effect: Effect::PowerLoss,
        });
        self.rig.faults_everywhere(true);
        self.until_dark(120);
        let lost = !self.rig.vfs().powered();
        if lost {
            self.reboot();
            // Before the new engine has run: the window where the replica can hold commits the
            // database lost.
            self.judged(false, None);
        } else {
            self.rig.vfs().disarm();
            self.rig.faults_everywhere(false);
        }
        self.settle();
        self.judged(true, None);
        lost
    }

    fn finish(mut self) -> (Counts, Log) {
        for _ in 0..3 {
            self.commit();
        }
        self.settle();
        self.judged(true, Some(0));
        (self.judge.counts(), self.log)
    }
}

/// Runs `cycles` power-loss cycles from `seed`; returns how many losses fired, what the judge
/// counted, and the log.
fn run(seed: u64, cycles: u32) -> (u32, Counts, Log) {
    let mut run = Run::new(seed);
    let mut losses = 0;
    for _ in 0..cycles {
        losses = u32::from(run.cycle()).saturating_add(losses);
    }
    let (counts, log) = run.finish();
    (losses, counts, log)
}

/// Real SQLite, real engine, real restores: through power losses at WAL syncs, every restore
/// (latest, as-of, every restore point) is a node of the history tree, latest never goes back on
/// a branch without a recorded rewind, and once settled with no fault, latest is the head.
#[test]
fn every_real_restore_through_power_losses_is_a_node_of_the_history() {
    let (losses, counts, log) = run(1, 4);
    eprintln!(
        "losses {losses}, rewinds {}, nodes {}, counts {counts:?}",
        log.rewinds().len(),
        log.len()
    );
    assert!(losses > 0, "a power loss fired");
    assert!(
        counts.latest > 0 && counts.points > 0 && counts.as_of > 0,
        "{counts:?}"
    );
}

/// The same over many seeds, for a trial count stated in advance: `LSR_HISTORY_SEEDS` seeds
/// (default 50) of 6 cycles each, from `LSR_HISTORY_BASE` (default 1).
#[test]
#[ignore = "long: LSR_HISTORY_SEEDS seeds of real power-loss cycles"]
fn real_power_loss_trials() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .map_or(default, |v| v.parse().unwrap())
    };
    let (base, seeds) = (env("LSR_HISTORY_BASE", 1), env("LSR_HISTORY_SEEDS", 50));
    let mut total = (0, 0, 0, 0);
    for seed in base..base + seeds {
        let (losses, counts, log) = run(seed, 6);
        total.0 += losses;
        total.1 += log.rewinds().len();
        total.2 += counts.ahead_latest;
        total.3 += counts.ahead_as_of;
    }
    eprintln!(
        "{seeds} seeds: {} losses, {} rewinds, ahead: {} latest, {} as-of",
        total.0, total.1, total.2, total.3
    );
}
