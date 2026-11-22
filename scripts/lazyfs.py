#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""The real `lsr replicate` on a filesystem that loses unsynced data, judged from outside.

    uv run scripts/lazyfs.py build [--dir DIR]
    uv run scripts/lazyfs.py run --cycles N [--lsr BIN] [--judge BIN] [--seed S] [--work DIR]
                                 [--stop-on-anomaly]

`build` clones LazyFS (github.com/dsrhaslab/lazyfs, MIT; a FUSE 3 filesystem with its own page
cache that drops unsynced data on command, used by Jepsen) at a pinned commit and builds it. It
needs cmake, g++, git and libfuse3-dev. LazyFS is a separate test tool: nothing of it is linked
into this repository's code.

`run` mounts LazyFS over a data directory and loops, for each cycle:

  1. start the workload (this file's `workload` command, Python's sqlite3) on `<mnt>/app.db`
     under synchronous=NORMAL, then `lsr replicate` on it with a file replica on a directory
     LazyFS does not cover;
  2. let them run for a random time, sometimes restoring and judging mid-run;
  3. a fault, chosen per cycle from a frozen menu (FAULTS): kill -9 both processes and drop
     LazyFS's unsynced data (clear-cache), or arm a torn-seq / torn-op fault on the WAL that
     persists a subset of one burst of writes (or part of one write) and then kills LazyFS;
  4. unmount and remount.

Every restore goes to a fresh directory and is judged by `lsr-history` (crates/history: the
tree-of-histories judge, which depends on no engine crate). It reduces the restored file to its
chain digest (integrity_check, then every row's hash) and judges it against the commit log, kept
outside the mount, in the line format crates/history/src/bin/lsr-history.rs documents. A
restore on a branch the database rewound past (the replica holds commits the database lost) is
an anomaly. Cycles run in segments (SEGMENT_CYCLES) that each start from an empty disk and
replica and end with a clean stop (SIGTERM, drain) and a final restore that must be the last
acknowledged commit. Only failed restores are kept, with the commit and engine logs beside them.

Exit status: 0 with no anomaly, 1 with any, 2 when the rig itself failed. The last line printed
is a JSON summary. Needs Linux, /dev/fuse and fusermount3; run as root or with FUSE allowed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# The chain workload of crates/history/src/chain.rs: row k holds sha256(k as 8 big-endian bytes
# || row k-1's hash || payload), from this genesis.
GENESIS = hashlib.sha256(b"lsr-history chain genesis").digest()
SCHEMA = "CREATE TABLE IF NOT EXISTS chain(k INTEGER PRIMARY KEY, h BLOB NOT NULL, payload BLOB NOT NULL)"


def link(k: int, prev: bytes, payload: bytes) -> bytes:
    return hashlib.sha256(k.to_bytes(8, "big") + prev + payload).digest()


def now_ms() -> int:
    return time.time_ns() // 1_000_000

LAZYFS_REPO = "https://github.com/dsrhaslab/lazyfs.git"
LAZYFS_COMMIT = "3edad01aa936b81fed9bd64e47e725fc5ef6c977"
# libpcache fetches spdlog as a GitHub archive tarball, which some proxies refuse; a git clone of
# the same tag is handed to CMake instead (FETCHCONTENT_SOURCE_DIR_SPDLOG).
SPDLOG_REPO = "https://github.com/gabime/spdlog.git"
SPDLOG_TAG = "v1.10.0"

REPO = Path(__file__).resolve().parent.parent

# The fault menu and the rig's timing law, frozen before the first judged run. Changing any of
# these makes earlier cycle counts incomparable; say so in the commit that does.
FAULTS = [("clear", 0.60), ("torn-seq", 0.25), ("torn-op", 0.15)]
RUN_SECONDS = (0.5, 2.5)
RESTORE_MID_RUN = 0.25
SYNC_INTERVAL = "100ms"
MIN_CHECKPOINT_PAGES = 200
# Cycles per segment: a fresh disk, replica and log, ending in a clean stop judged like the run's
# end. 100 cycles keep a segment under about 1 GB on disk.
SEGMENT_CYCLES = 100


def default_lazyfs_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(os.environ.get("LSR_LAZYFS_DIR", Path(base) / "lsr-lazyfs"))


def lazyfs_binary(d: Path) -> Path:
    return d / "lazyfs" / "lazyfs" / "build" / "lazyfs"


# ---------------------------------------------------------------- build


def build(d: Path) -> Path:
    src = d / "lazyfs"
    if not src.exists():
        subprocess.run(["git", "clone", "-q", LAZYFS_REPO, str(src)], check=True)
    subprocess.run(["git", "-C", str(src), "fetch", "-q", "origin"], check=False)
    subprocess.run(["git", "-C", str(src), "checkout", "-q", LAZYFS_COMMIT], check=True)
    spd = d / "spdlog"
    if not spd.exists():
        subprocess.run(
            ["git", "clone", "-q", "--depth", "1", "-b", SPDLOG_TAG, SPDLOG_REPO, str(spd)],
            check=True,
        )
    pc = src / "libs" / "libpcache" / "build"
    pc.mkdir(parents=True, exist_ok=True)
    subprocess.run(["cmake", f"-DFETCHCONTENT_SOURCE_DIR_SPDLOG={spd}", ".."], cwd=pc, check=True)
    subprocess.run(["cmake", "--build", ".", "-j", str(os.cpu_count() or 2)], cwd=pc, check=True)
    # lazyfs's CMakeLists includes spdlog from where FetchContent would have unpacked it.
    link = pc / "_deps" / "spdlog-src"
    if not link.exists():
        link.symlink_to(spd)
    lb = src / "lazyfs" / "build"
    lb.mkdir(parents=True, exist_ok=True)
    subprocess.run(["cmake", "-DLAZYFS_BUILD_TESTS=OFF", ".."], cwd=lb, check=True)
    subprocess.run(["cmake", "--build", ".", "-j", str(os.cpu_count() or 2)], cwd=lb, check=True)
    return lazyfs_binary(d)


# ---------------------------------------------------------------- workload


def workload(db: str, log: str, seconds_per_commit: float) -> int:
    """Appends to the self-certifying chain until SIGTERM (clean) or death (a fault)."""
    stop = False

    def on_term(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_term)
    out = open(log, "a", buffering=1, encoding="ascii")
    con = sqlite3.connect(db, isolation_level=None, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute(SCHEMA)
    # The database the loss left must itself be sound; a broken one is reported, not built on.
    ok = con.execute("PRAGMA integrity_check").fetchall()
    prev, k = GENESIS, 0
    for row_k, h, payload in con.execute("SELECT k, h, payload FROM chain ORDER BY k"):
        if row_k != k + 1 or link(row_k, prev, payload) != h:
            ok = [("chain broken at k=%d" % row_k,)]
            break
        prev, k = h, row_k
    if ok != [("ok",)]:
        out.write(f"X {now_ms()} {ok[:3]!r}\n")
        return 3
    out.write(f"E {now_ms()} {prev.hex()} {k}\n")
    rng = random.Random(os.urandom(8))
    while not stop:
        k += 1
        payload = os.urandom(rng.randrange(0, 3000))
        h = link(k, prev, payload)
        out.write(f"I {now_ms()} {h.hex()} {k}\n")
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT INTO chain(k, h, payload) VALUES (?, ?, ?)", (k, h, payload))
        con.execute("COMMIT")
        out.write(f"A {now_ms()} {k}\n")
        prev = h
        time.sleep(seconds_per_commit)
    con.close()
    return 0


def log_stats(path: Path) -> dict:
    """Commits, and how often and how far the database rewound at a start (synchronous=NORMAL
    loses acknowledged commits on a power loss; that is SQLite's contract, not a finding)."""
    commits = rewinds = lost_acked = 0
    acked = intended = 0
    if not path.exists():
        return {"commits": 0, "local_rewinds": 0, "lost_acked_commits": 0}
    for line in path.read_text().splitlines():
        f = line.split()
        if len(f) < 3:
            continue
        if f[0] == "I":
            intended = int(f[3]) if len(f) > 3 else intended
        elif f[0] == "A":
            acked = int(f[2])
            commits = max(commits, acked)
        elif f[0] == "E" and len(f) > 3:
            base = int(f[3])
            if base < intended:
                rewinds += 1
                lost_acked += max(0, acked - base)
            acked = intended = base
    return {"commits": commits, "local_rewinds": rewinds, "lost_acked_commits": lost_acked}


# ---------------------------------------------------------------- rig


class Rig:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.rng = random.Random(args.seed)
        self.root = Path(args.work).resolve()
        # One segment's world: wiped at each segment start, so disk use stays bounded (every
        # snapshot the engine takes is a whole database, at level 0, locally and in the replica).
        self.work = self.root / "segment"
        self.disk = self.work / "disk"  # what survives a "power loss": LazyFS's backing dir
        self.mnt = self.work / "mnt"
        self.replica = self.work / "replica"  # never faulted
        self.restores = self.root / "restores"  # only failed restores stay, with their evidence
        self.log = self.work / "commits.log"
        self.engine_log = self.work / "engine.log"
        self.lazyfs_log = self.work / "lazyfs.log"
        self.fifo = self.work / "faults.fifo"
        self.done_fifo = self.work / "faults-done.fifo"
        self.db = self.mnt / "app.db"
        self.lazyfs: subprocess.Popen | None = None
        self.app: subprocess.Popen | None = None
        self.engine: subprocess.Popen | None = None
        self.epoch = 0
        self.judged_line = 0
        self.counts = ""
        self.totals: dict[str, int] = {}
        self.engine_errors: dict[str, int] = {}
        self.judge_counts: list[str] = []
        self.anomalies: list[dict] = []
        self.restores_done = 0
        self.faults_fired: dict[str, int] = {}

    # -- setup

    def fresh(self) -> None:
        """A new segment: an empty disk, replica and commit log."""
        self.unmount()
        self.absorb()
        if self.work.exists():
            shutil.rmtree(self.work)
        for d in (self.disk, self.mnt, self.replica):
            d.mkdir(parents=True)
        self.restores.mkdir(parents=True, exist_ok=True)
        self.judged_line = 0
        cfg = self.work / "litestream.yml"
        cfg.write_text(
            "dbs:\n"
            f"  - path: {self.db}\n"
            f"    min-checkpoint-page-count: {MIN_CHECKPOINT_PAGES}\n"
            "    replica:\n"
            f"      path: {self.replica}\n"
            f"      sync-interval: {SYNC_INTERVAL}\n"
        )
        self.cfg = cfg

    def lazyfs_config(self, fault: str) -> Path:
        lines = [
            "[faults]",
            f'fifo_path="{self.fifo}"',
            f'fifo_path_completed="{self.done_fifo}"',
            "[cache]",
            "apply_eviction=false",
            "[cache.simple]",
            'custom_size="1gb"',
            "blocks_per_page=1",
            "[filesystem]",
            "log_all_operations=false",
            f'logfile="{self.lazyfs_log}"',
        ]
        wal = self.disk / "app.db-wal"  # torn faults name the backing path
        if fault == "torn-seq":
            persist = sorted(self.rng.sample(range(1, 41), self.rng.randint(1, 4)))
            lines += [
                "[[injection]]",
                'type="torn-seq"',
                'op="write"',
                f'file="{wal}"',
                f"persist={persist}",
                f"occurrence={self.rng.randint(1, 10)}",
            ]
        elif fault == "torn-op":
            parts = 8
            persist = sorted(self.rng.sample(range(1, parts + 1), self.rng.randint(1, parts - 1)))
            lines += [
                "[[injection]]",
                'type="torn-op"',
                f'file="{wal}"',
                f"occurrence={self.rng.randint(1, 300)}",
                f"parts={parts}",
                f"persist={persist}",
            ]
        path = self.work / "lazyfs.toml"
        path.write_text("\n".join(lines) + "\n")
        return path

    def mount(self, fault: str) -> None:
        cfg = self.lazyfs_config(fault)
        for f in (self.fifo, self.done_fifo):
            if f.exists():
                f.unlink()
        self.lazyfs = subprocess.Popen(
            [
                str(self.args.lazyfs),
                str(self.mnt),
                "--config-path",
                str(cfg),
                "-o",
                "modules=subdir",
                "-o",
                f"subdir={self.disk}",
                "-f",
            ],
            stdout=subprocess.DEVNULL,
            stderr=open(self.work / "lazyfs.stderr", "a"),
        )
        deadline = time.monotonic() + 20
        while not self.mounted() or not self.fifo.exists():
            if self.lazyfs.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("lazyfs did not mount; see lazyfs.stderr")
            time.sleep(0.05)

    def mounted(self) -> bool:
        """From /proc/mounts: a mount whose LazyFS died fails os.path.ismount but is still there."""
        with open("/proc/mounts") as f:
            return any(line.split()[1] == str(self.mnt) for line in f)

    def unmount(self) -> None:
        if self.mounted():
            r = subprocess.run(["fusermount3", "-u", str(self.mnt)], capture_output=True)
            if r.returncode != 0:
                subprocess.run(["fusermount3", "-uz", str(self.mnt)], capture_output=True)
        if self.lazyfs is not None:
            try:
                self.lazyfs.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.lazyfs.kill()
                self.lazyfs.wait()
            self.lazyfs = None

    def clear_cache(self) -> bool:
        """Drops every unsynced byte LazyFS holds; False when LazyFS is already gone."""
        if self.lazyfs is None or self.lazyfs.poll() is not None:
            return False
        rfd = os.open(self.done_fifo, os.O_RDONLY | os.O_NONBLOCK)
        try:
            # Non-blocking: a torn fault can kill LazyFS after the check above, and a blocking
            # open of a FIFO with no reader never returns. LazyFS reopens its end after each
            # command, so a missing reader is retried while LazyFS lives.
            wfd = None
            while wfd is None:
                try:
                    wfd = os.open(self.fifo, os.O_WRONLY | os.O_NONBLOCK)
                except OSError:
                    if self.lazyfs.poll() is not None:
                        return False
                    time.sleep(0.01)
            try:
                os.write(wfd, b"lazyfs::clear-cache\n")
            finally:
                os.close(wfd)
            deadline = time.monotonic() + 30
            buf = b""
            while b"finished::clear-cache" not in buf:
                if self.lazyfs.poll() is not None:
                    return False
                if time.monotonic() > deadline:
                    raise RuntimeError("lazyfs did not finish clear-cache")
                try:
                    buf += os.read(rfd, 4096)
                except BlockingIOError:
                    pass
                time.sleep(0.01)
        finally:
            os.close(rfd)
        return True

    # -- processes

    def start_app(self) -> bool:
        self.epoch += 1
        before = self.log.stat().st_size if self.log.exists() else 0
        self.app = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "workload",
                "--db",
                str(self.db),
                "--log",
                str(self.log),
            ],
            stderr=open(self.work / "workload.stderr", "a"),
        )
        deadline = time.monotonic() + 60
        while True:
            if self.log.exists() and self.log.stat().st_size > before:
                text = self.log.read_text()[before:]
                if text.startswith("X "):
                    self.anomaly("LOCAL_CORRUPT", text.strip())
                    return False
                if text.startswith("E ") and text.endswith("\n"):
                    return True
            if self.app.poll() is not None or time.monotonic() > deadline:
                self.anomaly("RIG", f"workload did not start (exit {self.app.poll()})")
                return False
            time.sleep(0.02)

    def start_engine(self) -> None:
        with open(self.engine_log, "a") as f:
            f.write(f"--- epoch {self.epoch}\n")
        self.engine = subprocess.Popen(
            [str(self.args.lsr), "replicate", "-config", str(self.cfg), "-log-level", "info"],
            stdout=open(self.engine_log, "a"),
            stderr=subprocess.STDOUT,
        )

    def kill_all(self) -> None:
        for p in (self.app, self.engine):
            if p is not None and p.poll() is None:
                p.send_signal(signal.SIGKILL)
        for p in (self.app, self.engine):
            if p is not None:
                p.wait()
        self.app = self.engine = None

    # -- judging

    def anomaly(self, kind: str, detail: str, cycle: int | None = None) -> None:
        a = {"kind": kind, "detail": detail, "cycle": cycle, "epoch": self.epoch}
        self.anomalies.append(a)
        print(json.dumps({"anomaly": a}), flush=True)

    def restore_and_judge(self, cycle: int, final: bool = False) -> None:
        """Restores the newest state, appends its digest to the commit log as an `L` (or `F`)
        line, and judges the whole log; anomalies on lines judged before are not repeated."""
        out = self.restores / f"r{self.restores_done:05d}"
        out.mkdir()
        target = out / "restored.db"
        r = subprocess.run(
            [str(self.args.lsr), "restore", "--replica", str(self.replica), "--output", str(target)],
            capture_output=True,
            text=True,
        )
        self.restores_done += 1
        if r.returncode != 0:
            if not any((self.replica / "ltx").glob("*/*.ltx")):
                shutil.rmtree(out)
                return  # nothing replicated yet
            self.anomaly("RESTORE_FAILED", (r.stderr or r.stdout).strip()[-500:], cycle)
            return
        d = subprocess.run([str(self.args.judge), "digest", str(target)], capture_output=True, text=True)
        if d.returncode not in (0, 3):
            self.anomaly("RIG", f"lsr-history digest failed: {d.stderr.strip()}", cycle)
            return
        with open(self.log, "a", encoding="ascii") as f:
            f.write(f"{'F' if final else 'L'} {now_ms()} {d.stdout.strip()}\n")
        seen = self.judged_line
        self.judged_line = self.log.read_bytes().count(b"\n")
        j = subprocess.run([str(self.args.judge), "judge", str(self.log)], capture_output=True, text=True)
        if j.returncode not in (0, 1):
            self.anomaly("RIG", f"lsr-history judge failed: {j.stderr.strip()}", cycle)
            return
        fresh = False
        for line in j.stdout.splitlines():
            m = re.match(r"line (\d+): (.*)", line)
            if m and int(m.group(1)) > seen:
                self.anomaly("JUDGE", line, cycle)
                fresh = True
            elif line.startswith("counts "):
                self.counts = line
        if fresh:
            shutil.copy(self.log, out / "commits.log")  # keep the evidence
            shutil.copy(self.engine_log, out / "engine.log")
        else:
            shutil.rmtree(out)

    # -- the loop

    def pick_fault(self) -> str:
        x, acc = self.rng.random(), 0.0
        for name, p in FAULTS:
            acc += p
            if x < acc:
                return name
        return FAULTS[-1][0]

    def cycle(self, n: int) -> None:
        fault = self.pick_fault()
        self.mount(fault)
        try:
            self.faulted_run(n, fault)
        finally:
            self.kill_all()
            self.unmount()

    def faulted_run(self, n: int, fault: str) -> None:
        if not self.start_app():
            return
        self.start_engine()
        run = self.rng.uniform(*RUN_SECONDS)
        restore_at = run * self.rng.random() if self.rng.random() < RESTORE_MID_RUN else None
        t0 = time.monotonic()
        if restore_at is not None:
            time.sleep(restore_at)
            self.restore_and_judge(n)
        time.sleep(max(0.0, run - (time.monotonic() - t0)))
        torn_fired = self.lazyfs is not None and self.lazyfs.poll() is not None
        self.kill_all()
        if fault == "clear" or not torn_fired:
            # A torn fault that never fired falls back to a plain loss; one that fired between
            # the check above and now has already lost everything unsynced.
            torn_fired = not self.clear_cache() and fault != "clear"
        fired = fault if fault == "clear" or torn_fired else f"{fault}-unfired"
        self.faults_fired[fired] = self.faults_fired.get(fired, 0) + 1
        self.unmount()

    def finish(self, cycles: int) -> None:
        """A clean stop: the last epoch drains, and the restore must be its last commit."""
        self.mount("clear")
        if not self.start_app():
            return
        self.start_engine()
        time.sleep(self.rng.uniform(*RUN_SECONDS))
        assert self.app is not None and self.engine is not None
        self.app.send_signal(signal.SIGTERM)
        self.app.wait(timeout=60)
        self.engine.send_signal(signal.SIGTERM)
        try:
            code = self.engine.wait(timeout=120)
        except subprocess.TimeoutExpired:
            self.anomaly("ENGINE_HUNG", "replicate did not drain within 120 s", cycles)
            self.engine.kill()
            self.engine.wait()
            code = None
        if code not in (0, None):
            self.anomaly("ENGINE_EXIT", f"replicate exited {code} after SIGTERM", cycles)
        self.app = self.engine = None
        self.restore_and_judge(cycles, final=True)
        self.unmount()

    def engine_log_findings(self) -> dict[str, int]:
        """Counts each error or cause the engine logged; a bug or a panic is an anomaly."""
        counts: dict[str, int] = {}
        if not self.engine_log.exists():
            return counts
        bug = None
        for line in self.engine_log.read_text(errors="replace").splitlines():
            for m in re.finditer(r"\b(?:error|cause)=(\S+)", line):
                counts[m.group(1)] = counts.get(m.group(1), 0) + 1
            if bug is None and ("panicked" in line or re.search(r"\bBug\b", line)):
                bug = line
        if bug is not None:
            self.anomaly("ENGINE_BUG", bug[:500])
        return counts

    def absorb(self) -> None:
        """Adds the ending segment's log statistics and engine errors to the run's totals."""
        for k, v in log_stats(self.log).items():
            self.totals[k] = self.totals.get(k, 0) + v
        for k, v in self.engine_log_findings().items():
            self.engine_errors[k] = self.engine_errors.get(k, 0) + v
        if self.counts:
            self.judge_counts.append(self.counts)
            self.counts = ""

    def stopping(self) -> bool:
        if any(a["kind"] == "RIG" for a in self.anomalies):
            return True
        return bool(self.args.stop_on_anomaly and self.anomalies)

    def run(self) -> int:
        self.unmount()
        if self.root.exists():
            shutil.rmtree(self.root)
        try:
            for n in range(self.args.cycles):
                if n % self.args.segment == 0:
                    if n > 0:
                        self.finish(n)
                        if self.stopping():
                            break
                    self.fresh()
                self.cycle(n)
                if self.stopping():
                    break
            if not self.stopping():
                if not self.work.exists():
                    self.fresh()
                self.finish(self.args.cycles)
        finally:
            self.kill_all()
            self.unmount()
        self.absorb()
        first = self.anomalies[0]["cycle"] if self.anomalies else None
        summary = {
            "cycles_requested": self.args.cycles,
            "segment": self.args.segment,
            "epochs": self.epoch,
            "restores": self.restores_done,
            "faults": self.faults_fired,
            **self.totals,
            "judge": self.judge_counts,
            "anomalies": len(self.anomalies),
            "first_anomaly_cycle": first,
            "kinds": sorted({a["kind"] for a in self.anomalies}),
            "engine_errors": self.engine_errors,
            "seed": self.args.seed,
            "work": str(self.root),
        }
        print(json.dumps({"summary": summary}), flush=True)
        if any(a["kind"] == "RIG" for a in self.anomalies):
            return 2
        return 1 if self.anomalies else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="clone and build LazyFS at the pinned commit")
    b.add_argument("--dir", type=Path, default=default_lazyfs_dir())
    r = sub.add_parser("run", help="the fault loop")
    r.add_argument("--cycles", type=int, required=True)
    r.add_argument("--lsr", type=Path, default=REPO / "target" / "release" / "lsr")
    r.add_argument("--judge", type=Path, default=REPO / "target" / "release" / "lsr-history")
    r.add_argument("--lazyfs", type=Path, default=lazyfs_binary(default_lazyfs_dir()))
    r.add_argument("--seed", type=int, default=int.from_bytes(os.urandom(4), "big"))
    r.add_argument("--work", default=str(REPO / "target" / "lazyfs"))
    r.add_argument("--stop-on-anomaly", action="store_true")
    r.add_argument("--segment", type=int, default=SEGMENT_CYCLES, help="cycles per fresh world")
    w = sub.add_parser("workload", help="internal: the application process")
    w.add_argument("--db", required=True)
    w.add_argument("--log", required=True)
    w.add_argument("--seconds-per-commit", type=float, default=0.004)
    args = ap.parse_args()
    if args.cmd == "build":
        print(build(args.dir))
        return 0
    if args.cmd == "workload":
        return workload(args.db, args.log, args.seconds_per_commit)
    needs = (
        (args.lsr, "lsr (cargo build --release -p lsr-cli)"),
        (args.judge, "lsr-history (cargo build --release -p lsr-history --features sqlite)"),
        (args.lazyfs, "lazyfs (uv run scripts/lazyfs.py build)"),
    )
    for need, what in needs:
        if not Path(need).exists():
            print(f"lazyfs.py: missing {what}: {need}", file=sys.stderr)
            return 2
    return Rig(args).run()


if __name__ == "__main__":
    sys.exit(main())
