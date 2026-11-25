#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""The real "lsr replicate" beside a real SQLite writer, its replica on MinIO behind Toxiproxy,
under a seeded menu of faults, judged by point-in-time restores.

    uv run scripts/fault_rig.py run --cycles N [--seed S] [--work DIR] [--lsr BIN] [--judge BIN]
                                    [--go BIN] [--stop-on-anomaly] [--no-lazyfs]
                                    [--require-all-nemeses]
    uv run scripts/fault_rig.py canary   (the judge and the harness must catch damage planted in
                                          the replica; the engine plants are "just fault-rig-plant")

Needs MinIO (scripts/minio.sh up), Toxiproxy (scripts/toxiproxy.sh up) and the pinned Go
litestream (scripts/build-go-litestream.sh 041a8c3); "just fault-rig" starts the first two.

What one cycle does (all of it from this file's frozen tables below):

  1. the writer (scripts/lazyfs.py's chain workload: row k holds a hash of k and row k-1, one
     transaction per row, synchronous=NORMAL) and "lsr replicate" run on a database in a plain
     directory (on Linux with LazyFS and --lazyfs, on a LazyFS mount);
  2. after a random time one nemesis is drawn from MENU by the seeded generator and applied;
  3. the judge restores and judges (see below), and every anomaly keeps its evidence.

The judge shares nothing with the engine. The commit log (commits.log, on the plain disk the
faults do not touch) holds every commit the writer made as a hash of the database it left; a
commit whose acknowledgement a kill cut stays "possibly committed". Each cycle restores, from
MinIO directly (never through the proxy), with "lsr restore": every level-0 transaction id not
judged before (listed from the bucket by this file, not by the engine), a few earlier ones again
(the answer for a transaction id must not change), two random instants, and the latest; and the
latest with Go's restore. crates/history's "lsr-history judge --signals-required" then asks of
each restored database: is it a node of the tree of commits, did the replica's points move
forward on a branch, was an instant's state begun before the instant, and may a step back be
excused (only when the engine itself logged a loss cause after the rewind). A segment ends with
a clean stop whose final restore must be the last acknowledged commit.

Anomalies (kept under <work>/evidence/NNN-KIND with the seed, the cycle log, the commit log,
the engine log, the replica's objects and a REPRO.txt): JUDGE (the judge's own verdict),
RESTORE_REFUSED and GO_RESTORE_REFUSED (a refusal nothing injected explains), POINT_CHANGED (a
transaction id restored to different pages later), ENGINE_STOPPED (the engine ended on its own
where the nemesis gives no reason), UNEXPLAINED_LOSS_CAUSE (the engine logged a loss of history
under a nemesis that cannot lose any), ENGINE_BUG, ENGINE_HUNG, ENGINE_EXIT, LOCAL_CORRUPT.

The seed fixes the nemesis schedule and its parameters, not the interleaving with real time.
The summary line (JSON, last line printed) counts cycles, nemeses by kind, restores by kind,
refusals and anomalies. Exit 0 with no anomaly, 1 with any, 2 when the rig itself failed or a
required situation (REQUIRED_REACH) was never reached in a run long enough to reach it.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.client
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True  # importing lazyfs must not leave a __pycache__ in scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent))
import lazyfs  # noqa: E402  (the chain workload, the LazyFS driver and the commit-log format)

REPO = lazyfs.REPO

# ---------------------------------------------------------------- the frozen registration
# Frozen before the first counted run. Changing any of these makes earlier cycle counts
# incomparable; say so in the commit that does (simulation-testing, section 6).

# The nemesis menu, with weights. "once in N cycles, Go" is go-concurrent at 1 in 20.
MENU = [
    ("none", 0.15),  # a quiet cycle: the same judging with nothing injected
    ("kill9", 0.25),  # kill -9 of lsr at a random instant, restarted after a random wait
    ("partition", 0.15),  # a network partition, a Toxiproxy timeout toxic, 1 to 120 s
    ("slow-uploads", 0.10),  # Toxiproxy bandwidth toxic on the way up
    ("cut-uploads", 0.10),  # Toxiproxy limit_data on the way up: connections cut mid-request
    ("cut-reads", 0.05),  # the same on the way down: reads of the replica cut mid-response
    ("power-loss", 0.10),  # LazyFS clear-cache, Linux only
    ("clock-step", 0.05),  # libfaketime offset on the engine, Linux only
    ("go-concurrent", 0.05),  # the pinned Go "litestream replicate" on the same prefix, ~10 s
]
# A partition's length: (chance, (low, high) seconds), the short ones likeliest.
PARTITION_SECONDS = [(0.70, (1, 5)), (0.25, (5, 30)), (0.05, (30, 120))]
SLOW_KB_PER_S = (10, 200)
SLOW_SECONDS = (3, 12)
CUT_BYTES = (2_000, 200_000)
CUT_SECONDS = (2, 8)
CLOCK_STEP_SECONDS = (5, 3600)  # forwards or backwards
GO_SECONDS = 10
RUN_SECONDS = (0.5, 2.0)  # traffic before the nemesis
KILL_AFTER = (0.0, 1.5)  # kill9: how long after the nemesis starts
RESTART_AFTER = (0.0, 1.0)  # kill9: how long dead
SYNC_INTERVAL = "1s"
MIN_CHECKPOINT_PAGES = 200
SECONDS_PER_COMMIT = 0.05
SEGMENT_CYCLES = 10  # a fresh database and prefix, ended by a clean stop and a final restore
UPLOADS_WITHIN_SECONDS = 45  # a segment's engine must have this many level-0 files by then
UPLOADS_MIN_FILES = 3
POINTS_NEW_CAP = 12  # level-0 transaction ids judged for the first time, per cycle
EVIDENCE_REPLICAS = 4  # anomalies that keep a copy of the replica's objects
POINTS_AGAIN = 2  # earlier ones restored again, per cycle
AS_OF_PER_CYCLE = 2
JUDGE_STREAM = 0x4A75646765526967  # the judge's random choices come from their own stream
# What may legitimately end the engine on its own, or log a loss of history.
EXPLAINS_LOSS = {"power-loss", "go-concurrent", "clock-step"}
# Loss-of-history causes the engine logs (the simulator's snapshot_reports_loss and
# stop_reports_loss tables). FirstSync is one only after the first start of a segment.
LOSS_CAUSES = {
    "RemoteRegressed", "OtherWriter", "WalCorrupt", "WalTruncated", "WalUnlinked",
    "WalSyncFailed", "BehindRemote", "NewestStagedFileUnreadable", "WalMissing",
    "GenerationChanged", "PrevFrameMismatch", "LastPageMismatch", "LocalRewound",
    "StagedUnverified",
}
# Causes that rewind or doubt history: logged under a nemesis that cannot explain one (this
# cycle's or the one before, since a restart's snapshot is logged a few seconds in) they are an
# anomaly. The rest of LOSS_CAUSES are ordinary reasons to snapshot after a restart (the WAL
# restarted while the engine was down): they excuse a rewind but are no finding alone.
UNEXPLAINED_CAUSES = {
    "RemoteRegressed", "OtherWriter", "WalCorrupt", "WalTruncated", "WalUnlinked",
    "WalSyncFailed", "LocalRewound", "BehindRemote", "StagedUnverified",
    "NewestStagedFileUnreadable",
}
# Situations a long enough run must have reached (counts must be at least this, from this many
# cycles on). A situation nothing reached prints as a missing line nobody reads; here it fails.
REACH_FROM_CYCLES = 100
REQUIRED_REACH = {
    "judged.latest": 20, "judged.point": 20, "judged.as_of": 10, "judged.go_latest": 10,
    "judged.again": 5, "judged.final": 1, "nemesis.kill9": 1, "nemesis.partition": 1,
    "nemesis.cut-uploads": 1, "nemesis.slow-uploads": 1, "judged.during_outage": 1,
}

# The Go nemesis (Go Litestream and lsr on one prefix) is a known defect, filed as issue #346:
# restores of past points and instants can be unsound databases, and the engine does not always
# log OtherWriter. Anomalies inside a segment that nemesis touched are counted and printed under
# "known" and do not fail the run; an anomaly in any other segment does. REMOVE this exemption
# (KNOWN_GO_ISSUE and every use of known_issue) when issue #346 closes. See docs/decisions/D195.md.
KNOWN_GO_ISSUE = "#346"

MS = 1000
LOOPBACK = ("127.0.0.1", "localhost", "[::1]")
# restore's documented refusals of a target inside a compacted file whose finer files are gone
# (an as-of instant or a transaction id). Never a shorter history: an error, which is the contract.
PORTS_EXHAUSTED = re.compile(r"assign requested address|address not available|too many open files", re.I)
REFUSED_COMPACTED = re.compile(r"compact|straddl|inside ltx/|finer files", re.I)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def rfc3339(ms: int) -> str:
    t = datetime.fromtimestamp(ms // MS, timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % MS:03d}Z"


# ---------------------------------------------------------------- the store, minimally


class S3:
    """A signed client of MinIO for what the judge needs: list, get, put, delete. Written here
    rather than reusing the engine's so the judge's view of the bucket shares nothing with it."""

    def __init__(self, endpoint: str, bucket: str, access: str, secret: str):
        u = urllib.parse.urlsplit(endpoint)
        if u.scheme != "http" or u.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise SystemExit(f"fault_rig: only a loopback http endpoint, not {endpoint}")
        self.endpoint, self.host, self.port = endpoint.rstrip("/"), u.hostname, u.port or 80
        self.bucket, self.access, self.secret = bucket, access, secret

    def _sign(self, method: str, path: str, query: dict, payload: bytes) -> dict:
        amz = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        day = amz[:8]
        ph = hashlib.sha256(payload).hexdigest()
        enc = lambda v: urllib.parse.quote(v, safe="-_.~")
        cq = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(query.items()))
        host = f"{self.host}:{self.port}"
        headers = f"host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n"
        signed = "host;x-amz-content-sha256;x-amz-date"
        canon = f"{method}\n{urllib.parse.quote(path, safe='/-_.~')}\n{cq}\n{headers}\n{signed}\n{ph}"
        scope = f"{day}/us-east-1/s3/aws4_request"
        sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
        key = ("AWS4" + self.secret).encode()
        for part in (day, "us-east-1", "s3", "aws4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(key, sts.encode(), hashlib.sha256).hexdigest()
        return {
            "x-amz-date": amz,
            "x-amz-content-sha256": ph,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={self.access}/{scope}, "
            f"SignedHeaders={signed}, Signature={sig}",
        }

    def _call(self, method: str, key: str, query: dict, payload: bytes = b"") -> tuple[int, bytes]:
        # A busy machine runs out of ephemeral ports for a while (TIME_WAIT); wait and try again
        # rather than end a run over it.
        for attempt in range(10):
            try:
                return self._call_once(method, key, query, payload)
            except OSError:
                if attempt == 9:
                    raise
                time.sleep(1 + attempt)
        raise AssertionError("unreachable")

    def _call_once(self, method: str, key: str, query: dict, payload: bytes) -> tuple[int, bytes]:
        path = f"/{self.bucket}/{key}" if key else f"/{self.bucket}"
        h = self._sign(method, path, query, payload)
        c = http.client.HTTPConnection(self.host, self.port, timeout=60)
        try:
            url = path + ("?" + urllib.parse.urlencode(sorted(query.items()), quote_via=urllib.parse.quote) if query else "")
            c.request(method, url, body=payload or None, headers=h)
            r = c.getresponse()
            return r.status, r.read()
        finally:
            c.close()

    def list(self, prefix: str) -> list[tuple[str, int]]:
        out, token = [], None
        while True:
            q = {"list-type": "2", "prefix": prefix}
            if token:
                q["continuation-token"] = token
            status, body = self._call("GET", "", q)
            if status != 200:
                raise RuntimeError(f"list {prefix}: {status} {body[:200]!r}")
            root = ET.fromstring(body)
            ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
            for c in root.findall(f"{ns}Contents"):
                out.append((c.findtext(f"{ns}Key") or "", int(c.findtext(f"{ns}Size") or 0)))
            if root.findtext(f"{ns}IsTruncated") == "true":
                token = root.findtext(f"{ns}NextContinuationToken")
            else:
                return out

    def get(self, key: str) -> bytes:
        status, body = self._call("GET", key, {})
        if status != 200:
            raise RuntimeError(f"get {key}: {status}")
        return body

    def put(self, key: str, data: bytes) -> None:
        status, body = self._call("PUT", key, {}, data)
        if status != 200:
            raise RuntimeError(f"put {key}: {status} {body[:200]!r}")

    def delete(self, key: str) -> None:
        self._call("DELETE", key, {})

    def delete_prefix(self, prefix: str) -> None:
        for key, _ in self.list(prefix):
            self.delete(key)


# An S3 replica's level 0 is "<prefix>/0000/<min>-<max>.ltx" (a file replica's is ltx/0/).
L0_DIR = "/0000/"
L0_KEY = re.compile(r"/0000/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$")


class Toxi:
    """One Toxiproxy proxy in front of MinIO, with the toxics this rig uses."""

    def __init__(self, api: str, name: str, upstream: str):
        self.api, self.name, self.upstream, self.listen = api.rstrip("/"), name, upstream, ""

    def _req(self, method: str, path: str, body: dict | None = None):
        for attempt in range(10):
            try:
                return self._req_once(method, path, body)
            except urllib.error.URLError:
                if attempt == 9:
                    raise
                time.sleep(1 + attempt)

    def _req_once(self, method: str, path: str, body: dict | None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.api + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            text = r.read()
            return json.loads(text) if text else None

    def create(self) -> str:
        try:
            self._req("DELETE", f"/proxies/{self.name}")
        except Exception:
            pass
        made = self._req("POST", "/proxies", {"name": self.name, "listen": "127.0.0.1:0",
                                              "upstream": self.upstream, "enabled": True})
        self.listen = made["listen"]
        return self.listen

    def add(self, name: str, kind: str, stream: str, attrs: dict) -> None:
        self._req("POST", f"/proxies/{self.name}/toxics",
                  {"name": name, "type": kind, "stream": stream, "toxicity": 1.0, "attributes": attrs})

    def clear(self) -> None:
        for t in self._req("GET", f"/proxies/{self.name}/toxics") or []:
            self._req("DELETE", f"/proxies/{self.name}/toxics/{t['name']}")

    def delete(self) -> None:
        try:
            self._req("DELETE", f"/proxies/{self.name}")
        except Exception:
            pass


# ---------------------------------------------------------------- the rig


def find_libfaketime() -> str | None:
    if sys.platform != "linux":
        return None
    for p in ("/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
              "/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1",
              "/usr/lib/faketime/libfaketime.so.1", "/usr/local/lib/faketime/libfaketime.so.1"):
        if Path(p).exists():
            return p
    return None


def go_binary() -> Path:
    common = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--path-format=absolute",
                             "--git-common-dir"], capture_output=True, text=True).stdout.strip()
    root = Path(common).parent if common else REPO
    return root / ".upstream" / "bin" / "litestream-041a8c3"


class Rig:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.rng = random.Random(args.seed)  # the nemesis schedule and its parameters
        self.judge_rng = random.Random(args.seed ^ JUDGE_STREAM)
        self.root = Path(args.work).resolve()
        self.evidence = self.root / "evidence"
        self.scratch = self.root / "restores"
        self.cycles_log = self.root / "cycles.jsonl"
        self.endpoint = os.environ.get("LSR_COMPAT_S3_ENDPOINT", "http://127.0.0.1:9000")
        self.bucket = os.environ.get("LSR_COMPAT_S3_BUCKET", "lsr-test")
        self.access = os.environ.get("LSR_COMPAT_S3_ACCESS_KEY", "lsrtestroot")
        self.secret = os.environ.get("LSR_COMPAT_S3_SECRET_KEY", "lsrtest-throwaway-0000")
        self.s3 = S3(self.endpoint, self.bucket, self.access, self.secret)
        self.toxi_api = os.environ.get("LSR_TOXIPROXY_API", "http://127.0.0.1:8474")
        self.run_id = f"fault-rig-{os.getpid()}-{int(time.time())}"
        self.toxi = Toxi(self.toxi_api, self.run_id, f"{self.s3.host}:{self.s3.port}")
        self.libfaketime = find_libfaketime()
        self.lazy = None  # a lazyfs.Rig used for mounting, when LazyFS is in use
        self.use_lazyfs = (sys.platform == "linux" and not args.no_lazyfs
                           and Path(args.lazyfs).exists())
        self.menu = [(n, w) for n, w in MENU if not args.menu or n in args.menu.split(",")]
        self.unavailable = {
            "power-loss": None if self.use_lazyfs else
            ("no LazyFS: it is Linux-only (FUSE) and this run has none" if sys.platform != "linux"
             else "LazyFS is not built or --no-lazyfs"),
            "clock-step": None if self.libfaketime else
            ("libfaketime is not available here: the clock step needs Linux and libfaketime"),
        }
        # segment state
        self.seg = 0
        self.epoch = 0
        self.app: subprocess.Popen | None = None
        self.engine: subprocess.Popen | None = None
        self.engine_started_at = 0.0
        self.tainted: str | None = None
        self.clock_stepped = False
        self.faked_offset_file: Path | None = None
        self.damage_done = False  # the canary's damage was applied (this segment)
        self.damaged_after_anomalies: int | None = None  # anomalies seen when it was
        # counters
        self.nemeses: dict[str, int] = {}
        self.skipped: dict[str, int] = {}
        self.judged: dict[str, int] = {}
        self.refusals: dict[str, int] = {}
        self.causes: dict[str, dict[str, int]] = {}
        self.anomalies: list[dict] = []
        self.cycle_no = -1
        self.nemesis_now = "none"
        self.recent: list[str] = []
        self.seen_anomalies: set[tuple] = set()
        self.known: list[dict] = []  # anomalies of segments the Go nemesis touched (KNOWN_GO_ISSUE)
        self.go_cycles: list[dict] = []
        self.other_writer_seen = False
        self.skipped_points: set[int] = set()
        self.restores = 0
        self.judge_counts: list[str] = []
        self.totals: dict[str, int] = {}
        self.t0 = time.monotonic()
        self.restore_seconds: dict[str, float] = {}

    # -- bookkeeping

    def bump(self, table: dict, key: str, n: int = 1) -> None:
        table[key] = table.get(key, 0) + n

    def jlog(self, **kw) -> None:
        with open(self.cycles_log, "a") as f:
            f.write(json.dumps({"t": round(time.monotonic() - self.t0, 2), **kw}) + "\n")

    def env(self) -> dict:
        home = self.root / "home"
        home.mkdir(parents=True, exist_ok=True)
        return {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(home),
                "AWS_ACCESS_KEY_ID": self.access, "AWS_SECRET_ACCESS_KEY": self.secret,
                "AWS_REGION": "us-east-1"}

    def s3_flags(self) -> list[str]:
        return ["--endpoint", self.endpoint, "--force-path-style"]

    # -- one segment's world

    def fresh(self) -> None:
        self.seg += 1
        self.work = self.root / "segment"
        if self.work.exists():
            shutil.rmtree(self.work)
        self.work.mkdir(parents=True)
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.prefix = f"{self.run_id}/seg{self.seg}"
        self.log = self.work / "commits.log"
        self.engine_log = self.work / "engine.log"
        self.engine_log_offset = 0
        self.tainted = None
        self.clock_stepped = False
        self.epoch = 0
        self.judged_line = 0
        self.uploads_checked = False
        self.points: dict[int, str] = {}  # txid -> digest, as first restored
        self.first_commit_ms: int | None = None
        self.replica_first_ms: int | None = None  # when a listing first showed a level-0 file
        if self.use_lazyfs:
            ns = argparse.Namespace(seed=self.args.seed, work=str(self.work / "lazy"),
                                    lazyfs=self.args.lazyfs, lsr=self.args.lsr, judge=self.args.judge,
                                    engine_log_level="info")
            self.lazy = lazyfs.Rig(ns)
            self.lazy.fresh()
            self.lazy.mount("clear")
            self.db = self.lazy.mnt / "app.db"
        else:
            (self.work / "db").mkdir()
            self.db = self.work / "db" / "app.db"
        self.toxi.clear()
        self.write_configs()

    def write_configs(self) -> None:
        def cfg(endpoint: str, sync: str) -> str:
            return (
                "dbs:\n"
                f"  - path: {self.db}\n"
                f"    min-checkpoint-page-count: {MIN_CHECKPOINT_PAGES}\n"
                "    replica:\n"
                "      type: s3\n"
                f"      bucket: {self.bucket}\n"
                f"      path: {json.dumps(self.prefix)}\n"
                f"      endpoint: {endpoint}\n"
                "      region: us-east-1\n"
                "      force-path-style: true\n"
                f"      access-key-id: {json.dumps(self.access)}\n"
                f"      secret-access-key: {json.dumps(self.secret)}\n"
                f"      sync-interval: {sync}\n"
            )
        self.lsr_cfg = self.work / "lsr.yml"
        self.lsr_cfg.write_text(cfg(f"http://{self.toxi.listen}", SYNC_INTERVAL))
        self.go_cfg = self.work / "go.yml"
        self.go_cfg.write_text(cfg(self.endpoint, "1s"))
        self.direct_cfg = self.go_cfg  # Go restores read the replica directly, never via the proxy

    def end_segment_cleanup(self) -> None:
        self.absorb_log_stats()
        try:
            self.s3.delete_prefix(self.prefix + "/")
        except Exception as err:
            print(f"fault_rig: cleanup of {self.prefix}: {err}", file=sys.stderr)

    def absorb_log_stats(self) -> None:
        for k, v in lazyfs.log_stats(self.log).items():
            self.bump(self.totals, k, v)

    # -- processes

    def start_app(self) -> bool:
        self.epoch += 1
        before = self.log.stat().st_size if self.log.exists() else 0
        self.app = subprocess.Popen(
            [sys.executable, str(Path(lazyfs.__file__).resolve()), "workload", "--db", str(self.db),
             "--log", str(self.log), "--seconds-per-commit", str(SECONDS_PER_COMMIT)],
            stderr=open(self.work / "workload.stderr", "a"))
        deadline = time.monotonic() + 60
        while True:
            if self.log.exists() and self.log.stat().st_size > before:
                text = self.log.read_text()[before:]
                if text.startswith("X "):
                    self.anomaly("LOCAL_CORRUPT", text.strip())
                    return False
                if text.startswith("E ") and text.endswith("\n"):
                    if self.first_commit_ms is None:
                        self.first_commit_ms = now_ms()
                    return True
            if self.app.poll() is not None or time.monotonic() > deadline:
                self.anomaly("RIG", f"workload did not start (exit {self.app.poll()})")
                return False
            time.sleep(0.02)

    def start_engine(self) -> None:
        with open(self.engine_log, "a") as f:
            f.write(f"--- epoch {self.epoch}\n")
        env = self.env()
        if self.libfaketime and any(name == "clock-step" for name, _ in self.menu):
            self.faked_offset_file = self.work / "faketime.rc"
            if not self.faked_offset_file.exists():
                self.faked_offset_file.write_text("+0\n")
            # A wall-clock step moves CLOCK_REALTIME only. Faking the monotonic clock too stalled the
            # engine after its first two uploads (the Rust runtime's absolute futex deadlines are
            # built from the faked monotonic clock and read by the real kernel one), so it stays real.
            env.update(LD_PRELOAD=self.libfaketime, FAKETIME_NO_CACHE="1", FAKETIME_DONT_FAKE_MONOTONIC="1",
                       FAKETIME_TIMESTAMP_FILE=str(self.faked_offset_file))
        self.engine = subprocess.Popen(
            [str(self.args.lsr), "replicate", "-config", str(self.lsr_cfg), "-log-level", "info"],
            stdout=open(self.engine_log, "a"), stderr=subprocess.STDOUT, env=env)
        self.engine_started_at = time.monotonic()
        self.engine_killed = False

    def kill_engine(self) -> None:
        if self.engine is not None and self.engine.poll() is None:
            self.engine.send_signal(signal.SIGKILL)
            self.engine.wait()
        self.engine_killed = True
        self.engine = None

    def kill_all(self) -> None:
        for p in (self.app, self.engine):
            if p is not None and p.poll() is None:
                p.send_signal(signal.SIGKILL)
        for p in (self.app, self.engine):
            if p is not None:
                p.wait()
        self.app = self.engine = None

    def watch(self, seconds: float) -> None:
        """Sleeps, noticing an engine that ended on its own."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.check_engine_alive()
            time.sleep(min(0.05, max(0.0, end - time.monotonic())))

    def check_engine_alive(self) -> None:
        if self.engine is None or self.engine.poll() is None:
            return
        code = self.engine.returncode
        self.engine = None
        explained = self.explained()
        self.bump(self.refusals, "engine-stopped-explained" if explained else "engine-stopped")
        if not explained:
            self.anomaly("ENGINE_STOPPED", f"lsr replicate exited {code} on its own under "
                         f"nemesis {self.nemesis_now}; log tail: {self.tail(self.engine_log)}")

    def explained(self) -> bool:
        """Whether this cycle's nemesis or the one before can account for a loss or a stop."""
        return bool(self.tainted) or any(n in EXPLAINS_LOSS for n in [*self.recent, self.nemesis_now])

    def tail(self, path: Path, n: int = 400) -> str:
        try:
            return path.read_text(errors="replace")[-n:].replace("\n", " | ")
        except OSError:
            return ""

    def ensure_running(self) -> bool:
        if self.app is None or self.app.poll() is not None:
            if not self.start_app():
                return False
        if self.engine is None:
            self.start_engine()
        return True

    # -- engine log

    def scan_engine_log(self) -> bool:
        """Reads what the engine logged since the last scan: counts each cause by the cycle's
        nemesis, flags a bug, and says (with an S line) whether it reported a loss of history."""
        if not self.engine_log.exists():
            return False
        data = self.engine_log.read_bytes()
        new = data[self.engine_log_offset:].decode(errors="replace")
        self.engine_log_offset = len(data)
        lost = False
        for line in new.splitlines():
            if "panicked" in line or re.search(r"\bBug\b", line):
                self.anomaly("ENGINE_BUG", line[:500])
            for m in re.finditer(r"\bcause=(\w+)", line):
                cause = m.group(1)
                if cause == "OtherWriter":
                    self.other_writer_seen = True
                self.bump(self.causes.setdefault(self.nemesis_now, {}), cause)
                is_loss = cause in LOSS_CAUSES or (cause == "FirstSync" and self.epoch > 1)
                if is_loss and "cause=FirstSync" in line and "snapshot required" not in line:
                    is_loss = False
                if is_loss:
                    lost = True
                    if cause in UNEXPLAINED_CAUSES and not self.explained():
                        self.anomaly("UNEXPLAINED_LOSS_CAUSE",
                                     f"the engine logged cause={cause} under nemesis "
                                     f"{self.nemesis_now}: {line[:300]}")
        if lost:
            self.append(f"S {now_ms()}")
        return lost

    # -- the commit log

    def append(self, line: str) -> None:
        with open(self.log, "a", encoding="ascii") as f:
            f.write(line + "\n")

    # -- anomalies

    def anomaly(self, kind: str, detail: str) -> None:
        # One refusal usually repeats for every restore of the cycle; the first keeps the evidence
        # and the rest are counted, so the count of distinct findings stays honest.
        key = (self.seg, self.cycle_no, kind, re.sub(r"[0-9a-f]{16}", "#", detail)[:90])
        if key in self.seen_anomalies:
            self.bump(self.totals, "repeated_anomalies")
            return
        self.seen_anomalies.add(key)
        a = {"kind": kind, "detail": detail, "cycle": self.cycle_no, "segment": self.seg,
             "nemesis": self.nemesis_now, "seed": self.args.seed}
        # A rig failure is never the known issue's.
        known = self.tainted == "go-concurrent" and kind != "RIG"
        if known:
            a["known_issue"] = KNOWN_GO_ISSUE
            self.known.append(a)
        else:
            self.anomalies.append(a)
        a["evidence"] = str(self.keep_evidence(kind, detail))
        print(json.dumps({"known" if known else "anomaly": a}), flush=True)

    def keep_evidence(self, kind: str, detail: str) -> Path:
        out = self.evidence / f"{len(self.anomalies) + len(self.known):03d}-{kind.lower()}"
        out.mkdir(parents=True, exist_ok=True)

        def copy(src: Path, name: str) -> None:
            try:
                if src.exists():
                    shutil.copy(src, out / name)
            except OSError as err:
                (out / f"{name}.error").write_text(f"{err}\n")

        if hasattr(self, "log"):
            copy(self.log, "commits.log")
            copy(self.engine_log, "engine.log")
            copy(self.lsr_cfg, "lsr.yml")
        copy(self.cycles_log, "cycles.jsonl")
        try:
            objs = self.s3.list(self.prefix + "/")
            (out / "listing.txt").write_text("\n".join(f"{k} {s}" for k, s in objs) + "\n")
            # The objects themselves for the first anomalies only: a cycle that goes wrong makes
            # dozens of the same anomaly, and each full copy is megabytes.
            budget = 400 if len(self.anomalies) + len(self.known) <= EVIDENCE_REPLICAS else 0
            for key, size in objs[:budget]:
                dest = out / "replica" / key[len(self.prefix) + 1:]
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(self.s3.get(key))
        except Exception as err:
            (out / "replica.error").write_text(f"{err}\n")
        (out / "REPRO.txt").write_text(
            f"anomaly {kind}: {detail}\n"
            f"seed {self.args.seed}, segment {self.seg}, cycle {self.cycle_no}, nemesis {self.nemesis_now}\n"
            f"the seed fixes the schedule of nemeses and their parameters (cycles.jsonl lists them);\n"
            f"the interleaving with real time is not reproduced. Run again with:\n"
            f"  uv run scripts/fault_rig.py run --cycles {self.cycle_no + 1} --seed {self.args.seed} "
            f"--segment {self.args.segment}"
            + (f" --menu {self.args.menu}" if self.args.menu else "") + "\n")
        return out

    # -- restoring and judging

    def restore(self, tool: str, label: str, extra: list[str]) -> dict:
        """One restore into a fresh directory; returns rc, text, digest (or None) and the dir."""
        self.restores += 1
        t_start = time.monotonic()
        out = self.scratch / f"r{self.restores:06d}-{label}"
        out.mkdir(parents=True)
        target = out / "restored.db"
        if tool == "lsr":
            cmd = [str(self.args.lsr), "restore", "--replica", f"s3://{self.bucket}/{self.prefix}",
                   *self.s3_flags(), "--output", str(target), *extra]
        else:
            cmd = [str(self.args.go), "restore", "-config", str(self.go_cfg), "-o", str(target),
                   *extra, str(self.db)]
        for attempt in range(4):
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, env=self.env(), timeout=180)
                rc, text = r.returncode, (r.stderr or r.stdout).strip()
            except subprocess.TimeoutExpired:
                rc, text = -1, "restore did not finish within 180 s"
            # The machine, not the replica: no free local port for a moment. Try the restore again.
            if rc != 0 and PORTS_EXHAUSTED.search(text) and attempt < 3:
                self.bump(self.refusals, "restore-retried-no-free-port")
                time.sleep(5 * (attempt + 1))
                continue
            break
        res = {"rc": rc, "text": text, "digest": None, "broken": None, "dir": out}
        self.restore_seconds[tool] = self.restore_seconds.get(tool, 0.0) + time.monotonic() - t_start
        if rc == 0:
            d = subprocess.run([str(self.args.judge), "digest", str(target)],
                               capture_output=True, text=True)
            if d.returncode == 0:
                res["digest"] = d.stdout.strip()
            elif d.returncode == 3:
                res["broken"] = d.stdout.strip().removeprefix("broken ").replace("\n", " ")
            else:
                self.anomaly("RIG", f"lsr-history digest failed: {d.stderr.strip()}")
        return res

    def listing_l0(self) -> list[int]:
        txids = []
        for key, _ in self.s3.list(self.prefix + L0_DIR):
            m = L0_KEY.search(key)
            if m:
                txids.append(int(m.group(2), 16))
        return sorted(set(txids))

    def refusal(self, res: dict, what: str, tool: str) -> None:
        """A restore that refused: explained only by an injected cause or a documented refusal."""
        text = res["text"]
        if what in ("as_of", "point") and REFUSED_COMPACTED.search(text):
            self.bump(self.refusals, f"{what}-compacted (documented)")
            return
        if self.tainted:
            self.bump(self.refusals, f"{tool}-{what}-after-{self.tainted}")
            return
        if what == "nothing-yet":
            return
        self.bump(self.refusals, f"{tool}-{what}-unexplained")
        kind = "RESTORE_REFUSED" if tool == "lsr" else "GO_RESTORE_REFUSED"
        self.anomaly(kind, f"{tool} restore ({what}) refused with no injected cause: "
                           f"exit {res['rc']}: {text[-400:]}")

    def judge_round(self, during_outage: bool = False, final: bool = False) -> None:
        """Restores each kind, appends the results to the commit log, and judges the log."""
        if not self.log.exists():
            return
        self.scan_engine_log()
        try:
            txids = self.listing_l0()
        except Exception as err:
            self.anomaly("RIG", f"cannot list the bucket: {err}")
            return
        rng = self.judge_rng
        if txids and self.replica_first_ms is None:
            self.replica_first_ms = now_ms()
        points: list[int] = []
        if not during_outage:
            new = [t for t in txids if t not in self.points]
            if len(new) > POINTS_NEW_CAP:
                keep = sorted(rng.sample(new, POINTS_NEW_CAP - 2) + [new[0], new[-1]])
                dropped = set(new) - set(keep) - self.skipped_points
                self.skipped_points |= dropped
                self.bump(self.judged, "point_skipped_over_cap", len(dropped))
                new = sorted(set(keep))
            again = [t for t in sorted(self.points) if t in txids]
            again = rng.sample(again, min(POINTS_AGAIN, len(again)))
            points = sorted(set(new) | set(again))
        # the latest, first
        latest_label = "final" if final else ("outage" if during_outage else "latest")
        res = self.restore("lsr", latest_label, [])
        if res["rc"] != 0:
            if not txids:
                self.refusal(res, "nothing-yet", "lsr")
            else:
                self.refusal(res, "latest", "lsr")
        else:
            self.record_state("F" if (final and not self.tainted) else "L", res)
            self.bump(self.judged, "final" if final else ("during_outage" if during_outage else "latest"))
        if during_outage:
            self.run_judge()
            return
        # points, in the replica's order
        wrote = 0
        for t in points:
            r = self.restore("lsr", f"point-{t:x}", ["--txid", f"{t:016x}"])
            if r["rc"] != 0:
                if t in txids and t not in self.listing_l0():
                    self.bump(self.refusals, "point-raced-with-compaction")
                    continue
                self.refusal(r, "point", "lsr")
                continue
            if t in self.points:
                self.bump(self.judged, "again")
                if r["digest"] != self.points[t] and not self.tainted:
                    self.anomaly("POINT_CHANGED", f"transaction {t:016x} restored to {self.points[t]} "
                                 f"first and {r['digest'] or r['broken']} later")
            elif r["digest"] is not None or r["broken"] is not None:
                self.points[t] = r["digest"] or f"broken {r['broken']}"
            self.append_point(t, r)
            wrote += 1
            self.bump(self.judged, "point")
        if wrote:
            self.append(f"W {now_ms()}")
        # as of two instants
        for _ in range(AS_OF_PER_CYCLE):
            # Instants from when the replica first held a file: an older instant is a target no
            # replica could meet, and restore refuses it by design.
            if self.replica_first_ms is None or self.clock_stepped:
                self.bump(self.judged, "as_of_skipped_clock_step" if self.clock_stepped else "as_of_skipped")
                continue
            instant = rng.randint(self.replica_first_ms, now_ms())
            r = self.restore("lsr", "asof", ["--timestamp", rfc3339(instant)])
            if r["rc"] != 0:
                self.refusal(r, "as_of", "lsr")
                continue
            if r["digest"] is None and r["broken"] is None:
                continue
            body = r["digest"] if r["digest"] else f"broken {r['broken']}"
            self.append(f"T {now_ms()} {instant} {body}")
            self.bump(self.judged, "as_of")
        # Go's restore of the latest, after ours: it must not be behind it
        g = self.restore("go", "go", [])
        if g["rc"] != 0:
            self.refusal(g, "latest", "go")
        elif g["digest"] is not None or g["broken"] is not None:
            self.record_state("L", g)
            self.bump(self.judged, "go_latest")
        self.run_judge()
        for d in (self.scratch).glob("r*"):
            shutil.rmtree(d, ignore_errors=True)

    def record_state(self, tag: str, res: dict) -> None:
        body = res["digest"] if res["digest"] else f"broken {res['broken']}"
        if res["digest"] is None and res["broken"] is None:
            return
        self.append(f"{tag} {now_ms()} {body}")

    def append_point(self, txid: int, res: dict) -> None:
        body = res["digest"] if res["digest"] else f"broken {res['broken']}"
        if res["digest"] is None and res["broken"] is None:
            return
        self.append(f"P {now_ms()} {txid:016x} {body}")

    def run_judge(self) -> None:
        seen = self.judged_line
        self.judged_line = self.log.read_bytes().count(b"\n")
        j = subprocess.run([str(self.args.judge), "judge", str(self.log), "--signals-required"],
                           capture_output=True, text=True)
        if j.returncode not in (0, 1):
            self.anomaly("RIG", f"lsr-history judge failed: {j.stderr.strip()}")
            return
        for line in j.stdout.splitlines():
            m = re.match(r"line (\d+): (.*)", line)
            if m and int(m.group(1)) > seen:
                self.anomaly("JUDGE", line)
            elif line.startswith("counts "):
                self.last_counts = line

    # -- nemeses

    def draw(self) -> str:
        x, acc = self.rng.random(), 0.0
        total = sum(w for _, w in MENU)
        for name, w in MENU:
            acc += w / total
            if x < acc:
                break
        else:
            name = MENU[-1][0]
        # The parameters are drawn from the same stream for every nemesis, whether or not the
        # menu is restricted, so a restricted menu replays the full menu's draws where it can.
        return name

    def partition_seconds(self) -> float:
        x, acc = self.rng.random(), 0.0
        for p, (lo, hi) in PARTITION_SECONDS:
            acc += p
            if x < acc:
                return self.rng.uniform(lo, hi)
        return self.rng.uniform(*PARTITION_SECONDS[-1][1])

    def cycle(self, n: int) -> None:
        self.cycle_no = n
        name = self.draw()
        params: dict = {}
        if name == "partition":
            params["seconds"] = round(self.partition_seconds(), 1)
        elif name == "slow-uploads":
            params = {"kb_per_s": self.rng.randint(*SLOW_KB_PER_S),
                      "seconds": round(self.rng.uniform(*SLOW_SECONDS), 1)}
        elif name in ("cut-uploads", "cut-reads"):
            params = {"bytes": self.rng.randint(*CUT_BYTES),
                      "seconds": round(self.rng.uniform(*CUT_SECONDS), 1)}
        elif name == "kill9":
            params = {"after": round(self.rng.uniform(*KILL_AFTER), 2),
                      "dead": round(self.rng.uniform(*RESTART_AFTER), 2)}
        elif name == "clock-step":
            params = {"seconds": self.rng.randint(*CLOCK_STEP_SECONDS) * self.rng.choice([-1, 1])}
        run = round(self.rng.uniform(*RUN_SECONDS), 2)
        if self.args.menu and name not in self.args.menu.split(","):
            name = "none"
        self.bump(self.nemeses, name)
        self.recent = (self.recent + [name])[-2:]
        self.nemesis_now = "none"  # what ran before the nemesis is judged as a quiet start
        self.jlog(cycle=n, segment=self.seg, nemesis=name, params=params, run=run,
                  unavailable=self.unavailable.get(name))
        if not self.ensure_running():
            return
        if not self.uploads_checked:
            self.await_uploads()
        self.watch(run)
        why = self.unavailable.get(name)
        self.nemesis_now = name
        if why:
            self.bump(self.skipped, name)
            self.nemesis_now = "none"
            self.watch(0.5)
        else:
            getattr(self, "nemesis_" + name.replace("-", "_"))(params)
        self.ensure_running()
        self.watch(0.3)
        t_judge = time.monotonic()
        self.judge_round()
        self.jlog(cycle=n, judge_seconds=round(time.monotonic() - t_judge, 1),
                  restore_seconds={k: round(v, 1) for k, v in self.restore_seconds.items()},
                  restores=self.restores)

    def await_uploads(self) -> None:
        """A segment's first cycle, before any nemesis: the engine must put level-0 files on the
        replica under the commit load. A run whose engine uploads next to nothing judges next to
        nothing (the Linux canary's damage had no file to land on and no restore to read it), so
        this is a rig failure, loud, with the engine log, and not a quiet pass."""
        self.uploads_checked = True
        t0 = time.monotonic()
        while time.monotonic() - t0 < UPLOADS_WITHIN_SECONDS:
            self.check_engine_alive()
            try:
                if len(self.listing_l0()) >= UPLOADS_MIN_FILES:
                    self.jlog(first_uploads_seconds=round(time.monotonic() - t0, 1))
                    return
            except Exception as err:
                self.anomaly("RIG", f"cannot list the bucket: {err}")
                return
            time.sleep(0.5)
        listed = self.listing_l0()
        commits = sum(1 for line in self.log.read_text().splitlines() if line.startswith("I "))
        self.anomaly("RIG", f"the engine put {len(listed)} level-0 files on the replica in "
                            f"{UPLOADS_WITHIN_SECONDS} s under load, fewer than {UPLOADS_MIN_FILES} "
                            f"(newest transaction {listed[-1] if listed else None}; the writer made "
                            f"{commits} commits; libfaketime preloaded: "
                            f"{bool(self.faked_offset_file)}; LazyFS: {self.use_lazyfs}); "
                            f"engine log tail: {self.tail(self.engine_log)}")

    def nemesis_none(self, _p: dict) -> None:
        self.watch(0.5)

    def nemesis_kill9(self, p: dict) -> None:
        self.watch(p["after"])
        self.kill_engine()
        time.sleep(p["dead"])
        self.start_engine()
        self.watch(0.5)

    def toxic_window(self, seconds: float, toxics: list[tuple], judge: bool = False) -> None:
        for i, (kind, stream, attrs) in enumerate(toxics):
            self.toxi.add(f"t{i}", kind, stream, attrs)
        try:
            if judge:
                self.watch(min(seconds, 1.0))
                self.judge_round(during_outage=True)
                self.watch(max(0.0, seconds - 1.0))
            else:
                self.watch(seconds)
        finally:
            self.toxi.clear()

    def nemesis_partition(self, p: dict) -> None:
        self.toxic_window(p["seconds"], [("timeout", "downstream", {"timeout": 0}),
                                         ("timeout", "upstream", {"timeout": 0})], judge=True)
        self.watch(1.0)

    def nemesis_slow_uploads(self, p: dict) -> None:
        self.toxic_window(p["seconds"], [("bandwidth", "upstream", {"rate": p["kb_per_s"]})])

    def nemesis_cut_uploads(self, p: dict) -> None:
        self.toxic_window(p["seconds"], [("limit_data", "upstream", {"bytes": p["bytes"]})])

    def nemesis_cut_reads(self, p: dict) -> None:
        self.toxic_window(p["seconds"], [("limit_data", "downstream", {"bytes": p["bytes"]})])

    def nemesis_power_loss(self, _p: dict) -> None:
        assert self.lazy is not None
        self.kill_all()
        self.lazy.clear_cache()
        self.lazy.unmount()
        self.lazy.mount("clear")
        self.engine_killed = True
        self.ensure_running()

    def nemesis_clock_step(self, p: dict) -> None:
        assert self.faked_offset_file is not None
        self.faked_offset_file.write_text(f"{p['seconds']:+d}\n")
        self.clock_stepped = True
        self.watch(3.0)
        self.faked_offset_file.write_text("+0\n")
        self.watch(1.0)

    def nemesis_go_concurrent(self, _p: dict) -> None:
        go = subprocess.Popen([str(self.args.go), "replicate", "-config", str(self.go_cfg)],
                              stdout=open(self.work / "go.log", "a"), stderr=subprocess.STDOUT,
                              env=self.env())
        self.tainted = "go-concurrent"
        self.other_writer_seen = False
        self.go_cycles.append({"segment": self.seg, "cycle": self.cycle_no, "known_before": len(self.known)})
        self.watch(GO_SECONDS)
        go.send_signal(signal.SIGTERM)
        try:
            go.wait(timeout=60)
        except subprocess.TimeoutExpired:
            go.kill()
            go.wait()
        self.jlog(cycle=self.cycle_no, go_exit=go.returncode, go_log_tail=self.tail(self.work / "go.log"))
        self.watch(1.0)

    # -- the loop

    def finish(self) -> None:
        """A clean stop: the writer ends, the engine drains, and the final restore is judged."""
        self.toxi.clear()
        self.nemesis_now = "none" if not self.tainted else "go-concurrent"
        if self.app is None or self.app.poll() is not None:
            if not self.start_app():
                return
        if self.engine is None:
            self.start_engine()
        self.watch(self.rng.uniform(*RUN_SECONDS))
        if self.engine is None:
            self.judge_round(final=True)
            self.kill_all()
            return
        assert self.app is not None
        self.app.send_signal(signal.SIGTERM)
        try:
            self.app.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.app.kill()
        self.engine.send_signal(signal.SIGTERM)
        try:
            code = self.engine.wait(timeout=30 if self.tainted else 120)
        except subprocess.TimeoutExpired:
            if self.tainted:
                self.bump(self.refusals, f"engine-drain-timeout-after-{self.tainted}")
            else:
                self.anomaly("ENGINE_HUNG", "replicate did not drain within 120 s of SIGTERM")
            self.engine.kill()
            self.engine.wait()
            code = None
        if code not in (0, None) and not self.tainted:
            self.anomaly("ENGINE_EXIT", f"replicate exited {code} after SIGTERM")
        self.app = self.engine = None
        self.scan_engine_log()
        self.judge_round(final=True)

    def stopping(self) -> bool:
        if any(a["kind"] == "RIG" for a in self.anomalies):
            return True
        return bool(self.args.stop_on_anomaly and self.anomalies)

    def run(self) -> int:
        if self.root.exists():
            shutil.rmtree(self.root)
        missing = {k: v for k, v in self.unavailable.items()
                   if v and any(k == name for name, _ in self.menu)}
        if self.args.require_all_nemeses and missing:
            for k, v in missing.items():
                print(f"fault_rig: nemesis {k} was required and cannot run: {v}", flush=True)
            return 2
        self.root.mkdir(parents=True)
        self.evidence.mkdir()
        listen = self.toxi.create()
        n = 0
        try:
            while n < self.args.cycles and not self.stopping() and not self.out_of_time():
                self.fresh_with_listen(listen)
                for k in range(self.args.segment):
                    if n >= self.args.cycles or self.stopping() or self.out_of_time():
                        break
                    self.cycle(n)
                    n += 1
                    if self.tainted:  # a concurrent writer ends the segment
                        break
                    self.check_damage_hook(n)
                self.finish()
                self.close_go_cycle()
                self.last_prefix_done()
        finally:
            self.kill_all()
            if self.lazy is not None:
                self.lazy.unmount()
            self.toxi.delete()
        return self.report(n)

    def out_of_time(self) -> bool:
        """The wall-clock budget (--max-minutes) ended: no new cycle starts, and the summary
        says how many ran, so a short run never reads as the full count."""
        return bool(self.args.max_minutes) and time.monotonic() - self.t0 > self.args.max_minutes * 60

    def fresh_with_listen(self, listen: str) -> None:
        self.toxi.listen = listen
        self.fresh()

    def close_go_cycle(self) -> None:
        """At a segment's end: did lsr log OtherWriter during the Go cycle, and how many known
        anomalies did the segment collect? A silent Go cycle (no OtherWriter) is the part of the
        known issue that matters most, so it gets a line of its own."""
        if not self.go_cycles or "segment_known" in self.go_cycles[-1] or self.go_cycles[-1]["segment"] != self.seg:
            return
        g = self.go_cycles[-1]
        g["other_writer_logged"] = self.other_writer_seen
        g["segment_known"] = len(self.known) - g.pop("known_before")
        silent = "" if self.other_writer_seen else " SILENT: lsr logged no OtherWriter"
        print(f"go-cycle: segment {g['segment']} cycle {g['cycle']}: OtherWriter logged: "
              f"{'yes' if self.other_writer_seen else 'NO'}; known anomalies in the segment: "
              f"{g['segment_known']} (known: issue {KNOWN_GO_ISSUE}){silent}", flush=True)

    def last_prefix_done(self) -> None:
        self.end_segment_cleanup()

    def restore_plan(self) -> list[tuple[int, str]]:
        """The files a restore of the latest state would fetch, as (level, object key): the
        engine's own plan (`restore --dry-run --json`), read directly from the bucket. Damage
        goes where a restore reads, never where a snapshot or a higher level has made a file
        redundant: a dropped level-0 file under a snapshot that covers it is not damage a
        restore can see (the canary's damage went there, and no restore read it)."""
        r = subprocess.run([str(self.args.lsr), "restore", "--replica", f"s3://{self.bucket}/{self.prefix}",
                            *self.s3_flags(), "--dry-run", "--json", "--output", str(self.root / "plan.db")],
                           capture_output=True, text=True, env=self.env())
        if r.returncode != 0:
            return []
        files = json.loads(r.stdout).get("files", [])
        return [(f["level"], f"{self.prefix}/{f['level']:04d}/{f['name']}") for f in files]

    def check_damage_hook(self, n: int) -> None:
        """The canary's harness-level damage: done once, after the cycle named, to a file the
        next restore of the latest state reads, and judged at once, before the engine can
        compact the file out of the plan. A damage that cannot be applied is not silent: the
        canary reports it as NOT APPLIED, and a run asked to damage the replica exits 2."""
        if not self.args.damage or self.damage_done:
            return
        kind, _, at = self.args.damage.partition("@")
        if n < int(at or "3"):
            return
        level0 = [k for lvl, k in self.restore_plan() if lvl == 0]
        if len(level0) < 3:
            return  # try again after the next cycle
        self.damage_done = True
        self.damaged_after_anomalies = len(self.anomalies)
        if kind == "drop-middle":
            victim = level0[len(level0) // 2]
            self.s3.delete(victim)
            self.jlog(damage=kind, key=victim, plan_level0_files=len(level0))
        elif kind == "swap":
            a, b = level0[len(level0) // 2], level0[len(level0) // 2 + 1]
            da, db = self.s3.get(a), self.s3.get(b)
            self.s3.put(a, db)
            self.s3.put(b, da)
            self.jlog(damage=kind, keys=[a, b], plan_level0_files=len(level0))
        elif kind == "flip":
            victim = level0[-2]
            data = bytearray(self.s3.get(victim))
            data[len(data) // 2] ^= 0xFF
            self.s3.put(victim, bytes(data))
            self.jlog(damage=kind, key=victim, plan_level0_files=len(level0))
        else:
            raise SystemExit(f"fault_rig.py: unknown damage kind {kind}")
        self.judge_round()

    def report(self, cycles: int) -> int:
        reach_fail = []
        if cycles >= REACH_FROM_CYCLES and not self.args.menu:
            have = {**{f"judged.{k}": v for k, v in self.judged.items()},
                    **{f"nemesis.{k}": v for k, v in self.nemeses.items()}}
            for key, floor in REQUIRED_REACH.items():
                if have.get(key, 0) < floor:
                    reach_fail.append(f"{key}: {have.get(key, 0)} < {floor}")
        summary = {
            "seed": self.args.seed, "cycles": cycles, "segments": self.seg,
            "wall_seconds": round(time.monotonic() - self.t0),
            "menu_restricted": self.args.menu or None,
            "lazyfs": self.use_lazyfs, "libfaketime": bool(self.libfaketime),
            "nemeses_drawn": self.nemeses,
            "nemeses_ran": {k: v - self.skipped.get(k, 0) for k, v in self.nemeses.items()
                            if v - self.skipped.get(k, 0) > 0},
            "nemeses_NOT_RUN": {k: f"{v}x drawn, not run: {self.unavailable[k]}" for k, v in self.skipped.items()},
            "known_issue": {
                "issue": KNOWN_GO_ISSUE, "anomalies": len(self.known),
                "kinds": sorted({a["kind"] for a in self.known}),
                "go_cycles": self.go_cycles,
                "silent_go_cycles": [g for g in self.go_cycles if g.get("other_writer_logged") is False],
            },
            "restores": self.restores, "judged": self.judged, "refusals": self.refusals,
            "engine_causes_by_nemesis": self.causes,
            **self.totals,
            "anomalies": len(self.anomalies),
            "anomaly_kinds": sorted({a["kind"] for a in self.anomalies}),
            "first_anomaly_cycle": self.anomalies[0]["cycle"] if self.anomalies else None,
            "reach_failures": reach_fail, "work": str(self.root),
        }
        for k, v in self.skipped.items():
            print(f"NOT RUN: nemesis {k} was drawn {v}x and did not run: {self.unavailable[k]}", flush=True)
        print(f"known: issue {KNOWN_GO_ISSUE}: {len(self.known)} anomalies in segments the Go nemesis touched "
              f"({len([g for g in self.go_cycles if g.get('other_writer_logged') is False])} Go cycles with no "
              f"OtherWriter logged); they do not fail the run. Failing anomalies: {len(self.anomalies)}", flush=True)
        print(json.dumps({"summary": summary}), flush=True)
        if self.args.damage and self.damaged_after_anomalies is None and cycles >= 1:
            print(f"fault_rig: the damage {self.args.damage} was asked for and never applied", flush=True)
            return 2
        if any(a["kind"] == "RIG" for a in self.anomalies) or reach_fail:
            return 2
        return 1 if self.anomalies else 0


# ---------------------------------------------------------------- canaries


def exemption_canary(args: argparse.Namespace) -> str:
    """With the known-issue exemption on, an anomaly in a segment the Go nemesis touched must be
    counted as known and not fail the run, and the same anomaly in an untainted segment must
    still fail it (exit 1)."""
    ns = argparse.Namespace(**{**vars(args), "menu": "", "work": str(Path(args.work) / "canary-exemption"),
                               "damage": "", "stop_on_anomaly": False})
    rig = Rig(ns)
    rig.root.mkdir(parents=True, exist_ok=True)
    rig.evidence.mkdir(exist_ok=True)
    rig.prefix = f"{rig.run_id}/canary-exemption"
    rig.seg = 1
    rig.tainted = "go-concurrent"
    rig.anomaly("JUDGE", "canary: a restore in a tainted segment")
    code_tainted = rig.report(1)
    rig.tainted = None
    rig.cycle_no = 1
    rig.anomaly("JUDGE", "canary: a restore in an untainted segment")
    code_untainted = rig.report(1)
    ok = code_tainted == 0 and len(rig.known) == 1 and code_untainted == 1 and len(rig.anomalies) == 1
    return "caught (tainted: known and exit 0, untainted: exit 1)" if ok else (
        f"WRONG (tainted exit {code_tainted}, untainted exit {code_untainted})")


def canary(args: argparse.Namespace) -> int:
    """Damage planted in the replica must be reported: the harness can fail. Each damage kind
    runs a short quiet rig and must end with an anomaly; a rig that reports 0 anomalies for
    planted damage is blind, and this says so."""
    verdicts = {}
    for kind in ("drop-middle", "swap", "flip"):
        ns = argparse.Namespace(**{**vars(args), "damage": f"{kind}@3", "cycles": 8, "menu": "none",
                                   "stop_on_anomaly": True, "work": str(Path(args.work) / f"canary-{kind}"),
                                   "segment": 8})
        print(f"canary {kind}: running", flush=True)
        rig = Rig(ns)
        code = rig.run()
        # Caught means an anomaly after the damage was applied: one before it proves nothing,
        # and damage that was never applied proves nothing either (a distinct, failing verdict).
        mark = rig.damaged_after_anomalies
        if mark is None:
            verdicts[kind] = (f"NOT APPLIED (exit {code}: the replica never offered three level-0 "
                              f"files in a restore's plan; {len(rig.anomalies)} anomalies)")
            continue
        after = rig.anomalies[mark:]
        verdicts[kind] = (f"caught ({after[0]['kind']} at cycle {after[0]['cycle']})" if after
                          else f"MISSED (exit {code}, the damage was read by no restore)")
    verdicts["known-exemption"] = exemption_canary(args)
    print(json.dumps({"canary": verdicts}), flush=True)
    return 0 if all(v.startswith("caught") for v in verdicts.values()) else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "canary"):
        r = sub.add_parser(name)
        r.add_argument("--cycles", type=int, default=20 if name == "run" else 8)
        r.add_argument("--lsr", type=Path, default=REPO / "target" / "release" / "lsr")
        r.add_argument("--judge", type=Path, default=REPO / "target" / "release" / "lsr-history")
        r.add_argument("--go", type=Path, default=go_binary())
        r.add_argument("--lazyfs", type=Path, default=lazyfs.lazyfs_binary(lazyfs.default_lazyfs_dir()))
        r.add_argument("--no-lazyfs", action="store_true")
        r.add_argument("--seed", type=int, default=int.from_bytes(os.urandom(4), "big"))
        r.add_argument("--work", default=str(REPO / "target" / "fault-rig"))
        r.add_argument("--segment", type=int, default=SEGMENT_CYCLES)
        r.add_argument("--stop-on-anomaly", action="store_true")
        r.add_argument("--require-all-nemeses", action="store_true",
                       help="exit 2 before any cycle when a nemesis of the menu cannot run here (the nightly on Linux)")
        r.add_argument("--max-minutes", type=float, default=0, help="stop starting cycles after this long (0: no limit)")
        r.add_argument("--menu", default="", help="exploring only: restrict the nemeses (comma list)")
        r.add_argument("--damage", default="", help="canary: drop-middle|swap|flip@CYCLE")
    args = ap.parse_args()
    for need, what in ((args.lsr, "lsr (cargo build --release -p lsr-cli)"),
                       (args.judge, "lsr-history (cargo build --release -p lsr-history --features sqlite)"),
                       (args.go, "Go litestream (scripts/build-go-litestream.sh 041a8c3)")):
        if not Path(need).exists():
            print(f"fault_rig.py: missing {what}: {need}", file=sys.stderr)
            return 2
    if args.cmd == "canary":
        return canary(args)
    return Rig(args).run()


if __name__ == "__main__":
    sys.exit(main())
