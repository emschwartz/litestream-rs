#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""The trial drill: everything that must pass before lsr replaces the Go litestream binary on a
database of tens of GB, as one command with a named, numbered phase for each step.

    uv run scripts/trial_drill.py run --gib 1 [--store minio|provider] [--work DIR] [--lsr BIN] [--go BIN] ...
    uv run scripts/trial_drill.py canary     (the checkers must catch damage planted in a database)

"just trial-drill <gib>" runs it against the local MinIO; "just trial-drill-provider <provider> <gib>"
runs it against a real provider's bucket under one run prefix of its own (scripts/provider.sh, which
also deletes the prefix afterwards and fails if it does not list empty). docs/trial-drill.md says what
a full-size run needs. The load generator, the checkers and the sampler all live in this file.

Phases (each prints one PASS or FAIL line with its numbers; the last prints the table):

  P1 generate     a synthetic database of the requested size (and a small one) with a ledger table
  P2 go-first     the pinned Go build replicates it through its first snapshot, under load; stopped cleanly
  P3 takeover     lsr starts on the same database, working folder, prefix and config
  P4 lsr-load     lsr under load: catch-up, a scheduled snapshot, compaction to level 3, a retention pass,
                  and kill -9 during a large upload, a compaction and a snapshot, with a restart each time
  P5 restore      stop lsr; restore with both binaries at the newest state, one transaction id and one
                  instant; compare pages with the ledger's truth and with each other; integrity_check
  P6 rollback     the Go binary starts again on the same state, continues under load, stops; restore again
  P7 summary      the table, summary.json, the cleanup of the run prefix

The truth. The load generator keeps, in the same transaction as each change, a one-row table
drill_state (seq, ts_ms, xor) where xor is the XOR of a 64-bit BLAKE2 digest of every row of the data
table. Before each COMMIT it appends "I seq ts xor" to a ledger file, after it "A seq ts". A restored
database is judged by recomputing the XOR over its rows (the content is what the state row says), by
finding (seq, xor) in the ledger (the state existed), by the instant's bound (nothing newer than the
instant), and, at the newest state after a clean stop, by equality with the live database's pages.
Pages are compared between the two binaries' restores byte for byte, never as LTX bytes.

Exit 0: every phase passed. 1: a phase failed or a budget was exceeded. 2: the drill itself broke.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import hmac
import http.client
import json
import os
import random
import re
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zlib
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parent.parent
PAGE = 4096
SNAPSHOT_LEVEL = 9
KEY = re.compile(r"/(\d{4})/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$")
SIZES = (120, 480, 1000, 1900, 3200)
RELOAD_BOUNDS = "SELECT (SELECT min(id) FROM t), (SELECT max(id) FROM t)"  # what a restarted writer reads first
RUN_ROWS = 64  # rows of one kind in a row, so most pages are wholly compressible or wholly not


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def rfc3339(ms: int) -> str:
    t = datetime.fromtimestamp(ms // 1000, timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


NL = chr(10)


def parse_duration(text: str) -> float:
    total, found = 0.0, False
    for num, unit in re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", text):
        total += float(num) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]
        found = True
    if not found:
        raise SystemExit(f"trial_drill: {text!r} is not a duration such as 90s or 5m")
    return total


def parse_size(text: str) -> int:
    m = re.fullmatch(r"(\d+)\s*(B|KB|MB|GB|KiB|MiB|GiB)?", text.strip())
    if not m:
        raise SystemExit(f"trial_drill: {text!r} is not a size such as 8MB")
    mult = {"B": 1, "KB": 10**3, "MB": 10**6, "GB": 10**9, "KiB": 1024, "MiB": 1 << 20, "GiB": 1 << 30}
    return int(m.group(1)) * mult[m.group(2) or "B"]


def burst_bytes(conf: dict, burst_mib: int) -> int:
    """The one large transaction, sized from the part-size the binaries read (after the overlay), so its level 0
    file is a multipart upload to kill. Three parts of raw rows: half the rows compress, and the file must still
    be past one part once compressed. One level 0 file holds a whole transaction, whatever max-sync-wal-bytes says."""
    if burst_mib:
        return burst_mib << 20
    part = conf["dbs"][0]["replica"].get("part-size")
    return parse_size(str(part)) * 3 if part else 64 << 20


def mib(n: float) -> float:
    return round(n / (1 << 20), 1)


# ---------------------------------------------------------------- the database model


def run_kind(i: int, ratio: float) -> int:
    """0 for an incompressible run of rows, 1 for a compressible one; a pure function of the id, so an
    update or a re-run keeps a row's kind."""
    h = ((i // RUN_ROWS) * 2654435761) % 1000
    return 0 if h < ratio * 1000 else 1


def corpus(seed: int) -> bytes:
    rng = random.Random(seed ^ 0xC0FFEE)
    words = ["".join(rng.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(rng.randint(3, 9))) for _ in range(256)]
    return " ".join(rng.choice(words) for _ in range(20000)).encode()


def make_payload(rng: random.Random, text: bytes, kind: int) -> bytes:
    n = SIZES[rng.randrange(len(SIZES))]
    if kind == 0:
        return rng.randbytes(n)
    o = rng.randrange(len(text) - n)
    return text[o : o + n]


def row_digest(i: int, kind: int, payload: bytes) -> int:
    h = hashlib.blake2b(i.to_bytes(8, "big") + bytes((kind,)) + payload, digest_size=8)
    return int.from_bytes(h.digest(), "big")


SCHEMA = """
CREATE TABLE t(id INTEGER PRIMARY KEY, kind INTEGER NOT NULL, payload BLOB NOT NULL);
CREATE TABLE drill_state(id INTEGER PRIMARY KEY CHECK (id = 1), seq INTEGER NOT NULL, ts_ms INTEGER NOT NULL, xor TEXT NOT NULL);
"""


def generate(path: Path, target_bytes: int, ratio: float, seed: int) -> dict:
    """Writes the database (rollback journal off, then WAL mode) until it holds target_bytes of pages."""
    rng, text = random.Random(seed), corpus(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)
    c = sqlite3.connect(path, isolation_level=None)
    c.execute(f"PRAGMA page_size={PAGE}")
    c.execute("PRAGMA journal_mode=OFF")
    c.execute("PRAGMA synchronous=OFF")
    c.execute("PRAGMA cache_size=-262144")
    c.executescript(SCHEMA)
    state = {"xor": 0, "next": 1, "raw": [0, 0]}

    def rows(n):
        for _ in range(n):
            i = state["next"]
            state["next"] = i + 1
            kind = run_kind(i, ratio)
            p = make_payload(rng, text, kind)
            state["xor"] ^= row_digest(i, kind, p)
            state["raw"][kind] += len(p)
            yield (i, kind, p)

    while True:
        c.execute("BEGIN")
        c.executemany("INSERT INTO t(id, kind, payload) VALUES (?, ?, ?)", rows(4000))
        c.execute("COMMIT")
        if c.execute("PRAGMA page_count").fetchone()[0] * PAGE >= target_bytes:
            break
    ts = now_ms()
    c.execute("INSERT INTO drill_state VALUES (1, 0, ?, ?)", (ts, f"{state['xor']:016x}"))
    pages = c.execute("PRAGMA page_count").fetchone()[0]
    c.execute("PRAGMA journal_mode=WAL")
    c.close()
    ratio_sample = zlib_ratio(path)
    return {"pages": pages, "rows": state["next"] - 1, "xor": f"{state['xor']:016x}", "ts": ts,
            "incompressible_row_bytes": state["raw"][0], "compressible_row_bytes": state["raw"][1],
            "incompressible_fraction": round(state["raw"][0] / max(1, sum(state["raw"])), 3),
            "zlib1_ratio_of_file_sample": ratio_sample}


def zlib_ratio(path: Path, sample: int = 64 << 20) -> float:
    size = path.stat().st_size
    with open(path, "rb") as f:
        f.seek(max(0, (size - sample) // 2))
        data = f.read(sample)
    return round(len(zlib.compress(data, 1)) / max(1, len(data)), 3)


def scan_db(path: Path) -> dict:
    """The state row and the recomputed XOR of a database file with no WAL, opened immutable so the scan
    writes nothing beside it."""
    c = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    try:
        seq, ts, xs = c.execute("SELECT seq, ts_ms, xor FROM drill_state").fetchone()
        x, n = 0, 0
        for i, kind, p in c.execute("SELECT id, kind, payload FROM t"):
            x ^= row_digest(i, kind, p)
            n += 1
        return {"seq": seq, "ts": ts, "state_xor": xs, "xor": f"{x:016x}", "rows": n}
    finally:
        c.close()


def integrity(path: Path, mode: str) -> str:
    c = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    try:
        rows = [r[0] for r in c.execute("PRAGMA integrity_check" if mode == "full" else "PRAGMA quick_check")]
    finally:
        c.close()
    return "ok" if rows == ["ok"] else "; ".join(rows[:5])


def ledger_scan(path: Path, seqs: set[int], instants: list[int]) -> dict:
    """One pass over a ledger file: every I line of the wanted seqs, the newest seq begun at or before
    each instant, the newest acknowledged and the newest begun seq."""
    found: dict[int, list] = {s: [] for s in seqs}
    before = {t: 0 for t in instants}
    last_ack = last_begun = 0
    if not path.exists():
        return {"found": found, "before": before, "last_ack": 0, "last_begun": 0}
    with open(path, "r", encoding="ascii", errors="replace") as f:
        for line in f:
            p = line.split()
            if len(p) == 4 and p[0] == "I" and p[3] and line.endswith("\n"):
                s, ts = int(p[1]), int(p[2])
                last_begun = max(last_begun, s)
                if s in found:
                    found[s].append((p[3], ts))
                for t in instants:
                    if ts <= t:
                        before[t] = max(before[t], s)
            elif len(p) == 3 and p[0] == "A" and line.endswith("\n"):
                last_ack = max(last_ack, int(p[1]))
    return {"found": found, "before": before, "last_ack": last_ack, "last_begun": last_begun}


def judge_state(scan: dict, ledger: Path, instant_ms: int | None = None) -> list[str]:
    """What is wrong with a restored database's content, as a list of sentences (empty: nothing)."""
    bad = []
    if scan["xor"] != scan["state_xor"]:
        bad.append(f"the rows hash to {scan['xor']} but the state row says {scan['state_xor']} "
                   "(the restored pages are not one committed state)")
    led = ledger_scan(ledger, {scan["seq"]}, [instant_ms] if instant_ms else [])
    if not any(x == scan["state_xor"] for x, _ in led["found"][scan["seq"]]):
        bad.append(f"seq {scan['seq']} with xor {scan['state_xor']} is not in the ledger (a state that never existed)")
    if instant_ms and scan["seq"] > led["before"][instant_ms]:
        bad.append(f"seq {scan['seq']} was begun after the instant (newest begun by then: {led['before'][instant_ms]})")
    return bad


# ---------------------------------------------------------------- the load generator


class DbWriter:
    def __init__(self, name: str, path: str, ledger: str, seed: int, ratio: float, busy: float):
        self.name, self.ratio = name, ratio
        self.rng, self.text = random.Random(seed), corpus(seed)
        self.c = sqlite3.connect(path, timeout=busy, isolation_level=None)
        self.c.execute("PRAGMA wal_autocheckpoint=0")  # lsr and Go own checkpoints
        self.c.execute("PRAGMA synchronous=NORMAL")
        self.lfd = os.open(ledger, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        self.reload()
        self.busy_errors = 0

    def reload(self) -> None:
        self.seq, _ts, xs = self.c.execute("SELECT seq, ts_ms, xor FROM drill_state").fetchone()
        self.xor = int(xs, 16)
        # One aggregate per query: SQLite answers a lone min() or max() from the rowid b-tree's ends, but
        # scans the whole table when both share a query, which held each restarted writer for a minute
        # on a 24 GiB database.
        lo, hi = self.c.execute(RELOAD_BOUNDS).fetchone()
        self.lo, self.hi = lo or 1, hi or 0

    def pick(self):
        r = self.rng.randint(self.lo, self.hi)
        return (self.c.execute("SELECT id, kind, payload FROM t WHERE id >= ? LIMIT 1", (r,)).fetchone()
                or self.c.execute("SELECT id, kind, payload FROM t ORDER BY id DESC LIMIT 1").fetchone())

    def op(self, which: str, xor: int, kind: int | None = None) -> int:
        if which == "insert":
            self.hi += 1
            k = run_kind(self.hi, self.ratio) if kind is None else kind
            p = make_payload(self.rng, self.text, k)
            self.c.execute("INSERT INTO t(id, kind, payload) VALUES (?, ?, ?)", (self.hi, k, p))
            return xor ^ row_digest(self.hi, k, p)
        row = self.pick()
        if row is None:
            return xor
        i, k, old = row
        xor ^= row_digest(i, k, old)
        if which == "update":
            p = make_payload(self.rng, self.text, k)
            self.c.execute("UPDATE t SET payload = ? WHERE id = ?", (p, i))
            return xor ^ row_digest(i, k, p)
        self.c.execute("DELETE FROM t WHERE id = ?", (i,))
        return xor

    def transaction(self, ops: list[str], burst_bytes: int = 0) -> tuple[float, float]:
        """One committed transaction; returns (seconds to get the write lock, seconds to commit)."""
        t0 = time.monotonic()
        self.c.execute("BEGIN IMMEDIATE")
        t1 = time.monotonic()
        try:
            xor, written = self.xor, 0
            for which in ops:
                xor = self.op(which, xor)
            while written < burst_bytes:
                before = self.hi
                xor = self.op("insert", xor, kind=0)  # a bulk import of data that does not compress
                written += SIZES[2]
                if self.hi == before:
                    break
            seq, ts = self.seq + 1, now_ms()
            self.c.execute("UPDATE drill_state SET seq = ?, ts_ms = ?, xor = ? WHERE id = 1", (seq, ts, f"{xor:016x}"))
            os.write(self.lfd, f"I {seq} {ts} {xor:016x}\n".encode())
            t2 = time.monotonic()
            self.c.execute("COMMIT")
        except BaseException:
            try:
                self.c.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self.reload()
            raise
        t3 = time.monotonic()
        os.write(self.lfd, f"A {seq} {now_ms()}\n".encode())
        self.seq, self.xor = seq, xor
        return t1 - t0, t3 - t2


def run_load(a: argparse.Namespace) -> int:
    """The application: inserts, updates and deletes at a stated rate on every database until told to
    stop (SIGTERM from the supervisor, or a stop file)."""
    stop = {"v": False}
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.__setitem__("v", True))
    work = Path(a.work)
    writers = []
    for spec in a.db:
        name, path = spec.split("=", 1)
        writers.append(DbWriter(name, path, str(work / f"ledger-{name}.log"), a.seed ^ os.getpid() ^ now_ms(), a.ratio, a.busy_timeout))
    weights = [float(w) for w in a.weights.split(",")]
    mix = [(k, float(v)) for k, v in zip(("insert", "update", "delete"), a.mix.split(","))]
    rng = random.Random(a.seed ^ now_ms())
    stats = os.open(work / "writer.stats", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    burst_file = work / "burst"
    window = {w.name: [0, 0.0, 0.0, 0] for w in writers}  # commits, max begin wait, max commit, busy errors
    last_flush, due = time.monotonic(), time.monotonic()
    while not stop["v"]:
        if burst_file.exists():
            name = burst_file.read_text().strip()
            burst_file.unlink(missing_ok=True)
            w = next((w for w in writers if w.name == name), writers[0])
            t = time.monotonic()
            w.transaction([], burst_bytes=a.burst_bytes)
            os.write(stats, f"B {now_ms()} {w.name} {time.monotonic() - t:.2f} {a.burst_bytes}\n".encode())
            continue
        w = rng.choices(writers, weights)[0]
        ops = [rng.choices([k for k, _ in mix], [v for _, v in mix])[0] for _ in range(rng.randint(1, 4))]
        win = window[w.name]
        try:
            begin, commit = w.transaction(ops)
            win[0] += 1
            win[1], win[2] = max(win[1], begin), max(win[2], commit)
        except sqlite3.OperationalError as err:
            win[3] += 1
            os.write(stats, f"E {now_ms()} {w.name} {str(err)[:80]!r}\n".encode())
            time.sleep(1)
        now = time.monotonic()
        if now - last_flush >= 1.0:
            for n, win in window.items():
                os.write(stats, f"S {now_ms()} {n} {win[0]} {win[1] * 1000:.1f} {win[2] * 1000:.1f} {win[3]}\n".encode())
                win[:] = [0, 0.0, 0.0, 0]
            last_flush = now
        due += 1.0 / a.tps
        if due > now:
            time.sleep(min(due - now, 0.5))
        elif now - due > 1.0:
            due = now
        if (work / "stop").exists():
            break
    for w in writers:
        w.c.close()
    return 0


# ---------------------------------------------------------------- the store, minimally


class S3:
    """A signed path-style client for what the drill needs of the bucket: list, delete, list and abort
    multipart uploads. Written here so the drill's view of the bucket shares nothing with the engine's."""

    def __init__(self, endpoint: str, bucket: str, access: str, secret: str, region: str):
        u = urllib.parse.urlsplit(endpoint)
        self.https = u.scheme == "https"
        self.host, self.port = u.hostname, u.port or (443 if self.https else 80)
        self.bucket, self.access, self.secret, self.region = bucket, access, secret, region
        self.requests = 0

    def _sign(self, method: str, path: str, query: dict) -> dict:
        amz = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        day, ph = amz[:8], hashlib.sha256(b"").hexdigest()
        enc = lambda v: urllib.parse.quote(v, safe="-_.~")
        cq = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(query.items()))
        host = self.host if self.port in (80, 443) else f"{self.host}:{self.port}"
        headers = f"host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n"
        signed = "host;x-amz-content-sha256;x-amz-date"
        canon = f"{method}\n{urllib.parse.quote(path, safe='/-_.~')}\n{cq}\n{headers}\n{signed}\n{ph}"
        scope = f"{day}/{self.region}/s3/aws4_request"
        sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
        key = ("AWS4" + self.secret).encode()
        for part in (day, self.region, "s3", "aws4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(key, sts.encode(), hashlib.sha256).hexdigest()
        return {"x-amz-date": amz, "x-amz-content-sha256": ph, "Host": host,
                "Authorization": f"AWS4-HMAC-SHA256 Credential={self.access}/{scope}, SignedHeaders={signed}, Signature={sig}"}

    def call(self, method: str, key: str, query: dict) -> tuple[int, bytes]:
        path = f"/{self.bucket}/{key}" if key else f"/{self.bucket}"
        for attempt in range(8):
            try:
                self.requests += 1
                cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
                c = cls(self.host, self.port, timeout=60)
                try:
                    q = urllib.parse.urlencode(sorted(query.items()), quote_via=urllib.parse.quote)
                    c.request(method, path + ("?" + q if q else ""), headers=self._sign(method, path, query))
                    r = c.getresponse()
                    return r.status, r.read()
                finally:
                    c.close()
            except OSError:  # no free local port for a moment on a busy machine, or a dropped connection
                if attempt == 7:
                    raise
                time.sleep(1 + attempt)
        raise AssertionError("unreachable")

    def _pages(self, query: dict, item: str, fields: tuple[str, ...], token_name: str, marker_names: tuple[str, ...]):
        out, extra = [], {}
        while True:
            status, body = self.call("GET", "", {**query, **extra})
            if status != 200:
                raise RuntimeError(f"list {query}: {status} {body[:200]!r}")
            root = ET.fromstring(body)
            ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
            for c in root.findall(f"{ns}{item}"):
                out.append(tuple(c.findtext(f"{ns}{f}") or "" for f in fields))
            if root.findtext(f"{ns}IsTruncated") != "true":
                return out
            extra = {token_name: root.findtext(f"{ns}{marker_names[0]}") or ""}
            if len(marker_names) > 1:
                extra["upload-id-marker"] = root.findtext(f"{ns}{marker_names[1]}") or ""

    def list(self, prefix: str) -> list[tuple[str, int]]:
        rows = self._pages({"list-type": "2", "prefix": prefix}, "Contents", ("Key", "Size"), "continuation-token", ("NextContinuationToken",))
        return [(k, int(s or 0)) for k, s in rows]

    def uploads(self, prefix: str) -> list[tuple[str, str]]:
        return self._pages({"uploads": "", "prefix": prefix}, "Upload", ("Key", "UploadId"), "key-marker", ("NextKeyMarker", "NextUploadIdMarker"))

    def delete(self, key: str) -> None:
        self.call("DELETE", key, {})

    def abort(self, key: str, upload_id: str) -> None:
        self.call("DELETE", key, {"uploadId": upload_id})

    def purge(self, prefix: str) -> dict:
        """Deletes every object and aborts every multipart upload under the prefix, then lists both
        again; returns the counts and what was left (which must be nothing)."""
        n_obj = n_up = 0
        for _ in range(5):
            for k, _s in self.list(prefix):
                self.delete(k)
                n_obj += 1
            for k, uid in self.uploads(prefix):
                self.abort(k, uid)
                n_up += 1
            left_o, left_u = self.list(prefix), self.uploads(prefix)
            if not left_o and not left_u:
                break
        return {"deleted": n_obj, "aborted": n_up, "left_objects": len(left_o), "left_uploads": len(left_u)}


class Throttle(threading.Thread):
    """A loopback proxy in front of the local MinIO that limits the bytes going up, so that on a laptop an
    upload of a few megabytes stays in flight long enough to be killed, as a hundred megabytes over a
    real link does. One shared bucket for every connection; downloads pass untouched."""

    def __init__(self, host: str, port: int, rate: float):
        super().__init__(daemon=True)
        import asyncio
        self.asyncio, self.up, self.rate, self.next_free = asyncio, (host, port), rate, 0.0
        self.open: dict[str, float] = {}  # keys of multipart uploads begun and not completed or aborted
        self.parts = 0
        self.mplog = None  # a file for the multipart requests, set by the drill
        self.recent: collections.deque = collections.deque(maxlen=40)  # the last requests seen, for a failure report
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.port = 0

    REQ = re.compile(rb"(POST|PUT|DELETE|GET|HEAD) /(\S+) HTTP/1\.[01]")

    def observe(self, data: bytes) -> None:
        """Multipart uploads in flight, read off the request lines: this MinIO never lists them."""
        for m in self.REQ.finditer(data[:8192]):
            target = m.group(2).split(b"?", 1)
            verb, key, query = m.group(1), urllib.parse.unquote(target[0].decode(errors="replace")), (target[1] if len(target) > 1 else b"")
            names = {p.split(b"=")[0] for p in query.split(b"&")}  # lsr signs in the query string, so its subresource is not first
            line = f"{verb.decode()} {key[-70:]} {sorted(n.decode(errors='replace') for n in names if not n.startswith(b'X-Amz'))}"
            self.recent.append(line)
            if self.mplog is not None and names & {b"uploads", b"uploadId"}:
                self.mplog.write(f"{time.strftime('%H:%M:%S')} {line}\n")
                self.mplog.flush()
            if verb == b"POST" and b"uploads" in names:
                self.open[key] = time.monotonic()
            elif verb == b"PUT" and b"partNumber" in names:
                self.parts += 1
            elif verb in (b"POST", b"DELETE") and b"uploadId" in names:
                self.open.pop(key, None)

    async def pump(self, r, w, limited: bool) -> None:
        try:
            while data := await r.read(65536):
                if limited:
                    self.observe(data)
                if limited and self.rate:
                    now = self.loop.time()
                    start = max(now, self.next_free)
                    self.next_free = start + len(data) / self.rate
                    if start > now:
                        await self.asyncio.sleep(start - now)
                w.write(data)
                await w.drain()
        except (ConnectionError, self.asyncio.CancelledError):
            pass
        finally:
            w.close()

    async def handle(self, cr, cw) -> None:
        try:
            ur, uw = await self.asyncio.open_connection(*self.up)
        except OSError:
            cw.close()
            return
        await self.asyncio.gather(self.pump(cr, uw, True), self.pump(ur, cw, False))

    def run(self) -> None:
        self.asyncio.set_event_loop(self.loop)
        server = self.loop.run_until_complete(self.asyncio.start_server(self.handle, "127.0.0.1", 0))
        self.port = server.sockets[0].getsockname()[1]
        self.ready.set()
        self.loop.run_forever()


# ---------------------------------------------------------------- processes and sampling


def mem_detail(pid: int) -> dict:
    """What a process holds, split so a file-backed or cached page is not read as heap. Linux:
    smaps_rollup (Rss, Anonymous, Pss). macOS: footprint (the dirty and swapped pages the kernel
    charges the process; clean file-backed pages are left out)."""
    out: dict = {}
    try:
        for line in open(f"/proc/{pid}/smaps_rollup"):
            k, _, v = line.partition(":")
            if k in ("Rss", "Pss", "Anonymous", "Pss_Anon", "Pss_File", "Private_Dirty", "Swap"):
                out[k.lower() + "_kib"] = int(v.split()[0])
        return out
    except OSError:
        pass
    try:
        r = subprocess.run(["footprint", "-p", str(pid)], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return out
    unit = {"B": 1, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}
    m = re.search(r"Footprint:\s*([0-9.]+)\s*(B|KB|MB|GB)", r.stdout)
    if m:
        out["footprint_bytes"] = int(float(m.group(1)) * unit[m.group(2)])
    for cat in ("Malloc Large", "Malloc Small"):
        m = re.search(r"^\s*([0-9.]+)\s*(B|KB|MB|GB)\s.*\s" + cat + r"\s*$", r.stdout, re.M)
        if m:
            out[cat.lower().replace(" ", "_") + "_dirty_bytes"] = int(float(m.group(1)) * unit[m.group(2)])
    return out


def ps_usage(pid: int) -> tuple[int, float] | None:
    r = subprocess.run(["ps", "-o", "rss=,time=", "-p", str(pid)], capture_output=True, text=True)
    p = r.stdout.split()
    if r.returncode != 0 or len(p) < 2:
        return None
    t, days = p[1], 0
    if "-" in t:
        d, t = t.split("-", 1)
        days = int(d)
    secs = 0.0
    for part in t.split(":"):
        secs = secs * 60 + float(part)
    return int(p[0]) * 1024, secs + days * 86400


def dir_bytes(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total


METRIC = re.compile(r'^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)$')


def parse_metrics(text: str) -> dict:
    """The series the drill reads, by name; each a list of (labels, value)."""
    out: dict = {}
    for line in text.splitlines():
        m = METRIC.match(line)
        if not m or not (m.group(1).startswith("lsr_") or m.group(1).startswith("litestream_")):
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2) or ""))
        try:
            out.setdefault(m.group(1), []).append((labels, float(m.group(3))))
        except ValueError:
            pass
    return out


class Daemon:
    """One replicate process (lsr or Go) running the load generator as its -exec child, in a process
    group of its own so that a kill -9 takes the application with it, as a crashed container would."""

    def __init__(self, drill: "Drill", tool: str, label: str):
        self.drill, self.tool, self.label = drill, tool, label
        self.p: subprocess.Popen | None = None
        self.log = drill.logs / f"{label}.log"
        self.t0 = 0.0
        self.t0_ms = 0
        self.rc: int | None = None

    def start(self) -> None:
        d = self.drill
        exec_cmd = shlex.join([sys.executable, str(Path(__file__).resolve()), "load", "--work", str(d.work), *d.load_args()])
        binary = d.args.lsr if self.tool == "lsr" else d.args.go
        self.p = subprocess.Popen([str(binary), "replicate", "-config", str(d.cfg), "-exec", exec_cmd],
                                  stdout=open(self.log, "ab"), stderr=subprocess.STDOUT, env=d.env(), start_new_session=True)
        self.t0, self.t0_ms = time.monotonic(), now_ms()
        d.epoch += 1
        d.current = self
        d.epochs.append({"epoch": d.epoch, "label": self.label, "tool": self.tool, "t0": self.t0, "t0_ms": self.t0_ms})

    def alive(self) -> bool:
        return self.p is not None and self.p.poll() is None

    @property
    def pid(self) -> int:
        return self.p.pid if self.p else 0

    def group_alive(self) -> bool:
        try:
            os.killpg(self.pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def stop(self, timeout: float) -> dict:
        """SIGTERM, wait, and prove it: the exit code, and that no process of the group is left."""
        assert self.p is not None
        t = time.monotonic()
        self.p.send_signal(signal.SIGTERM)
        try:
            self.rc = self.p.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.pid, signal.SIGKILL)
            self.rc = self.p.wait()
            return {"exit": "did not stop within %ds, killed" % timeout, "rc": self.rc, "clean": False, "secs": round(time.monotonic() - t, 1)}
        left = 0
        for _ in range(100):
            if not self.group_alive():
                break
            left += 1
            time.sleep(0.1)
        gone = not self.group_alive()
        if not gone:
            os.killpg(self.pid, signal.SIGKILL)
        self.drill.current = None
        return {"rc": self.rc, "clean": self.rc == 0 and gone, "group_gone": gone, "secs": round(time.monotonic() - t, 1)}

    def kill9(self) -> None:
        assert self.p is not None
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.p.wait()
        self.drill.current = None

    def notable(self) -> list[str]:
        """Log lines worth reading: warnings and errors, snapshots and their causes, takeovers."""
        out = []
        pat = re.compile(r'snapshot|cause|takeover|resync|recover|rewind|truncat|reset|restart|not found|mismatch', re.I)
        for line in self.log.read_text(errors="replace").splitlines():
            level = ""
            m = re.search(r'"level":\s*"(\w+)"', line) or re.search(r'\blevel=(\w+)', line)
            if m:
                level = m.group(1).upper()
            if level in ("WARN", "WARNING", "ERROR") or pat.search(line):
                out.append(line[:300])
        return out

    def held_ms(self) -> dict:
        """How long the daemon itself says it held the application's writes ("writes blocked" to
        "writes allowed", per database), from its own log."""
        out, blocked = {}, {}
        for line in self.log.read_text(errors="replace").splitlines():
            try:
                j = json.loads(line)
                t = datetime.fromisoformat(j["time"].replace("Z", "+00:00"))
            except (ValueError, KeyError, TypeError):
                continue
            if j.get("msg") == "writes blocked":
                blocked[j.get("db")] = t
            elif j.get("msg") == "writes allowed" and j.get("db") in blocked:
                out[j["db"]] = round((t - blocked.pop(j["db"])).total_seconds() * 1000, 1)
        return out

    def errors(self) -> list[str]:
        return [l for l in self.notable() if re.search(r'"level":\s*"ERROR"|\blevel=ERROR', l)]


class Sampler(threading.Thread):
    """Once a second: the running daemon's resident memory and CPU, the working folders' and the WAL's
    sizes, and its /metrics (the unreplicated age, bytes sent, requests by kind, uploads)."""

    def __init__(self, drill: "Drill"):
        super().__init__(daemon=True)
        self.d, self.stop_flag, self.rows = drill, threading.Event(), []
        self.out = open(drill.work / "samples.jsonl", "a")

    def run(self) -> None:
        import urllib.request
        last_cpu = None
        while not self.stop_flag.wait(1.0):
            cur = self.d.current
            if cur is None or not cur.alive():
                continue
            row: dict = {"t": round(time.monotonic() - self.d.t0, 1), "epoch": self.d.epoch, "label": cur.label, "tool": cur.tool}
            u = ps_usage(cur.pid)
            if u:
                row["rss"], row["cpu_s"] = u
            self.tick = getattr(self, "tick", 0) + 1
            if self.tick % self.d.args.mem_every == 0:
                row["mem"] = mem_detail(cur.pid)
            row["staging"] = sum(dir_bytes(Path(p).parent / f".{Path(p).name}-litestream") for p in self.d.dbs.values())
            row["wal"] = {n: (Path(str(p) + "-wal").stat().st_size if Path(str(p) + "-wal").exists() else 0) for n, p in self.d.dbs.items()}
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.d.port}/metrics", timeout=2) as r:
                    m = parse_metrics(r.read().decode(errors="replace"))
                ages = [v for _, v in m.get("lsr_unreplicated_age_seconds", [])]
                if ages:
                    row["age"] = max(ages)
                row["uploads"] = sum(v for _, v in m.get("lsr_uploads_total", []))
                row["snapshots"] = {l.get("cause", "?"): v for l, v in m.get("lsr_snapshots_total", [])}
                ops: dict = {}
                for l, v in m.get("litestream_replica_operation_total", []):
                    ops[l.get("operation", "?")] = ops.get(l.get("operation", "?"), 0) + v
                row["ops"] = ops
                row["sent"] = sum(v for _, v in m.get("litestream_replica_operation_bytes", []))
            except OSError:
                pass
            self.rows.append(row)
            self.out.write(json.dumps(row) + "\n")
            self.out.flush()


# ---------------------------------------------------------------- the drill


class Abort(Exception):
    """A phase cannot go on and the later phases have nothing to stand on."""


class Drill:
    def __init__(self, a: argparse.Namespace):
        self.args = a
        self.t0 = time.monotonic()
        self.work = Path(a.work).resolve()
        self.logs, self.scratch = self.work / "logs", self.work / "restores"
        for d in (self.logs, self.scratch, self.work / "db", self.work / "home"):
            d.mkdir(parents=True, exist_ok=True)
        self.dbs = {"big": self.work / "db" / "big.db", "small": self.work / "db" / "small.db"}
        self.epoch, self.epochs, self.current = 0, [], None
        self.phases: list[dict] = []
        self.findings: list[str] = []
        self.cur: list[str] = []
        self.digests: dict = {}
        self.port = free_port()
        self.rng = random.Random(a.seed)
        self.sampler = Sampler(self)
        self.cleanup_result: dict = {}
        self.store_setup()
        self.conf = self.build_conf()
        self.levels = [str(l["interval"]) for l in self.conf.get("levels", [])]
        self.sync_s = min(parse_duration(str(db["replica"]["sync-interval"])) for db in self.conf["dbs"])
        self.snapshot_s = parse_duration(str(self.conf["snapshot"]["interval"]))

    # -- the store

    def store_setup(self) -> None:
        a, e = self.args, os.environ
        if a.store == "minio":
            self.endpoint = e.get("LSR_COMPAT_S3_ENDPOINT", "http://127.0.0.1:9000")
            self.bucket = e.get("LSR_COMPAT_S3_BUCKET", "lsr-test")
            self.access = e.get("LSR_COMPAT_S3_ACCESS_KEY", "lsrtestroot")
            self.secret = e.get("LSR_COMPAT_S3_SECRET_KEY", "lsrtest-throwaway-0000")
            self.region = "us-east-1"
            u = urllib.parse.urlsplit(self.endpoint)
            if u.scheme != "http" or u.hostname not in ("127.0.0.1", "localhost", "::1"):
                raise SystemExit(f"trial_drill: --store minio takes a loopback http endpoint, not {self.endpoint}")
            self.prefix = f"trial-drill/{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{os.getpid()}"
        else:
            try:
                self.endpoint, self.bucket = e["LSR_PROVIDER_ENDPOINT"], e["LSR_PROVIDER_BUCKET"]
                self.access, self.secret, self.prefix = e["AWS_ACCESS_KEY_ID"], e["AWS_SECRET_ACCESS_KEY"], e["LSR_PROVIDER_PREFIX"]
            except KeyError as err:
                raise SystemExit(f"trial_drill: --store provider needs {err} (run it through scripts/provider.sh <provider>)")
            self.region = e.get("AWS_REGION") or "auto"
            self.prefix += "/trial-drill"
        self.s3 = S3(self.endpoint, self.bucket, self.access, self.secret, self.region)
        self.cfg = self.work / "litestream.yml"
        self.engine_endpoint = self.endpoint
        self.throttle = None
        if a.store == "minio":
            u = urllib.parse.urlsplit(self.endpoint)
            self.throttle = Throttle(u.hostname, u.port or 80, a.throttle_mib_s * (1 << 20))
            self.throttle.mplog = open(self.logs / "multipart-requests.log", "a")
            self.throttle.start()
            self.throttle.ready.wait()
            self.engine_endpoint = f"http://127.0.0.1:{self.throttle.port}"
        elif a.throttle_mib_s:
            raise SystemExit("trial_drill: --throttle-mib-s is for the local MinIO only")

    def env(self) -> dict:
        return {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(self.work / "home"),
                "AWS_ACCESS_KEY_ID": self.access, "AWS_SECRET_ACCESS_KEY": self.secret, "AWS_REGION": self.region,
                "AWS_EC2_METADATA_DISABLED": "true"}

    def build_conf(self) -> dict:
        """The one config both binaries read: the flags, then the overlay merged over them."""
        a = self.args
        conf: dict = {"addr": f"127.0.0.1:{self.port}", "logging": {"level": "info", "type": "json"},
                      "snapshot": {"interval": a.snapshot_interval, "retention": a.snapshot_retention},
                      "l0-retention-check-interval": a.l0_check, "l0-retention": a.l0_retention,
                      "levels": [{"interval": i} for i in a.levels.split(",")], "dbs": []}
        for name, path in self.dbs.items():
            db: dict = {"path": str(path)}
            if a.busy_timeout:
                db["busy-timeout"] = a.busy_timeout
            if a.truncate_page_n:
                db["truncate-page-n"] = a.truncate_page_n
            rep = {"type": "s3", "bucket": self.bucket, "path": f"{self.prefix}/{name}", "endpoint": self.engine_endpoint,
                   "region": self.region, "force-path-style": True, "sync-interval": a.sync_interval}
            if a.part_size:
                rep["part-size"] = a.part_size
            if a.concurrency:
                rep["concurrency"] = a.concurrency
            db["replica"] = rep
            conf["dbs"].append(db)
        if a.config_overlay:
            import yaml
            over = yaml.safe_load(a.config_overlay.read_text()) or {}
            per_db = over.pop("db", {}) or {}
            per_rep = per_db.pop("replica", {}) or {}
            for k, v in over.items():
                conf[k] = v
            for db in conf["dbs"]:
                db.update(per_db)
                db["replica"].update(per_rep)
        return conf

    def write_config(self) -> None:
        def emit(v, ind: int) -> list[str]:
            pad = "  " * ind
            out: list[str] = []
            if isinstance(v, dict):
                for k, x in v.items():
                    if isinstance(x, (dict, list)) and x:
                        out += [f"{pad}{k}:"] + emit(x, ind + 1)
                    else:
                        out.append(f"{pad}{k}: {scalar(x)}")
            else:
                for x in v:
                    if isinstance(x, dict):
                        sub = emit(x, ind + 1)
                        out.append(f"{pad}- " + sub[0].lstrip())
                        out += sub[1:]
                    else:
                        out.append(f"{pad}- {scalar(x)}")
            return out

        def scalar(x) -> str:
            if isinstance(x, bool):
                return "true" if x else "false"
            return json.dumps(x) if isinstance(x, (str, list, dict)) else str(x)

        self.cfg.write_text(NL.join(emit(self.conf, 0)) + NL)

    def load_args(self) -> list[str]:
        a = self.args
        out = ["--tps", str(a.tps), "--mix", a.mix, "--weights", a.weights, "--ratio", str(a.ratio), "--seed", str(a.seed),
               "--busy-timeout", "30", "--burst-bytes", str(self.burst_bytes())]
        for n, p in self.dbs.items():
            out += ["--db", f"{n}={p}"]
        return out

    def burst_bytes(self) -> int:
        return burst_bytes(self.conf, self.args.burst_mib)

    # -- reporting

    def fail(self, msg: str) -> None:
        self.cur.append(msg)

    def phase(self, n: int, name: str, fn) -> bool:
        t = time.monotonic()
        self.cur, nums, aborted = [], {}, False
        try:
            nums = fn() or {}
        except Abort as err:
            self.fail(str(err))
            aborted = True
        ok = not self.cur
        self.phases.append({"n": n, "name": name, "ok": ok, "secs": round(time.monotonic() - t, 1), "numbers": nums, "failures": self.cur})
        self.findings += [f"P{n} {name}: {m}" for m in self.cur]
        print(f"P{n} {name:<10} {'PASS' if ok else 'FAIL'}  {fmt(nums)}", flush=True)
        for m in self.cur:
            print(f"    ! {m}", flush=True)
        if aborted:
            raise Abort(name)
        return ok

    def wait(self, pred, timeout: float, what: str, every: float = 1.0):
        end = time.monotonic() + timeout
        while True:
            r = pred()
            if r:
                return r
            if self.current is not None and not self.current.alive() and not getattr(self, "expect_dead", False):
                raise Abort(f"{self.current.label} ended on its own (exit {self.current.p.returncode}) while waiting for {what}; "
                            f"log tail: {tail(self.current.log)}")
            if time.monotonic() > end:
                seen = list(self.throttle.recent)[-8:] if self.throttle is not None else []
                raise Abort(f"timed out after {int(timeout)}s waiting for {what}; the last requests seen: {seen}")
            time.sleep(every)

    # -- the bucket

    def listing(self) -> dict:
        """The replica's objects by database: {db: {"l0": [...], "levels": {n: [...]}, "snap": [...]}} with
        (min, max, size, key) per file."""
        keys = self.s3.list(self.prefix + "/")
        out = {n: {"snap": [], "levels": {}} for n in self.dbs}
        for k, size in keys:
            m = KEY.search(k)
            db = k[len(self.prefix) + 1:].split("/", 1)[0]
            if not m or db not in out:
                continue
            lvl, lo, hi = int(m.group(1)), int(m.group(2), 16), int(m.group(3), 16)
            (out[db]["snap"] if lvl == SNAPSHOT_LEVEL else out[db]["levels"].setdefault(lvl, [])).append((lo, hi, size, k))
        return out

    def inflight(self) -> list[str]:
        """Keys of multipart uploads in flight: from the real listing on a provider, from the proxy's
        reading of the requests on the local MinIO (which lists none)."""
        if self.throttle is not None:
            return list(self.throttle.open)
        return [k for k, _ in self.s3.uploads(self.prefix + "/")]

    def keyset(self) -> set:
        return {k for k, _ in self.s3.list(self.prefix + "/")}

    # -- restoring and judging

    def restore(self, tool: str, db: str, label: str, extra: list[str]) -> dict:
        out = self.scratch / f"{label}-{tool}.db"
        for s in ("", "-wal", "-shm"):
            Path(str(out) + s).unlink(missing_ok=True)
        binary = self.args.lsr if tool == "lsr" else self.args.go
        t = time.monotonic()
        r = subprocess.run([str(binary), "restore", "-config", str(self.cfg), "-o", str(out), *extra, str(self.dbs[db])],
                           capture_output=True, text=True, env=self.env(), timeout=self.args.restore_timeout)
        text = (r.stderr or r.stdout).strip()
        return {"tool": tool, "rc": r.returncode, "secs": round(time.monotonic() - t, 1), "out": out, "text": text[-400:]}

    def check_target(self, db: str, label: str, extra: list[str], instant_ms: int | None = None, expect_seq: int | None = None,
                     vs_live: bool = False, again_key: str | None = None, compare_go: bool = True) -> dict:
        """Restores one target with both binaries and judges both against the ledger and each other."""
        ledger = self.work / f"ledger-{db}.log"
        res: dict = {"target": label, "db": db}
        got = {}
        for tool in ("lsr", "go") if compare_go else ("lsr",):
            r = self.restore(tool, db, f"{db}-{label}", extra)
            if r["rc"] != 0:
                res[f"{tool}_refused"] = r["text"][-200:]
                continue
            got[tool] = r
            res[f"{tool}_secs"] = r["secs"]
        if "lsr" not in got:
            if re.search(r"compact|straddl|inside ltx|finer files", res.get("lsr_refused", ""), re.I):
                res["note"] = "lsr refused a target inside a compacted file (the documented refusal)"
                if "go" in got:
                    # Go answered where lsr refused: its answer must still be a committed state no newer than the instant
                    gs = scan_db(got["go"]["out"])
                    bad = judge_state(gs, ledger, instant_ms)
                    res["go_answered"] = {"seq": gs["seq"], "judged": "ok" if not bad else bad[0]}
                    for b in bad:
                        self.fail(f"{db} {label}: lsr refused, and Go's answer is wrong: {b}")
                    got["go"]["out"].unlink(missing_ok=True)
                return res
            self.fail(f"{db} {label}: lsr restore refused: {res.get('lsr_refused')}")
            return res
        if compare_go and "go" not in got:
            self.fail(f"{db} {label}: lsr restored but Go refused: {res.get('go_refused')}")
        lsr_out = got["lsr"]["out"]
        scan = scan_db(lsr_out)
        res.update(seq=scan["seq"], rows=scan["rows"], pages=lsr_out.stat().st_size // PAGE)
        for bad in judge_state(scan, ledger, instant_ms):
            self.fail(f"{db} {label}: {bad}")
        if expect_seq is not None and scan["seq"] != expect_seq:
            self.fail(f"{db} {label}: restored seq {scan['seq']}, the ledger's last acknowledged commit is {expect_seq}")
        if instant_ms:
            res["instant_gap_s"] = round((instant_ms - scan["ts"]) / 1000, 1)
        t = time.monotonic()
        res["integrity"] = integrity(lsr_out, self.args.integrity)
        res["integrity_secs"] = round(time.monotonic() - t, 1)
        if res["integrity"] != "ok":
            self.fail(f"{db} {label}: integrity_check on the lsr restore: {res['integrity']}")
        res["sha"] = file_sha(lsr_out)
        if "go" in got:
            same, first, n = files_differ(lsr_out, got["go"]["out"])
            res["lsr_eq_go"] = same
            if not same:
                self.fail(f"{db} {label}: lsr and Go restored different pages ({n} differ, first page {first})")
                res["go_integrity"] = integrity(got["go"]["out"], self.args.integrity)
                gs = scan_db(got["go"]["out"])
                res["go_seq"] = gs["seq"]
                if gs["xor"] != gs["state_xor"]:
                    self.fail(f"{db} {label}: the Go restore is not one committed state either")
            got["go"]["out"].unlink(missing_ok=True)
        if vs_live:
            live = self.dbs[db]
            wal = Path(str(live) + "-wal")
            if wal.exists() and wal.stat().st_size > 0 and live.stat().st_size > self.args.live_copy_max_gib * (1 << 30):
                res["vs_live"] = "skipped: the live database still has a WAL and is too big to copy (the ledger check stands)"
            else:
                if wal.exists() and wal.stat().st_size > 0:
                    # a copy, checkpointed, holds the pages SQLite reads; the live files stay as lsr left them
                    live = self.scratch / f"{db}-live-copy.db"
                    shutil.copyfile(self.dbs[db], live)
                    shutil.copyfile(wal, str(live) + "-wal")
                    c = sqlite3.connect(live)
                    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    c.close()
                same, first, n = files_differ(lsr_out, live)
                if live != self.dbs[db]:
                    for s in ("", "-wal", "-shm"):
                        Path(str(live) + s).unlink(missing_ok=True)
                res["vs_live"] = "equal" if same else f"{n} pages differ, first {first}"
                if not same:
                    self.fail(f"{db} {label}: restored pages differ from the live database's ({n}, first page {first})")
        if again_key:
            before = self.digests.get(again_key)
            if before and before != res["sha"]:
                self.fail(f"{db} {label}: the same target restored to different pages than before the rollback")
            self.digests[again_key] = res["sha"]
        lsr_out.unlink(missing_ok=True)
        return res

    def pick_txid(self, db: str) -> int | None:
        """A transaction id the replica promises exactly: the end of a compacted file, else of a level 0
        file, from the middle of what is listed (the newest is left alone)."""
        lst = self.listing()[db]
        ends = sorted({hi for lvl, fs in lst["levels"].items() if lvl > 0 for _lo, hi, _s, _k in fs})
        if not ends:
            ends = sorted({hi for _lo, hi, _s, _k in lst["levels"].get(0, [])})
        ends = ends[:-1] or ends
        return self.rng.choice(ends[len(ends) // 2 :]) if ends else None

    def restore_matrix(self, tag: str, instant_by_db: dict, txids: dict, latest: bool, again: bool) -> list[dict]:
        results = []
        for db in self.dbs:
            led = ledger_scan(self.work / f"ledger-{db}.log", set(), [])
            if latest:
                results.append(self.check_target(db, f"{tag}-latest", [], expect_seq=led["last_ack"], vs_live=True))
            if txids.get(db):
                results.append(self.check_target(db, f"{tag}-txid", ["--txid", f"{txids[db]:016x}"], again_key=(db, "txid") if again else None))
            if instant_by_db.get(db):
                ms = instant_by_db[db]
                results.append(self.check_target(db, f"{tag}-instant", ["--timestamp", rfc3339(ms)], instant_ms=ms))
        return results


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def tail(path: Path, n: int = 300) -> str:
    try:
        return path.read_text(errors="replace")[-n:].replace("\n", " | ")
    except OSError:
        return ""


def fmt(nums: dict) -> str:
    def one(v):
        if isinstance(v, float):
            return f"{v:g}"
        if isinstance(v, (dict, list)):
            return json.dumps(v, separators=(",", ":"))[:160]
        return str(v)
    return " ".join(f"{k}={one(v)}" for k, v in nums.items())


def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def files_differ(a: Path, b: Path) -> tuple[bool, int, int]:
    """(equal, first differing page (1-based), number of differing pages); a length difference counts."""
    first = n = 0
    sa, sb = a.stat().st_size, b.stat().st_size
    with open(a, "rb") as fa, open(b, "rb") as fb:
        page = 0
        chunk = 256 * PAGE
        while True:
            xs, ys = fa.read(chunk), fb.read(chunk)
            if not xs and not ys:
                break
            if xs != ys:  # a mismatch somewhere in the megabyte: count its pages
                for o in range(0, max(len(xs), len(ys)), PAGE):
                    if xs[o : o + PAGE] != ys[o : o + PAGE]:
                        n += 1
                        first = first or page + o // PAGE + 1
            page += chunk // PAGE
    return (n == 0 and sa == sb), first, n


# ---------------------------------------------------------------- the phases


def scrape(d: Drill) -> dict:
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{d.port}/metrics", timeout=2) as r:
            return parse_metrics(r.read().decode(errors="replace"))
    except OSError:
        return {}


def total(m: dict, name: str, label: str | None = None, value: str | None = None) -> float:
    return sum(v for l, v in m.get(name, []) if label is None or l.get(label) == value)


def writer_window(d: Drill, t0_ms: int, t1_ms: int) -> dict:
    """The application's own view of the stalls: the longest wait for the write lock and the longest
    commit in the window, its busy errors, and the large transactions it ran."""
    begin = commit = busy = 0.0
    bursts = []
    p = d.work / "writer.stats"
    if p.exists():
        for line in p.read_text().splitlines():
            f = line.split()
            try:
                if f[0] == "S" and t0_ms <= int(f[1]) <= t1_ms:
                    begin, commit, busy = max(begin, float(f[4])), max(commit, float(f[5])), busy + float(f[6])
                elif f[0] == "B" and t0_ms <= int(f[1]) <= t1_ms:
                    bursts.append(float(f[3]))
            except (IndexError, ValueError):
                pass
    return {"lock_wait_max_ms": round(begin, 1), "commit_max_ms": round(commit, 1), "busy_errors": int(busy), "bursts_s": bursts}


def first_sample(d: Drill, label: str, pred) -> float | None:
    for r in d.sampler.rows:
        if r["label"] == label and pred(r):
            return r["t"] - (next(e["t0"] for e in d.epochs if e["label"] == label) - d.t0)
    return None


def catch_up(d: Drill, label: str, timeout: float) -> float:
    thresh = max(2 * d.sync_s + 5, 15)
    d.wait(lambda: any(r["label"] == label and r.get("age") is not None and r["age"] <= thresh for r in d.sampler.rows[-5:]),
           timeout, f"{label} to catch up (unreplicated age at most {thresh:g}s)")
    return round(time.monotonic() - d.current.t0, 1)


def p1_generate(d: Drill) -> dict:
    a = d.args
    t = time.monotonic()
    nums: dict = {}
    sizes = {"big": int(a.gib * (1 << 30)), "small": min(64 << 20, max(4 << 20, int(a.gib * (1 << 30)) // 20))}
    for name, path in d.dbs.items():
        info = generate(path, sizes[name], a.ratio, a.seed + (name == "small"))
        led = d.work / f"ledger-{name}.log"
        led.write_text(f"I 0 {info['ts']} {info['xor']}\nA 0 {info['ts']}\n")
        nums[name] = {"mib": mib(path.stat().st_size), "pages": info["pages"], "rows": info["rows"],
                      "incompressible_fraction": info["incompressible_fraction"], "zlib1_ratio": info["zlib1_ratio_of_file_sample"]}
    nums["page_size"] = PAGE
    nums["write_mix"] = f"{a.tps} tx/s of 1-4 ops, insert/update/delete {a.mix} %, big:small {a.weights}, burst {mib(d.burst_bytes())} MiB"
    nums["gen_secs"] = round(time.monotonic() - t, 1)
    d.write_config()
    return nums


def go_stop(d: Drill, dm: Daemon) -> dict:
    r = dm.stop(d.args.stop_timeout)
    if not r["clean"]:
        d.fail(f"{dm.label} did not stop cleanly: {r}")
    return r


def p2_go_first(d: Drill) -> dict:
    a = d.args
    dm = Daemon(d, "go", "go1")
    dm.start()
    d.wait(lambda: all(d.listing()[n]["snap"] for n in d.dbs), a.first_timeout, "Go's first snapshot of both databases", every=2)
    t_snap = round(time.monotonic() - dm.t0, 1)
    time.sleep(a.go_seconds)
    lst = d.listing()
    stop = go_stop(d, dm)
    nums = {"first_snapshot_secs": t_snap, "snapshot_mib": {n: [mib(f[2]) for f in lst[n]["snap"]] for n in d.dbs},
            "l0_files": {n: len(lst[n]["levels"].get(0, [])) for n in d.dbs}, "stop": stop}
    u = [r for r in d.sampler.rows if r["label"] == "go1" and "rss" in r]
    nums["go_peak_rss_mib"] = mib(max((r["rss"] for r in u), default=0))
    if dm.errors():
        d.fail(f"Go logged {len(dm.errors())} error lines, e.g. {dm.errors()[0][:200]}")
    return nums


def p3_takeover(d: Drill) -> dict:
    a = d.args
    before = d.listing()
    snaps_before = {n: len(before[n]["snap"]) for n in d.dbs}
    dm = Daemon(d, "lsr", "lsr1")
    dm.start()
    d.p3_start_ms = dm.t0_ms
    first = d.wait(lambda: next((r["t"] for r in d.sampler.rows if r["label"] == "lsr1" and r.get("uploads", 0) > 0), None),
                   a.budget_catchup, "lsr's first upload", every=0.5)
    t_first = round(first - (dm.t0 - d.t0), 1)
    caught = catch_up(d, "lsr1", a.budget_catchup)
    time.sleep(max(0.0, 20 - (time.monotonic() - dm.t0)))  # a window long enough to hold stall samples
    after = d.listing()
    m = scrape(d)
    snaps = {c: v for l, v in m.get("lsr_snapshots_total", []) for c in [l.get("cause", "?")] if v}
    new_snaps = {n: len(after[n]["snap"]) - snaps_before[n] for n in d.dbs}
    nums = {"first_upload_secs": t_first, "caught_up_secs": caught,
            "continued_or_snapshotted": "snapshotted" if snaps or any(v > 0 for v in new_snaps.values()) else "continued",
            "new_snapshots": new_snaps, "snapshot_causes": snaps,
            "writes_held": writer_window(d, dm.t0_ms, now_ms()), "lsr_logged_hold_ms": dm.held_ms(), "notable_log": [l[:200] for l in dm.notable()[:8]]}
    errs = dm.errors()
    if errs:
        d.fail(f"lsr logged {len(errs)} error lines at takeover, e.g. {errs[0][:240]}")
    held = nums["writes_held"]
    if max(held["lock_wait_max_ms"], held["commit_max_ms"]) > a.budget_stall * 1000:
        d.fail(f"the application's writes were held {max(held['lock_wait_max_ms'], held['commit_max_ms']):.0f} ms at takeover (budget {a.budget_stall}s)")
    return nums


def kill_and_restart(d: Drill, point: str, n: int, trigger_info: dict) -> dict:
    a = d.args
    dm = d.current
    keys = d.keyset()
    dm.kill9()
    if d.throttle is not None:
        d.throttle.open.clear()  # the killed process's uploads will never complete
    time.sleep(1.0)
    nd = Daemon(d, "lsr", f"lsr{n}")
    nd.start()
    caught = catch_up(d, nd.label, a.budget_catchup)
    out = {"point": point, **trigger_info, "restart_caught_up_secs": caught, "objects_at_kill": len(keys),
           "restart_hold_ms": nd.held_ms()}
    # a restore while lsr runs, of the small database: some committed state, judged against the ledger
    r = d.check_target("small", f"after-{point}", [], compare_go=False)  # the writer moves between two restores
    out["restore_check"] = {k: r.get(k) for k in ("seq", "integrity", "lsr_eq_go")}
    errs = nd.errors()
    if errs:
        d.fail(f"lsr logged an error after the {point} kill: {errs[0][:240]}")
    return out


def p4_lsr_load(d: Drill) -> dict:
    a = d.args
    t_start = d.current.t0
    caught = catch_up(d, "lsr1", a.budget_catchup)
    kills = []
    seen_l0: set = set()
    # 1. kill -9 during a large upload: the application commits one transaction of more than a part
    (d.work / "burst").write_text("big")

    def multipart():
        return [k for k in d.inflight() if f"/{SNAPSHOT_LEVEL:04d}/" not in k] or None

    ups = d.wait(multipart, 1800, "a multipart upload in flight (the burst's level 0 file)", every=0.2)
    seen_l0.update(k for k in d.keyset() if "/0000/" in k)
    kills.append(kill_and_restart(d, "large-upload", 2, {"in_flight": ups[0].rsplit("/", 2)[-2:]}))
    # 2. kill -9 during a compaction: lsr reads its inputs (GETs) only when it compacts
    d.wait(lambda: (scrape(d) and True), 120, "lsr's metrics", every=1)
    gets0 = total(scrape(d), "litestream_replica_operation_total", "operation", "GET")

    def compacting():
        seen_l0.update(k for k in d.keyset() if "/0000/" in k)
        return total(scrape(d), "litestream_replica_operation_total", "operation", "GET") > gets0 or None

    to = parse_duration(d.levels[0]) * 3 + 900
    d.wait(compacting, to, "a compaction reading its inputs", every=0.2)
    kills.append(kill_and_restart(d, "compaction", 3, {"trigger": "GET count rose"}))
    # 3. kill -9 during a snapshot: the scheduled one, as it is staged or its upload is in flight
    snaps0 = total(scrape(d), "lsr_snapshots_total")
    snap_keys0 = {n: {f[3] for f in d.listing()[n]["snap"]} for n in d.dbs}

    def snapshotting():
        if total(scrape(d), "lsr_snapshots_total") > snaps0:
            return {"trigger": "snapshot staged"}
        if any(f"/{SNAPSHOT_LEVEL:04d}/" in k for k in d.inflight()):
            return {"trigger": "snapshot upload in flight"}
        return None

    to = d.snapshot_s + 900
    info = d.wait(snapshotting, to, "the scheduled snapshot", every=0.3)
    t_k3 = time.monotonic()
    kills.append(kill_and_restart(d, "snapshot", 4, info))
    # the rest: run until compaction reached the top level, a scheduled snapshot completed after the kill, a
    # retention pass deleted something, and the minimum run time passed
    top = len(d.levels)

    def settled():
        for k in d.keyset():
            if "/0000/" in k:
                seen_l0.add(k)
        lst = d.listing()
        have_top = all(lst[n]["levels"].get(top) for n in d.dbs)
        snap_done = any({f[3] for f in lst[n]["snap"]} - snap_keys0[n] for n in d.dbs)
        gone = len(seen_l0 - d.keyset()) > 0
        old = time.monotonic() - t_start >= a.min_run * 60
        d.progress = {"top_level": have_top, "snapshot_after_kill": snap_done, "l0_retention_deleted": gone, "min_run": old}
        return have_top and snap_done and gone and old

    try:
        d.wait(settled, a.max_run * 60, "compaction to the top level, a snapshot after the kill and a retention pass", every=5 if a.store == "minio" else 30)
    except Abort as err:
        d.fail(f"{err}; state: {getattr(d, 'progress', None)}")
    lst = d.listing()
    dm = d.current
    nums = {"kills": kills, "levels_present": {n: sorted(lst[n]["levels"]) for n in d.dbs},
            "snapshots": {n: len(lst[n]["snap"]) for n in d.dbs}, "progress": getattr(d, "progress", None),
            "writer": writer_window(d, d.epochs[1]["t0_ms"], now_ms())}
    return nums


def budgets(d: Drill) -> dict:
    """The sampler's numbers for every lsr epoch against the budgets (the Go epochs have no age series)."""
    a = d.args
    rows = [r for r in d.sampler.rows if r["tool"] == "lsr"]
    pages = sum(Path(p).stat().st_size for p in d.dbs.values()) // PAGE
    peak = max((r.get("rss", 0) for r in rows), default=0)
    limit = (a.budget_rss_base_mib << 20) + a.budget_rss_bytes_per_page * pages
    out = {"rss_peak_mib": mib(peak), "rss_limit_mib": mib(limit), "pages": pages,
           "rss_bytes_per_page": round(peak / max(1, pages), 1), "cpu_s": 0.0}
    wal_max = max((max(r["wal"].values(), default=0) for r in rows), default=0)
    out.update(wal_max_mib=mib(wal_max), staging_max_mib=mib(max((r.get("staging", 0) for r in rows), default=0)))
    out["age_max_s"], out["age_violations"], quiet = 0.0, 0, 0.0
    thresh = max(2 * d.sync_s + 5, 15)
    for ep in sorted({r["epoch"] for r in rows}):
        er = [r for r in rows if r["epoch"] == ep]
        out["cpu_s"] += max((r.get("cpu_s", 0) for r in er), default=0)
        caught = False
        last_up, last_t = None, er[0]["t"]
        for r in er:
            if "age" in r:
                out["age_max_s"] = max(out["age_max_s"], r["age"])
                caught = caught or r["age"] <= thresh
                if caught and r["age"] > a.budget_lag:
                    out["age_violations"] += 1
            if "uploads" in r:
                if r["uploads"] != last_up:
                    last_up, last_t = r["uploads"], r["t"]
                quiet = max(quiet, r["t"] - last_t)
    out["cpu_s"] = round(out["cpu_s"], 1)
    out["longest_no_upload_s"] = round(quiet, 1)
    sent = [r.get("sent", 0) for r in rows]
    out["bytes_sent_mib_last_epoch"] = mib(sent[-1] if sent else 0)
    out["requests_last_epoch"] = next((r["ops"] for r in reversed(rows) if r.get("ops")), {})
    if peak > limit:
        d.fail(f"resident memory peaked at {mib(peak)} MiB, over the budget {mib(limit)} MiB "
               f"({a.budget_rss_base_mib} MiB + {a.budget_rss_bytes_per_page} B x {pages} pages)")
    if wal_max > a.budget_wal_mib << 20:
        d.fail(f"the WAL reached {mib(wal_max)} MiB, over the budget {a.budget_wal_mib} MiB")
    if out["age_violations"]:
        d.fail(f"the unreplicated age exceeded {a.budget_lag}s on {out['age_violations']} samples after catching up (max {out['age_max_s']:g}s)")
    if quiet > a.budget_quiet:
        d.fail(f"no successful upload for {quiet:g}s while lsr ran (budget {a.budget_quiet}s)")
    return out


def p5_restore(d: Drill) -> dict:
    a = d.args
    stop = go_stop(d, d.current)
    nums = {"stop": stop, "budgets": budgets(d)}
    txids = {db: d.pick_txid(db) for db in d.dbs}
    instants = {}
    for db in d.dbs:
        led = ledger_scan(d.work / f"ledger-{db}.log", set(), [])
        last = next((int(l.split()[2]) for l in reversed((d.work / f"ledger-{db}.log").read_text().splitlines()) if l.startswith("A ")), now_ms())
        instants[db] = last - 30_000
    d.txids, d.instants_p5 = txids, instants
    res = d.restore_matrix("p5", instants, txids, latest=True, again=True)
    nums["txids"] = {k: (f"{v:016x}" if v else None) for k, v in txids.items()}
    nums["targets"] = [{k: r[k] for k in ("db", "target", "seq", "integrity", "lsr_eq_go", "vs_live", "instant_gap_s", "lsr_secs", "go_secs", "note", "go_refused") if k in r} for r in res]
    return nums


def p6_rollback(d: Drill) -> dict:
    a = d.args
    before = d.listing()
    snaps_before = {n: {f[3] for f in before[n]["snap"]} for n in d.dbs}
    newest_before = {n: max((f[1] for fs in before[n]["levels"].values() for f in fs), default=0) for n in d.dbs}
    dm = Daemon(d, "go", "go2")
    dm.start()

    def advanced():
        lst = d.listing()
        return all(max((f[1] for fs in lst[n]["levels"].values() for f in fs), default=0) > newest_before[n] for n in d.dbs)

    d.wait(advanced, a.budget_catchup, "Go to upload past lsr's newest transaction", every=1)
    t_up = round(time.monotonic() - dm.t0, 1)
    time.sleep(a.go_seconds)
    after = d.listing()
    new_snaps = {n: len({f[3] for f in after[n]["snap"]} - snaps_before[n]) for n in d.dbs}
    stop = go_stop(d, dm)
    nums = {"first_new_upload_secs": t_up, "go_resumed": "snapshotted" if any(new_snaps.values()) else "continued",
            "new_snapshots": new_snaps, "stop": stop, "writes_held": writer_window(d, dm.t0_ms, now_ms()),
            "notable_log": [l[:200] for l in dm.notable()[:8]]}
    errs = dm.errors()
    if errs:
        d.fail(f"Go logged {len(errs)} error lines after the rollback, e.g. {errs[0][:240]}")
    instants = {}
    for db in d.dbs:
        last = next((int(l.split()[2]) for l in reversed((d.work / f"ledger-{db}.log").read_text().splitlines()) if l.startswith("A ")), now_ms())
        instants[db] = last - 30_000
    res = d.restore_matrix("p6", instants, d.txids, latest=True, again=True)
    nums["targets"] = [{k: r[k] for k in ("db", "target", "seq", "integrity", "lsr_eq_go", "vs_live", "instant_gap_s", "note") if k in r} for r in res]
    return nums


def p7_summary(d: Drill) -> dict:
    """The run prefix is removed, multipart uploads included, and must list empty."""
    r = d.s3.purge(d.prefix + "/")
    if d.args.store == "minio":
        r["note"] = "this MinIO lists no multipart uploads, so none could be aborted or checked; a provider run does"
    d.cleanup_result = r
    if r["left_objects"] or r["left_uploads"]:
        d.fail(f"the run prefix is not empty after cleanup: {r}")
    return {"cleanup": r, "store_requests_by_the_drill": d.s3.requests}


def print_table(d: Drill, ok: bool) -> None:
    print("\n== trial drill summary ==")
    print(f"{'phase':<14}{'result':<8}{'secs':>8}")
    for p in d.phases:
        print(f"P{p['n']} {p['name']:<11}{'PASS' if p['ok'] else 'FAIL':<8}{p['secs']:>8}")
    for name, why in d.not_run:
        print(f"{name:<14}{'NOT RUN':<8}  ({why})")
    for f in d.findings:
        print(f"finding: {f}")
    print(f"result: {'PASS' if ok else 'FAIL'} (store {d.args.store}, {d.args.gib} GiB, work {d.work})")


def run_drill(a: argparse.Namespace) -> int:
    d = Drill(a)
    d.not_run = []
    d.sampler.start()
    started = now_ms()
    plan = [(1, "generate", p1_generate), (2, "go-first", p2_go_first), (3, "takeover", p3_takeover),
            (4, "lsr-load", p4_lsr_load), (5, "restore", p5_restore), (6, "rollback", p6_rollback)]
    rig_error = None
    try:
        for i, (n, name, fn) in enumerate(plan):
            try:
                d.phase(n, name, lambda fn=fn: fn(d))
                if n >= a.stop_after:
                    d.not_run = [(f"P{m} {nm}", f"stopped after P{n} by --stop-after") for m, nm, _ in plan[i + 1:]]
                    break
            except Abort:
                d.not_run = [(f"P{m} {nm}", f"P{n} {name} aborted") for m, nm, _ in plan[i + 1:]]
                break
    except BaseException as err:  # the drill itself broke
        rig_error = err
        import traceback
        traceback.print_exc()
    finally:
        if d.current is not None:
            if d.current.alive():
                d.current.kill9()
        d.sampler.stop_flag.set()
        try:
            d.phase(7, "summary", lambda: p7_summary(d))
        except Abort:
            pass
        except Exception as err:  # noqa: BLE001
            print(f"cleanup failed: {err}", file=sys.stderr)
            d.findings.append(f"cleanup failed: {err}")
    ok = not d.findings and not d.not_run and rig_error is None
    print_table(d, ok)
    summary = {"ok": ok, "store": a.store, "gib": a.gib, "prefix": d.prefix, "started_ms": started, "finished_ms": now_ms(),
               "phases": d.phases, "not_run": d.not_run, "findings": d.findings, "config": d.cfg.read_text() if d.cfg.exists() else "",
               "budgets": {k: v for k, v in vars(a).items() if k.startswith("budget_")}, "epochs": d.epochs}
    (d.work / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
    print(f"summary: {d.work / 'summary.json'}")
    return 2 if rig_error else (0 if ok else 1)


# ---------------------------------------------------------------- the canary


def run_canary(_a: argparse.Namespace) -> int:
    """The checkers must catch damage: a database with a flipped payload byte, a stale state row and a
    state that never existed must each be reported, and the intact database must pass."""
    import tempfile
    with tempfile.TemporaryDirectory() as t:
        w = Path(t)
        db = w / "c.db"
        info = generate(db, 2 << 20, 0.5, 7)
        led = w / "ledger-c.log"
        led.write_text(f"I 0 {info['ts']} {info['xor']}\nA 0 {info['ts']}\n")
        wr = DbWriter("c", str(db), str(led), 9, 0.5, 5.0)
        for _ in range(200):
            wr.transaction(["insert", "update", "delete"])
        wr.c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        wr.c.close()
        problems = []
        base = scan_db(db)
        if judge_state(base, led):
            problems.append(f"the intact database was reported: {judge_state(base, led)}")
        if base["seq"] != 200 or base["rows"] < 100:
            problems.append(f"the writer's state is odd: {base['seq']} {base['rows']}")
        flipped = w / "flipped.db"
        shutil.copy(db, flipped)
        c = sqlite3.connect(flipped)
        rid, rp = c.execute("SELECT id, payload FROM t ORDER BY id LIMIT 1").fetchone()
        c.execute("UPDATE t SET payload = ? WHERE id = ?", (bytes((rp[0] ^ 1,)) + rp[1:], rid))
        c.commit()
        c.close()
        if not judge_state(scan_db(flipped), led):
            problems.append("a flipped payload byte was not reported")
        stale = w / "stale.db"
        shutil.copy(db, stale)
        c = sqlite3.connect(stale)
        c.execute("UPDATE drill_state SET seq = seq + 1000")
        c.commit()
        c.close()
        if not judge_state(scan_db(stale), led):
            problems.append("a state that never existed was not reported")
        early = ledger_scan(led, set(), [info["ts"] - 1])
        if early["before"][info["ts"] - 1] != 0:
            problems.append("the instant bound counted a commit begun after the instant")
        if not judge_state(base, led, instant_ms=info["ts"] - 1):
            problems.append("a state newer than the instant was not reported")
        if files_differ(db, flipped)[0]:
            problems.append("the page comparison did not see a flipped byte")
        # The burst must be a multipart upload under the part-size an overlay sets, not only under --part-size.
        over = {"dbs": [{"replica": {"part-size": "100MB"}}]}
        if burst_bytes(over, 0) < 2 * parse_size("100MB"):
            problems.append(f"a 100MB part-size from the overlay gives a burst of {burst_bytes(over, 0)} bytes")
        # A restarted writer must not read the whole table before its first commit (a minute at 24 GiB).
        plan = " ".join(str(r[-1]) for r in sqlite3.connect(db).execute("EXPLAIN QUERY PLAN " + RELOAD_BOUNDS))
        if re.search(r"\bSCAN t\b", plan):
            problems.append(f"the writer's restart reads the whole table: {plan}")
        if problems:
            print("canary FAILED:\n  " + "\n  ".join(problems))
            return 1
    print("canary: the checkers reported every planted fault and passed the intact database")
    return 0


# ---------------------------------------------------------------- the command line


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run the drill")
    run.add_argument("--gib", type=float, required=True, help="the large database's size in GiB (the small one is a twentieth, 4 to 64 MiB)")
    run.add_argument("--store", choices=("minio", "provider"), default="minio")
    run.add_argument("--work", type=Path, default=None, help="working directory (default target/trial-drill/<time>)")
    run.add_argument("--lsr", type=Path, default=REPO / "target" / "release" / "lsr")
    run.add_argument("--go", type=Path, default=None, help="the pinned Go build (default .upstream/bin/litestream-041a8c3)")
    run.add_argument("--seed", type=int, default=1)
    g = run.add_argument_group("config and load (config defaults are Litestream's own documented defaults)")
    g.add_argument("--snapshot-interval", default="24h")
    g.add_argument("--snapshot-retention", default="24h")
    g.add_argument("--levels", default="30s,5m,1h", help="compaction level intervals, level 1 first")
    g.add_argument("--l0-check", default="15s", help="l0-retention-check-interval")
    g.add_argument("--l0-retention", default="5m")
    g.add_argument("--sync-interval", default="1s")
    g.add_argument("--part-size", default=None, help="the replica's part-size (default: unset, the client's own)")
    g.add_argument("--concurrency", type=int, default=None, help="the replica's concurrency (default: unset)")
    g.add_argument("--truncate-page-n", type=int, default=None, help="the database's truncate-page-n (default: unset)")
    g.add_argument("--busy-timeout", default=None, help="the database's busy-timeout (default: unset)")
    g.add_argument("--config-overlay", type=Path, default=None, help="a YAML fragment merged over the generated config: top-level keys as they are, plus db: merged into every database and db.replica: into every replica, so a shape under test is passed at run time")
    g.add_argument("--tps", type=float, default=40, help="transactions per second, all databases")
    g.add_argument("--mix", default="50,40,10", help="insert,update,delete percent of operations")
    g.add_argument("--weights", default="85,15", help="share of transactions for the large and the small database")
    g.add_argument("--ratio", type=float, default=0.5, help="fraction of rows that are incompressible")
    g.add_argument("--burst-mib", type=int, default=0, help="size of the one large transaction (default 3 x the configured part-size, or 64 when none is set)")
    t = run.add_argument_group("durations")
    t.add_argument("--go-seconds", type=float, default=120, help="how long each Go phase runs after it is replicating")
    t.add_argument("--min-run", type=float, default=90, help="minutes lsr runs under load at least (compaction to level 3 needs the last level's interval)")
    t.add_argument("--max-run", type=float, default=600, help="minutes after which phase 4 gives up")
    t.add_argument("--first-timeout", type=float, default=7200, help="seconds Go may take to its first snapshot")
    t.add_argument("--stop-timeout", type=float, default=600)
    t.add_argument("--restore-timeout", type=float, default=7200)
    b = run.add_argument_group("budgets (a run over any of them fails)")
    b.add_argument("--budget-lag", type=float, default=60, help="seconds of unreplicated age once caught up")
    b.add_argument("--budget-catchup", type=float, default=900, help="seconds to a first upload and to caught up after a start")
    b.add_argument("--budget-quiet", type=float, default=120, help="seconds without a successful upload while lsr runs")
    b.add_argument("--budget-stall", type=float, default=25, help="seconds the application's writes may be held at takeover")
    b.add_argument("--budget-rss-base-mib", type=int, default=256)
    b.add_argument("--budget-rss-bytes-per-page", type=int, default=32)
    b.add_argument("--budget-wal-mib", type=int, default=1024)
    run.add_argument("--stop-after", type=int, default=7, help="stop after this phase (4 = through the lsr run under load); the summary and cleanup still run")
    run.add_argument("--mem-every", type=int, default=5, help="seconds between the memory breakdown samples (smaps_rollup on Linux, footprint on macOS)")
    run.add_argument("--integrity", choices=("full", "quick"), default="full")
    run.add_argument("--live-copy-max-gib", type=float, default=4, help="the newest restore is compared page for page with a checkpointed copy of the live database up to this size")
    run.add_argument("--throttle-mib-s", type=float, default=0, help="local MinIO only: limit what lsr and Go upload to this rate, so a small run's uploads stay in flight long enough to kill")
    ld = sub.add_parser("load", help="the load generator (run by the drill as the -exec command)")
    ld.add_argument("--work", required=True)
    ld.add_argument("--db", action="append", required=True)
    ld.add_argument("--tps", type=float, default=40)
    ld.add_argument("--mix", default="50,40,10")
    ld.add_argument("--weights", default="85,15")
    ld.add_argument("--ratio", type=float, default=0.5)
    ld.add_argument("--seed", type=int, default=1)
    ld.add_argument("--busy-timeout", type=float, default=30)
    ld.add_argument("--burst-bytes", type=int, default=150 << 20)
    sub.add_parser("canary", help="the checkers must catch planted damage")
    a = ap.parse_args()
    if a.cmd == "load":
        return run_load(a)
    if a.cmd == "canary":
        return run_canary(a)
    if a.work is None:
        a.work = REPO / "target" / "trial-drill" / datetime.now().strftime("%Y%m%d-%H%M%S")
    if a.go is None:
        common = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                                capture_output=True, text=True).stdout.strip()
        a.go = (Path(common).parent if common else REPO) / ".upstream" / "bin" / "litestream-041a8c3"
    for b in (a.lsr, a.go):
        if not Path(b).exists():
            print(f"trial_drill: {b} does not exist (cargo build --release -p lsr-cli; scripts/build-go-litestream.sh 041a8c3)", file=sys.stderr)
            return 2
    return run_drill(a)


if __name__ == "__main__":
    sys.exit(main())
