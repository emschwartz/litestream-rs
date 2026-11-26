#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""The planted-bug ledger runner (docs/planted-bugs.md).

For each entry in docs/planted-bugs/entries/: the named tests must pass on a clean scratch copy of
HEAD, then must each fail once the entry's patch is applied. Exits 1 when a planted bug survives
(a defect in the tests), a patch no longer applies (re-express the entry's patch against the
current tree, or retire the entry with a reason), a test does not run, or a build breaks.

    uv run scripts/planted-bugs.py [--only NAME]... [--shard K/N] [--keep] [--no-wait]
    uv run scripts/planted-bugs.py --names
    uv run scripts/planted-bugs.py --changed BASE [--list] [--shard K/N]

`--names` checks, without building anything, that every entry's tests exist as `fn <name>` in its
package (a renamed test leaves its entry unable to run). `--changed BASE` keeps the entries a pull
request against BASE can have affected: the diff touches the entry file, its patch, a file the
patch edits or a file that defines a named test, or the patch no longer applies. `--list` prints
their names and exits.

`--shard K/N` runs every N-th entry starting at the K-th (by file name), plus the canaries, so
the nightly can split the ledger across N runners.

One run at a time: the scratch tree and its target dir are shared, so a run takes a lock on
`target/planted-bugs/lock` before it touches them, and waits for one already held (`--no-wait`
refuses instead). Every build and test process the run starts inherits the lock, so a run
killed mid-way still holds it until the last process it left behind exits: a new run never
applies its patches under an old run's cargo or tests, nor builds into a target dir they are
still reading. Each build and test command also runs in a session of its own, and every process
still in that session when the command ends is killed (where `/proc` exists): a planted bug can
leave a test's child process running, and an orphan must not outlive the run.

`--unaided` asks another question: would the simulator's swarm find each planted bug with no
test written for it? The named tests are not run. Each patch is applied and the swarm runs from
`--base` (disjoint from the fixed seed set), and the run judges (D296): an entry of the swarm's
reach (UNAIDED_PACKAGES) carries `unaided = <seeds>`, a budget the swarm must fail within, or
`unaided_miss = "<why it cannot>"`; an entry with neither is refused by `--names`. A budgeted
plant runs exactly its budget; found later or never is a failure. A plant marked a miss runs
`--seeds` seeds, and a failure found there is a failure too: the explanation is stale.

    uv run scripts/planted-bugs.py --unaided [--seeds N] [--base B] [--only NAME]... [--shard K/N]
"""

import argparse
import fcntl
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PATCHES = ROOT / "docs" / "planted-bugs"
# One file per entry, so adding one never edits a file another branch also appends to.
ENTRIES = PATCHES / "entries"
SCRATCH = ROOT / "target" / "planted-bugs"
TREE = SCRATCH / "tree"
LOCK = SCRATCH / "lock"
# Wall-clock bound for one nextest invocation (build excluded). Each test also has nextest's own
# 90 s limit from docs/planted-bugs/nextest.toml.
RUN_BOUND = 300
BUILD_BOUND = 1800
# The packages whose plants the swarm can be asked about: the swarm drives lsr-core's engine
# through lsr-sim. Every non-canary entry of these carries `unaided` or `unaided_miss`.
UNAIDED_PACKAGES = ("lsr-sim", "lsr-core")
# A planted loop must not eat the box: virtual memory cap for test runs, in KiB.
MEM_CAP_KIB = 8 * 1024 * 1024


# The lock's descriptor, once taken: passed to every process the run starts, which keeps the lock
# held while any of them lives (flock belongs to the open file, not to this process).
LOCK_FD: int | None = None


class Busy(Exception):
    """Another run holds the lock and this one was told not to wait."""


def take_lock(wait: bool) -> None:
    """Takes the run lock, waiting for a holder unless `wait` is false (then raises Busy)."""
    global LOCK_FD
    SCRATCH.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        holder = os.pread(fd, 64, 0).decode(errors="replace").strip() or "unknown"
        if not wait:
            os.close(fd)
            raise Busy(f"another run holds {LOCK} (pid {holder}, or processes it left)")
        print(f"planted-bugs: waiting for another run (pid {holder}, or processes it left) "
              f"to release {LOCK}", file=sys.stderr, flush=True)
        fcntl.flock(fd, fcntl.LOCK_EX)
    os.ftruncate(fd, 0)
    os.pwrite(fd, f"{os.getpid()}\n".encode(), 0)
    os.set_inheritable(fd, True)
    LOCK_FD = fd


def held() -> tuple[int, ...]:
    """The descriptors a started process must inherit: the lock's."""
    return () if LOCK_FD is None else (LOCK_FD,)


def reap_session(sid: int) -> None:
    """Kills every process left in session `sid`: a test's child a planted bug never stopped.
    Linux only (it reads /proc); elsewhere such a process outlives the command."""
    proc = Path("/proc")
    if not proc.is_dir():
        return
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            # Fields after the parenthesised command name: state, ppid, pgrp, session, ...
            stat = (entry / "stat").read_text()
            fields = stat[stat.rindex(")") + 2:].split()
            if int(fields[3]) == sid:
                os.kill(int(entry.name), signal.SIGKILL)
        except (OSError, ValueError, IndexError):
            continue


def run(args: list[str], timeout: float, **kw) -> subprocess.CompletedProcess:
    """`subprocess.run` for a build or test command, holding the lock, in a session of its own
    that is emptied when the command ends, whether it finished, timed out or failed. Output goes
    to files, not pipes: a process left behind holding a pipe would keep the read open until the
    timeout, though the command itself had finished."""
    with tempfile.TemporaryFile("w+") as out, tempfile.TemporaryFile("w+") as err:
        p = subprocess.Popen(args, text=True, stdout=out, stderr=err, start_new_session=True,
                             pass_fds=held(), **kw)
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            reap_session(p.pid)
            p.wait()
            raise
        finally:
            reap_session(p.pid)
        out.seek(0)
        err.seek(0)
        return subprocess.CompletedProcess(args, p.returncode, out.read(), err.read())


def git(*args: str, cwd: Path = ROOT, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, check=check, text=True, capture_output=True,
                          pass_fds=held(), **kw)


def checkout_head() -> str:
    """The commit this checkout's HEAD names, which every verdict must be about."""
    return git("rev-parse", "HEAD").stdout.strip()


class StaleTree(Exception):
    """The scratch tree is not at this checkout's HEAD: any verdict would be about other code."""


def assert_tree_at(head: str) -> None:
    """Raises StaleTree unless the scratch tree's commit is `head`. Called before every verdict,
    so a regression in how the tree is reset cannot pass as a result."""
    at = git("rev-parse", "HEAD", cwd=TREE).stdout.strip()
    if at != head:
        raise StaleTree(f"the scratch tree {TREE} is at {at[:12]}, this checkout at {head[:12]}")


def reset_tree(head: str) -> None:
    """The scratch tree at `head`, with no change and no untracked file but the target dir.
    `head` is resolved in this checkout: inside the scratch tree `HEAD` names the commit an
    earlier run left it at."""
    git("checkout", "--force", "--detach", head, cwd=TREE)
    git("clean", "-fdxq", "-e", "target", cwd=TREE)
    assert_tree_at(head)


def prepare_tree(head: str) -> None:
    """A detached worktree of `head`, reset to a clean state."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    git("worktree", "prune")
    if not (TREE / ".git").exists():
        shutil.rmtree(TREE, ignore_errors=True)
        git("worktree", "add", "--detach", str(TREE), head)
    reset_tree(head)


def env() -> dict[str, str]:
    e = dict(os.environ)
    e["CARGO_TARGET_DIR"] = str(SCRATCH / "target")
    # The patched tree has dead code and unused imports by design; lints are not the question.
    e["RUSTFLAGS"] = (e.get("RUSTFLAGS", "") + " --cap-lints warn").strip()
    e["CARGO_TERM_COLOR"] = "never"
    # CI selects the `ci` profile; the ledger's own config file has only `default`.
    e.pop("NEXTEST_PROFILE", None)
    return e


def nextest_args(bug: dict) -> list[str]:
    args = ["cargo", "nextest", "run", "--locked", "--no-tests=fail", "-p", bug["package"],
            "--config-file", str(PATCHES / "nextest.toml")]
    if "features" in bug:
        args += ["--features", ",".join(bug["features"])]
    if "bin" in bug:
        args += ["--bin", bug["bin"]]
    elif "test_bin" in bug:
        args += ["--test", bug["test_bin"]]
    else:
        args += ["--lib", "--tests"]
    return args


def build(bug: dict) -> str | None:
    """None when the tests build, else the tail of the compiler output."""
    args = nextest_args(bug) + ["--no-run"]
    try:
        p = run(args, BUILD_BOUND, cwd=TREE, env=env())
    except subprocess.TimeoutExpired:
        return "build timed out"
    return None if p.returncode == 0 else "\n".join((p.stdout + p.stderr).splitlines()[-25:])


def run_test(bug: dict, test: str) -> tuple[str, str]:
    """('pass' | 'fail' | 'timeout' | 'error', output) for one named test."""
    args = nextest_args(bug) + ["--no-fail-fast", "-E", f"test(~{test})"]
    cmd = f"ulimit -v {MEM_CAP_KIB}; exec \"$@\""
    try:
        p = run(["bash", "-c", cmd, "bash", *args], RUN_BOUND, cwd=TREE, env=env())
    except subprocess.TimeoutExpired:
        return "timeout", f"no verdict within {RUN_BOUND}s"
    out = p.stdout + p.stderr
    if p.returncode == 0:
        return "pass", out
    if p.returncode == 100:
        return ("timeout" if "TIMEOUT" in out else "fail"), out
    return "error", out


# A canary entry's verdict is known in advance: `no-op` must survive (its patch changes nothing a
# test sees), `fatal` must be killed (its patch breaks a test for certain). Either one reporting
# otherwise means the runner, not the code, is wrong: a patch not applied, a filter that runs
# no test, an exit code misread.
CANARIES = {"survived": "SURVIVED", "killed": "killed"}


def canary_verdict(bug: dict, word: str) -> str:
    """The row's verdict: `word` for a real entry, `canary ok` or `CANARY` for a canary."""
    if "canary" not in bug:
        return word
    expected = CANARIES[bug["canary"]]
    if word == expected:
        return f"canary ok ({word})"
    return f"CANARY: expected {expected}, got {word}"


def passed(verdict: str) -> bool:
    return verdict.startswith("killed") or verdict.startswith("canary ok")


def patch_files(patch: Path) -> set[str]:
    """The repository files a patch edits."""
    found = set()
    for line in patch.read_text().splitlines():
        if line.startswith(("--- a/", "+++ b/")):
            found.add(line[6:].split("\t")[0].strip())
    return found


_FN_FILES: dict[str, set[str]] = {}


def fn_files(name: str) -> set[str]:
    """The files under crates/ that define `fn <name>`. One scan of the tree answers every
    entry: a scan per test made the check take half a minute."""
    if not _FN_FILES:
        out = git("grep", "-o", "-E", r"fn [A-Za-z0-9_]+", "--", "crates").stdout
        for line in out.splitlines():
            path, _, text = line.partition(":")
            _FN_FILES.setdefault(text.removeprefix("fn "), set()).add(path)
    return _FN_FILES.get(name, set())


def test_files(bug: dict) -> tuple[set[str], list[str]]:
    """The files that define the entry's named tests, and the tests with no definition (a name
    is the last path segment). The whole workspace is searched, not the entry's package: the
    shared conformance cases are defined in lsr-core and run by the store crates' test binaries."""
    files, missing = set(), []
    for test in bug.get("tests", []):
        hit = fn_files(test.split("::")[-1])
        files.update(hit)
        if not hit:
            missing.append(test)
    return files, missing


def unaided_problems(bugs: list[dict]) -> list[str]:
    """What is wrong with the entries' unaided fields: one line each. An entry of
    UNAIDED_PACKAGES has exactly one of `unaided` (a positive integer) and `unaided_miss` (a
    sentence); entries that plant the same patch agree, since one swarm run answers them all."""
    found, by_patch = [], {}
    for b in bugs:
        if "canary" in b:
            continue
        budget, miss = b.get("unaided"), b.get("unaided_miss")
        name = b["name"]
        if budget is None and miss is None:
            if b["package"] in UNAIDED_PACKAGES:
                found.append(f"{name}: needs `unaided = <seeds>` or `unaided_miss = \"<why>\"`")
            continue
        if budget is not None and miss is not None:
            found.append(f"{name}: has both `unaided` and `unaided_miss`")
        elif miss is not None and not (isinstance(miss, str) and miss.strip()):
            found.append(f"{name}: `unaided_miss` must say why the swarm cannot reach it")
        elif budget is not None and not (isinstance(budget, int) and not isinstance(budget, bool)
                                         and budget > 0):
            found.append(f"{name}: `unaided` must be a positive number of seeds")
        by_patch.setdefault(b.get("patch", name), []).append(b)
    for patch, group in by_patch.items():
        if len({(g.get("unaided"), g.get("unaided_miss")) for g in group}) > 1:
            found.append(f"{patch}: entries {[g['name'] for g in group]} plant one patch and must "
                         "carry the same unaided field")
    return found


def unaided_verdict(bug: dict, got: str, clean: str) -> tuple[str, str]:
    """The judged row for one plant: `got` is `swarm()`'s answer for it and `clean` the clean
    tree's. Verdicts that pass start with `ok`; every other one fails the run."""
    first = got.splitlines()[0] if got else ""
    if first == "STALE" or first.startswith(("BUILD", "ERROR", "timeout")):
        return first.split(" ")[0].upper(), "no answer: " + first
    found = first[:1].isdigit()
    if found and first.partition("\t")[0].partition(" ")[2] == clean.partition("\t")[0].partition(" ")[2] \
            and clean[:1].isdigit():
        return "MASKED", "the clean tree's own failure is the first failure"
    seeds_run = first.partition("\t")[0].partition(" ")[2] if not found else ""
    if "unaided_miss" in bug:
        if found:
            return "STALE MISS", (f"the swarm found it at seed {first.split(' ')[0]}, so \"{bug['unaided_miss']}\" "
                                  "is no longer true: set `unaided` from this run and drop the miss")
        return "ok miss", f"not found in {seeds_run} seeds, as marked"
    budget = bug["unaided"]
    if not found:
        return "NOT FOUND", f"no failure in the budget of {budget} seeds"
    k = int(first.split(" ")[0])
    if k > budget:
        return "OVER BUDGET", f"first failure at seed {k}, budget {budget}"
    return "ok", f"first failure at seed {k} of budget {budget}"


def missing_tests(bugs: list[dict]) -> list[str]:
    """`entry: test` for each named test that no `fn` defines."""
    return [f"{b['name']}: {t}" for b in bugs for t in test_files(b)[1]]


def affected(bugs: list[dict], base: str) -> list[dict]:
    """The non-canary entries a change since `base` can have stopped killing."""
    changed = set(git("diff", "--name-only", f"{base}...HEAD").stdout.split())
    picked = []
    for bug in bugs:
        if "canary" in bug:
            continue
        patch = PATCHES / f"{bug.get('patch', bug['name'])}.patch"
        rel = patch.relative_to(ROOT).as_posix()
        entry = (ENTRIES / f"{bug['file']}.toml").relative_to(ROOT).as_posix()
        stale = git("apply", "--check", str(patch), check=False).returncode != 0
        touched = {rel, entry} | patch_files(patch) | test_files(bug)[0]
        if stale or touched & changed:
            picked.append(bug)
    return picked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", action="append", default=[], help="run just this entry (repeatable)")
    ap.add_argument("--shard", default="0/1", metavar="K/N",
                    help="run only the K-th of N interleaved slices of the entries (canaries always run)")
    ap.add_argument("--names", action="store_true",
                    help="only check that every entry's named tests are defined, then exit")
    ap.add_argument("--changed", metavar="BASE",
                    help="run only the entries a diff against BASE can have affected")
    ap.add_argument("--list", action="store_true", help="--changed: print the names and exit")
    ap.add_argument("--keep", action="store_true", help="leave the scratch worktree and target dir")
    ap.add_argument("--unaided", action="store_true",
                    help="seeds the swarm needs to find each plant, with no named test run")
    ap.add_argument("--seeds", type=int, default=100,
                    help="--unaided: seeds run for a plant marked a miss (a budgeted plant runs its budget)")
    ap.add_argument("--base", type=int, default=1_000_001, help="--unaided: first swarm seed")
    ap.add_argument("--no-wait", action="store_true",
                    help="refuse to start while another run holds the lock, instead of waiting")
    opts = ap.parse_args()
    bugs = load_ledger()
    if not bugs:
        print(f"planted-bugs: no entries under {ENTRIES}", file=sys.stderr)
        return 1
    misnamed = [b["name"] for b in bugs if b["name"] != b["file"]]
    if misnamed:
        print(f"planted-bugs: an entry's file must be named <name>.toml: {misnamed}", file=sys.stderr)
        return 1
    names = [b["name"] for b in bugs]
    if opts.names:
        gone = missing_tests(bugs)
        for line in gone:
            print(f"planted-bugs: no test function for {line}", file=sys.stderr)
        unjudged = unaided_problems(bugs)
        for line in unjudged:
            print(f"planted-bugs: unaided field: {line}", file=sys.stderr)
        if not gone and not unjudged:
            print(f"planted-bugs: every named test of {len(bugs)} entries is defined, "
                  "every swarm-reachable plant has its unaided budget or miss")
        return 1 if gone or unjudged else 0
    unknown = [n for n in opts.only if n not in names]
    if unknown:
        print(f"planted-bugs: no such entry: {unknown}", file=sys.stderr)
        return 1
    kinds = sorted(b["canary"] for b in bugs if "canary" in b)
    if kinds != sorted(CANARIES):
        print(f"planted-bugs: the ledger needs exactly one canary of each kind {sorted(CANARIES)}, "
              f"found {kinds}", file=sys.stderr)
        return 1
    # The canaries always run: they are what says the verdicts below mean anything.
    bugs = [b for b in bugs if not opts.only or b["name"] in opts.only or "canary" in b]
    if opts.changed:
        picked = {b["name"] for b in affected(bugs, opts.changed)}
        bugs = [b for b in bugs if b["name"] in picked or "canary" in b]
        if opts.list:
            print("\n".join(sorted(picked)))
            return 0
        if not picked:
            print("planted-bugs: the diff affects no entry")
            return 0
    try:
        k, n = (int(part) for part in opts.shard.split("/"))
    except ValueError:
        k, n = -1, 0
    if not 0 <= k < n:
        print(f"planted-bugs: --shard wants K/N with 0 <= K < N, got {opts.shard}", file=sys.stderr)
        return 1
    real = [b for b in bugs if "canary" not in b]
    mine = {b["name"] for b in real[k::n]}
    bugs = [b for b in bugs if "canary" in b or b["name"] in mine]
    try:
        take_lock(wait=not opts.no_wait)
    except Busy as busy:
        print(f"planted-bugs: {busy}", file=sys.stderr)
        return 2

    if opts.unaided:
        return unaided([b for b in bugs if "unaided" in b or "unaided_miss" in b], opts.seeds, opts.base)

    rows: list[tuple[str, str, str, str]] = []  # name, fix, verdict, detail
    head = checkout_head()
    try:
        prepare_tree(head)
        run_ledger(bugs, head, rows)
    except StaleTree as stale:
        print(f"planted-bugs: {stale}; no verdict is about this checkout", file=sys.stderr)
        return 1
    return report(rows, opts.keep)


def load_ledger() -> list[dict]:
    """Every entry, sorted by file name; each carries its file's stem as `file`."""
    return [{**tomllib.loads(path.read_text()), "file": path.stem}
            for path in sorted(ENTRIES.glob("*.toml"))]


def run_ledger(bugs: list[dict], head: str, rows: list[tuple[str, str, str, str]]) -> None:

    # Phase 1: the named tests pass on the clean tree, so a later failure comes from the patch.
    clean_bad: dict[str, str] = {}
    for bug in bugs:
        assert_tree_at(head)
        err = build(bug)
        if err:
            clean_bad[bug["name"]] = "clean tree does not build:\n" + err
            continue
        for t in bug["tests"]:
            verdict, out = run_test(bug, t)
            if verdict != "pass":
                clean_bad[bug["name"]] = f"{t}: {verdict} on the clean tree:\n" + "\n".join(out.splitlines()[-20:])
                break

    # Phase 2: plant each bug alone and require every named test to fail.
    for bug in bugs:
        name, fix = bug["name"], bug["fix"]
        started = time.monotonic()
        if name in clean_bad:
            rows.append((name, fix, "BROKEN", clean_bad[name]))
            continue
        reset_tree(head)
        patch = PATCHES / f"{bug.get('patch', name)}.patch"
        applied = git("apply", "--whitespace=nowarn", str(patch), cwd=TREE, check=False)
        if applied.returncode != 0:
            rows.append((name, fix, "STALE", f"{patch.name} no longer applies; re-express it against the "
                         f"current tree or retire the entry:\n{applied.stderr.strip()}"))
            continue
        assert_tree_at(head)
        err = build(bug)
        if err:
            rows.append((name, fix, "BUILD", "the planted tree does not build (a plant that breaks the "
                         "build proves nothing):\n" + err))
            continue
        details, verdicts = [], []
        for t in bug["tests"]:
            verdict, out = run_test(bug, t)
            verdicts.append(verdict)
            details.append(f"{t.replace('::tests::', '/')}: {verdict}")
            if verdict == "pass":
                details.append("   SURVIVED: the test stays green with the bug planted")
            elif verdict == "error":
                details.append("\n".join(out.splitlines()[-15:]))
        if all(v in ("fail", "timeout") for v in verdicts):
            word = "killed" if "timeout" not in verdicts else "killed (timeout)"
        elif "pass" in verdicts:
            word = "SURVIVED"
        else:
            word = "ERROR"
        rows.append((name, fix, canary_verdict(bug, word),
                     "; ".join(details) + f" [{time.monotonic() - started:.0f}s]"))


SWARM = {"package": "lsr-sim", "test_bin": "sim"}


# Seeds per nextest invocation: each run stays well inside the ledger's 90 s per-test limit, with
# room for a loaded runner (a seed takes 0.4 s on a quiet one and up to 1.5 s on a busy box; at 100
# seeds a busy box hit the limit, which showed as ERROR with no first-failure line).
SWARM_CHUNK = 25


def swarm(seeds: int, base: int) -> str:
    """The swarm's first-failure line on the tree as it is, or what went wrong instead."""
    err = build(SWARM)
    if err:
        return "BUILD\n" + err
    args = nextest_args(SWARM) + ["--run-ignored", "only", "--no-capture", "-E", "test(=tally::first_failure)"]
    cmd = f"ulimit -v {MEM_CAP_KIB}; exec \"$@\""
    for start in range(0, seeds, SWARM_CHUNK):
        count = min(SWARM_CHUNK, seeds - start)
        e = env()
        e["LSR_SIM_TALLY_SEEDS"], e["LSR_SIM_BASE_SEED"] = str(count), str(base + start)
        try:
            p = run(["bash", "-c", cmd, "bash", *args], RUN_BOUND, cwd=TREE, env=e)
        except subprocess.TimeoutExpired:
            return f"timeout in seeds {base + start}..{base + start + count - 1}"
        lines = (p.stdout + p.stderr).splitlines()
        found = [i for i, line in enumerate(lines) if line.startswith("first-failure: ")]
        if not found:
            return "ERROR\n" + "\n".join(lines[-15:])
        first = lines[found[0]].removeprefix("first-failure: ")
        if not first.startswith("none"):
            k, _, seed = first.partition(" ")
            why = lines[found[0] + 1].strip() if found[0] + 1 < len(lines) else ""
            return f"{int(k) + start} {seed}\t{why[:160]}"
    return f"none {seeds}\t"


def unaided(bugs: list[dict], seeds: int, base: int) -> int:
    """Judges the swarm against each plant's unaided field; see the module docs."""
    head = checkout_head()
    # One swarm run answers every entry that plants the same patch.
    answers: dict[str, str] = {}
    rows = []
    try:
        prepare_tree(head)
        # The clean tree runs as many seeds as the longest plant run: a clean failure inside any
        # of them is the clean tree's own and says nothing about a plant.
        runs = [b["unaided"] if "unaided" in b else seeds for b in bugs]
        clean = swarm(max(runs, default=seeds), base)
        for bug, count in zip(bugs, runs):
            patch_name = bug.get("patch", bug["name"])
            patch = PATCHES / f"{patch_name}.patch"
            started = time.monotonic()
            key = f"{patch_name}/{count}"
            if key not in answers:
                reset_tree(head)
                applied = git("apply", "--whitespace=nowarn", str(patch), cwd=TREE, check=False)
                assert_tree_at(head)
                answers[key] = swarm(count, base) if applied.returncode == 0 else "STALE"
            verdict, detail = unaided_verdict(bug, answers[key], clean)
            rows.append((bug["name"], verdict, detail, answers[key]))
            print(f"{bug['name']}: {verdict}, {detail} [{time.monotonic() - started:.0f}s]", flush=True)
    except StaleTree as stale:
        print(f"planted-bugs: {stale}; no verdict is about this checkout", file=sys.stderr)
        return 1
    finally:
        git("checkout", "--force", "--detach", "HEAD", cwd=TREE, check=False)
    print(f"\nswarm from seed {base} at {head[:12]}; clean tree: {clean.splitlines()[0]}")
    print("| entry | verdict | detail | first seed |\n|---|---|---|---|")
    for name, verdict, detail, got in rows:
        seed = got.split("\t")[0].split(" ")[1] if got[:1].isdigit() else ""
        print(f"| {name} | {verdict} | {detail} | {seed} |")
    bad = [r for r in rows if not r[1].startswith("ok")]
    print(f"\nunaided: {len(rows) - len(bad)} of {len(rows)} plants judged as recorded")
    if bad or not clean.startswith("none"):
        print(f"planted-bugs: clean tree {clean.splitlines()[0]!r}; failed: "
              + ", ".join(f"{n} ({v})" for n, v, _, _ in bad), file=sys.stderr)
        return 1
    return 0


def report(rows: list[tuple[str, str, str, str]], keep: bool) -> int:
    width = max(len(r[0]) for r in rows)
    print(f"\n{'entry':<{width}}  {'fix':<8}  verdict")
    for name, fix, verdict, detail in rows:
        print(f"{name:<{width}}  {fix:<8}  {verdict}")
    for name, fix, verdict, detail in rows:
        if not passed(verdict):
            print(f"\n--- {name} ({fix}): {verdict}\n{detail}")
        else:
            print(f"    {name}: {detail}")
    if not keep:
        git("checkout", "--force", "--detach", "HEAD", cwd=TREE, check=False)
    bad = [r for r in rows if not passed(r[2])]
    if bad:
        print(f"\nplanted-bugs: {len(bad)} of {len(rows)} entries failed: "
              + ", ".join(f"{r[0]} ({r[2]})" for r in bad), file=sys.stderr)
        return 1
    print(f"\nplanted-bugs: all {len(rows)} planted bugs are caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
