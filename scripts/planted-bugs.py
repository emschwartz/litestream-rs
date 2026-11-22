#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""The planted-bug ledger runner (docs/planted-bugs.md).

For each entry of docs/planted-bugs.toml: the named tests must pass on a clean scratch copy of
HEAD, then must each fail once the entry's patch is applied. Exits 1 when a planted bug survives
(a defect in the tests), a patch no longer applies (re-express the entry's patch against the
current tree, or retire the entry with a reason), a test does not run, or a build breaks.

    uv run scripts/planted-bugs.py [--only NAME]... [--keep]
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "docs" / "planted-bugs.toml"
PATCHES = ROOT / "docs" / "planted-bugs"
SCRATCH = ROOT / "target" / "planted-bugs"
TREE = SCRATCH / "tree"
# Wall-clock bound for one nextest invocation (build excluded). Each test also has nextest's own
# 90 s limit from docs/planted-bugs/nextest.toml.
RUN_BOUND = 300
BUILD_BOUND = 1800
# A planted loop must not eat the box: virtual memory cap for test runs, in KiB.
MEM_CAP_KIB = 8 * 1024 * 1024


def git(*args: str, cwd: Path = ROOT, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, check=check, text=True, capture_output=True, **kw)


def prepare_tree() -> None:
    """A detached worktree of HEAD, reset to a clean state."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    git("worktree", "prune")
    # HEAD of this checkout, resolved here: inside the scratch tree `HEAD` names the commit the
    # tree was left at by an earlier run, so a reused tree would test a stale commit.
    head = git("rev-parse", "HEAD").stdout.strip()
    if not (TREE / ".git").exists():
        shutil.rmtree(TREE, ignore_errors=True)
        git("worktree", "add", "--detach", str(TREE), head)
    else:
        git("checkout", "--force", "--detach", head, cwd=TREE)
    git("clean", "-fdxq", "-e", "target", cwd=TREE)


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
        p = subprocess.run(args, cwd=TREE, env=env(), text=True, capture_output=True, timeout=BUILD_BOUND)
    except subprocess.TimeoutExpired:
        return "build timed out"
    return None if p.returncode == 0 else "\n".join((p.stdout + p.stderr).splitlines()[-25:])


def run_test(bug: dict, test: str) -> tuple[str, str]:
    """('pass' | 'fail' | 'timeout' | 'error', output) for one named test."""
    args = nextest_args(bug) + ["--no-fail-fast", "-E", f"test(~{test})"]
    cmd = f"ulimit -v {MEM_CAP_KIB}; exec \"$@\""
    try:
        p = subprocess.run(["bash", "-c", cmd, "bash", *args], cwd=TREE, env=env(), text=True,
                           capture_output=True, timeout=RUN_BOUND)
    except subprocess.TimeoutExpired:
        return "timeout", f"no verdict within {RUN_BOUND}s"
    out = p.stdout + p.stderr
    if p.returncode == 0:
        return "pass", out
    if p.returncode == 100:
        return ("timeout" if "TIMEOUT" in out else "fail"), out
    return "error", out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", action="append", default=[], help="run just this entry (repeatable)")
    ap.add_argument("--keep", action="store_true", help="leave the scratch worktree and target dir")
    opts = ap.parse_args()
    bugs = tomllib.loads(LEDGER.read_text())["bug"]
    names = [b["name"] for b in bugs]
    if len(set(names)) != len(names):
        print("planted-bugs: duplicate entry name", file=sys.stderr)
        return 1
    unknown = [n for n in opts.only if n not in names]
    if unknown:
        print(f"planted-bugs: no such entry: {unknown}", file=sys.stderr)
        return 1
    bugs = [b for b in bugs if not opts.only or b["name"] in opts.only]

    rows: list[tuple[str, str, str, str]] = []  # name, fix, verdict, detail
    prepare_tree()

    # Phase 1: the named tests pass on the clean tree, so a later failure comes from the patch.
    clean_bad: dict[str, str] = {}
    for bug in bugs:
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
        git("checkout", "--force", "--detach", "HEAD", cwd=TREE)
        git("clean", "-fdxq", "-e", "target", cwd=TREE)
        patch = PATCHES / f"{bug.get('patch', name)}.patch"
        applied = git("apply", "--whitespace=nowarn", str(patch), cwd=TREE, check=False)
        if applied.returncode != 0:
            rows.append((name, fix, "STALE", f"{patch.name} no longer applies; re-express it against the "
                         f"current tree or retire the entry:\n{applied.stderr.strip()}"))
            continue
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
        rows.append((name, fix, word, "; ".join(details) + f" [{time.monotonic() - started:.0f}s]"))

    width = max(len(r[0]) for r in rows)
    print(f"\n{'entry':<{width}}  {'fix':<8}  verdict")
    for name, fix, verdict, detail in rows:
        print(f"{name:<{width}}  {fix:<8}  {verdict}")
    for name, fix, verdict, detail in rows:
        if not verdict.startswith("killed"):
            print(f"\n--- {name} ({fix}): {verdict}\n{detail}")
        else:
            print(f"    {name}: {detail}")
    if not opts.keep:
        git("checkout", "--force", "--detach", "HEAD", cwd=TREE, check=False)
    bad = [r for r in rows if not r[2].startswith("killed")]
    if bad:
        print(f"\nplanted-bugs: {len(bad)} of {len(rows)} entries failed: "
              + ", ".join(f"{r[0]} ({r[2]})" for r in bad), file=sys.stderr)
        return 1
    print(f"\nplanted-bugs: all {len(rows)} planted bugs are caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
