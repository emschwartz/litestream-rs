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
    if "test_bin" in bug:
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
    kinds = sorted(b["canary"] for b in bugs if "canary" in b)
    if kinds != sorted(CANARIES):
        print(f"planted-bugs: the ledger needs exactly one canary of each kind {sorted(CANARIES)}, "
              f"found {kinds}", file=sys.stderr)
        return 1
    # The canaries always run: they are what says the verdicts below mean anything.
    bugs = [b for b in bugs if not opts.only or b["name"] in opts.only or "canary" in b]

    rows: list[tuple[str, str, str, str]] = []  # name, fix, verdict, detail
    head = checkout_head()
    try:
        prepare_tree(head)
        run_ledger(bugs, head, rows)
    except StaleTree as stale:
        print(f"planted-bugs: {stale}; no verdict is about this checkout", file=sys.stderr)
        return 1
    return report(rows, opts.keep)


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
        patch = PATCHES / f"{name}.patch"
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
