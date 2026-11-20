#!/usr/bin/env python3
"""Lists non-test functions with exactly one call site (decision D5's checkpoint).

Usage: scripts/single-caller.py [--markdown]

Reads crates/*/src, skipping test code: files named tests.rs, anything under a
tests directory, and `#[cfg(test)] mod` blocks. It matches names, not types, so
a function is counted only when its name is defined once in the workspace; the
rest are reported as a count ("ambiguous") and never guessed at. A call site is
`name(`, `.name(`, `::name(`, `name::<`, or the bare name passed as a value.
Macros, trait dispatch and re-exports under another name are not followed.

For each single-caller function it prints its length, its caller's length and
their sum, in lines that hold code (signature included, blank and comment-only
lines not, which is close to what clippy's too_many_lines counts). A sum above
the limit means folding the callee back in would break the 70-line lint; a sum
at or below it means the split was a choice.
"""

import pathlib
import re
import sys
from collections import defaultdict

LIMIT = 70
ROOT = pathlib.Path(__file__).resolve().parent.parent
FN = re.compile(r"^\s*(pub(?:\([a-z: ]+\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([a-z_][a-z0-9_]*)")
SKIP_VALUE = {"self", "super", "crate"}


def is_test_path(path):
    parts = path.relative_to(ROOT).parts
    return path.name == "tests.rs" or "tests" in parts[:-1] or "benches" in parts


def strip(line):
    """The line without string literals and line comments."""
    line = re.sub(r'"(?:\\.|[^"\\])*"', '""', line)
    return line.split("//", 1)[0]


def code_lines(lines, first, last):
    """Lines from first to last that hold code: not blank, not only a comment."""
    held = lines[first - 1 : last]
    return sum(1 for line in held if line.strip() and not line.strip().startswith("//"))


def functions(path):
    """Yields (name, public, first line, last line, code lines) outside test modules."""
    lines = path.read_text().splitlines()
    depth, skip_until, pending_cfg_test = 0, None, False
    open_fns = []
    for number, raw in enumerate(lines, 1):
        line = strip(raw)
        if skip_until is None and re.match(r"\s*#\[cfg\(test\)\]", line):
            pending_cfg_test = True
        elif skip_until is None and pending_cfg_test and re.match(r"\s*(pub\s+)?mod\s+\w+\s*\{", line):
            skip_until, pending_cfg_test = depth, False
        elif re.match(r"\s*(pub(\([a-z: ]+\))?\s+)?(async\s+|const\s+)*(fn|struct|enum|impl|use|static|type|trait)\b", line):
            # The attribute was on another item; a multi-line attribute between
            # `#[cfg(test)]` and its `mod` does not end the wait.
            pending_cfg_test = False
        match = FN.match(line) if skip_until is None else None
        if match:
            open_fns.append([match.group(2), bool(match.group(1)), number, depth, False])
        for char in line:
            if char == "{":
                depth += 1
                for fn in open_fns:
                    if fn[3] == depth - 1:
                        fn[4] = True
            elif char == "}":
                depth -= 1
                if skip_until is not None and depth == skip_until:
                    skip_until = None
                for fn in list(open_fns):
                    if fn[4] and fn[3] == depth:
                        open_fns.remove(fn)
                        yield fn[0], fn[1], fn[2], number, code_lines(lines, fn[2], number)
        # A declaration without a body (a trait method) ends at its semicolon.
        for fn in list(open_fns):
            if not fn[4] and fn[3] == depth and line.rstrip().endswith(";"):
                open_fns.remove(fn)


def main():
    files = sorted(p for p in ROOT.glob("crates/*/src/**/*.rs") if not is_test_path(p))
    defs = defaultdict(list)
    spans = {}
    for path in files:
        spans[path] = []
        for name, public, first, last, length in functions(path):
            defs[name].append((path, public, first, last, length))
            spans[path].append((first, last, name, length))
    unique = {name: found[0] for name, found in defs.items() if len(found) == 1}
    calls = defaultdict(list)
    word = re.compile(r"(?<![A-Za-z0-9_])([a-z_][a-z0-9_]*)(?![A-Za-z0-9_!])")
    for path in files:
        for number, raw in enumerate(path.read_text().splitlines(), 1):
            line = strip(raw)
            # A re-export names a function without calling it.
            if re.match(r"\s*(pub(\([a-z: ]+\))?\s+)?use\s", line):
                continue
            if FN.match(line):
                line = line[line.index("fn ") + 3 :]
                line = line[re.match(r"[a-z_0-9]+", line).end() :]
            for match in word.finditer(line):
                name = match.group(1)
                if name in unique and name not in SKIP_VALUE:
                    enclosing = [s for s in spans[path] if s[0] <= number <= s[1]]
                    caller = max(enclosing, key=lambda s: s[0]) if enclosing else None
                    calls[name].append((path, number, caller))
    rows = []
    for name, (path, public, first, last, length) in sorted(unique.items()):
        sites = [c for c in calls[name] if not (c[0] == path and first <= c[1] <= last and c[2] and c[2][2] == name)]
        if len(sites) != 1:
            continue
        site_path, site_line, caller = sites[0]
        caller_len = caller[3] if caller else 0
        rows.append((path, name, public, length, caller[2] if caller else "(not in a function)", caller_len, site_path))
    ambiguous = sum(1 for found in defs.values() if len(found) > 1)
    total = sum(len(found) for found in defs.values())
    private = [r for r in rows if not r[2]]
    forced = [r for r in private if r[3] + r[5] > LIMIT]
    lengths = [found[4] for founds in defs.values() for found in founds]
    near = sum(1 for length in lengths if LIMIT - 10 < length <= LIMIT)
    over = sum(1 for length in lengths if length > LIMIT)
    markdown = "--markdown" in sys.argv
    print(f"functions outside tests: {total}; names defined more than once (not judged): {ambiguous} names")
    print(f"single-caller functions: {len(rows)} ({len(private)} private, {len(rows) - len(private)} public)")
    print(f"private single-caller, callee + caller above {LIMIT} lines: {len(forced)}; at or below: {len(private) - len(forced)}")
    print(f"functions of {LIMIT - 9} to {LIMIT} lines: {near}; above {LIMIT}: {over}")
    print()
    if markdown:
        print("| File | Function | Lines | Only caller | Caller lines | Sum | Above the limit if folded |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
    for path, name, public, length, caller, caller_len, site_path in rows:
        rel = path.relative_to(ROOT / "crates")
        total_len = length + caller_len
        where = "" if site_path == path else f" ({site_path.relative_to(ROOT / 'crates')})"
        vis = "pub " if public else ""
        if markdown:
            print(f"| `{rel}` | `{vis}{name}` | {length} | `{caller}`{where} | {caller_len} | {total_len} | {'yes' if total_len > LIMIT else 'no'} |")
        else:
            print(f"{rel}\t{vis}{name}\t{length}\t{caller}{where}\t{caller_len}\t{total_len}")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
