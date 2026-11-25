#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Counts the production lines of each shipped crate, and their change against a base ref.

Production means Rust under crates/<crate>/src that a release build compiles: an item, module or
file under a cfg that is false when neither cfg(test) nor the "testing" feature is on is test
code and is left out (#[cfg(test)] modules, tests.rs, the fakes and harnesses behind "testing").
Crates that ship nothing (sim, compat, fuzzing, spec, history) are not counted. Each line is
code, comment (any line holding only a comment, doc comments included) or blank; the size that
matters is code. AGENTS.md says why: production code should not grow without need, tests may.

    uv run scripts/prod-lines.py [--base REF] [--markdown]

--base REF also counts the crates where this branch forked from REF and prints the change. --markdown prints a
table for a CI job summary.
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

CRATES = ["core", "ltx", "wal", "store-s3", "store-fs", "config", "replica", "sqlite", "sidecar", "cli"]

# cfg predicates are judged three-valued: False when definitely off with test and "testing" off,
# None when another predicate decides (a target, another feature), never True for test code.
def cfg_value(expr):
    expr = expr.strip()
    m = re.fullmatch(r"(any|all|not)\s*\((.*)\)", expr, re.S)
    if m:
        args = [cfg_value(a) for a in split_args(m.group(2))]
        if m.group(1) == "not":
            return None if args[0] is not True else False
        if m.group(1) == "any":
            if any(a is True for a in args):
                return True
            return False if all(a is False for a in args) else None
        if any(a is False for a in args):
            return False
        return True if all(a is True for a in args) else None
    if expr == "test" or re.fullmatch(r'feature\s*=\s*"testing"', expr):
        return False
    return None


def split_args(s):
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
            continue
        depth += ch == "("
        depth -= ch == ")"
        cur += ch
    if cur.strip():
        out.append(cur)
    return out


def test_only_attr(line):
    m = re.match(r"\s*#\[cfg\((.*)\)\]\s*$", line)
    return bool(m) and cfg_value(m.group(1)) is False


def lex(text):
    """Per line: whether it holds code, and the code characters (strings and comments blanked)."""
    lines, code, has_code, has_comment = [], [], False, False
    i, n, state, nest, hashes = 0, len(text), "code", 0, 0

    def newline():
        nonlocal code, has_code, has_comment
        lines.append(("".join(code), has_code, has_comment))
        code, has_code, has_comment = [], False, False

    while i < n:
        c = text[i]
        if c == "\n":
            if state == "line":
                state = "code"
            newline()
            i += 1
            continue
        if state == "line":
            i += 1
        elif state == "block":
            has_comment = True
            if text.startswith("/*", i):
                nest, i = nest + 1, i + 2
            elif text.startswith("*/", i):
                nest, i = nest - 1, i + 2
                state = "code" if nest == 0 else "block"
            else:
                i += 1
        elif state == "str":
            has_code = True
            if c == "\\":
                i += 1 if text.startswith("\n", i + 1) else 2
            else:
                state = "code" if c == '"' else "str"
                i += 1
        elif state == "raw":
            has_code = True
            if text.startswith('"' + "#" * hashes, i):
                i += 1 + hashes
                state = "code"
            else:
                i += 1
        elif text.startswith("//", i):
            state, has_comment = "line", True
            i += 2
        elif text.startswith("/*", i):
            state, nest, has_comment = "block", 1, True
            i += 2
        elif c == '"':
            state, has_code = "str", True
            i += 1
        elif (m := re.match(r'b?r(#*)"', text[i:i + 20])) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            state, hashes, has_code = "raw", len(m.group(1)), True
            i += m.end()
        elif c == "'" and (m := re.match(r"'(\\u\{[0-9a-fA-F]+\}|\\.|[^\\'\n])'", text[i:i + 12])):
            has_code = True
            i += m.end()
        else:
            if not c.isspace():
                has_code = True
                code.append(c)
            i += 1
    newline()
    if len(lines) != text.count("\n") + 1:
        raise AssertionError("prod-lines: the lexer lost a line")
    return lines


def item_end(lines, start):
    """The last line of the item starting at line start: brackets balanced at a ; or a closing },
    or at a , for a field, or before the } that closes the enclosing struct or literal."""
    depth = 0
    for j in range(start, len(lines)):
        for ch in lines[j][0]:
            if ch in "{[(":
                depth += 1
            elif ch in "}])":
                depth -= 1
                if depth < 0:
                    return max(start, j - 1)
                if depth == 0 and ch == "}":
                    return j
            elif ch in ";," and depth == 0:
                return j
    return len(lines) - 1


def attr_end(lines, start):
    """The last line of an attribute starting at line start, which may span lines."""
    depth = 0
    for j in range(start, len(lines)):
        for ch in lines[j][0]:
            depth += ch == "["
            depth -= ch == "]"
            if depth == 0 and ch == "]":
                return j
    return len(lines) - 1


def count_file(path, excluded_dirs):
    """(code, comment, blank) lines of one file, and the module files a test-only mod excludes."""
    raw = path.read_text().split("\n")
    lines = lex(path.read_text())
    counts = [0, 0, 0]
    skip_to, j = -1, 0
    if any(re.match(r"\s*#!\[cfg\((.*)\)\]", l) and cfg_value(re.match(r"\s*#!\[cfg\((.*)\)\]", l).group(1)) is False for l in raw):
        return counts
    if path.name in ("lib.rs", "main.rs", "mod.rs"):
        child_dir = path.parent
    else:
        child_dir = path.with_suffix("")
    while j < len(lines):
        if test_only_attr(raw[j]):
            k = j + 1
            while k < len(raw) and re.match(r"\s*#\[", raw[k]):
                k = attr_end(lines, k) + 1
            if k < len(raw) and (m := re.match(r"\s*(pub(\([^)]*\))?\s+)?mod\s+(\w+)\s*;", raw[k])):
                excluded_dirs.add(child_dir / m.group(3))
            j = item_end(lines, k) + 1
            continue
        _, has_code, has_comment = lines[j]
        counts[0 if has_code else 1 if has_comment else 2] += 1
        j += 1
    return counts


def count_tree(root, base=False):
    """Lines per crate; a crate the base ref does not have yet counts as empty."""
    totals = {}
    for crate in CRATES:
        src = root / "crates" / crate / "src"
        files = sorted(src.rglob("*.rs"))
        if not files and not base:
            sys.exit(f"prod-lines: no Rust files under {src}")
        excluded = set()
        counted = {f: count_file(f, excluded) for f in files}
        kept = [n for f, n in counted.items() if not any(f == d.with_suffix(".rs") or d in f.parents for d in excluded)]
        totals[crate] = [sum(n[i] for n in kept) for i in range(3)]
    return totals


def checkout(ref, into):
    # The fork point, not the ref's tip: a base that moved on would count its own changes.
    fork = subprocess.run(["git", "merge-base", ref, "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    archive = subprocess.run(["git", "archive", fork, "crates"], check=True, capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", into], input=archive, check=True)
    return Path(into)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--base", help="git ref to compare against")
    p.add_argument("--markdown", action="store_true", help="a table for a CI job summary")
    args = p.parse_args()
    top = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True).stdout.strip())
    head = count_tree(top)
    base = None
    if args.base:
        with tempfile.TemporaryDirectory() as tmp:
            base = count_tree(checkout(args.base, tmp), base=True)
    rows = [(c, head[c], base[c] if base else None) for c in CRATES]
    rows.append(("total", [sum(h[i] for _, h, _ in rows) for i in range(3)],
                 [sum(b[i] for _, _, b in rows) for i in range(3)] if base else None))
    if args.markdown:
        title = f"Production lines (code) against {args.base}" if base else "Production lines"
        print(f"### {title}\n")
        print("| crate | code | change | comment | blank |" if base else "| crate | code | comment | blank |")
        print("|---|---:|---:|---:|---:|" if base else "|---|---:|---:|---:|")
        for name, h, b in rows:
            mid = f" {h[0] - b[0]:+d} |" if base else ""
            print(f"| {name} | {h[0]} |{mid} {h[1]} | {h[2]} |")
        return
    print(f"{'crate':<10}{'code':>8}{'change' if base else '':>8}{'comment':>9}{'blank':>7}")
    for name, h, b in rows:
        change = f"{h[0] - b[0]:+d}" if base else ""
        print(f"{name:<10}{h[0]:>8}{change:>8}{h[1]:>9}{h[2]:>7}")


if __name__ == "__main__":
    main()
