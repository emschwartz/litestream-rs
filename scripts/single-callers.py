#!/usr/bin/env python3
"""Every non-test function with exactly one caller, for the D5 checkpoint (docs/function-length.md).

usage:
    rust-analyzer lsif . > /tmp/lsif.json
    python3 -I scripts/single-callers.py /tmp/lsif.json . > docs/function-length/single-callers.tsv

Callers come from rust-analyzer's reference index, so a method call, a path call and a function
passed by name all count. Not counted: references from test code (tests/ and benches/ trees,
tests.rs files, #[cfg(test)] modules, #[test] functions), `use` re-exports, and the function's own
definition. Trait methods and methods in trait impls are left out: their callers go through the
trait, so a reference count says nothing about them. Lengths are body lines as clippy's
too_many_lines counts them (inside the braces, no blank or comment lines); spans assume rustfmt
layout. `inlined_len` is caller plus helper, the length the caller would have with the helper
pasted in. A tally of all functions by caller count (0, 1, 2 or more) goes to stderr.
"""
import collections
import json
import re
import sys

lsif, root = sys.argv[1], sys.argv[2].rstrip("/") + "/"
ranges, docs, items = {}, {}, []
range_doc = {}
for line in open(lsif):
    o = json.loads(line)
    lab = o["label"]
    if lab == "range":
        ranges[o["id"]] = (o["start"]["line"], o["start"]["character"])
    elif lab == "document":
        docs[o["id"]] = o["uri"].replace("file://", "")
    elif lab == "contains" and o["outV"] in docs:
        for v in o["inVs"]:
            range_doc[v] = o["outV"]
    elif lab == "item":
        items.append(o)

src = {}


def lines(path):
    if path not in src:
        src[path] = open(path).read().split("\n")
    return src[path]


def code_lines(ls, a, b):
    """Body lines as clippy counts them: inside the braces, no blanks or comments."""
    while a < b and not ls[a].rstrip().endswith("{"):
        a += 1
    a, b = a + 1, b - 1
    n, in_block = 0, False
    for s in ls[a : b + 1]:
        s = s.strip()
        if in_block:
            if "*/" in s:
                in_block = False
            continue
        if s.startswith("/*"):
            in_block = "*/" not in s
            continue
        if s and not s.startswith("//"):
            n += 1
    return n


def body_end(ls, start):
    """rustfmt puts the closing brace at the item's indent; None if no body."""
    indent = len(ls[start]) - len(ls[start].lstrip())
    for i in range(start, len(ls)):
        s = ls[i]
        if i > start and s.strip() and len(s) - len(s.lstrip()) <= indent:
            if s.strip().startswith("}"):
                return i
            if s.strip().startswith((")", "where", "{", ">")):
                if s.rstrip().endswith(";"):
                    return None
                continue
            return None
        if i == start or len(s) - len(s.lstrip()) == indent:
            st = s.rstrip()
            if st.endswith(";") and "{" not in st:
                return None
            if st.endswith("}") and st.lstrip().startswith(("fn", "pub", "const", "async", "unsafe")) and "{" in st:
                return i
    return None


# Blocks: test modules and trait impls, by indent matching.
def spans(path):
    ls = lines(path)
    out = {"test": [], "traitimpl": [], "trait": [], "fns": []}
    for i, s in enumerate(ls):
        st = s.strip()
        if st.startswith("#[cfg(test)]"):
            j = i + 1
            while j < len(ls) and ls[j].strip().startswith("#"):
                j += 1
            if j < len(ls) and re.match(r"\s*(pub(\(\w+\))? )?mod \w+\s*\{", ls[j]):
                e = body_end(ls, j)
                out["test"].append((i, e if e is not None else len(ls)))
        if re.match(r"\s*(unsafe )?impl\b.*\bfor\b", s) and not st.endswith(";"):
            e = body_end(ls, i)
            if e:
                out["traitimpl"].append((i, e))
        if re.match(r"\s*(pub(\([^)]*\))? )?(unsafe )?trait \w+", s):
            e = body_end(ls, i)
            if e:
                out["trait"].append((i, e))
        m = re.match(r"\s*(pub(\([^)]*\))? )?(const )?(async )?(unsafe )?(extern \"C\" )?fn (\w+)", s)
        if m:
            e = body_end(ls, i)
            if e is not None:
                out["fns"].append((i, e, m.group(7)))
    return out


span_cache = {}


def sp(path):
    if path not in span_cache:
        span_cache[path] = spans(path)
    return span_cache[path]


def is_test_path(path):
    rel = path[len(root) :]
    parts = rel.split("/")
    return (
        "tests" in parts
        or "benches" in parts
        or rel.startswith("fuzz/")
        or parts[-1] in ("tests.rs",)
        or ".upstream" in parts
        or "target" in parts
    )


def inside(lst, line):
    return any(a <= line <= b for a, b in lst)


def enclosing_fn(path, line):
    best = None
    for a, b, name in sp(path)["fns"]:
        if a <= line <= b and (best is None or a > best[0]):
            best = (a, b, name)
    return best


def in_test(path, line):
    if is_test_path(path):
        return True
    if inside(sp(path)["test"], line):
        return True
    ls = lines(path)
    f = enclosing_fn(path, line)
    if f and f[0] > 0:
        k = f[0] - 1
        while k >= 0 and ls[k].strip().startswith(("#", "///")):
            if ls[k].strip().startswith(("#[test]", "#[tokio::test", "#[cfg(test)]")):
                return True
            k -= 1
    return False


def is_use(path, line):
    ls = lines(path)
    k = line
    while k >= 0:
        st = ls[k].strip()
        if re.match(r"(pub(\([^)]*\))? )?use ", st):
            return True
        if k != line and (st.endswith(";") or st.endswith("}") or st.endswith("{") and not st.startswith("pub use") ):
            return False
        k -= 1
    return False


# referenceResult -> definitions / references
defs = collections.defaultdict(list)
uses = collections.defaultdict(list)
for o in items:
    p = o.get("property")
    tgt = defs if p == "definitions" else uses if p == "references" else None
    if tgt is not None:
        tgt[o["outV"]].extend(o["inVs"])

rows = []
tally = collections.Counter()
for rr in set(defs) | set(uses):
    for d in defs.get(rr, []):
        if d not in range_doc:
            continue
        path = docs[range_doc[d]]
        if not path.startswith(root) or is_test_path(path):
            continue
        line, ch = ranges[d]
        ls = lines(path)
        if not re.search(r"\bfn\s+$", ls[line][:ch]):
            continue
        if in_test(path, line):
            continue
        s = sp(path)
        if inside(s["traitimpl"], line) or inside(s["trait"], line):
            continue
        f = [x for x in s["fns"] if x[0] == line]
        if not f:
            continue
        a, b, name = f[0]
        if name == "main":
            continue
        callers = []
        for u in uses.get(rr, []):
            if u == d or u not in range_doc:
                continue
            up = docs[range_doc[u]]
            ul, _ = ranges[u]
            if in_test(up, ul) or is_use(up, ul):
                continue
            callers.append((up, ul))
        tally[min(len(callers),2)] += 1
        if len(callers) != 1:
            continue
        cp, cl = callers[0]
        cf = enclosing_fn(cp, cl)
        clen = code_lines(lines(cp), cf[0], cf[1]) if cf else 0
        flen = code_lines(ls, a, b)
        rows.append(
            (
                path[len(root) :],
                a + 1,
                name,
                flen,
                cp[len(root) :],
                cl + 1,
                cf[2] if cf else "-",
                clen,
                flen + clen,
            )
        )

rows.sort()
print("file\tline\tfn\tlen\tcaller_file\tcaller_line\tcaller\tcaller_len\tinlined_len")
for r in rows:
    print("\t".join(map(str, r)))

print("functions by caller count (2 is two or more):", dict(sorted(tally.items())), file=sys.stderr)
