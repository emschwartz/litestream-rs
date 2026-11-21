#!/bin/sh
# Lists every upstream Go test the test map (docs/upstream-test-map.toml) must map: each top-level
# `func TestX(t *testing.T)` and each `t.Run("literal", ...)` inside one, nested as
# TestX/outer/inner, one per line as `<repo>/<file><TAB><name>`, sorted. Reads the
# pinned clones in .upstream/ (`just upstream`); its output is committed as
# docs/upstream-test-map/upstream-tests.txt (`just upstream-test-map`) so the test map test needs no clone.
#
# Subtests named by an expression (a table's tt.name, fmt.Sprintf) are not listed: their
# names exist only at run time, and the enclosing top-level test's entry covers them.
set -eu
cd "$(dirname "$0")/.."
# A worktree has no .upstream of its own; the main checkout's is shared.
if [ ! -d .upstream ]; then
    common=$(git rev-parse --path-format=absolute --git-common-dir)
    cd "$(dirname "$common")"
fi
for repo in litestream ltx; do
    if [ ! -d ".upstream/$repo/.git" ]; then
        echo "upstream-tests: .upstream/$repo is missing; run \`just upstream\`" >&2
        exit 1
    fi
done
want_litestream=041a8c382c63
case "$(git -C .upstream/litestream rev-parse HEAD)" in
    "$want_litestream"*) ;;
    *)
        echo "upstream-tests: .upstream/litestream is not at $want_litestream; run \`just upstream\`" >&2
        exit 1
        ;;
esac
if [ "$(git -C .upstream/ltx rev-parse HEAD)" != "$(git -C .upstream/ltx rev-parse 'v0.5.3^{commit}')" ]; then
    echo "upstream-tests: .upstream/ltx is not at v0.5.3; run \`just upstream\`" >&2
    exit 1
fi

# Only files tracked at the pinned commit, so a stray local file cannot join the list.
for repo in litestream ltx; do
    git -C ".upstream/$repo" ls-files -- '*_test.go' | while read -r f; do
        printf '%s\n' "$repo/$f"
    done
done | LC_ALL=C sort | while read -r path; do
    LC_ALL=C awk -v file="$path" '
        # Drops what cannot hold a real brace: comments, strings, runes and raw strings,
        # which may span lines (`inraw` carries that state across them).
        function code(s,    out, c, q, i, n) {
            out = ""; n = length(s)
            for (i = 1; i <= n; i++) {
                c = substr(s, i, 1)
                if (inraw) { if (c == "`") inraw = 0; continue }
                if (c == "`") { inraw = 1; continue }
                if (c == "/" && substr(s, i + 1, 1) == "/") break
                if (c == "\"" || c == "\047") {
                    q = c
                    for (i++; i <= n; i++) {
                        c = substr(s, i, 1)
                        if (c == "\\") { i++; continue }
                        if (c == q) break
                    }
                    continue
                }
                out = out c
            }
            return out
        }
        {
            line = $0
            was_raw = inraw
            stripped = code(line)
            opens = gsub(/\{/, "{", stripped)
            closes = gsub(/\}/, "}", stripped)
            if (!was_raw && match(line, /^func Test[A-Za-z0-9_]*\(/) && line ~ /\*testing\.T\)/) {
                top = substr(line, 6, RLENGTH - 6); sp = 0; topdepth = depth
                print file "\t" top
            } else if (!was_raw && top != "" && match(line, /(^|[^A-Za-z0-9_.])t\.Run\("([^"\\]|\\.)*"/)) {
                s = substr(line, RSTART, RLENGTH)
                sub(/^.*t\.Run\("/, "", s); sub(/"$/, "", s)
                prefix = top
                for (k = 1; k <= sp; k++) prefix = prefix "/" stack[k]
                print file "\t" prefix "/" s
                if (opens > closes) { sp++; stack[sp] = s; sdepth[sp] = depth }
            }
            depth += opens - closes
            while (sp > 0 && depth <= sdepth[sp]) sp--
            if (top != "" && depth <= topdepth) { top = ""; sp = 0 }
        }
    ' ".upstream/$path"
done | LC_ALL=C sort -u
